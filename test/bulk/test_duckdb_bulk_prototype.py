import json
from pathlib import Path

import duckdb
import pytest
from rdflib import BNode, Literal, RDF, URIRef, XSD

from rdflib_neo4j.bulk import AggregationMode, BulkImportConfig, DuckDBBulkPrototype
from rdflib_neo4j.bulk.cli import main as bulk_cli
from rdflib_neo4j.bulk.ingest import ingest


EX = "http://example.com/"


def uri(name):
    return URIRef(f"{EX}{name}")


def test_stages_raw_triples_and_builds_projection_facts():
    prototype = DuckDBBulkPrototype(config=BulkImportConfig(language_projection=True))
    blank = BNode("friend")

    try:
        count = prototype.ingest_triples(
            [
                (uri("alice"), RDF.type, uri("Person")),
                (uri("alice"), uri("name"), Literal("Alice", lang="en")),
                (uri("alice"), uri("age"), Literal(42, datatype=XSD.integer)),
                (uri("alice"), uri("knows"), uri("bob")),
                (uri("bob"), RDF.type, uri("Person")),
                (uri("bob"), uri("knows"), blank),
            ]
        )
        prototype.build_facts()

        assert count == 6
        assert prototype.connection.execute("SELECT count(*) FROM rdf_triples").fetchone()[0] == 6

        alice = prototype.connection.execute(
            "SELECT primary_label, labels FROM node_rows WHERE uri = ?",
            [str(uri("alice"))],
        ).fetchone()
        assert alice[0] == "Person"
        assert set(alice[1]) == {"Resource", "Person"}

        projected_name = prototype.connection.execute(
            """
            SELECT projected_property_name, value, lang
            FROM property_facts
            WHERE uri = ? AND property_name = 'name'
            """,
            [str(uri("alice"))],
        ).fetchone()
        assert projected_name == ("name_en", "Alice", "en")

        assert prototype.connection.execute(
            "SELECT count(*) FROM node_rows WHERE uri = 'bnode://friend'"
        ).fetchone()[0] == 1
    finally:
        prototype.close()


def test_pivots_last_value_and_exports_deduplicated_relationship_parquet(tmp_path):
    config = BulkImportConfig(aggregation_mode=AggregationMode.LAST)
    prototype = DuckDBBulkPrototype(config=config, batch_size=2)

    try:
        prototype.ingest_triples(
            [
                (uri("alice"), RDF.type, uri("Person")),
                (uri("alice"), uri("name"), Literal("Alice")),
                (uri("alice"), uri("name"), Literal("Alicia")),
                (uri("alice"), uri("knows"), uri("bob")),
                (uri("alice"), uri("knows"), uri("bob")),
                (uri("bob"), RDF.type, uri("Person")),
            ]
        )
        prototype.build_facts()
        prototype.export_parquet(str(tmp_path))

        prototype.connection.execute(
            "CREATE TABLE exported_person AS SELECT * FROM read_parquet(?)",
            [str(Path(tmp_path, "nodes", "Person.parquet"))],
        )
        assert prototype.connection.execute(
            "SELECT name FROM exported_person WHERE uri = ?",
            [str(uri("alice"))],
        ).fetchone()[0] == "Alicia"

        prototype.connection.execute(
            "CREATE TABLE exported_knows AS SELECT * FROM read_parquet(?)",
            [str(Path(tmp_path, "relationships", "knows.parquet"))],
        )
        assert prototype.connection.execute("SELECT count(*) FROM exported_knows").fetchone()[0] == 1

        counts = json.loads(Path(tmp_path, "metadata", "counts.json").read_text())
        assert counts["rdf_triples"] == 6
        assert counts["relationship_rows"] == 1

        prototype.connection.execute(
            "CREATE TABLE label_explain AS SELECT * FROM read_parquet(?)",
            [str(Path(tmp_path, "metadata", "primary_label_explain.parquet"))],
        )
        assert prototype.connection.execute(
            "SELECT rule FROM label_explain WHERE uri = ?",
            [str(uri("alice"))],
        ).fetchone()[0] == "first_non_generic_lexical_label"
    finally:
        prototype.close()


def test_array_aggregation_collects_distinct_values():
    config = BulkImportConfig(aggregation_mode=AggregationMode.ARRAY)
    prototype = DuckDBBulkPrototype(config=config)

    try:
        prototype.ingest_triples(
            [
                (uri("alice"), RDF.type, uri("Person")),
                (uri("alice"), uri("alias"), Literal("Al")),
                (uri("alice"), uri("alias"), Literal("Allie")),
                (uri("alice"), uri("alias"), Literal("Al")),
            ]
        )
        prototype.build_facts()
        prototype.pivot_nodes()

        aliases = prototype.connection.execute(
            "SELECT alias FROM nodes_wide WHERE uri = ?",
            [str(uri("alice"))],
        ).fetchone()[0]
        assert aliases == ["Al", "Allie"]
    finally:
        prototype.close()


def test_cli_exports_fixture_file(tmp_path):
    fixture = tmp_path / "fixture.ttl"
    output = tmp_path / "out"
    fixture.write_text(
        """
        @prefix ex: <http://example.com/> .

        ex:alice a ex:Person ;
            ex:name "Alice" ;
            ex:knows ex:bob .
        """,
        encoding="utf-8",
    )

    assert bulk_cli([str(fixture), "--format", "turtle", "--output", str(output)]) == 0

    assert Path(output, "nodes", "Person.parquet").exists()
    assert Path(output, "relationships", "knows.parquet").exists()
    graph_config = json.loads(Path(output, "metadata", "graph_config.json").read_text())
    assert graph_config["handleVocabUris"] == "IGNORE"


# ---------------------------------------------------------------------------
# Tests for the oxigraph streaming backend
# ---------------------------------------------------------------------------

_TURTLE_FIXTURE = """\
@prefix ex: <http://example.com/> .

ex:alice a ex:Person ;
    ex:name "Alice"@en ;
    ex:age 42 ;
    ex:knows ex:bob .

ex:bob a ex:Person ;
    ex:name "Bob" .
"""

_RDFXML_FIXTURE = """\
<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.com/">
  <ex:Person rdf:about="http://example.com/alice">
    <ex:name xml:lang="en">Alice</ex:name>
    <ex:age rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">42</ex:age>
    <ex:knows rdf:resource="http://example.com/bob"/>
  </ex:Person>
</rdf:RDF>
"""


def _make_staging(tmp_path, suffix=".duckdb"):
    conn = duckdb.connect(str(tmp_path / f"staging{suffix}"))
    conn.execute("""
        CREATE TABLE rdf_triples (
            source_order UBIGINT,
            subject VARCHAR NOT NULL,
            predicate VARCHAR NOT NULL,
            object_kind VARCHAR NOT NULL,
            object_value VARCHAR NOT NULL,
            datatype VARCHAR,
            lang VARCHAR
        )
    """)
    return conn


@pytest.mark.parametrize("backend", ["rdflib", "oxigraph"])
def test_oxigraph_turtle_streaming(tmp_path, backend):
    fixture = tmp_path / "fixture.ttl"
    fixture.write_text(_TURTLE_FIXTURE, encoding="utf-8")
    conn = _make_staging(tmp_path)
    try:
        count = ingest(str(fixture), "turtle", conn, backend=backend, batch_size=10, progress=False)
        # 2 rdf:type + 2 ex:name (alice@en, bob plain) + 1 ex:age + 1 ex:knows = 6
        assert count == 6
        rows = conn.execute(
            "SELECT object_kind, object_value, lang FROM rdf_triples "
            "WHERE predicate = 'http://example.com/name'"
        ).fetchall()
        assert len(rows) == 2
        alice_name = next(r for r in rows if r[2] == "en")
        assert alice_name[0] == "literal"
        assert alice_name[1] == "Alice"
    finally:
        conn.close()


@pytest.mark.parametrize("backend", ["rdflib", "oxigraph"])
def test_oxigraph_rdfxml_streaming(tmp_path, backend):
    fixture = tmp_path / "fixture.rdf"
    fixture.write_text(_RDFXML_FIXTURE, encoding="utf-8")
    conn = _make_staging(tmp_path)
    try:
        count = ingest(str(fixture), "xml", conn, backend=backend, batch_size=10, progress=False)
        # 1 rdf:type + 1 name@en + 1 age + 1 knows = 4 triples
        assert count == 4
        # Verify iri/bnode/literal split
        kinds = {r[0] for r in conn.execute(
            "SELECT object_kind FROM rdf_triples WHERE predicate = 'http://example.com/knows'"
        ).fetchall()}
        assert kinds == {"iri"}
        age_row = conn.execute(
            "SELECT object_value, datatype FROM rdf_triples "
            "WHERE predicate = 'http://example.com/age'"
        ).fetchone()
        assert age_row[0] == "42"
        assert "integer" in age_row[1]
    finally:
        conn.close()


def test_cli_oxigraph_backend(tmp_path):
    fixture = tmp_path / "fixture.ttl"
    fixture.write_text(_TURTLE_FIXTURE, encoding="utf-8")
    output = tmp_path / "out"
    result = bulk_cli([
        str(fixture), "--format", "turtle", "--output", str(output),
        "--parser", "oxigraph", "--no-progress",
    ])
    assert result == 0
    assert Path(output, "nodes", "Person.parquet").exists()
    assert Path(output, "relationships", "knows.parquet").exists()
