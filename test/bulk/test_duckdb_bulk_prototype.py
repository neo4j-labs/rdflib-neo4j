import json
from pathlib import Path

from rdflib import BNode, Literal, RDF, URIRef, XSD

from rdflib_neo4j.bulk import AggregationMode, BulkImportConfig, DuckDBBulkPrototype
from rdflib_neo4j.bulk.cli import main as bulk_cli


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
