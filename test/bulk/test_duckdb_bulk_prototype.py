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


def test_array_aggregation_collects_distinct_values(tmp_path):
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
        prototype.export_parquet(str(tmp_path))

        aliases = prototype.connection.execute(
            "SELECT alias FROM read_parquet(?) WHERE uri = ?",
            [str(tmp_path / "nodes" / "Person.parquet"), str(uri("alice"))],
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


# ---------------------------------------------------------------------------
# Tests for remap_labels
# ---------------------------------------------------------------------------

def _write_label_map(path: Path, exclude=None, map_=None):
    data = {}
    if exclude is not None:
        data["exclude"] = exclude
    if map_ is not None:
        data["map"] = map_
    path.write_text(json.dumps(data))
    return str(path)


def test_remap_labels_maps_canonical_label(tmp_path):
    """A node with people.person should get primary_label='Person' after remapping."""
    prototype = DuckDBBulkPrototype(config=BulkImportConfig())
    try:
        prototype.ingest_triples([
            (uri("alice"), RDF.type, uri("people.person")),
            (uri("alice"), RDF.type, uri("celebrities.celebrity")),
        ])
        prototype.build_facts()

        # Before remapping: alphabetical first non-generic label
        row_before = prototype.connection.execute(
            "SELECT primary_label FROM node_rows WHERE uri = ?",
            [str(uri("alice"))],
        ).fetchone()
        # celebrities.celebrity < people.person alphabetically
        assert row_before[0] == "celebrities.celebrity"

        label_map_file = _write_label_map(
            tmp_path / "label_map.json",
            map_={"people.person": "Person", "celebrities.celebrity": "Person"},
        )
        prototype.remap_labels(label_map_file)

        row_after = prototype.connection.execute(
            "SELECT primary_label, labels FROM node_rows WHERE uri = ?",
            [str(uri("alice"))],
        ).fetchone()
        assert row_after[0] == "Person"
        # Verify the labels array is unchanged — remap only updates primary_label
        assert "people.person" in row_after[1]
        assert "celebrities.celebrity" in row_after[1]
    finally:
        prototype.close()


def test_remap_labels_excludes_generic_labels(tmp_path):
    """Labels in the exclude list should be skipped; node falls back to first eligible label."""
    prototype = DuckDBBulkPrototype(config=BulkImportConfig())
    try:
        prototype.ingest_triples([
            (uri("bob"), RDF.type, uri("base.type_ontology.agent")),
            (uri("bob"), RDF.type, uri("organization.organization")),
        ])
        prototype.build_facts()

        label_map_file = _write_label_map(
            tmp_path / "label_map.json",
            exclude=["base.type_ontology.agent"],
            map_={"organization.organization": "Organization"},
        )
        prototype.remap_labels(label_map_file)

        row = prototype.connection.execute(
            "SELECT primary_label FROM node_rows WHERE uri = ?",
            [str(uri("bob"))],
        ).fetchone()
        assert row[0] == "Organization"
    finally:
        prototype.close()


def test_remap_labels_glob_patterns(tmp_path):
    """Glob patterns in exclude and map should match correctly."""
    prototype = DuckDBBulkPrototype(config=BulkImportConfig())
    try:
        # charlie: has a base.type_ontology.* label (should be excluded) + people.person
        # dave: has sports.player (matches *.player glob) — only type, freq=1
        prototype.ingest_triples([
            (uri("charlie"), RDF.type, uri("base.type_ontology.animate")),
            (uri("charlie"), RDF.type, uri("people.person")),
            (uri("dave"), RDF.type, uri("sports.player")),
        ])
        prototype.build_facts()

        label_map_file = _write_label_map(
            tmp_path / "label_map.json",
            exclude=["base.type_ontology.*"],
            map_={"people.person": "Person", "*.player": "Person"},
        )
        prototype.remap_labels(label_map_file)

        charlie = prototype.connection.execute(
            "SELECT primary_label FROM node_rows WHERE uri = ?",
            [str(uri("charlie"))],
        ).fetchone()
        assert charlie[0] == "Person"

        dave = prototype.connection.execute(
            "SELECT primary_label FROM node_rows WHERE uri = ?",
            [str(uri("dave"))],
        ).fetchone()
        assert dave[0] == "Person"
    finally:
        prototype.close()


def test_remap_labels_all_excluded_falls_back_to_resource(tmp_path):
    """A node with only excluded labels should fall back to 'Resource'."""
    prototype = DuckDBBulkPrototype(config=BulkImportConfig())
    try:
        prototype.ingest_triples([
            (uri("meta"), RDF.type, uri("common.notable_for")),
            (uri("meta"), RDF.type, uri("type.content")),
        ])
        prototype.build_facts()

        label_map_file = _write_label_map(
            tmp_path / "label_map.json",
            exclude=["common.notable_for", "type.content"],
        )
        prototype.remap_labels(label_map_file)

        row = prototype.connection.execute(
            "SELECT primary_label FROM node_rows WHERE uri = ?",
            [str(uri("meta"))],
        ).fetchone()
        assert row[0] == "Resource"
    finally:
        prototype.close()


def test_remap_labels_via_cli(tmp_path):
    """CLI --stage remap-labels should update the DB correctly."""
    # Build a minimal staging DB first
    db_path = str(tmp_path / "staging.duckdb")
    fixture = tmp_path / "fixture.ttl"
    fixture.write_text(
        "@prefix ex: <http://example.com/> .\n"
        "@prefix fb: <http://fb.com/> .\n"
        "ex:alice a fb:people.person, fb:celebrities.celebrity .\n",
        encoding="utf-8",
    )
    output = tmp_path / "out"

    # Ingest + build (not export)
    result = bulk_cli([
        str(fixture), "--format", "turtle",
        "--output", str(output),
        "--db", db_path,
        "--stage", "all",
        "--no-progress",
    ])
    assert result == 0

    label_map_file = _write_label_map(
        tmp_path / "label_map.json",
        map_={"people.person": "Person", "celebrities.celebrity": "Person"},
    )

    # Now remap labels on the existing DB via analyze stage
    result2 = bulk_cli([
        "--db", db_path,
        "--stage", "analyze",
        "--label-map-file", label_map_file,
        "--no-progress",
    ])
    assert result2 == 0

    # Check that the DB was updated
    import duckdb as _duckdb
    conn = _duckdb.connect(db_path)
    try:
        row = conn.execute(
            "SELECT primary_label FROM node_rows WHERE uri = 'http://example.com/alice'"
        ).fetchone()
        assert row is not None
        assert row[0] == "Person"
    finally:
        conn.close()


def test_freebase_label_map_json_is_valid():
    """configs/freebase_label_map.json should be valid JSON with the expected structure."""
    config_path = Path(__file__).parent.parent.parent / "configs" / "freebase_label_map.json"
    assert config_path.exists(), f"Missing config file: {config_path}"
    data = json.loads(config_path.read_text())
    assert "exclude" in data, "Config must have 'exclude' key"
    assert "map" in data, "Config must have 'map' key"
    assert isinstance(data["exclude"], list), "'exclude' must be a list"
    assert isinstance(data["map"], dict), "'map' must be a dict"
    assert len(data["exclude"]) >= 10, "Expected at least 10 exclude patterns"
    assert len(data["map"]) >= 20, "Expected at least 20 label mappings"
    # All canonical values (non-null) should be non-empty strings
    for original, canonical in data["map"].items():
        assert canonical is None or (isinstance(canonical, str) and canonical), (
            f"Canonical for '{original}' must be non-empty string or null, got: {canonical!r}"
        )


def _make_prototype_with_rels(triples, rel_triples):
    """Helper: ingest node triples + relationship triples, run build_facts, remap_labels."""
    prototype = DuckDBBulkPrototype(config=BulkImportConfig())
    prototype.ingest_triples(triples + rel_triples)
    prototype.build_facts()
    prototype.remap_labels()
    return prototype


def test_profile_relationships_excludes_by_pattern():
    """Rel types matching builtin glob patterns must be pattern_excluded=true."""
    prototype = _make_prototype_with_rels(
        triples=[
            (uri("a"), RDF.type, uri("Person")),
            (uri("b"), RDF.type, uri("Person")),
        ],
        rel_triples=[
            (uri("a"), uri("sameAs"), uri("b")),   # builtin exclude: sameAs
            (uri("a"), uri("knows"), uri("b")),     # domain rel — kept
        ],
    )
    try:
        prototype.profile_relationships(min_rel_coverage=0.0)
        rows = {
            r[0]: (r[1], r[2])
            for r in prototype.connection.execute(
                "SELECT rel_type, pattern_excluded, is_excluded FROM relationship_profile"
            ).fetchall()
        }
        assert rows["sameAs"][0] is True   # pattern_excluded
        assert rows["sameAs"][1] is True   # is_excluded
        assert rows["knows"][0] is False   # not pattern_excluded
        assert rows["knows"][1] is False   # kept
    finally:
        prototype.close()


def test_profile_relationships_coverage_threshold():
    """Rel types below the coverage threshold should be excluded but not pattern_excluded."""
    prototype = _make_prototype_with_rels(
        triples=[
            (uri("a"), RDF.type, uri("Person")),
            (uri("b"), RDF.type, uri("Person")),
            (uri("c"), RDF.type, uri("Person")),
        ],
        rel_triples=[
            (uri("a"), uri("knows"), uri("b")),   # 1 triple
            (uri("a"), uri("knows"), uri("c")),   # 2 total
            (uri("b"), uri("knows"), uri("c")),   # 3 total
            (uri("a"), uri("rare"), uri("b")),    # 1 triple → below 50% threshold
        ],
    )
    try:
        # threshold = 50% → rare (1/4 = 25%) excluded; knows (3/4 = 75%) kept
        prototype.profile_relationships(min_rel_coverage=0.5)
        rows = {
            r[0]: (r[1], r[2])
            for r in prototype.connection.execute(
                "SELECT rel_type, pattern_excluded, is_excluded FROM relationship_profile"
            ).fetchall()
        }
        assert rows["knows"][1] is False   # kept
        assert rows["rare"][0] is False    # not pattern_excluded
        assert rows["rare"][1] is True     # excluded by coverage
    finally:
        prototype.close()


def test_profile_relationships_inverse_pair_dedup():
    """Pairs of rel types with near-identical counts should have the lex-larger one excluded."""
    prototype = _make_prototype_with_rels(
        triples=[
            (uri("a"), RDF.type, uri("Person")),
            (uri("b"), RDF.type, uri("Person")),
        ],
        rel_triples=[
            (uri("a"), uri("alpha_rel"), uri("b")),
            (uri("b"), uri("zeta_rel"), uri("a")),   # same count as alpha_rel
        ],
    )
    try:
        prototype.profile_relationships(
            min_rel_coverage=0.0,
            dedup_inverse_pairs=True,
            inverse_pair_tolerance=0.01,
        )
        rows = {
            r[0]: r[1]
            for r in prototype.connection.execute(
                "SELECT rel_type, is_excluded FROM relationship_profile"
            ).fetchall()
        }
        # lex-smaller is kept, lex-larger is excluded
        assert rows["alpha_rel"] is False
        assert rows["zeta_rel"] is True
    finally:
        prototype.close()


def test_profile_relationships_orphan_rescue():
    """A node label with zero kept rels should get a rel rescued to avoid disconnection."""
    prototype = _make_prototype_with_rels(
        triples=[
            (uri("alice"), RDF.type, uri("Person")),
            (uri("doc"),   RDF.type, uri("Document")),
        ],
        rel_triples=[
            # Person–Person rel (kept)
            (uri("alice"), uri("knows"), uri("alice")),
            # Document is only connected via a rare rel below threshold
            (uri("doc"), uri("links"), uri("alice")),
        ],
    )
    try:
        # Set threshold high enough that 'links' (1 triple / 2 total = 50%) is excluded
        prototype.profile_relationships(
            min_rel_coverage=0.9,   # only rels with ≥90% of triples survive
            dedup_inverse_pairs=False,
            ensure_rel_per_label=True,
        )
        rows = {
            r[0]: r[1]
            for r in prototype.connection.execute(
                "SELECT rel_type, is_excluded FROM relationship_profile"
            ).fetchall()
        }
        # 'links' should be rescued (Document would be orphaned otherwise)
        assert rows["links"] is False
    finally:
        prototype.close()


def test_profile_relationships_no_rescue_for_pattern_excluded():
    """Pattern-excluded rels must NOT be rescued even if a label is orphaned."""
    prototype = _make_prototype_with_rels(
        triples=[
            (uri("a"), RDF.type, uri("Person")),
            (uri("b"), RDF.type, uri("Ontology")),
        ],
        rel_triples=[
            (uri("a"), uri("knows"), uri("a")),
            # Ontology only connected via sameAs, which is pattern-excluded
            (uri("b"), uri("sameAs"), uri("a")),
        ],
    )
    try:
        prototype.profile_relationships(
            min_rel_coverage=0.0,
            dedup_inverse_pairs=False,
            ensure_rel_per_label=True,
        )
        rows = {
            r[0]: r[1]
            for r in prototype.connection.execute(
                "SELECT rel_type, is_excluded FROM relationship_profile"
            ).fetchall()
        }
        # sameAs is pattern-excluded and must stay excluded
        assert rows["sameAs"] is True
    finally:
        prototype.close()


def test_analyze_requires_db_when_no_output(tmp_path):
    """CLI validation: --stage analyze requires --db when --output is not provided."""
    # Missing both --db and --output: should error
    with pytest.raises(SystemExit) as exc_info:
        bulk_cli(["--stage", "analyze"])
    assert exc_info.value.code != 0
