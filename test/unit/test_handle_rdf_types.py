"""
Unit tests for HandleRDFTypesStrategy — no Neo4j connection required.
Exercises Neo4jTriple.parse_triple for LABELS, NODES, and LABELS_AND_NODES modes.
"""
from rdflib import URIRef, RDF
from rdflib_neo4j.Neo4jTriple import Neo4jTriple
from rdflib_neo4j.config.const import (
    HANDLE_VOCAB_URI_STRATEGY,
    HANDLE_MULTIVAL_STRATEGY,
    HandleRDFTypesStrategy,
)
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig

EX = "http://example.org/"
SUBJECT = URIRef(f"{EX}thing")
PERSON_CLASS = URIRef(f"{EX}Person")


def make_triple(strategy=HandleRDFTypesStrategy.LABELS):
    return Neo4jTriple(
        uri=SUBJECT,
        prefixes={},
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
        handle_multival_strategy=HANDLE_MULTIVAL_STRATEGY.OVERWRITE,
        multival_props_names=[],
        handle_rdf_types_strategy=strategy,
    )


def rdf_type_triple():
    """Returns an (subject, rdf:type, ex:Person) triple."""
    return (SUBJECT, RDF.type, PERSON_CLASS)


# ---------------------------------------------------------------------------
# LABELS strategy (default)
# ---------------------------------------------------------------------------

class TestLabelsStrategy:
    def test_label_added(self):
        triple = make_triple(HandleRDFTypesStrategy.LABELS)
        triple.parse_triple(rdf_type_triple(), {})
        assert "Person" in triple.labels

    def test_no_rel_added(self):
        triple = make_triple(HandleRDFTypesStrategy.LABELS)
        triple.parse_triple(rdf_type_triple(), {})
        assert triple.extract_rels() == {}

    def test_class_uris_empty(self):
        triple = make_triple(HandleRDFTypesStrategy.LABELS)
        triple.parse_triple(rdf_type_triple(), {})
        assert triple.extract_rdf_type_class_uris() == set()


# ---------------------------------------------------------------------------
# NODES strategy
# ---------------------------------------------------------------------------

class TestNodesStrategy:
    def test_no_label_added(self):
        triple = make_triple(HandleRDFTypesStrategy.NODES)
        triple.parse_triple(rdf_type_triple(), {})
        assert triple.labels == set()

    def test_rel_added(self):
        triple = make_triple(HandleRDFTypesStrategy.NODES)
        triple.parse_triple(rdf_type_triple(), {})
        rels = triple.extract_rels()
        assert "rdf__type" in rels
        assert PERSON_CLASS in rels["rdf__type"]

    def test_class_uri_tracked(self):
        triple = make_triple(HandleRDFTypesStrategy.NODES)
        triple.parse_triple(rdf_type_triple(), {})
        assert PERSON_CLASS in triple.extract_rdf_type_class_uris()


# ---------------------------------------------------------------------------
# LABELS_AND_NODES strategy
# ---------------------------------------------------------------------------

class TestLabelsAndNodesStrategy:
    def test_label_added(self):
        triple = make_triple(HandleRDFTypesStrategy.LABELS_AND_NODES)
        triple.parse_triple(rdf_type_triple(), {})
        assert "Person" in triple.labels

    def test_rel_added(self):
        triple = make_triple(HandleRDFTypesStrategy.LABELS_AND_NODES)
        triple.parse_triple(rdf_type_triple(), {})
        rels = triple.extract_rels()
        assert "rdf__type" in rels
        assert PERSON_CLASS in rels["rdf__type"]

    def test_class_uri_tracked(self):
        triple = make_triple(HandleRDFTypesStrategy.LABELS_AND_NODES)
        triple.parse_triple(rdf_type_triple(), {})
        assert PERSON_CLASS in triple.extract_rdf_type_class_uris()


# ---------------------------------------------------------------------------
# Enum canonical string values (match n10s GraphConfig)
# ---------------------------------------------------------------------------

class TestEnumValues:
    def test_labels_value(self):
        assert HandleRDFTypesStrategy.LABELS.value == "LABELS"

    def test_nodes_value(self):
        assert HandleRDFTypesStrategy.NODES.value == "NODES"

    def test_labels_and_nodes_value(self):
        assert HandleRDFTypesStrategy.LABELS_AND_NODES.value == "LABELS_AND_NODES"


# ---------------------------------------------------------------------------
# Default strategy in Neo4jStoreConfig
# ---------------------------------------------------------------------------

class TestNeo4jStoreConfigDefault:
    def test_default_strategy_is_labels(self):
        config = Neo4jStoreConfig()
        assert config.handle_rdf_types_strategy == HandleRDFTypesStrategy.LABELS

    def test_custom_strategy_stored(self):
        config = Neo4jStoreConfig(handle_rdf_types_strategy=HandleRDFTypesStrategy.NODES)
        assert config.handle_rdf_types_strategy == HandleRDFTypesStrategy.NODES
