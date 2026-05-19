"""Unit tests for BNode → bnode:// URI rewriting (issue #53)."""

from rdflib import Literal
from rdflib.term import BNode, URIRef

from rdflib_neo4j.Neo4jTriple import Neo4jTriple
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY, HANDLE_MULTIVAL_STRATEGY
from rdflib_neo4j.utils import bnode_to_uri


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EX = "http://www.example.org/indiv/"
SCHEMA = "http://schema.org/"


def make_triple(uri):
    """Construct a Neo4jTriple without a real Neo4j connection."""
    return Neo4jTriple(
        uri=uri,
        prefixes={"http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf"},
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
        handle_multival_strategy=HANDLE_MULTIVAL_STRATEGY.OVERWRITE,
        multival_props_names=[],
    )


# ---------------------------------------------------------------------------
# bnode_to_uri utility
# ---------------------------------------------------------------------------

class TestBnodeToUri:
    def test_produces_bnode_scheme(self):
        b = BNode()
        result = bnode_to_uri(b)
        assert result.startswith("bnode://")

    def test_includes_bnode_identifier(self):
        b = BNode("myid")
        assert bnode_to_uri(b) == "bnode://myid"

    def test_same_bnode_same_uri(self):
        b = BNode("stable")
        assert bnode_to_uri(b) == bnode_to_uri(b)

    def test_different_bnodes_different_uris(self):
        b1 = BNode()
        b2 = BNode()
        assert bnode_to_uri(b1) != bnode_to_uri(b2)


# ---------------------------------------------------------------------------
# BNode subject rewriting in Neo4jTriple
# ---------------------------------------------------------------------------

class TestBnodeSubject:
    def test_bnode_subject_rewritten_to_bnode_uri(self):
        b = BNode("subj1")
        triple_obj = make_triple(bnode_to_uri(b))
        assert triple_obj.uri == "bnode://subj1"

    def test_uriref_subject_not_rewritten(self):
        uri = URIRef(f"{EX}Person1")
        triple_obj = make_triple(uri)
        assert triple_obj.uri == uri

    def test_bnode_subject_gets_resource_label_when_no_type(self):
        b = BNode("subj2")
        triple_obj = make_triple(bnode_to_uri(b))
        # No rdf:type triple added — label key defaults to "Resource"
        assert triple_obj.extract_label_key() == "Resource"

    def test_two_bnodes_same_id_produce_same_uri(self):
        b1 = BNode("shared")
        b2 = BNode("shared")
        assert bnode_to_uri(b1) == bnode_to_uri(b2)


# ---------------------------------------------------------------------------
# BNode object rewriting in relationship triples
# ---------------------------------------------------------------------------

class TestBnodeObject:
    def _make_triple_with_rel(self, subject_uri, predicate, obj):
        """Build a Neo4jTriple and parse one relationship triple into it."""
        triple_obj = make_triple(subject_uri)
        triple_obj.parse_triple(
            triple=(URIRef(subject_uri), predicate, obj),
            mappings={},
        )
        return triple_obj

    def test_bnode_object_rewritten_in_relationship(self):
        b = BNode("obj1")
        predicate = URIRef(f"{SCHEMA}knows")
        triple_obj = self._make_triple_with_rel(
            f"{EX}Person1", predicate, b
        )
        rels = triple_obj.extract_rels()
        # IGNORE strategy → rel_type is local part "knows"
        assert "knows" in rels
        assert "bnode://obj1" in rels["knows"]

    def test_uriref_object_not_rewritten_in_relationship(self):
        obj = URIRef(f"{EX}Person2")
        predicate = URIRef(f"{SCHEMA}knows")
        triple_obj = self._make_triple_with_rel(
            f"{EX}Person1", predicate, obj
        )
        rels = triple_obj.extract_rels()
        assert obj in rels["knows"]

    def test_literal_object_stored_as_property_not_relationship(self):
        predicate = URIRef(f"{SCHEMA}name")
        triple_obj = self._make_triple_with_rel(
            f"{EX}Person1", predicate, Literal("Alice")
        )
        # Literals → props, not rels
        assert triple_obj.extract_rels() == {}
        assert "name" in triple_obj.extract_props_names()

    def test_bnode_object_uri_is_stable(self):
        """Two BNodes with the same identifier yield the same rewritten URI."""
        b1 = BNode("stable_obj")
        b2 = BNode("stable_obj")
        predicate = URIRef(f"{SCHEMA}knows")
        t1 = self._make_triple_with_rel(f"{EX}Person1", predicate, b1)
        t2 = self._make_triple_with_rel(f"{EX}Person2", predicate, b2)
        assert list(t1.extract_rels()["knows"]) == list(t2.extract_rels()["knows"])
