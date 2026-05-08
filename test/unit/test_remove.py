"""
Unit tests for Neo4jStore.remove() and DeleteQueryComposer.

These tests use mocks and do not require a running Neo4j instance.
"""
from unittest.mock import MagicMock
from rdflib import URIRef, Literal, RDF
from rdflib.term import BNode

from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY
from rdflib_neo4j.query_composers.DeleteQueryComposer import DeleteQueryComposer

EX = "http://example.org/"

SUBJECT = URIRef(f"{EX}subject")
OBJ_URI = URIRef(f"{EX}object")
PRED_NAME = URIRef(f"{EX}name")
PRED_KNOWS = URIRef(f"{EX}knows")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_store():
    """Return an open Neo4jStore backed by a MagicMock session.

    The store is constructed with a driver only (no auth_data), because the
    Neo4jStore constructor rejects both being supplied simultaneously.
    """
    config = Neo4jStoreConfig(
        # No auth_data — we supply a driver below
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
    )
    store = Neo4jStore(config=config, neo4j_driver=MagicMock())
    store.session = MagicMock()
    store.session.run.return_value = iter([])
    # Bypass the open flag directly (avoid needing a live DB for constraint check)
    store._Neo4jStore__open = True
    return store


# ---------------------------------------------------------------------------
# DeleteQueryComposer unit tests
# ---------------------------------------------------------------------------

class TestDeleteQueryComposer:

    def test_remove_property_query(self):
        query, params = DeleteQueryComposer.remove_property("http://ex/a", "name")
        assert "MATCH (n:Resource {uri: $uri})" in query
        assert "REMOVE n.`name`" in query
        assert params == {"uri": "http://ex/a"}

    def test_remove_label_query(self):
        query, params = DeleteQueryComposer.remove_label("http://ex/a", "Person")
        assert "MATCH (n:Resource {uri: $uri})" in query
        assert "REMOVE n:`Person`" in query
        assert params == {"uri": "http://ex/a"}

    def test_remove_relationship_query(self):
        query, params = DeleteQueryComposer.remove_relationship(
            "http://ex/a", "knows", "http://ex/b"
        )
        assert "MATCH (a:Resource {uri: $from_uri})" in query
        assert "-[r:`knows`]->" in query
        assert "(b:Resource {uri: $to_uri})" in query
        assert "DELETE r" in query
        assert params == {"from_uri": "http://ex/a", "to_uri": "http://ex/b"}

    def test_remove_all_properties_query(self):
        query, params = DeleteQueryComposer.remove_all_properties("http://ex/a")
        assert "MATCH (n:Resource {uri: $uri})" in query
        assert "SET n = {uri: n.uri}" in query
        assert params == {"uri": "http://ex/a"}

    def test_remove_all_relationships_query(self):
        query, params = DeleteQueryComposer.remove_all_relationships("http://ex/a")
        assert "MATCH (n:Resource {uri: $uri})-[r]->()" in query
        assert "DELETE r" in query
        assert params == {"uri": "http://ex/a"}

    def test_remove_all_outgoing_of_type_query(self):
        query, params = DeleteQueryComposer.remove_all_outgoing_of_type("http://ex/a", "knows")
        assert "-[r:`knows`]->()" in query
        assert "DELETE r" in query
        assert params == {"uri": "http://ex/a"}

    def test_remove_node_if_empty_query(self):
        query, params = DeleteQueryComposer.remove_node_if_empty("http://ex/a")
        assert "size(keys(n)) <= 1" in query
        assert "DELETE n" in query
        assert params == {"uri": "http://ex/a"}


# ---------------------------------------------------------------------------
# Neo4jStore.remove() tests — literal (property) retraction
# ---------------------------------------------------------------------------

class TestRemoveLiteral:

    def test_remove_literal_triple_calls_session_run(self):
        store = make_store()
        triple = (SUBJECT, PRED_NAME, Literal("Alice"))
        store.remove(triple)
        store.session.run.assert_called_once()
        call_args = store.session.run.call_args
        query = call_args[0][0]
        assert "REMOVE n.`name`" in query
        params = call_args[1]["params"]
        assert params["uri"] == str(SUBJECT)

    def test_remove_literal_uses_ignore_strategy(self):
        """With IGNORE strategy the property name is the local part of the predicate."""
        store = make_store()
        triple = (SUBJECT, URIRef(f"{EX}fullName"), Literal("Bob"))
        store.remove(triple)
        query = store.session.run.call_args[0][0]
        assert "REMOVE n.`fullName`" in query

    def test_remove_literal_with_none_subject_logs_warning(self, caplog):
        import logging
        store = make_store()
        triple = (None, PRED_NAME, Literal("Alice"))
        with caplog.at_level(logging.WARNING):
            store.remove(triple)
        store.session.run.assert_not_called()
        assert "wildcard" in caplog.text.lower() or "not yet supported" in caplog.text.lower()


# ---------------------------------------------------------------------------
# Neo4jStore.remove() tests — rdf:type (label) retraction
# ---------------------------------------------------------------------------

class TestRemoveLabel:

    def test_remove_rdf_type_triple_removes_label(self):
        store = make_store()
        triple = (SUBJECT, RDF.type, URIRef(f"{EX}Person"))
        store.remove(triple)
        store.session.run.assert_called_once()
        query = store.session.run.call_args[0][0]
        assert "REMOVE n:`Person`" in query

    def test_remove_rdf_type_with_none_object_keeps_resource_label(self):
        store = make_store()
        triple = (SUBJECT, RDF.type, None)
        store.remove(triple)
        store.session.run.assert_called_once()
        query = store.session.run.call_args[0][0]
        assert "SET n:Resource" in query

    def test_remove_rdf_type_with_none_subject_returns_early(self):
        store = make_store()
        triple = (None, RDF.type, URIRef(f"{EX}Person"))
        store.remove(triple)
        store.session.run.assert_not_called()


# ---------------------------------------------------------------------------
# Neo4jStore.remove() tests — URI object (relationship) retraction
# ---------------------------------------------------------------------------

class TestRemoveRelationship:

    def test_remove_uri_object_triple_deletes_relationship(self):
        store = make_store()
        triple = (SUBJECT, PRED_KNOWS, OBJ_URI)
        store.remove(triple)
        store.session.run.assert_called_once()
        query = store.session.run.call_args[0][0]
        assert "DELETE r" in query
        assert "-[r:`knows`]->" in query

    def test_remove_uri_object_params_contain_both_uris(self):
        store = make_store()
        triple = (SUBJECT, PRED_KNOWS, OBJ_URI)
        store.remove(triple)
        params = store.session.run.call_args[1]["params"]
        assert params["from_uri"] == str(SUBJECT)
        assert params["to_uri"] == str(OBJ_URI)

    def test_remove_uri_with_none_predicate_deletes_any_rel_to_object(self):
        store = make_store()
        triple = (SUBJECT, None, OBJ_URI)
        store.remove(triple)
        store.session.run.assert_called_once()
        query = store.session.run.call_args[0][0]
        # No specific rel type — should delete any relationship between the two nodes
        assert "DELETE r" in query
        assert "$from_uri" in query
        assert "$to_uri" in query

    def test_remove_uri_with_none_subject_returns_early(self):
        store = make_store()
        triple = (None, PRED_KNOWS, OBJ_URI)
        store.remove(triple)
        store.session.run.assert_not_called()


# ---------------------------------------------------------------------------
# BNode normalisation
# ---------------------------------------------------------------------------

class TestBNodeNormalisation:

    def test_bnode_subject_normalised_to_bnode_uri(self):
        store = make_store()
        bnode = BNode("abc123")
        triple = (bnode, PRED_NAME, Literal("Alice"))
        store.remove(triple)
        store.session.run.assert_called_once()
        params = store.session.run.call_args[1]["params"]
        assert params["uri"] == "bnode://abc123"

    def test_bnode_object_normalised_to_bnode_uri(self):
        store = make_store()
        bnode = BNode("xyz789")
        triple = (SUBJECT, PRED_KNOWS, bnode)
        store.remove(triple)
        store.session.run.assert_called_once()
        params = store.session.run.call_args[1]["params"]
        assert params["to_uri"] == "bnode://xyz789"


# ---------------------------------------------------------------------------
# Node cache eviction
# ---------------------------------------------------------------------------

class TestNodeCacheEviction:

    def test_cache_evicted_when_subject_removed(self):
        store = make_store()
        subject_uri_str = str(SUBJECT)
        # Pre-populate the cache
        store._node_cache[subject_uri_str] = True

        triple = (SUBJECT, PRED_NAME, Literal("Alice"))
        store.remove(triple)

        assert subject_uri_str not in store._node_cache

    def test_cache_not_touched_for_uncached_subject(self):
        store = make_store()
        # Cache is empty — removal of an uncached subject should not raise
        triple = (SUBJECT, PRED_NAME, Literal("Alice"))
        store.remove(triple)  # should not raise KeyError

    def test_other_cache_entries_untouched(self):
        store = make_store()
        other_uri = str(URIRef(f"{EX}other"))
        store._node_cache[other_uri] = True
        store._node_cache[str(SUBJECT)] = True

        triple = (SUBJECT, PRED_NAME, Literal("Alice"))
        store.remove(triple)

        # Only the subject entry is evicted
        assert other_uri in store._node_cache
        assert str(SUBJECT) not in store._node_cache
