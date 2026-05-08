"""
Unit tests for the triples(), __len__(), and __iter__() read interface,
plus expander.py helpers.

All tests use a mocked Neo4j session — no real database connection needed.
"""
from unittest.mock import MagicMock

from rdflib import URIRef, Literal, RDF, XSD
from rdflib.term import BNode

from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY
from rdflib_neo4j.expander import expand_uri, neo4j_value_to_literal, BNODE_PREFIX

EX = "http://example.org/"
RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
FOAF_NS = "http://xmlns.com/foaf/0.1/"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_store(strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE):
    """
    Build a Neo4jStore with a mocked driver/session.

    We pass auth_data=None and supply a driver mock so Neo4jStore does not
    require real credentials.  The __open flag is set directly to bypass the
    open() call that needs a real DB connection.
    """
    from rdflib_neo4j.Neo4jStore import Neo4jStore

    config = Neo4jStoreConfig(
        auth_data=None,
        handle_vocab_uri_strategy=strategy,
    )
    mock_driver = MagicMock()
    store = Neo4jStore(config=config, neo4j_driver=mock_driver)
    store.session = MagicMock()
    # Directly set the private attribute that is_open() reads
    store._Neo4jStore__open = True
    return store


def _mock_run(store, *result_sequences):
    """
    Configure store.session.run() to return successive mock result objects.

    Each item in result_sequences is a list of dicts (rows) for one run() call.
    """
    def make_result(rows):
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter(rows))
        return mock_result

    store.session.run.side_effect = [make_result(rows) for rows in result_sequences]


# ---------------------------------------------------------------------------
# Tests for expand_uri
# ---------------------------------------------------------------------------

class TestExpandUri:
    def test_bnode(self):
        result = expand_uri(f"{BNODE_PREFIX}xyz", {})
        assert result == BNode("xyz")
        assert isinstance(result, BNode)

    def test_full_uri(self):
        result = expand_uri("http://example.org/foo", {})
        assert result == URIRef("http://example.org/foo")
        assert isinstance(result, URIRef)

    def test_shortened_ns_local(self):
        pm = {"rdf": RDF_NS}
        result = expand_uri("rdf__type", pm)
        assert result == URIRef(f"{RDF_NS}type")

    def test_shortened_unknown_prefix_falls_back(self):
        # Unknown prefix → returned as-is (IGNORE mode fallback)
        result = expand_uri("unknown__foo", {})
        assert result == URIRef("unknown__foo")

    def test_bare_local_name_fallback(self):
        # IGNORE mode stores bare local names; we return URIRef as-is
        result = expand_uri("name", {})
        assert result == URIRef("name")

    def test_https_uri(self):
        result = expand_uri("https://schema.org/name", {})
        assert result == URIRef("https://schema.org/name")


# ---------------------------------------------------------------------------
# Tests for neo4j_value_to_literal
# ---------------------------------------------------------------------------

class TestNeo4jValueToLiteral:
    def test_int(self):
        result = neo4j_value_to_literal(42)
        assert result == Literal(42, datatype=XSD.integer)

    def test_float(self):
        result = neo4j_value_to_literal(3.14)
        assert result == Literal(3.14, datatype=XSD.double)

    def test_bool_true(self):
        result = neo4j_value_to_literal(True)
        assert result == Literal(True, datatype=XSD.boolean)

    def test_bool_false(self):
        result = neo4j_value_to_literal(False)
        assert result == Literal(False, datatype=XSD.boolean)

    def test_bool_before_int(self):
        # True would be Literal(1) if int were checked first — ensure correct order
        result = neo4j_value_to_literal(True)
        assert result.datatype == XSD.boolean

    def test_string(self):
        result = neo4j_value_to_literal("hello")
        assert result == Literal("hello")

    def test_list(self):
        result = neo4j_value_to_literal([1, 2])
        assert result == [Literal(1, datatype=XSD.integer), Literal(2, datatype=XSD.integer)]

    def test_string_with_custom_type(self):
        result = neo4j_value_to_literal("2024-01-01^^http://www.w3.org/2001/XMLSchema#date")
        assert result.datatype == XSD.date
        assert str(result) == "2024-01-01"

    def test_string_with_lang_tag(self):
        result = neo4j_value_to_literal("hello@en")
        assert result.language == "en"
        assert str(result) == "hello"


# ---------------------------------------------------------------------------
# Tests for Neo4jStore.triples()
# ---------------------------------------------------------------------------

class TestTriples:
    def _make_node_record(self, uri, labels=None, props=None):
        """Helper to build a dict resembling a Neo4j record for a node query."""
        rec = MagicMock()
        rec.__getitem__ = lambda self, key: {
            "uri": uri,
            "extra_labels": labels or [],
            "props": dict({"uri": uri}, **(props or {})),
        }[key]
        return rec

    def _make_rel_record(self, from_uri, rel_type, to_uri):
        rec = MagicMock()
        rec.__getitem__ = lambda self, key: {
            "from_uri": from_uri,
            "rel_type": rel_type,
            "to_uri": to_uri,
        }[key]
        return rec

    def test_wildcard_yields_property_triple(self):
        store = make_store()
        node_rec = self._make_node_record(
            f"{EX}donna",
            props={"name": "Donna"},
        )
        _mock_run(store, [node_rec], [])  # nodes, then rels
        results = list(store.triples((None, None, None)))
        # Should yield at least one property triple
        subjects = [t for (t, _) in results]
        assert any(
            s == URIRef(f"{EX}donna") and str(o) == "Donna"
            for (s, p, o) in subjects
        )

    def test_wildcard_yields_rdf_type_triple(self):
        store = make_store(strategy=HANDLE_VOCAB_URI_STRATEGY.KEEP)
        node_rec = self._make_node_record(
            f"{EX}donna",
            labels=["Person"],
        )
        _mock_run(store, [node_rec], [])
        results = list(store.triples((None, None, None)))
        triples_only = [t for (t, _) in results]
        assert any(p == RDF.type for (s, p, o) in triples_only)

    def test_wildcard_yields_relationship_triple(self):
        store = make_store(strategy=HANDLE_VOCAB_URI_STRATEGY.KEEP)
        rel_rec = self._make_rel_record(
            f"{EX}donna",
            f"{EX}knows",
            f"{EX}bob",
        )
        _mock_run(store, [], [rel_rec])  # no node props, one rel
        results = list(store.triples((None, None, None)))
        triples_only = [t for (t, _) in results]
        assert (URIRef(f"{EX}donna"), URIRef(f"{EX}knows"), URIRef(f"{EX}bob")) in triples_only

    def test_subject_bound_uses_specific_query(self):
        store = make_store()
        node_rec = self._make_node_record(f"{EX}donna")
        _mock_run(store, [node_rec], [])
        list(store.triples((URIRef(f"{EX}donna"), None, None)))
        # First run() call should use the node_by_uri_query with uri param
        first_call_kwargs = store.session.run.call_args_list[0]
        assert first_call_kwargs[1].get("uri") == f"{EX}donna" or \
               (len(first_call_kwargs[0]) > 1 and first_call_kwargs[0][1] == f"{EX}donna")

    def test_bnode_subject_is_converted_to_bnode_uri(self):
        store = make_store()
        bn = BNode("xyz")
        _mock_run(store, [], [])
        list(store.triples((bn, None, None)))
        first_call = store.session.run.call_args_list[0]
        # The URI passed should be bnode://xyz
        passed_uri = first_call[1].get("uri", "") or (
            first_call[0][1] if len(first_call[0]) > 1 else ""
        )
        assert passed_uri == f"{BNODE_PREFIX}xyz"

    def test_rdf_type_predicate_skips_rel_query(self):
        """When p=rdf:type, relationship queries should not be issued."""
        store = make_store()
        node_rec = self._make_node_record(f"{EX}donna", labels=["Person"])
        _mock_run(store, [node_rec])  # only one run() call expected
        list(store.triples((URIRef(f"{EX}donna"), RDF.type, None)))
        # Only the node query should have been called (no rel query)
        assert store.session.run.call_count == 1

    def test_literal_object_skips_rel_query(self):
        """When o is a Literal, relationship queries should not be issued."""
        store = make_store()
        node_rec = self._make_node_record(f"{EX}donna", props={"name": "Donna"})
        _mock_run(store, [node_rec])  # only one run() call expected
        list(store.triples((URIRef(f"{EX}donna"), None, Literal("Donna"))))
        assert store.session.run.call_count == 1

    def test_o_is_uriref_includes_node_triples(self):
        """(None, None, URIRef) should still yield rdf:type triples (fix for spec bug)."""
        store = make_store(strategy=HANDLE_VOCAB_URI_STRATEGY.KEEP)
        target = URIRef(f"{EX}Person")
        node_rec = self._make_node_record(f"{EX}donna", labels=[f"{EX}Person"])
        _mock_run(store, [node_rec], [])
        results = list(store.triples((None, None, target)))
        triples_only = [t for (t, _) in results]
        assert (URIRef(f"{EX}donna"), RDF.type, target) in triples_only


# ---------------------------------------------------------------------------
# Tests for __len__
# ---------------------------------------------------------------------------

class TestLen:
    def test_len_returns_count(self):
        store = make_store()
        mock_result = MagicMock()
        mock_single = {"cnt": 42}
        mock_result.single.return_value = mock_single
        store.session.run.return_value = mock_result
        assert len(store) == 42

    def test_len_returns_zero_when_no_records(self):
        store = make_store()
        mock_result = MagicMock()
        mock_result.single.return_value = None
        store.session.run.return_value = mock_result
        assert len(store) == 0

    def test_len_returns_zero_when_closed(self):
        store = make_store()
        store._Neo4jStore__open = False
        assert len(store) == 0


# ---------------------------------------------------------------------------
# Tests for __iter__
# ---------------------------------------------------------------------------

class TestIter:
    def test_iter_delegates_to_triples(self):
        """__iter__ should return the same triples as triples((None,None,None))."""
        store = make_store(strategy=HANDLE_VOCAB_URI_STRATEGY.KEEP)

        def make_node_result():
            rec = MagicMock()
            rec.__getitem__ = lambda self, key: {
                "uri": f"{EX}donna",
                "extra_labels": [],
                "props": {"uri": f"{EX}donna", "name": "Donna"},
            }[key]
            result = MagicMock()
            result.__iter__ = MagicMock(return_value=iter([rec]))
            return result

        # iter calls triples((None,None,None)) which does 2 run() calls
        store.session.run.side_effect = [make_node_result(), MagicMock(__iter__=lambda s: iter([]))]
        triples_list = list(store.__iter__())
        assert len(triples_list) > 0
        assert all(isinstance(t, tuple) and len(t) == 3 for t in triples_list)
