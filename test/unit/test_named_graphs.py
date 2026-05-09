"""
Unit tests for the named-graph (quad) feature (#63).

All tests use a mocked Neo4j session — no real database connection needed.
"""
from unittest.mock import MagicMock

from rdflib import URIRef, Literal, Graph

from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY
from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.Neo4jTriple import Neo4jTriple
from rdflib_neo4j.query_composers.NodeQueryComposer import NodeQueryComposer
from rdflib_neo4j.query_composers.RelationshipQueryComposer import RelationshipQueryComposer
from rdflib_neo4j.query_composers.ExportQueryComposer import ExportQueryComposer

EX = "http://example.org/"
G1 = "http://example.org/graphs/g1"
G2 = "http://example.org/graphs/g2"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_store(named_graphs=False, strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE):
    """Build a Neo4jStore with a mocked driver/session."""
    config = Neo4jStoreConfig(
        auth_data=None,
        handle_vocab_uri_strategy=strategy,
        named_graphs=named_graphs,
    )
    mock_driver = MagicMock()
    store = Neo4jStore(config=config, neo4j_driver=mock_driver)
    store.session = MagicMock()
    store._Neo4jStore__open = True
    return store


def make_graph(uri_str):
    """Return an rdflib Graph whose identifier is uri_str."""
    g = Graph(identifier=URIRef(uri_str))
    return g


def _mock_run(store, *result_sequences):
    """Configure store.session.run() to return successive mock result objects."""
    def make_result(rows):
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter(rows))
        return mock_result

    store.session.run.side_effect = [make_result(rows) for rows in result_sequences]


# ---------------------------------------------------------------------------
# Neo4jStoreConfig — named_graphs flag
# ---------------------------------------------------------------------------

class TestNeo4jStoreConfigNamedGraphs:
    def test_default_is_false(self):
        config = Neo4jStoreConfig(auth_data=None)
        assert config.named_graphs is False

    def test_can_set_true(self):
        config = Neo4jStoreConfig(auth_data=None, named_graphs=True)
        assert config.named_graphs is True

    def test_reflected_in_store(self):
        store = make_store(named_graphs=True)
        assert store.named_graphs is True

    def test_default_store_named_graphs_false(self):
        store = make_store(named_graphs=False)
        assert store.named_graphs is False


# ---------------------------------------------------------------------------
# Neo4jTriple — graph_uri parameter and extract_params
# ---------------------------------------------------------------------------

class TestNeo4jTripleGraphUri:
    def _make_triple(self, graph_uri=None):
        return Neo4jTriple(
            uri=URIRef(f"{EX}s"),
            handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
            handle_multival_strategy=MagicMock(),
            multival_props_names=[],
            prefixes={},
            graph_uri=graph_uri,
        )

    def test_no_graph_uri_not_in_params(self):
        triple = self._make_triple(graph_uri=None)
        params = triple.extract_params()
        assert "graphUri" not in params

    def test_graph_uri_in_params(self):
        triple = self._make_triple(graph_uri=G1)
        params = triple.extract_params()
        assert params["graphUri"] == G1

    def test_graph_uri_preserved_alongside_props(self):
        triple = self._make_triple(graph_uri=G2)
        triple.add_prop("name", "Alice")
        params = triple.extract_params()
        assert params["graphUri"] == G2
        assert params["name"] == "Alice"


# ---------------------------------------------------------------------------
# NodeQueryComposer — graph_uri_aware flag
# ---------------------------------------------------------------------------

class TestNodeQueryComposerGraphUriAware:
    def test_default_merge_uses_uri_only(self):
        composer = NodeQueryComposer(
            labels=set(),
            handle_multival_strategy=MagicMock(),
            multival_props_predicates=[],
        )
        q = composer.write_query()
        assert 'graphUri' not in q
        assert 'param["uri"]' in q

    def test_graph_uri_aware_merge_includes_graphUri(self):
        composer = NodeQueryComposer(
            labels=set(),
            handle_multival_strategy=MagicMock(),
            multival_props_predicates=[],
            graph_uri_aware=True,
        )
        q = composer.write_query()
        assert 'graphUri' in q
        assert 'param["graphUri"]' in q
        assert 'param["uri"]' in q

    def test_graph_uri_aware_false_by_default(self):
        composer = NodeQueryComposer(
            labels=set(),
            handle_multival_strategy=MagicMock(),
            multival_props_predicates=[],
        )
        assert composer.graph_uri_aware is False


# ---------------------------------------------------------------------------
# RelationshipQueryComposer — graph_uri_aware flag
# ---------------------------------------------------------------------------

class TestRelationshipQueryComposerGraphUriAware:
    def test_default_no_graphUri_in_query(self):
        composer = RelationshipQueryComposer("KNOWS")
        q = composer.write_query()
        assert 'graphUri' not in q

    def test_graph_uri_aware_query_includes_graphUri(self):
        composer = RelationshipQueryComposer("KNOWS", graph_uri_aware=True)
        q = composer.write_query()
        assert 'graphUri' in q

    def test_add_query_param_with_graph_uri(self):
        composer = RelationshipQueryComposer("KNOWS", graph_uri_aware=True)
        composer.add_query_param(
            from_node="http://a", to_node="http://b", graph_uri=G1
        )
        assert composer.query_params[0]["graphUri"] == G1

    def test_add_query_param_without_graph_uri(self):
        composer = RelationshipQueryComposer("KNOWS")
        composer.add_query_param(from_node="http://a", to_node="http://b")
        assert "graphUri" not in composer.query_params[0]


# ---------------------------------------------------------------------------
# ExportQueryComposer — graph_uri filtering
# ---------------------------------------------------------------------------

class TestExportQueryComposerGraphUri:
    def test_all_nodes_no_graph_uri(self):
        q = ExportQueryComposer.all_nodes_query()
        assert "graphUri" not in q

    def test_all_nodes_with_graph_uri(self):
        q = ExportQueryComposer.all_nodes_query(graph_uri=G1)
        assert "graphUri" in q
        assert "$graphUri" in q

    def test_node_by_uri_no_graph_uri(self):
        q = ExportQueryComposer.node_by_uri_query()
        assert "graphUri" not in q

    def test_node_by_uri_with_graph_uri(self):
        q = ExportQueryComposer.node_by_uri_query(graph_uri=G1)
        assert "graphUri" in q

    def test_all_relationships_no_graph_uri(self):
        q = ExportQueryComposer.all_relationships_query()
        assert "graphUri" not in q

    def test_all_relationships_with_graph_uri(self):
        q = ExportQueryComposer.all_relationships_query(graph_uri=G1)
        assert "graphUri" in q

    def test_relationships_from_uri_no_graph_uri(self):
        q = ExportQueryComposer.relationships_from_uri_query()
        assert "graphUri" not in q

    def test_relationships_from_uri_with_graph_uri(self):
        q = ExportQueryComposer.relationships_from_uri_query(graph_uri=G1)
        assert "graphUri" in q


# ---------------------------------------------------------------------------
# Neo4jStore.add() — graph_uri propagation
# ---------------------------------------------------------------------------

class TestNeo4jStoreAddNamedGraphs:
    def test_add_without_named_graphs_ignores_context(self):
        """When named_graphs=False, context is ignored (existing behaviour)."""
        store = make_store(named_graphs=False)
        store.batching = False
        store.session.run = MagicMock()

        ctx = make_graph(G1)
        store.add(
            (URIRef(f"{EX}s"), URIRef(f"{EX}p"), Literal("v")),
            context=ctx,
        )
        store.commit()

        # The query params must NOT contain graphUri
        all_params = []
        for c in store.session.run.call_args_list:
            params_arg = c[1].get("params") or (c[0][1] if len(c[0]) > 1 else None)
            if isinstance(params_arg, list):
                all_params.extend(params_arg)
        for param in all_params:
            assert "graphUri" not in param

    def test_add_with_named_graphs_stores_graph_uri(self):
        """When named_graphs=True, graphUri is included in node params."""
        store = make_store(named_graphs=True)
        store.batching = False
        store.session.run = MagicMock()

        ctx = make_graph(G1)
        store.add(
            (URIRef(f"{EX}s"), URIRef(f"{EX}p"), Literal("v")),
            context=ctx,
        )
        store.commit()

        # Find the UNWIND call and check its params
        graph_uri_found = False
        for c in store.session.run.call_args_list:
            params_arg = c[1].get("params")
            if isinstance(params_arg, list):
                for row in params_arg:
                    if row.get("graphUri") == G1:
                        graph_uri_found = True
        assert graph_uri_found, "graphUri not found in any node query param"

    def test_add_without_context_in_named_graphs_mode(self):
        """When named_graphs=True but no context provided, graphUri is absent."""
        store = make_store(named_graphs=True)
        store.batching = False
        store.session.run = MagicMock()

        store.add(
            (URIRef(f"{EX}s"), URIRef(f"{EX}p"), Literal("v")),
            context=None,
        )
        store.commit()

        for c in store.session.run.call_args_list:
            params_arg = c[1].get("params")
            if isinstance(params_arg, list):
                for row in params_arg:
                    assert "graphUri" not in row


# ---------------------------------------------------------------------------
# Neo4jStore.triples() — context-based graph_uri filtering
# ---------------------------------------------------------------------------

class TestNeo4jStoreTriplesNamedGraphs:
    def _node_row(self, uri, props=None, labels=None, graph_uri=None):
        p = {"uri": uri}
        if graph_uri is not None:
            p["graphUri"] = graph_uri
        if props:
            p.update(props)
        return {"uri": uri, "props": p, "extra_labels": labels or []}

    def test_triples_no_context_no_graph_uri_filter(self):
        """No context → all_nodes_query without graphUri parameter."""
        store = make_store(named_graphs=True)
        row = self._node_row(f"{EX}s", {"name": "Alice"})
        _mock_run(store, [row], [])  # nodes, then rels

        list(store.triples((None, None, None)))
        # Verify session.run was called without graphUri kwarg for first call
        first_call = store.session.run.call_args_list[0]
        assert "graphUri" not in first_call[1]

    def test_triples_with_context_passes_graphUri(self):
        """context supplied → query includes graphUri kwarg."""
        store = make_store(named_graphs=True)
        row = self._node_row(f"{EX}s", {"name": "Alice"}, graph_uri=G1)
        _mock_run(store, [row], [])

        ctx = make_graph(G1)
        list(store.triples((None, None, None), context=ctx))

        first_call = store.session.run.call_args_list[0]
        assert first_call[1].get("graphUri") == G1

    def test_triples_graphUri_prop_skipped_in_output(self):
        """graphUri node property must NOT appear as a predicate in yielded triples."""
        store = make_store(named_graphs=True)
        row = self._node_row(f"{EX}s", {"name": "Alice", "graphUri": G1})
        _mock_run(store, [row], [])

        ctx = make_graph(G1)
        results = list(store.triples((None, None, None), context=ctx))

        predicates = [str(t[0][1]) for t in results]
        assert f"{EX}graphUri" not in predicates
        assert "graphUri" not in predicates

    def test_triples_with_subject_and_context(self):
        """Subject + context → node_by_uri_query with both uri and graphUri."""
        store = make_store(named_graphs=True)
        row = self._node_row(f"{EX}s", {"name": "Bob"}, graph_uri=G2)
        _mock_run(store, [row], [])

        ctx = make_graph(G2)
        list(store.triples((URIRef(f"{EX}s"), None, None), context=ctx))

        first_call = store.session.run.call_args_list[0]
        assert first_call[1].get("graphUri") == G2
        assert first_call[1].get("uri") == f"{EX}s"

    def test_named_graphs_false_context_ignored_in_triples(self):
        """named_graphs=False → context passed to triples() is silently ignored."""
        store = make_store(named_graphs=False)
        row = self._node_row(f"{EX}s", {"name": "Carol"})
        _mock_run(store, [row], [])

        ctx = make_graph(G1)
        list(store.triples((None, None, None), context=ctx))

        first_call = store.session.run.call_args_list[0]
        assert "graphUri" not in first_call[1]
