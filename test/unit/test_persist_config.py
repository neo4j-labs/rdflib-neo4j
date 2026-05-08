from unittest.mock import MagicMock
from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY, HANDLE_MULTIVAL_STRATEGY


def make_store(extra_kwargs=None):
    kwargs = dict(
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.SHORTEN,
        handle_multival_strategy=HANDLE_MULTIVAL_STRATEGY.OVERWRITE,
    )
    if extra_kwargs:
        kwargs.update(extra_kwargs)
    config = Neo4jStoreConfig(**kwargs)
    store = Neo4jStore(config=config, neo4j_driver=MagicMock())
    store.session = MagicMock()
    store.session.run.return_value = iter([])
    store._Neo4jStore__open = True
    return store


def test_write_graph_config_calls_merge_query():
    store = make_store()
    store._write_graph_config()
    assert store.session.run.called
    call_args = store.session.run.call_args
    query = call_args[0][0]
    assert "MERGE (gc:_GraphConfig" in query


def test_write_graph_config_uses_additive_set():
    store = make_store()
    store._write_graph_config()
    call_args = store.session.run.call_args
    query = call_args[0][0]
    assert "SET gc += $params" in query


def get_graph_config_params(store):
    """Extract _GraphConfig params from the last session.run call."""
    call_args = store.session.run.call_args
    # __query_database calls session.run(query, params={"params": {...}})
    return call_args[1]["params"]["params"]


def test_write_graph_config_includes_handle_vocab_uris():
    store = make_store()
    store._write_graph_config()
    params = get_graph_config_params(store)
    assert params["_handleVocabUris"] == "SHORTEN"


def test_write_graph_config_includes_apply_neo4j_naming_false():
    store = make_store()
    store._write_graph_config()
    params = get_graph_config_params(store)
    assert "_applyNeo4jNaming" in params
    assert params["_applyNeo4jNaming"] is False


def test_write_graph_config_apply_neo4j_naming_true():
    store = make_store(extra_kwargs={"apply_neo4j_naming": True})
    store._write_graph_config()
    params = get_graph_config_params(store)
    assert params["_applyNeo4jNaming"] is True


def test_write_ns_prefixes_calls_merge_query():
    store = make_store()
    store._write_ns_prefixes()
    assert store.session.run.called
    call_args = store.session.run.call_args
    query = call_args[0][0]
    assert "MERGE (nsp:_NsPrefDef" in query


def test_write_ns_prefixes_includes_rdf_prefix():
    store = make_store()
    store._write_ns_prefixes()
    call_args = store.session.run.call_args
    # __query_database calls session.run(query, params={"props": {...}})
    props = call_args[1]["params"]["props"]
    assert "rdf" in props


def test_load_ns_prefixes_from_db_adds_new_prefix():
    store = make_store()
    mock_result = MagicMock()
    mock_result.single.return_value = {"props": {"_id": 1, "myns": "http://my.ns/"}}
    store.session.run.return_value = mock_result
    store._load_ns_prefixes_from_db()
    prefixes = store.config.get_prefixes()
    assert "myns" in prefixes
    assert str(prefixes["myns"]) == "http://my.ns/"


def test_load_ns_prefixes_from_db_skips_id_field():
    store = make_store()
    mock_result = MagicMock()
    mock_result.single.return_value = {"props": {"_id": 1, "myns": "http://my.ns/"}}
    store.session.run.return_value = mock_result
    store._load_ns_prefixes_from_db()
    prefixes = store.config.get_prefixes()
    # _id should not be added as a prefix
    assert "_id" not in prefixes


def test_load_ns_prefixes_from_db_no_node_does_not_crash():
    store = make_store()
    mock_result = MagicMock()
    mock_result.single.return_value = None
    store.session.run.return_value = mock_result
    # Should not raise
    store._load_ns_prefixes_from_db()


def test_load_ns_prefixes_from_db_exception_does_not_crash():
    store = make_store()
    store.session.run.side_effect = Exception("Connection error")
    # Should not raise
    store._load_ns_prefixes_from_db()


def test_apply_neo4j_naming_defaults_to_false():
    config = Neo4jStoreConfig()
    assert config.apply_neo4j_naming is False


def test_load_ns_prefixes_does_not_overwrite_existing_prefix():
    store = make_store()
    # "rdf" is already in default prefixes
    original_rdf = str(store.config.get_prefixes()["rdf"])
    mock_result = MagicMock()
    mock_result.single.return_value = {"props": {"_id": 1, "rdf": "http://other.ns/"}}
    store.session.run.return_value = mock_result
    store._load_ns_prefixes_from_db()
    # rdf prefix should remain unchanged (not overwritten by DB value)
    assert str(store.config.get_prefixes()["rdf"]) == original_rdf
