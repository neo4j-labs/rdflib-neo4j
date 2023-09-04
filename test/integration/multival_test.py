from test.integration.constants import LOCAL
from rdflib import Graph, Namespace
from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from test.integration.utils import records_equal, read_file_n10s_and_rdflib, create_graph_store, get_credentials
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY, HANDLE_MULTIVAL_STRATEGY
import pytest
from test.integration.fixtures import neo4j_container, neo4j_driver, graph_store, graph_store_batched, \
    cleanup_databases


def test_read_file_multival_with_strategy_no_predicates(neo4j_container, neo4j_driver):
    """Compare data imported with n10s procs and n10s + rdflib in single add mode for multivalues"""

    auth_data = get_credentials(LOCAL, neo4j_container)

    # Define your prefixes
    prefixes = {}

    # Define your custom mappings
    custom_mappings = []

    multival_props_names = []

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes=prefixes,
                              custom_mappings=custom_mappings,
                              multival_props_names=multival_props_names,
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
                              handle_multival_strategy=HANDLE_MULTIVAL_STRATEGY.ARRAY,
                              batching=False)

    graph_store = Graph(store=Neo4jStore(config=config))

    n10s_params = {"handleVocabUris": "IGNORE", "handleMultival": "ARRAY"}

    records_from_rdf_lib, records, _, _ = read_file_n10s_and_rdflib(neo4j_driver, graph_store, n10s_params=n10s_params)
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])


def test_read_file_multival_with_strategy_and_predicates(neo4j_container, neo4j_driver):
    """Compare data imported with n10s procs and n10s + rdflib in single add mode for multivalues"""
    """Compare data imported with n10s procs and n10s + rdflib in single add mode for multivalues"""
    auth_data = get_credentials(LOCAL, neo4j_container)

    # Define your prefixes
    prefixes = {
        'neo4voc': Namespace('http://neo4j.org/vocab/sw#')
    }

    # Define your custom mappings
    custom_mappings = []

    multival_props_names = [("neo4voc", "author")]

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes=prefixes,
                              custom_mappings=custom_mappings,
                              multival_props_names=multival_props_names,
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
                              handle_multival_strategy=HANDLE_MULTIVAL_STRATEGY.ARRAY,
                              batching=False)

    graph_store = Graph(store=Neo4jStore(config=config))

    n10s_params = {"handleVocabUris": "IGNORE", "handleMultival": "ARRAY",
                   "multivalPropList": ["http://neo4j.org/vocab/sw#author"]}
    records_from_rdf_lib, records, _, _ = read_file_n10s_and_rdflib(neo4j_driver, graph_store, n10s_params=n10s_params)
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])


def test_read_file_multival_with_no_strategy_and_predicates(neo4j_container, neo4j_driver):
    """Compare data imported with n10s procs and n10s + rdflib in single add mode for multivalues"""
    auth_data = get_credentials(LOCAL, neo4j_container)

    # Define your prefixes
    prefixes = {
        'neo4voc': Namespace('http://neo4j.org/vocab/sw#')
    }

    # Define your custom mappings
    custom_mappings = []

    multival_props_names = [("neo4voc", "author")]

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes=prefixes,
                              custom_mappings=custom_mappings,
                              multival_props_names=multival_props_names,
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
                              batching=False)

    graph_store = Graph(store=Neo4jStore(config=config))

    n10s_params = {"handleVocabUris": "IGNORE", "multivalPropList": ["http://neo4j.org/vocab/sw#author"]}
    records_from_rdf_lib, records, _, _ = read_file_n10s_and_rdflib(neo4j_driver, graph_store, n10s_params=n10s_params)
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])
