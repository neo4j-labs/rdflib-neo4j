import pytest
from rdflib import Graph, Namespace
from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from test.integration.constants import LOCAL
from test.integration.utils import records_equal, read_file_n10s_and_rdflib, get_credentials
from rdflib_neo4j.utils import HANDLE_VOCAB_URI_STRATEGY
import os
from dotenv import load_dotenv
from test.integration.fixtures import neo4j_container, neo4j_driver, graph_store, graph_store_batched, \
    cleanup_databases


def test_custom_mapping_match(neo4j_container, neo4j_driver):
    """
    If we define a custom mapping and the strategy is HANDLE_VOCAB_URI_STRATEGY.MAP, it should match it and use the mapping
    if the predicate satisfies the mapping.
    """

    auth_data = get_credentials(LOCAL, neo4j_container)
    # Define your prefixes
    prefixes = {
        'neo4voc': Namespace('http://neo4j.org/vocab/sw#')
    }

    # Define your custom mappings
    custom_mappings = [("neo4voc", "runsOn", "RUNS_ON")]

    multival_props_names = []

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes=prefixes,
                              custom_mappings=custom_mappings,
                              multival_props_names=multival_props_names,
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.MAP,
                              batching=False)

    graph_store = Graph(store=Neo4jStore(config=config))
    n10s_mappings = [("""CALL n10s.nsprefixes.add(
    'neo4voc', 
    'http://neo4j.org/vocab/sw#') """,
                      """CALL n10s.mapping.add(
        'http://neo4j.org/vocab/sw#runsOn',
        'RUNS_ON')""")]

    n10s_params = {"handleVocabUris": "MAP"}
    records_from_rdf_lib, records, rels_from_rdflib, rels = read_file_n10s_and_rdflib(neo4j_driver, graph_store,
                                                                                      n10s_params=n10s_params,
                                                                                      n10s_mappings=n10s_mappings,
                                                                                      get_rels=True)
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])
    assert len(rels_from_rdflib) == len(rels)
    for i in range(len(rels)):
        assert records_equal(rels[i], rels_from_rdflib[i], rels=True)


def test_custom_mapping_no_match(neo4j_container, neo4j_driver):
    """
    If we define a custom mapping and the strategy is HANDLE_VOCAB_URI_STRATEGY.MAP, it shouldn't apply the mapping if the
    predicate doesn't satisfy the mapping and use IGNORE as a strategy.
    """
    """
        If we define a custom mapping and the strategy is HANDLE_VOCAB_URI_STRATEGY.MAP, it should match it and use the mapping
        if the predicate satisfies the mapping.
        """

    auth_data = get_credentials(LOCAL, neo4j_container)

    # Define your prefixes
    prefixes = {
        'neo4voc': Namespace('http://neo4j.org/vocab/sw#')
    }

    # Define your custom mappings
    custom_mappings = [("neo4voc", "runson", "RUNS_ON")]

    multival_props_names = []

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes=prefixes,
                              custom_mappings=custom_mappings,
                              multival_props_names=multival_props_names,
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.MAP,
                              batching=False)

    graph_store = Graph(store=Neo4jStore(config=config))
    n10s_mappings = [("""CALL n10s.nsprefixes.add(
        'neo4voc', 
        'http://neo4j.org/vocab/sw#') """,
                      """CALL n10s.mapping.add(
        'http://neo4j.org/vocab/sw#runson',
        'RUNS_ON')""")]

    n10s_params = {"handleVocabUris": "MAP"}
    records_from_rdf_lib, records, rels_from_rdflib, rels = read_file_n10s_and_rdflib(neo4j_driver, graph_store,
                                                                                      n10s_params=n10s_params,
                                                                                      n10s_mappings=n10s_mappings,
                                                                                      get_rels=True)
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])
    assert len(rels_from_rdflib) == len(rels)
    for i in range(len(rels)):
        assert records_equal(rels[i], rels_from_rdflib[i], rels=True)


def test_custom_mapping_map_strategy_zero_custom_mappings(neo4j_container, neo4j_driver):
    """
    If we don't define custom mapping and the strategy is HANDLE_VOCAB_URI_STRATEGY.MAP, it shouldn't apply the mapping on anything and
    just use IGNORE mode.
    """
    auth_data = get_credentials(LOCAL, neo4j_container)

    # Define your prefixes
    prefixes = {
        'neo4voc': Namespace('http://neo4j.org/vocab/sw#')
    }

    # Define your custom mappings
    custom_mappings = []

    multival_props_names = []

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes=prefixes,
                              custom_mappings=custom_mappings,
                              multival_props_names=multival_props_names,
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.MAP,
                              batching=False)

    graph_store = Graph(store=Neo4jStore(config=config))
    n10s_mappings = []

    n10s_params = {"handleVocabUris": "MAP"}
    records_from_rdf_lib, records, rels_from_rdflib, rels = read_file_n10s_and_rdflib(neo4j_driver, graph_store,
                                                                                      n10s_params=n10s_params,
                                                                                      n10s_mappings=n10s_mappings,
                                                                                      get_rels=True)
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])
    assert len(rels_from_rdflib) == len(rels)
    for i in range(len(rels)):
        assert records_equal(rels[i], rels_from_rdflib[i], rels=True)
