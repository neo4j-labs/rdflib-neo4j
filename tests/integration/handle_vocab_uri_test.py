import pytest
from rdflib import Graph, Namespace

from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.config.const import ShortenStrictException
from tests.integration.utils import records_equal, read_file_n10s_and_rdflib, get_credentials
from rdflib_neo4j.utils import HANDLE_VOCAB_URI_STRATEGY
import os
from dotenv import load_dotenv

from tests.integration.fixtures import neo4j_container, neo4j_driver, graph_store, graph_store_batched, \
    cleanup_databases

N10S_PROC_DB = "neo4j"
RDFLIB_DB = "rdflib"
N10S_CONSTRAINT_QUERY = "CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS FOR (r:Resource) REQUIRE r.uri IS UNIQUE"
GET_DATA_QUERY = "MATCH (n:Resource) RETURN n.uri as uri, labels(n) as labels, properties(n) as props ORDER BY uri"
LOCAL = (os.getenv("RUN_TEST_LOCALLY", "False").lower() == "true")

load_dotenv()


def test_shorten_all_prefixes_defined(neo4j_container, neo4j_driver):
    """
    If we use the strategy HANDLE_VOCAB_URI_STRATEGY.SHORTEN and we provide all the required namespaces,
    it should load all the data without raising an error for a missing prefix
    """
    auth_data = get_credentials(LOCAL, neo4j_container)

    # Define your prefixes
    prefixes = {
        'neo4ind': Namespace('http://neo4j.org/ind#'),
        'neo4voc': Namespace('http://neo4j.org/vocab/sw#')
    }

    # Define your custom mappings
    custom_mappings = []

    multival_props_names = []

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes=prefixes,
                              custom_mappings=custom_mappings,
                              multival_props_names=multival_props_names,
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.SHORTEN,
                              batching=False)

    graph_store = Graph(store=Neo4jStore(config=config))

    n10s_params = {"handleVocabUris": "SHORTEN_STRICT"}

    # If we don't want to map anything, we can just add a placeholder query.
    n10s_mappings = [("""CALL n10s.nsprefixes.add(
            'neo4voc', 
            'http://neo4j.org/vocab/sw#') """,
                      """CALL n10s.nsprefixes.add(
            'neo4ind', 
            'http://neo4j.org/ind#') """)]

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


def test_shorten_missing_prefix(neo4j_container, neo4j_driver):
    auth_data = get_credentials(LOCAL, neo4j_container)

    # Define your prefixes
    prefixes = {
        'neo4ind': Namespace('http://neo4j.org/ind#'),
    }

    # Define your custom mappings
    custom_mappings = []

    multival_props_names = []

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes=prefixes,
                              custom_mappings=custom_mappings,
                              multival_props_names=multival_props_names,
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.SHORTEN,
                              batching=False)

    graph_store = Graph(store=Neo4jStore(config=config))

    try:
        graph_store.parse('../test_files/n10s_example.ttl')
    except Exception as e:
        assert isinstance(e, ShortenStrictException)
    assert True


# TODO: add test for each strategy in HANDLE_VOCAB_URI_STRATEGY