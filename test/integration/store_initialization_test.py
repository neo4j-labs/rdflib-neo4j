from rdflib import Literal, RDF, URIRef, Graph
from rdflib.namespace import FOAF

from rdflib_neo4j import HANDLE_VOCAB_URI_STRATEGY, Neo4jStoreConfig, Neo4jStore
from test.integration.constants import GET_DATA_QUERY, RDFLIB_DB
import pytest
from test.integration.fixtures import neo4j_connection_parameters, neo4j_driver, neo4j_container


def test_initialize_store_with_credentials(neo4j_connection_parameters, neo4j_driver):

    auth_data = neo4j_connection_parameters

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes={},
                              custom_mappings=[],
                              multival_props_names=[],
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.MAP,
                              batching=False)

    graph_store = Graph(store=Neo4jStore(config=config))
    donna = URIRef("https://example.org/donna")
    graph_store.add((donna, FOAF.name, Literal("Donna Fales")))
    graph_store.commit()
    records, summary, keys = neo4j_driver.execute_query(GET_DATA_QUERY, database_=RDFLIB_DB)
    assert len(records) == 1


def test_initialize_store_with_driver(neo4j_driver):

    config = Neo4jStoreConfig(auth_data=None,
                              custom_prefixes={},
                              custom_mappings=[],
                              multival_props_names=[],
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.MAP,
                              batching=False)

    graph_store = Graph(store=Neo4jStore(config=config, neo4j_driver=neo4j_driver))
    donna = URIRef("https://example.org/donna")
    graph_store.add((donna, FOAF.name, Literal("Donna Fales")))
    graph_store.commit()
    records, summary, keys = neo4j_driver.execute_query(GET_DATA_QUERY, database_=RDFLIB_DB)
    assert len(records) == 1


def test_initialize_with_both_credentials_and_driver_should_fail(neo4j_connection_parameters, neo4j_driver):

    config = Neo4jStoreConfig(auth_data=neo4j_connection_parameters,
                              custom_prefixes={},
                              custom_mappings=[],
                              multival_props_names=[],
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.MAP,
                              batching=False)

    with pytest.raises(Exception):
        Graph(store=Neo4jStore(config=config, neo4j_driver=neo4j_driver))
