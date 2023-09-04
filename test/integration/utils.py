from neo4j import Record
from rdflib import Graph
from testcontainers.neo4j import Neo4jContainer

from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY
import os

from test.integration.constants import RDFLIB_DB, GET_NODES_PROPS_QUERY, GET_RELS_QUERY


def records_equal(record1: Record, record2: Record, rels=False):
    """
    Used because a test is failing because the sorting of the labels is different:
    Full diff:
    - <Record uri='http://neo4j.org/ind#neo4j355' labels=['Resource', 'AwesomePlatform', 'GraphPlatform'] props={'name': 'neo4j', 'version': '3.5.5', 'uri': 'http://neo4j.org/ind#neo4j355'}>
    ?                                                                                  -----------------
    + <Record uri='http://neo4j.org/ind#neo4j355' labels=['Resource', 'GraphPlatform', 'AwesomePlatform'] props={'name': 'neo4j', 'version': '3.5.5', 'uri': 'http://neo4j.org/ind#neo4j355'}>
    ?                                                                 +++++++++++++++++
    """
    if not rels:
        for key in record1.keys():
            if key == 'props':
                for prop_name in record1[key]:
                    if not sorted(record1[key][prop_name]) == sorted(record2[key][prop_name]):
                        return False
            elif key == 'labels':
                if not sorted(record1[key]) == sorted(record2[key]):
                    return False
            else:
                if not record1[key] == record2[key]:
                    return False
    else:
        for key in record1.keys():
            if record1[key] != record2[key]:
                return False
    return True


def read_file_n10s_and_rdflib(neo4j_driver, graph_store, batching=False, n10s_params=None, n10s_mappings=None,
                              get_rels=False):
    """Compare data imported with n10s procs and n10s + rdflib"""
    if n10s_mappings is None:
        n10s_mappings = []
    if n10s_params is None:
        n10s_params = {"handleVocabUris": "IGNORE"}

    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../test_files/n10s_example.ttl"))
    rdf_payload = g.serialize(format='ttl')

    neo4j_driver.execute_query("CALL n10s.graphconfig.init($params)", params=n10s_params)
    for (prefix, mapping) in n10s_mappings:
        neo4j_driver.execute_query(prefix)
        neo4j_driver.execute_query(mapping)

    records = neo4j_driver.execute_query("CALL n10s.rdf.import.inline($payload, 'Turtle')",
                                         payload=rdf_payload)
    assert records[0][0]["terminationStatus"] == "OK"

    graph_store.parse(data=rdf_payload, format="ttl")
    # When batching we need to close the store to check that all the data is flushed
    if batching:
        graph_store.close(True)
    records, summary, keys = neo4j_driver.execute_query(GET_NODES_PROPS_QUERY)
    records_from_rdf_lib, summary, keys = neo4j_driver.execute_query(GET_NODES_PROPS_QUERY, database_=RDFLIB_DB)
    n10s_rels, rdflib_rels = None, None
    if get_rels:
        n10s_rels, summary, keys = neo4j_driver.execute_query(GET_RELS_QUERY)
        rdflib_rels, summary, keys = neo4j_driver.execute_query(GET_RELS_QUERY, database_=RDFLIB_DB)
    return records_from_rdf_lib, records, rdflib_rels, n10s_rels


def create_graph_store(neo4j_container, batching=False):
    if neo4j_container:
        auth_data = {'uri': neo4j_container.get_connection_url(),
                     'database': RDFLIB_DB,
                     'user': "neo4j",
                     'pwd': Neo4jContainer.NEO4J_ADMIN_PASSWORD}
        return config_graph_store(auth_data, batching)
    else:
        auth_data = {
            'uri': os.getenv("NEO4J_URI_LOCAL"),
            'database': RDFLIB_DB,
            'user': os.getenv("NEO4J_USER_LOCAL"),
            'pwd': os.getenv("NEO4J_PWD_LOCAL")
        }

        return config_graph_store(auth_data, batching)


def config_graph_store(auth_data, batching=False):
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
                              batching=batching)

    g = Graph(store=Neo4jStore(config=config))
    return g


def get_credentials(local, neo4j_container):
    if local:
        auth_data = {
            'uri': os.getenv("NEO4J_URI_LOCAL"),
            'database': RDFLIB_DB,
            'user': os.getenv("NEO4J_USER_LOCAL"),
            'pwd': os.getenv("NEO4J_PWD_LOCAL")
        }
    else:
        auth_data = {'uri': neo4j_container.get_connection_url(),
                     'database': RDFLIB_DB,
                     'user': "neo4j",
                     'pwd': Neo4jContainer.NEO4J_ADMIN_PASSWORD}
    return auth_data
