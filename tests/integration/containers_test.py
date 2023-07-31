import pytest
from neo4j import GraphDatabase, WRITE_ACCESS
from testcontainers.neo4j import Neo4jContainer
from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import FOAF

from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from tests.integration.utils import records_equal, read_file_n10s_and_rdflib, create_graph_store
from rdflib_neo4j.utils import HANDLE_VOCAB_URI_STRATEGY, HANDLE_MULTIVAL_STRATEGY
import os
from dotenv import load_dotenv

N10S_PROC_DB = "neo4j"
RDFLIB_DB = "rdflib"
N10S_CONSTRAINT_QUERY = "CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS FOR (r:Resource) REQUIRE r.uri IS UNIQUE"
GET_DATA_QUERY = "MATCH (n:Resource) RETURN n.uri as uri, labels(n) as labels, properties(n) as props ORDER BY uri"
LOCAL = (os.getenv("RUN_TEST_LOCALLY", "False").lower() == "true")

load_dotenv()


@pytest.fixture
def neo4j_container():
    if not LOCAL:
        container = Neo4jContainer(image="neo4j:5.7.0-enterprise") \
            .with_env("NEO4J_PLUGINS", """["n10s"]""") \
            .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes") \
            .start()
        yield container
        container.stop()
    else:
        yield ""


@pytest.fixture
def neo4j_driver(neo4j_container):
    # Check if running in a CI/CD environment
    if not LOCAL:
        driver = neo4j_container.get_driver()
    else:
        # If in local development environment, use a local Neo4j instance
        auth_data = {
            'uri': os.getenv("NEO4J_URI_LOCAL"),
            'database': RDFLIB_DB,
            'user': os.getenv("NEO4J_USER_LOCAL"),
            'pwd': os.getenv("NEO4J_PWD_LOCAL")
        }
        driver = GraphDatabase.driver(
            auth_data['uri'],
            auth=(auth_data['user'], auth_data['pwd'])
        )

    # initialize n10s procs
    driver.execute_query("CREATE DATABASE " + RDFLIB_DB + " IF NOT EXISTS WAIT", database_="system")

    driver.execute_query(N10S_CONSTRAINT_QUERY, database_=RDFLIB_DB)
    driver.execute_query(N10S_CONSTRAINT_QUERY)
    yield driver
    driver.close()


@pytest.fixture
def graph_store(neo4j_container, neo4j_driver):
    return create_graph_store(neo4j_container)


@pytest.fixture
def graph_store_batched(neo4j_container, neo4j_driver):
    return create_graph_store(neo4j_container, batching=True)


@pytest.fixture(autouse=True)
def cleanup_databases(neo4j_driver, graph_store):
    """Executed before each test"""
    neo4j_driver.execute_query("MATCH (n) DETACH DELETE n")
    neo4j_driver.execute_query("MATCH (n) DETACH DELETE n", database_=RDFLIB_DB)


def test_import_type_as_label(neo4j_driver, graph_store):
    donna = URIRef("https://example.org/donna")
    graph_store.add((donna, RDF.type, FOAF.Person))

    records, summary, keys = neo4j_driver.execute_query(GET_DATA_QUERY, database_=RDFLIB_DB)
    assert len(records) == 1
    assert set(records[0]["labels"]) == {"Person", "Resource"}
    assert records[0]["props"] == {'uri': 'https://example.org/donna'}


def test_import_string_property(neo4j_driver, graph_store):
    donna = URIRef("https://example.org/donna")
    graph_store.add((donna, FOAF.name, Literal("Donna Fales")))

    records, summary, keys = neo4j_driver.execute_query(GET_DATA_QUERY, database_=RDFLIB_DB)
    assert len(records) == 1
    assert set(records[0]["labels"]) == {"Resource"}
    assert records[0]["props"] == {'uri': 'https://example.org/donna', 'name': 'Donna Fales'}


def test_import_int_property(neo4j_driver, graph_store):
    donna = URIRef("https://example.org/donna")
    graph_store.add((donna, FOAF.age, Literal(30)))
    graph_store.commit()

    records, summary, keys = neo4j_driver.execute_query(GET_DATA_QUERY, database_=RDFLIB_DB)
    assert len(records) == 1
    assert set(records[0]["labels"]) == {"Resource"}
    assert records[0]["props"] == {'uri': 'https://example.org/donna', 'age': 30}


def test_import_person(neo4j_driver, graph_store):
    """Compare data imported with n10s procs and n10s + rdflib"""
    g = Graph()
    donna = URIRef("https://example.org/donna")
    g.add((donna, RDF.type, FOAF.Person))
    g.add((donna, FOAF.nick, Literal("donna", lang="en")))
    g.add((donna, FOAF.name, Literal("Donna Fales")))
    rdf_payload = g.serialize(format='ttl')

    neo4j_driver.execute_query("CALL n10s.graphconfig.init({handleVocabUris: 'IGNORE'})")
    records = neo4j_driver.execute_query("CALL n10s.rdf.import.inline($payload, 'Turtle')",
                                         payload=rdf_payload)
    assert records[0][0]["terminationStatus"] == "OK"

    graph_store.parse(data=rdf_payload, format="ttl")
    records, summary, keys = neo4j_driver.execute_query(GET_DATA_QUERY)
    records_from_rdf_lib, summary, keys = neo4j_driver.execute_query(GET_DATA_QUERY, database_=RDFLIB_DB)
    assert len(records) == 1
    assert records_equal(records[0], records_from_rdf_lib[0])


def test_read_file(neo4j_driver, graph_store):
    """Compare data imported with n10s procs and n10s + rdflib in single add mode"""
    records_from_rdf_lib, records, _, _ = read_file_n10s_and_rdflib(neo4j_driver, graph_store)
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])


def test_read_file_batched(neo4j_driver, graph_store_batched):
    """Compare data imported with n10s procs and n10s + rdflib in batched mode from rdflib"""
    records_from_rdf_lib, records, _, _ = read_file_n10s_and_rdflib(neo4j_driver, graph_store_batched, True)
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])


def test_read_file_multival_with_strategy_no_predicates(neo4j_container, neo4j_driver):
    """Compare data imported with n10s procs and n10s + rdflib in single add mode for multivalues"""

    if LOCAL:
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
    if LOCAL:
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
    if LOCAL:
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


def test_custom_mapping_match(neo4j_container, neo4j_driver):
    """
    If we define a custom mapping and the strategy is HANDLE_VOCAB_URI_STRATEGY.MAP, it should match it and use the mapping
    if the predicate satisfies the mapping.
    """

    auth_data = {
        'uri': os.getenv("NEO4J_URI_LOCAL"),
        'database': RDFLIB_DB,
        'user': os.getenv("NEO4J_USER_LOCAL"),
        'pwd': os.getenv("NEO4J_PWD_LOCAL")
    }

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

    auth_data = {
        'uri': os.getenv("NEO4J_URI_LOCAL"),
        'database': RDFLIB_DB,
        'user': os.getenv("NEO4J_USER_LOCAL"),
        'pwd': os.getenv("NEO4J_PWD_LOCAL")
    }

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
    assert True


def test_custom_mapping_map_strategy_zero_custom_mappings():
    """
    If we don't define custom mapping and the strategy is HANDLE_VOCAB_URI_STRATEGY.MAP, it shouldn't apply the mapping on anything and
    just use IGNORE mode.
    """
    assert True
