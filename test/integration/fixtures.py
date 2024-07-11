import pytest
from neo4j import GraphDatabase
from rdflib import Graph
from testcontainers.neo4j import Neo4jContainer

from rdflib_neo4j import HANDLE_VOCAB_URI_STRATEGY, Neo4jStoreConfig, Neo4jStore
from test.integration.constants import LOCAL, N10S_CONSTRAINT_QUERY, RDFLIB_DB
import os


@pytest.fixture(scope="session")
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
def graph_store(neo4j_connection_parameters):
    return config_graph_store(neo4j_connection_parameters)


@pytest.fixture
def graph_store_batched(neo4j_connection_parameters):
    return config_graph_store(neo4j_connection_parameters, True)


def config_graph_store(auth_data, batching=False):

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes={},
                              custom_mappings=[],
                              multival_props_names=[],
                              handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
                              batching=batching)

    g = Graph(store=Neo4jStore(config=config))
    return g


@pytest.fixture
def neo4j_connection_parameters(neo4j_container):
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
    return auth_data


@pytest.fixture(autouse=True)
def cleanup_databases(neo4j_driver, graph_store):
    """Executed before each test"""
    neo4j_driver.execute_query("MATCH (n) DETACH DELETE n")
    neo4j_driver.execute_query("MATCH (n) DETACH DELETE n", database_=RDFLIB_DB)
