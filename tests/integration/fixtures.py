import pytest
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer
from tests.integration.utils import create_graph_store
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
