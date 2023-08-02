import pytest
from rdflib import Literal, RDF, URIRef
from rdflib.namespace import FOAF

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