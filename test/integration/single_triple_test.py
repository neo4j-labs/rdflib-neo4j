from rdflib import Literal, RDF, URIRef
from rdflib.namespace import FOAF
from test.integration.constants import GET_DATA_QUERY, RDFLIB_DB
import pytest
from test.integration.fixtures import neo4j_container, neo4j_driver, graph_store, graph_store_batched, \
    cleanup_databases


def test_import_type_as_label(neo4j_driver, graph_store):
    donna = URIRef("https://example.org/donna")
    graph_store.add((donna, RDF.type, FOAF.Person))
    graph_store.commit()
    records, summary, keys = neo4j_driver.execute_query(GET_DATA_QUERY, database_=RDFLIB_DB)
    assert len(records) == 1
    assert set(records[0]["labels"]) == {"Person", "Resource"}
    assert records[0]["props"] == {'uri': 'https://example.org/donna'}


def test_import_string_property(neo4j_driver, graph_store):
    donna = URIRef("https://example.org/donna")
    graph_store.add((donna, FOAF.name, Literal("Donna Fales")))
    graph_store.commit()
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