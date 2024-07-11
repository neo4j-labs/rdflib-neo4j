from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import FOAF
from test.integration.constants import GET_DATA_QUERY, RDFLIB_DB
from test.integration.utils import records_equal, read_file_n10s_and_rdflib
import pytest
from test.integration.fixtures import neo4j_container, neo4j_driver, graph_store, graph_store_batched, \
    cleanup_databases, neo4j_connection_parameters


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
