# This is a test implemetation of the custom mapping with the test data from neosematics getting started page.
# It Works! However the lebsl is only Resource on all of them and not the labels as in n10s from the data
from rdflib import Namespace, URIRef, Graph
from dotenv import load_dotenv
from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.utils import HANDLE_VOCAB_URI_STRATEGY, HANDLE_MULTIVAL_STRATEGY
import os

load_dotenv()


def define_config():
    auth_data = {'uri': f'{os.getenv("NEO4J_URI")}',
                 'database': 'neo4j',
                 'user': 'neo4j',
                 'pwd': f'{os.getenv("NEO4J_PWD")}'}

    # Define your prefixes
    prefixes = {
        'neo4ind': Namespace('http://neo4j.org/ind#'),
        'neo4voc': Namespace('http://neo4j.org/vocab/sw#'),
        'nsmntx': Namespace('http://neo4j.org/vocab/NSMNTX#'),
        'apoc': Namespace('http://neo4j.org/vocab/APOC#'),
        'graphql': Namespace('http://neo4j.org/vocab/GraphQL#'),
        "ns4": Namespace('http://www.w3.org/2000/01/rdf-schema#'),
        "ns1": Namespace('http://dbpedia.org/ontology/')
    }

    # Define your custom mappings
    custom_mappings = [
        ("ns4", 'label', "name"),
        ("ns1", 'team', "ohno"),
        ("neo4voc", "GraphPlatform", "TigerGraph"),
        ("neo4voc", 'name', "blabalbal"),
        ("neo4voc",'runsOn',"RUNS_ON")
    ]

    multival_props_names = [("neo4voc", "author")]

    config = Neo4jStoreConfig(auth_data=auth_data,
                              custom_prefixes=prefixes,
                              custom_mappings=custom_mappings,
                              handle_multival_strategy=HANDLE_MULTIVAL_STRATEGY.OVERWRITE,
                              multival_props_names=multival_props_names
                              )

    return config


TEST_FILE = "https://github.com/jbarrasa/gc-2022/raw/main/search/onto/concept-scheme-skos.ttl"


def main():
    config = define_config()
    # create a neo4j backed Graph
    g = Graph(store=Neo4jStore(config=config))
    g.parse("test_data/n10s_example.ttl")
    g.store.close()


if __name__ == "__main__":
    main()
