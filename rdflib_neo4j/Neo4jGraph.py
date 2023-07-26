from rdflib import Graph

from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig


class Neo4jGraph:
    """
    Let's see if we should keep this class. Ideally a user should just change the store in their code instead of using a new Graph.
    """
    g: Graph

    def __init__(self, config: Neo4jStoreConfig):
        """
        Initializes a Neo4jGraph object.

        Args:
            config: The configuration for the Neo4j database.
        """
        self.g = Graph(store=Neo4jStore(config=config))

    def import_file(self, file_name, file_format="ttl"):
        """
        Imports an RDF file into the Neo4jGraph object.

        Args:
            file_name: The name of the RDF file.
            file_format: The format of the RDF file (default: "ttl").
        """
        self.g.parse(file_name, format=file_format)
        self.g.store.close()

    def add(self, triple):
        """
        Adds a triple to the Neo4jGraph object.

        Args:
            triple: The triple to add.
        """
        self.g.add(triple)
