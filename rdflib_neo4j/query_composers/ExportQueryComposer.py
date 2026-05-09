class ExportQueryComposer:
    """Generates Cypher queries for reading RDF triples from Neo4j."""

    @staticmethod
    def all_nodes_query(graph_uri: str = None) -> str:
        """Fetch all Resource nodes with their props and labels.

        Args:
            graph_uri: When provided, restrict results to nodes whose ``graphUri``
                property matches (named-graph filter).
        """
        if graph_uri is not None:
            return (
                "MATCH (n:Resource {graphUri: $graphUri}) "
                "RETURN n.uri AS uri, "
                "       [l IN labels(n) WHERE l <> 'Resource'] AS extra_labels, "
                "       properties(n) AS props"
            )
        return (
            "MATCH (n:Resource) "
            "RETURN n.uri AS uri, "
            "       [l IN labels(n) WHERE l <> 'Resource'] AS extra_labels, "
            "       properties(n) AS props"
        )

    @staticmethod
    def node_by_uri_query(graph_uri: str = None) -> str:
        """Fetch a specific Resource node.

        Args:
            graph_uri: When provided, also filter by ``graphUri`` property.
        """
        if graph_uri is not None:
            return (
                "MATCH (n:Resource {uri: $uri, graphUri: $graphUri}) "
                "RETURN n.uri AS uri, "
                "       [l IN labels(n) WHERE l <> 'Resource'] AS extra_labels, "
                "       properties(n) AS props"
            )
        return (
            "MATCH (n:Resource {uri: $uri}) "
            "RETURN n.uri AS uri, "
            "       [l IN labels(n) WHERE l <> 'Resource'] AS extra_labels, "
            "       properties(n) AS props"
        )

    @staticmethod
    def all_relationships_query(graph_uri: str = None) -> str:
        """Fetch all relationships between Resource nodes.

        Args:
            graph_uri: When provided, restrict to relationships whose ``graphUri``
                property matches.
        """
        if graph_uri is not None:
            return (
                "MATCH (a:Resource)-[r]->(b:Resource) "
                "WHERE r.graphUri = $graphUri "
                "RETURN a.uri AS from_uri, type(r) AS rel_type, b.uri AS to_uri"
            )
        return (
            "MATCH (a:Resource)-[r]->(b:Resource) "
            "RETURN a.uri AS from_uri, type(r) AS rel_type, b.uri AS to_uri"
        )

    @staticmethod
    def relationships_from_uri_query(graph_uri: str = None) -> str:
        """Fetch relationships from a specific subject.

        Args:
            graph_uri: When provided, also filter by relationship ``graphUri`` property.
        """
        if graph_uri is not None:
            return (
                "MATCH (a:Resource {uri: $uri})-[r]->(b:Resource) "
                "WHERE r.graphUri = $graphUri "
                "RETURN a.uri AS from_uri, type(r) AS rel_type, b.uri AS to_uri"
            )
        return (
            "MATCH (a:Resource {uri: $uri})-[r]->(b:Resource) "
            "RETURN a.uri AS from_uri, type(r) AS rel_type, b.uri AS to_uri"
        )

    @staticmethod
    def count_query() -> str:
        """
        Approximate triple count: sum of property triples (props - uri), label triples,
        and relationship triples.

        Note: This is an approximation that sums per-node costs with a relationship count.
        """
        return (
            "MATCH (n:Resource) "
            "WITH sum(size(keys(n)) - 1 + size([l IN labels(n) WHERE l <> 'Resource'])) AS node_triples "
            "MATCH ()-[r]->() "
            "RETURN node_triples + count(r) AS cnt"
        )
