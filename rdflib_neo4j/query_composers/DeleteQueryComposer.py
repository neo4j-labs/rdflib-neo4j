class DeleteQueryComposer:
    """Generates Cypher queries for retracting RDF triples from Neo4j."""

    @staticmethod
    def remove_property(subject_uri: str, prop_name: str) -> tuple[str, dict]:
        """Remove a single property from a node."""
        query = (
            "MATCH (n:Resource {uri: $uri}) "
            f"REMOVE n.`{prop_name}`"
        )
        return query, {"uri": str(subject_uri)}

    @staticmethod
    def remove_label(subject_uri: str, label: str) -> tuple[str, dict]:
        """Remove a label (rdf:type retraction) from a node."""
        query = (
            "MATCH (n:Resource {uri: $uri}) "
            f"REMOVE n:`{label}`"
        )
        return query, {"uri": str(subject_uri)}

    @staticmethod
    def remove_relationship(subject_uri: str, rel_type: str, object_uri: str) -> tuple[str, dict]:
        """Remove a specific relationship between two nodes."""
        query = (
            "MATCH (a:Resource {uri: $from_uri})"
            f"-[r:`{rel_type}`]->"
            "(b:Resource {uri: $to_uri}) "
            "DELETE r"
        )
        return query, {"from_uri": str(subject_uri), "to_uri": str(object_uri)}

    @staticmethod
    def remove_all_properties(subject_uri: str) -> tuple[str, dict]:
        """Remove all non-uri properties from a node (wildcard predicate)."""
        query = (
            "MATCH (n:Resource {uri: $uri}) "
            "SET n = {uri: n.uri}"  # keep only uri
        )
        return query, {"uri": str(subject_uri)}

    @staticmethod
    def remove_all_relationships(subject_uri: str) -> tuple[str, dict]:
        """Remove all outgoing relationships from a node."""
        query = (
            "MATCH (n:Resource {uri: $uri})-[r]->() "
            "DELETE r"
        )
        return query, {"uri": str(subject_uri)}

    @staticmethod
    def remove_all_outgoing_of_type(subject_uri: str, rel_type: str) -> tuple[str, dict]:
        """Remove all relationships of a given type from a subject."""
        query = (
            "MATCH (n:Resource {uri: $uri})"
            f"-[r:`{rel_type}`]->()"
            " DELETE r"
        )
        return query, {"uri": str(subject_uri)}

    @staticmethod
    def remove_node_if_empty(subject_uri: str) -> tuple[str, dict]:
        """Delete a node only if it has no remaining properties (except uri) and no relationships."""
        query = (
            "MATCH (n:Resource {uri: $uri}) "
            "WHERE size(keys(n)) <= 1 AND NOT (n)--() "
            "DELETE n"
        )
        return query, {"uri": str(subject_uri)}
