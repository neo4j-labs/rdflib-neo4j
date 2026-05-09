from typing import Set, List, Dict


class RelationshipQueryComposer:
    rel_type: str
    props: Set[str] = set()
    query_params: List[Dict]

    def __init__(self, rel_type, graph_uri_aware: bool = False):
        """
        Initializes a RelationshipQueryComposer object.

        Args:
            rel_type (str): The type of the relationship.
            graph_uri_aware: When True the MERGE for both endpoint nodes matches on
                both ``uri`` and ``graphUri``.  The relationship itself also receives a
                ``graphUri`` property so that it can be filtered by named graph on the
                read path.
        """
        self.rel_type = rel_type
        self.props = set()
        self.query_params = []
        self.graph_uri_aware = graph_uri_aware

    def add_props(self, props):
        """
        Adds properties to the set of properties.

        Args:
            props: The properties to add.
        """
        self.props.update(props)
        raise NotImplementedError("TO WORK ON THIS, WE NEED TEST DATA")

    def add_query_param(self, from_node, to_node, graph_uri=None):
        """
        Adds a query parameter consisting of 'from' (The URI of the node at the start of the relationship)
            and 'to' (The URI of the node at the end of the relationship).

        Args:
            from_node: The 'from' node (The URI of the node at the start of the relationship).
            to_node: The 'to' node (The URI of the node at the end of the relationship).
            graph_uri: Optional named-graph URI string; stored on the relationship as
                ``graphUri`` when ``graph_uri_aware`` is True.
        """
        param = {"from": from_node, "to": to_node}
        if graph_uri is not None:
            param["graphUri"] = graph_uri
        self.query_params.append(param)

    def write_query(self):
        """
        Writes the Neo4j query for creating relationships with properties.

        When ``graph_uri_aware`` is True, each endpoint node is looked up (or
        created) by both ``uri`` and ``graphUri``, and the relationship receives a
        ``graphUri`` property so the read path can filter by named graph.

        Returns:
            str: The Neo4j query.
        """
        if self.graph_uri_aware:
            q = (
                ' UNWIND $params as param'
                ' MERGE (from:Resource{ uri : param["from"], graphUri : param["graphUri"] })'
                ' MERGE (to:Resource{ uri : param["to"], graphUri : param["graphUri"] })'
                ' '
            )
            q += f' MERGE (from)-[r:`{self.rel_type}`]->(to)'
            q += ' SET r.graphUri = param["graphUri"]'
        else:
            q = ''' UNWIND $params as param
                 MERGE (from:Resource{ uri : param["from"] })
                 MERGE (to:Resource{ uri : param["to"] })
             '''
            q += f''' MERGE (from)-[r:`{self.rel_type}`]->(to)'''
        if self.props:
            raise NotImplementedError
            # q += f'''SET {', '.join([f"""r.`{prop}` = coalesce(param["{prop}"],null)""" for prop in self.props])}'''
        return q

    def is_redundant(self):
        """
        Checks if the RelationshipQueryComposer is redundant, i.e., if it has no query parameters.

        Returns:
            bool: True if redundant, False otherwise.
        """
        return not self.query_params

    def empty_query_params(self):
        """
        Empties the query parameters list.
        """
        del self.query_params
        self.query_params = []

    def __eq__(self, other):
        """
        Compares two RelationshipQueryComposer objects for equality.

        Args:
            other: The other RelationshipQueryComposer object to compare.

        Returns:
            bool: True if equal, False otherwise.
        """
        return self.rel_type == other.rel_type and self.props == other.props
