from typing import Set, List, Dict

from rdflib_neo4j.config.const import HANDLE_MULTIVAL_STRATEGY


def prop_query_append(prop):
    return  f"""n.`{prop}` = CASE WHEN COALESCE(param["{prop}"], NULL) IS NULL THEN n.`{prop}` ELSE REDUCE(i=COALESCE(n.`{prop}`,[]), val IN param["{prop}"] | CASE WHEN val IN i THEN i ELSE i+val END) END """



def prop_query_single(prop):
    return f"""n.`{prop}` = COALESCE(param["{prop}"], n.`{prop}`)"""


class NodeQueryComposer:
    labels: Set[str] = set()
    props: Set[str] = set()
    query_params: List[Dict]

    def __init__(self, labels, handle_multival_strategy, multival_props_predicates):
        """
        Initializes a NodeQueryComposer object.

        Args:
            labels: The labels to assign to the nodes.
        """
        self.labels = labels
        self.props = set()
        self.multi_props = set()
        self.query_params = []
        self.handle_multival_strategy = handle_multival_strategy
        self.multival_props_predicates = multival_props_predicates

    def add_props(self, props, multi=False):
        """
        Adds properties to the set of properties.

        Args:
            props: The properties to add.
            multi: If the property should be treated as multivalued. Default: False
        """
        if not multi:
            self.props.update(props)
        else:
            self.multi_props.update(props)

    def add_query_param(self, param):
        """
        Adds a query parameter.

        Args:
            param: The query parameter to add.
        """
        self.query_params.append(param)

    def write_query(self):
        """
        Writes the Neo4j query for creating nodes with labels and properties.

        Returns:
            str: The Neo4j query.
        """

        q = f''' UNWIND $params as param MERGE (n:Resource{{ uri : param["uri"] }}) '''
        if self.labels:
            q += f'''SET {', '.join([f"""n:`{label}`""" for label in self.labels])} '''
        if self.props or self.multi_props:
            q += self.write_prop_query()
        return q

    def write_prop_query(self):
        """
        Generates a Cypher query to handle property updates based on the chosen strategy.

        Returns:
        The generated Cypher query.
        """
        if self.handle_multival_strategy == HANDLE_MULTIVAL_STRATEGY.ARRAY:
            # Strategy to treat multiple values as an array
            if self.multival_props_predicates:
                # If there are properties treated as multivalued, use SET query for each property
                # and SET query for each property to append to the array
                q = f'''SET {', '.join([prop_query_single(prop) for prop in self.props])}''' if self.props else ''
                if self.multi_props:
                    q += f''' SET {', '.join([prop_query_append(prop) for prop in self.multi_props])}'''
            else:
                # If all properties are treated as multivalued, use SET query to append to the array
                q = f'''SET {', '.join([prop_query_append(prop) for prop in self.multi_props])}'''
        else:
            # Strategy to overwrite multiple values
            # Use SET query for each property
            q = f'''SET {', '.join([prop_query_single(prop) for prop in self.props])}'''
        return q

    def is_redundant(self):
        """
        Checks if the NodeQueryComposer is redundant, i.e., if it has no properties,labels and query parameters.

        Returns:
            bool: True if redundant, False otherwise.
        """
        return not self.props and not self.labels and not self.query_params

    def empty_query_params(self):
        """
        Empties the query parameters list.
        """
        del self.query_params
        self.query_params = []

    def __eq__(self, other):
        """
        Compares two NodeQueryComposer objects for equality.

        Args:
            other: The other NodeQueryComposer object to compare.

        Returns:
            bool: True if equal, False otherwise.
        """
        return self.labels == other.labels and self.props == other.props
