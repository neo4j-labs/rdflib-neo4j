from collections import defaultdict
from decimal import Decimal
from typing import Dict, Set, List
from rdflib import Literal, URIRef, RDF, Namespace
from rdflib.term import Node

from rdflib_neo4j.utils import handle_vocab_uri, HANDLE_MULTIVAL_STRATEGY, HANDLE_VOCAB_URI_STRATEGY


class Neo4jTriple:
    uri: Node
    labels: Set[str]
    props: Dict[str, Literal]
    multi_props: Dict[str, List[Literal]]
    relationships: Dict[str, Set[URIRef]]

    """
    Represents a triple extracted from RDF data for use in a Neo4j database.
    """

    def __init__(self, uri: Node,
                 handle_vocab_uri_strategy: HANDLE_VOCAB_URI_STRATEGY,
                 handle_multival_strategy: HANDLE_MULTIVAL_STRATEGY,
                 multival_props_names: List[str],
                 prefixes: Dict[str, str]):
        """
        Constructor for Neo4jTriple.

        Args:
            uri (Node): The subject URI of the triple.
            handle_vocab_uri_strategy: The strategy to handle vocabulary URIs.
            handle_multival_strategy: The strategy to handle multiple values.
            multival_props_names: A list containing URIs to be treated as multivalued.
            prefixes: A dictionary of namespace prefixes used for vocabulary URI handling.
        """
        self.uri = uri
        self.labels = set()
        self.props = {}
        self.multi_props = defaultdict(list)
        self.relationships = defaultdict(set)
        self.handle_vocab_uri_strategy = handle_vocab_uri_strategy
        self.handle_multival_strategy = handle_multival_strategy
        self.multival_props_names = multival_props_names
        self.prefixes = prefixes

    def add_label(self, label: str):
        """
        Adds a label to the `labels` set of the Neo4jTriple object.

        Args:
            label (str): The label to add.
        """
        self.labels.add(label)

    def add_prop(self, prop_name: str, value: Literal, multi=False):
        """
        Adds a property to the `props` dictionary of the Neo4jTriple object.

        Args:
            prop_name (str): The name of the property.
            value (Literal): The value of the property.
        """
        if multi:
            self.multi_props[prop_name].append(value)
        else:
            self.props[prop_name] = value

    def add_rel(self, rel_type, to_resource):
        """
        Adds a relationship to the `relationships` dictionary of the Neo4jTriple object.

        Args:
            rel_type: The type of the relationship.
            to_resource: The resource to which the relationship points.
        """
        self.relationships[rel_type].add(to_resource)

    def extract_label_key(self):
        """
        Extracts a label key from the `labels` set of the Neo4jTriple object.

        Returns:
            str: The extracted label key.
        """
        res = ",".join(list(self.labels))
        return res if res else "Resource"

    def extract_labels(self):
        """
        Extracts the labels from the `labels` set of the Neo4jTriple object.

        Returns:
            list: The extracted labels.
        """
        return list(self.labels)

    def extract_params(self):
        """
        Extracts the properties from the `props` dictionary of the Neo4jTriple object.

        Returns:
            dict: The extracted properties.
        """
        res = {key: value for key, value in self.props.items()}
        res["uri"] = self.uri
        res.update(self.multi_props)
        return res

    def extract_props_names(self, multi=False):
        """
        Extracts property names from the Neo4jTriple object.

        Args:
            multi (bool): If True, extract property names from multi_props, otherwise from props.

        Returns:
            set: A set containing the extracted property names.
        """
        if not multi:
            return set(key for key in self.props)
        return set(key for key in self.multi_props)

    def extract_rels(self):
        """
        Extracts the relationships from the `relationships` dictionary of the Neo4jTriple object.

        Returns:
            dict: The extracted relationships.
        """
        return {key: list(value) for key, value in self.relationships.items()}

    def handle_vocab_uri(self, mappings, predicate):
        """
        Handles a vocabulary URI according to the specified strategy, defined using the HANDLE_VOCAB_URI_STRATEGY Enum.

        Args:
            mappings (Dict[str, str]): A dictionary mapping URIs to their mapped values.
            predicate (URIRef): The predicate URI to be handled.

        Returns:
            str: The handled predicate URI based on the specified strategy.
        """
        return handle_vocab_uri(mappings, predicate, self.prefixes, self.handle_vocab_uri_strategy)

    def parse_triple(self, triple, mappings):
        """
        Parses a triple and updates the Neo4jTriple object accordingly.

        Args:
            triple: The triple to parse.
            mappings: A dictionary of mappings for predicate URIs.
        """
        (subject, predicate, object) = triple

        # Getting a property
        if isinstance(object, Literal):
            # Neo4j Python driver does not support decimal params
            value = float(object.toPython()) if type(object.toPython()) == Decimal else object.toPython()
            prop_name = self.handle_vocab_uri(mappings, predicate)

            # If at least a name is defined and the predicate is one of the properties defined by the user
            if self.handle_multival_strategy == HANDLE_MULTIVAL_STRATEGY.ARRAY and \
                    str(predicate) in self.multival_props_names:
                self.add_prop(prop_name, value, True)
            # If the user doesn't define any predicate to manage as an array, then everything is an array
            elif self.handle_multival_strategy == HANDLE_MULTIVAL_STRATEGY.ARRAY and not self.multival_props_names:
                self.add_prop(prop_name, value, True)
            else:
                self.add_prop(prop_name, value)

        # Getting a label
        elif predicate == RDF.type:
            self.add_label(self.handle_vocab_uri(mappings, object))

        # Getting its relationships
        else:
            rel_type = self.handle_vocab_uri(mappings, predicate)
            self.add_rel(rel_type, object)
