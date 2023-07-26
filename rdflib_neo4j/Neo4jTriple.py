from collections import defaultdict
from decimal import Decimal
from typing import Dict, Set, List
from rdflib import Literal, URIRef, RDF
from rdflib.term import Node

from rdflib_neo4j.utils import handle_vocab_uri, HANDLE_MULTIVAL_STRATEGY


class Neo4jTriple:
    uri: Node
    labels: Set[str]
    props: Dict[str, Literal]
    multi_props: Dict[str, List[Literal]]
    relationships: Dict[str, Set[URIRef]]

    def __init__(self, uri: Node, handle_vocab_uri_strategy, handle_multival_strategy, multival_props_names, prefixes):
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
        return handle_vocab_uri(mappings, predicate,self.prefixes, self.handle_vocab_uri_strategy)

    def parse_triple(self, triple, mappings):
        """
        Parses a triple and updates the Neo4jTriple object accordingly.

        Args:
            triple: The triple to parse.
            mappings: A dictionary of mappings for predicate URIs.
        """
        (subject, predicate, object) = triple

        if isinstance(object, Literal):
            # python driver does not support decimal params
            value = float(object.toPython()) if type(object.toPython()) == Decimal else object.toPython()
            prop_name = self.handle_vocab_uri(mappings, predicate)

            if self.handle_multival_strategy == HANDLE_MULTIVAL_STRATEGY.ARRAY and \
                    str(predicate) in self.multival_props_names:
                self.add_prop(prop_name, value, True)
            elif self.handle_multival_strategy == HANDLE_MULTIVAL_STRATEGY.ARRAY and not self.multival_props_names:
                self.add_prop(prop_name, value, True)
            else:
                self.add_prop(prop_name, value)

        elif predicate == RDF.type:
            self.add_label(self.handle_vocab_uri(mappings, object))

        else:
            rel_type = self.handle_vocab_uri(mappings, predicate)
            self.add_rel(rel_type, object)
