from typing import List, Tuple
from rdflib import Namespace, URIRef
from rdflib_neo4j.config.const import (
    DEFAULT_PREFIXES,
    PrefixNotFoundException,
    NEO4J_AUTH_REQUIRED_FIELDS,
    WrongAuthenticationException
)
from rdflib_neo4j.utils import HANDLE_VOCAB_URI_STRATEGY, HANDLE_MULTIVAL_STRATEGY


def check_auth_data(auth):
    """
    Checks if the required authentication fields are present.

    Parameters:
    - auth: A dictionary containing authentication data.

    Raises:
    - WrongAuthenticationException: If any of the required authentication fields is missing.
    """
    if auth is None:
        raise Exception(
            f"Please define the authentication dict. These are the required keys: {NEO4J_AUTH_REQUIRED_FIELDS}")
    for param_name in NEO4J_AUTH_REQUIRED_FIELDS:
        if param_name not in auth:
            raise WrongAuthenticationException(param_name=param_name)


class Neo4jStoreConfig:
    """
    Configuration class for Neo4j RDF store.

    Parameters:

    - auth_data: A dictionary containing authentication data (default: None).

    - custom_mappings: A list of tuples containing custom mappings for prefixes in the form (prefix, objectToReplace, newObject) (default: empty list).

    - custom_prefixes: A dictionary containing custom prefixes (default: empty dictionary).

    - batching: A boolean indicating whether batching is enabled (default: True).

    - batch_size: An integer representing the batch size (default: 5000).

    - handle_vocab_uri_strategy: The strategy to handle vocabulary URIs (default: HANDLE_VOCAB_URI_STRATEGY.SHORTEN).

    - handle_multival_strategy: The strategy to handle multiple values (default: HANDLE_MULTIVAL_STRATEGY.OVERWRITE).

    - multival_props_names: A list of tuples containing the prefix and property names to be treated as multivalued.
    """

    def __init__(
            self,
            auth_data=None,
            custom_mappings: List[Tuple[str, str, str]] = [],
            custom_prefixes={},
            batching=True,
            batch_size=5000,
            handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.SHORTEN,
            handle_multival_strategy=HANDLE_MULTIVAL_STRATEGY.OVERWRITE,
            multival_props_names: List[Tuple[str, str]] = []
    ):
        self.default_prefixes = DEFAULT_PREFIXES
        self.auth_data = auth_data
        self.custom_prefixes = custom_prefixes
        self.custom_mappings = {}
        for mapping in custom_mappings:
            self.set_custom_mapping(prefix_name=mapping[0], to_replace=mapping[1], new_value=mapping[2])
        self.batching = batching
        self.batch_size = batch_size
        self.handle_vocab_uri_strategy = handle_vocab_uri_strategy
        self.handle_multival_strategy = handle_multival_strategy
        self.multival_props_names = []
        for prop_name in multival_props_names:
            self.set_multival_prop_name(prefix_name=prop_name[0], prop_name=prop_name[1])

    def set_handle_vocab_uri_strategy(self, val: HANDLE_VOCAB_URI_STRATEGY):
        """
        Set the strategy to handle vocabulary URIs.

        Parameters:
        - val: The handle_vocab_uri_strategy value to be set.
        """
        self.handle_vocab_uri_strategy = val

    def set_handle_multival_strategy(self, val: HANDLE_MULTIVAL_STRATEGY):
        """
        Set the strategy to handle multiple values.

        Parameters:
        - val: The handle_multival_strategy value to be set.
        """
        self.handle_multival_strategy = val

    def set_default_prefix(self, name: str, value: str):
        """
        Set a default prefix.

        Parameters:
        - name: The name of the prefix.
        - value: The value of the prefix (namespace URI).
        """
        self.default_prefixes[name] = Namespace(value)

    def get_prefixes(self):
        """
        Get a dictionary containing all prefixes (default and custom).

        Returns:
        A dictionary containing all prefixes.
        """
        res = {}
        res.update(self.default_prefixes)
        res.update(self.custom_prefixes)
        return res

    def set_multival_prop_name(self, prefix_name, prop_name: str):
        """
        Set a property name to be treated as multivalued.

        Parameters:
        - prefix_name: The name of the prefix.
        - prop_name: The name of the property to be treated as multivalued.

        Raises:
        - PrefixNotFoundException: If the prefix is not found in the available prefixes.
        """
        total_prefixes = self.get_prefixes()
        if prefix_name not in total_prefixes:
            raise PrefixNotFoundException(prefix_name=prefix_name)
        predicate = f"{total_prefixes[prefix_name]}{prop_name}"
        if predicate not in self.multival_props_names:
            self.multival_props_names.append(predicate)

    def set_custom_prefix(self, name: str, value: str):
        """
        Set a custom prefix.

        Parameters:
        - name: The name of the prefix.
        - value: The value of the prefix (namespace URI).

        Raises:
        - Exception: If the namespace is already defined for another prefix.
        """
        if Namespace(value) in self.custom_prefixes.values():
            raise Exception(f"Namespace {name} already defined for another prefix.")
        self.custom_prefixes[name] = Namespace(value)

    def set_custom_mapping(self, prefix_name: str, to_replace: str, new_value: str):
        """
        Add a custom mapping for a prefix.

        Parameters:
        - prefix_name: The name of the prefix to be mapped.
        - to_replace: The value to be replaced in the namespace URI.
        - new_value: The new value for the mapping (namespace URI).

        Raises:
        - PrefixNotFoundException: If the prefix is not found in the available prefixes.
        """
        total_prefixes = self.get_prefixes()
        if prefix_name not in total_prefixes:
            raise PrefixNotFoundException(prefix_name=prefix_name)
        self.custom_mappings[URIRef(f'{total_prefixes[prefix_name]}{to_replace}')] = new_value

    def set_auth_data(self, auth):
        """
        Set authentication data.

        Parameters:
        - auth: A dictionary containing authentication data.

        Raises:
        - WrongAuthenticationException: If any of the required authentication fields is missing.
        """
        check_auth_data(auth=auth)
        self.auth_data = auth

    def set_batching(self, val: bool):
        """
        Set batching.

        Parameters:
        - val: A boolean indicating whether batching is enabled.
        """
        self.batching = val

    def set_batch_size(self, val: int):
        """
        Set the batch size.

        Parameters:
        - val: An integer representing the batch size.
        """
        self.batch_size = val

    def get_config_dict(self):
        """
        Get the configuration dictionary.

        Returns:
        A dictionary containing the configuration parameters.

        Raises:
        - WrongAuthenticationException: If any of the required authentication fields is missing.
        """
        check_auth_data(auth=self.auth_data)
        return vars(self)
