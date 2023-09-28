from enum import Enum

DEFAULT_PREFIXES = {
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "sch": "http://schema.org/",
    "sh": "http://www.w3.org/ns/shacl#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dct": "http://purl.org/dc/terms/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "exterms": "http://www.example.org/terms/",
    "ex": "http://www.example.org/indiv/"
}

NEO4J_AUTH_REQUIRED_FIELDS = ["uri", "database", "user", "pwd"]
NEO4J_DRIVER_USER_AGENT_NAME = "neo4j_labs_n10s_client_lib"

class PrefixNotFoundException(Exception):

    # Constructor or Initializer
    def __init__(self, prefix_name):
        self.value = prefix_name

    # __str__ is to print() the value
    def __str__(self):
        return f"Prefix {(repr(self.value))} not found inside the configuration. Please add it before adding any related custom mapping."


class ShortenStrictException(Exception):

    # Constructor or Initializer
    def __init__(self, namespace):
        self.namespace = namespace

    # __str__ is to print() the value
    def __str__(self):
        return f"Namespace {(repr(self.namespace))} not found inside the configuration. Please add it if you want to use the SHORTEN mode."


class WrongAuthenticationException(Exception):

    # Constructor or Initializer
    def __init__(self, param_name):
        self.param_name = param_name

    # __str__ is to print() the value
    def __str__(self):
        return f"""Missing {self.param_name} key inside the authentication definition. Remember that it should contain the following keys:
                : [uri, database, user, pwd]"""

class CypherMultipleTypesMultiValueException(Exception):

    # Constructor or Initializer
    def __init__(self):
        super().__init__()

    def __str__(self):
        return f"""Values of a multivalued property must have the same datatype."""

NEO4J_DRIVER_MULTIPLE_TYPE_ERROR_MESSAGE = """{code: Neo.ClientError.Statement.TypeError} {message: Neo4j only supports a subset of Cypher types for storage as singleton or array properties. Please refer to section cypher/syntax/values of the manual for more details.}"""
NEO4J_DRIVER_DICT_MESSAGE = {NEO4J_DRIVER_MULTIPLE_TYPE_ERROR_MESSAGE: CypherMultipleTypesMultiValueException}

class HANDLE_VOCAB_URI_STRATEGY(Enum):
    """
    Enum class defining different strategies for handling vocabulary URIs.

    - SHORTEN : Strategy to shorten the URIs (Every prefix that you will use must be defined in the config, otherwise Neo4jStore will throw a ShortenStrictException)
    - MAP : Strategy to map the URIs using provided mappings
    - KEEP : Strategy to keep the URIs
    - IGNORE : Strategy to ignore the Namespace and get only the local part

    """
    SHORTEN = "SHORTEN"  # Strategy to shorten the URIs
    MAP = "MAP"  # Strategy to map the URIs using provided mappings
    KEEP = "KEEP"  # Strategy to keep the URIs
    IGNORE = "IGNORE"  # Strategy to ignore the Namespace and get only the local part


class HANDLE_MULTIVAL_STRATEGY(Enum):
    """
    Enum class defining different strategies for handling multiple values.

    - OVERWRITE:  Strategy to overwrite multiple values
    - ARRAY:  Strategy to treat multiple values as an array

    TO NOTICE : If the strategy is ARRAY and the Neo4jStoreConfig doesn't contain any predicate marked as multivalued, EVERY field will be treated as multivalued.
    """
    OVERWRITE = 1  # Strategy to overwrite multiple values
    ARRAY = 2  # Strategy to treat multiple values as an array
