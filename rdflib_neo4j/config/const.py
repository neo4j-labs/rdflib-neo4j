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
