from functools import wraps
from time import time
from typing import Dict
from rdflib import URIRef
from enum import Enum
from rdflib_neo4j.config.const import ShortenStrictException


def timing(f):
    """
    Decorator function that measures the execution time of a function.

    Parameters:
    - f: The function to be measured.
    """

    @wraps(f)
    def wrap(*args, **kw):
        """
        Wrapper function that measures the execution time and calls the decorated function.

        Parameters:
        - args: Positional arguments passed to the function.
        - kw: Keyword arguments passed to the function.
        """
        ts = time()
        result = ""
        try:
            result = f(*args, **kw)
        except Exception as e:
            print(e)
        te = time()
        print('func:%r args:[%r, %r] took: %2.4f sec' % (f.__name__, args, kw, te - ts))
        return result

    return wrap


def getLocalPart(uri):
    """
    Extracts the local part of a URI.

    Parameters:
    - uri: The URI string.

    Returns:
    The local part of the URI.
    """
    pos = uri.rfind('#')
    if pos < 0:
        pos = uri.rfind('/')
    if pos < 0:
        pos = uri.rindex(':')
    return uri[pos + 1:]


def getNamespacePart(uri):
    """
    Extracts the namespace part of a URI.

    Parameters:
    - uri: The URI string.

    Returns:
    The namespace part of the URI.
    """
    pos = uri.rfind('#')
    if pos < 0:
        pos = uri.rfind('/')
    if pos < 0:
        pos = uri.rindex(':')
    return uri[0:pos + 1]


class HANDLE_VOCAB_URI_STRATEGY(Enum):
    """
    Enum class defining different strategies for handling vocabulary URIs.
    """
    SHORTEN = "SHORTEN"  # Strategy to shorten the URIs
    MAP = "MAP"  # Strategy to map the URIs using provided mappings
    KEEP = "KEEP"  # Strategy to keep the URIs
    IGNORE = "IGNORE"  # Strategy to ignore the Namespace and get only the local part


def handle_vocab_uri_ignore(predicate):
    """
    Shortens a URI by extracting the local part.

    Parameters:
    - uri: The URI string.

    Returns:
    The shortened URI.
    """
    return getLocalPart(str(predicate))


def create_shortened_predicate(namespace, local_part):
    """
    Creates a shortened predicate by combining the namespace and local part.

    Parameters:
    - namespace: The namespace part of the URI.
    - local_part: The local part of the URI.

    Returns:
    The shortened predicate.
    """
    return f"{namespace}__{local_part}"


def handle_vocab_uri_shorten(predicate, prefixes):
    """
    Shortens a URI by combining the namespace and local part based on provided prefixes.

    Parameters:
    - predicate: The URI to be shortened.
    - prefixes: A dictionary containing namespace prefixes.

    Returns:
    The shortened URI if the namespace exists in the prefixes, otherwise raises a ShortenStrictException.
    """
    ns = getNamespacePart(predicate)
    local_part = getLocalPart(predicate)
    if ns in prefixes:
        return create_shortened_predicate(namespace=prefixes[ns], local_part=local_part)
    raise ShortenStrictException(ns)


def handle_vocab_uri_map(mappings: Dict[str, str], predicate: URIRef):
    """
    Maps the given predicate URI using the provided mappings dictionary.

    Parameters:
    - mappings: A dictionary mapping URIs to their mapped values.
    - predicate: The predicate URI to be mapped.

    Returns:
    The mapped predicate URI if it exists in the mappings dictionary, otherwise returns the original predicate URI.
    """
    if isinstance(predicate, URIRef) and predicate in mappings:
        predicate = URIRef(mappings[predicate])
    return predicate


def handle_vocab_uri(mappings: Dict[str, str],
                     predicate: URIRef,
                     prefixes: Dict[str, str],
                     strategy: HANDLE_VOCAB_URI_STRATEGY):
    """
    Handles the given predicate URI based on the chosen strategy.

    Parameters:
    - mappings: A dictionary mapping URIs to their mapped values.
    - predicate: The predicate URI to be handled.
    - prefixes: A dictionary containing namespace prefixes.
    - strategy: The strategy to be used for handling the predicate URI.

    Returns:
    The handled predicate URI based on the chosen strategy.
    """
    if strategy == HANDLE_VOCAB_URI_STRATEGY.SHORTEN:
        return handle_vocab_uri_shorten(predicate, prefixes)
    elif strategy == HANDLE_VOCAB_URI_STRATEGY.MAP:
        res = handle_vocab_uri_map(mappings, predicate)
        if res == predicate:
            res = handle_vocab_uri_ignore(predicate)
        return res
    elif strategy == HANDLE_VOCAB_URI_STRATEGY.KEEP:
        return predicate
    elif strategy == HANDLE_VOCAB_URI_STRATEGY.IGNORE:
        return handle_vocab_uri_ignore(predicate)
    raise Exception(f"Strategy {strategy} not defined.")


class HANDLE_MULTIVAL_STRATEGY(Enum):
    """
    Enum class defining different strategies for handling multiple values.
    """
    OVERWRITE = 1  # Strategy to overwrite multiple values
    ARRAY = 2  # Strategy to treat multiple values as an array
