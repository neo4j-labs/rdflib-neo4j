"""
expander.py — Reverse URI expansion for rdflib-neo4j read path.

Converts Neo4j-stored property keys, relationship types, and node labels back
to rdflib URIRef/BNode/Literal terms.

Round-trip fidelity depends on the HANDLE_VOCAB_URI_STRATEGY used during write:
  - SHORTEN: full round-trip (ns__local -> full URI)
  - KEEP:    full round-trip (stored as full URI)
  - MAP:     partial (mapped names may not be reversible)
  - IGNORE:  local names only stored; URIRef("localName") is returned, which
             is not a valid absolute URI.  This is a known limitation.
"""

from rdflib import URIRef
from rdflib.term import BNode

BNODE_PREFIX = "bnode://"


def expand_uri(value: str, prefix_map: dict) -> URIRef | BNode:
    """
    Expand a shortened property/label name or full URI back to an rdflib term.

    Args:
        value: The stored string — may be a bnode URI, full URI, ns__local
               shortened form, or bare local name (IGNORE mode).
        prefix_map: Mapping of {prefix_name -> namespace_uri_string},
                    e.g. {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}.

    Returns:
        BNode if value starts with BNODE_PREFIX,
        URIRef expanded from ns__local if prefix found,
        URIRef(value) otherwise (full URI or bare local name).
    """
    if value.startswith(BNODE_PREFIX):
        return BNode(value[len(BNODE_PREFIX):])
    if "://" in value:
        return URIRef(value)
    # Try to expand ns__localName → full URI
    if "__" in value:
        parts = value.split("__", 1)
        prefix, local = parts[0], parts[1]
        if prefix in prefix_map:
            return URIRef(f"{prefix_map[prefix]}{local}")
    # Can't expand (IGNORE mode stores local parts only, or unknown prefix).
    # Return as-is; this may not be a valid absolute URI.
    return URIRef(value)


def neo4j_value_to_literal(value, prop_key: str = None):
    """
    Convert a Neo4j property value back to an rdflib Literal (or list thereof).

    Args:
        value: The Neo4j property value.
        prop_key: The property key (unused, kept for future type hints).

    Returns:
        An rdflib Literal, or a list of Literals for multi-value properties.

    Notes:
        - bool is checked before int because bool is a subclass of int in Python.
        - Strings with "^^<uri>" or "@<lang>" suffixes are decoded back to
          typed/lang-tagged literals.  These encodings are produced by the
          XSD coercion layer (feat/52) when keep_custom_data_types or
          keep_lang_tag is enabled.  The heuristic is best-effort; strings
          that happen to contain "^^" will be misparsed.
    """
    from rdflib import Literal, XSD

    # bool must come before int (bool is a subclass of int)
    if isinstance(value, bool):
        return Literal(value, datatype=XSD.boolean)
    if isinstance(value, int):
        return Literal(value, datatype=XSD.integer)
    if isinstance(value, float):
        return Literal(value, datatype=XSD.double)
    if isinstance(value, list):
        return [neo4j_value_to_literal(v, prop_key) for v in value]

    # Try neo4j temporal types
    try:
        from neo4j.time import Date, DateTime, Duration
        import datetime
        if isinstance(value, Date):
            return Literal(
                datetime.date(value.year, value.month, value.day),
                datatype=XSD.date,
            )
        if isinstance(value, DateTime):
            return Literal(str(value), datatype=XSD.dateTime)
        if isinstance(value, Duration):
            return Literal(str(value), datatype=XSD.duration)
    except ImportError:
        pass

    # String: check for encoded lang tag or custom datatype
    s = str(value)
    if "^^" in s:
        val, dtype = s.rsplit("^^", 1)
        return Literal(val, datatype=URIRef(dtype))
    if "@" in s and not s.startswith("http"):
        val, lang = s.rsplit("@", 1)
        if 1 <= len(lang) <= 8 and lang.replace("-", "").isalpha():
            return Literal(val, lang=lang)
    return Literal(s)
