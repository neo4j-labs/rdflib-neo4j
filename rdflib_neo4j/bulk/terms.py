from decimal import Decimal
from typing import Optional, Tuple

from rdflib import BNode, Literal, URIRef


BNODE_PREFIX = "bnode://"


def resource_id(term) -> str:
    if isinstance(term, BNode):
        return f"{BNODE_PREFIX}{term}"
    return str(term)


def literal_value(term: Literal):
    value = term.toPython()
    if isinstance(value, Decimal):
        return float(value)
    return value


def object_parts(term) -> Tuple[str, str, Optional[str], Optional[str]]:
    if isinstance(term, Literal):
        datatype = str(term.datatype) if term.datatype else None
        return "literal", str(literal_value(term)), datatype, term.language
    if isinstance(term, BNode):
        return "bnode", resource_id(term), None, None
    if isinstance(term, URIRef):
        return "iri", str(term), None, None
    return "literal", str(term), None, None
