"""Unit tests for rdflib_neo4j.datatypes.coerce_literal — no Neo4j connection needed."""
import pytest
from rdflib import Literal, XSD

from rdflib_neo4j.datatypes import coerce_literal


# ---------------------------------------------------------------------------
# Numeric types
# ---------------------------------------------------------------------------

def test_integer():
    assert coerce_literal(Literal("42", datatype=XSD.integer)) == 42
    assert isinstance(coerce_literal(Literal("42", datatype=XSD.integer)), int)


def test_long():
    assert coerce_literal(Literal("9999999999", datatype=XSD.long)) == 9999999999


def test_float():
    result = coerce_literal(Literal("3.14", datatype=XSD.float))
    assert isinstance(result, float)
    assert abs(result - 3.14) < 1e-6


def test_double():
    result = coerce_literal(Literal("2.718", datatype=XSD.double))
    assert isinstance(result, float)


def test_decimal():
    result = coerce_literal(Literal("1.5", datatype=XSD.decimal))
    assert isinstance(result, float)
    assert result == 1.5


# ---------------------------------------------------------------------------
# Boolean
# ---------------------------------------------------------------------------

def test_boolean_true():
    assert coerce_literal(Literal("true", datatype=XSD.boolean)) is True


def test_boolean_false():
    assert coerce_literal(Literal("false", datatype=XSD.boolean)) is False


# ---------------------------------------------------------------------------
# String types
# ---------------------------------------------------------------------------

def test_string():
    result = coerce_literal(Literal("hello", datatype=XSD.string))
    assert result == "hello"
    assert isinstance(result, str)


def test_normalized_string():
    result = coerce_literal(Literal("hello world", datatype=XSD.normalizedString))
    assert result == "hello world"


# ---------------------------------------------------------------------------
# Temporal types (requires neo4j package)
# ---------------------------------------------------------------------------

neo4j_time = pytest.importorskip("neo4j.time")


def test_date():
    from neo4j.time import Date
    result = coerce_literal(Literal("2024-01-15", datatype=XSD.date))
    assert isinstance(result, Date)
    assert result == Date(2024, 1, 15)


def test_datetime_naive():
    from neo4j.time import DateTime
    result = coerce_literal(Literal("2024-01-15T10:30:00", datatype=XSD.dateTime))
    assert isinstance(result, DateTime)
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15
    assert result.hour == 10
    assert result.minute == 30
    assert result.second == 0


def test_datetime_with_timezone():
    from neo4j.time import DateTime
    result = coerce_literal(Literal("2024-01-15T10:30:00+02:00", datatype=XSD.dateTime))
    assert isinstance(result, DateTime)
    assert result.year == 2024


def test_duration():
    from neo4j.time import Duration
    # P1DT1H = 1 day + 1 hour = 90000 seconds
    result = coerce_literal(Literal("P1DT1H", datatype=XSD.duration))
    assert isinstance(result, Duration)


# ---------------------------------------------------------------------------
# Language-tagged literals
# ---------------------------------------------------------------------------

def test_lang_tag_keep():
    lit = Literal("hello", lang="en")
    result = coerce_literal(lit, keep_lang_tag=True)
    assert result == "hello@en"


def test_lang_tag_no_keep():
    lit = Literal("hello", lang="en")
    result = coerce_literal(lit, keep_lang_tag=False)
    assert result == "hello"


def test_language_filter_match():
    lit = Literal("hello", lang="en")
    result = coerce_literal(lit, language_filter="en")
    assert result == "hello"


def test_language_filter_match_keep_tag():
    lit = Literal("hello", lang="en")
    result = coerce_literal(lit, keep_lang_tag=True, language_filter="en")
    assert result == "hello@en"


def test_language_filter_no_match():
    lit = Literal("hello", lang="en")
    result = coerce_literal(lit, language_filter="fr")
    assert result is None


def test_language_filter_case_insensitive():
    lit = Literal("hello", lang="EN")
    result = coerce_literal(lit, language_filter="en")
    assert result == "hello"


# ---------------------------------------------------------------------------
# Custom datatypes
# ---------------------------------------------------------------------------

def test_custom_datatype_keep():
    from rdflib import URIRef
    dt = URIRef("http://example.org/mytype")
    lit = Literal("myvalue", datatype=dt)
    result = coerce_literal(lit, keep_custom_data_types=True)
    assert result == "myvalue^^http://example.org/mytype"


def test_custom_datatype_no_keep():
    from rdflib import URIRef
    dt = URIRef("http://example.org/mytype")
    lit = Literal("myvalue", datatype=dt)
    result = coerce_literal(lit, keep_custom_data_types=False)
    # Falls back to toPython() which returns a string for unknown datatypes
    assert "myvalue" in str(result)
