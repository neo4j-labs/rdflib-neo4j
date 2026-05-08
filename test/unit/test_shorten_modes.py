"""
Unit tests for SHORTEN_STRICT and SHORTEN (dynamic) URI strategies.
"""
import pytest
from rdflib import URIRef

from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY, ShortenStrictException
from rdflib_neo4j.utils import (
    handle_vocab_uri_shorten_strict,
    handle_vocab_uri_shorten_dynamic,
    handle_vocab_uri,
)

# A minimal prefixes dict (namespace_uri -> prefix_name)
KNOWN_NS = "http://www.w3.org/2004/02/skos/core#"
KNOWN_PREFIX = "skos"
PREFIXES = {KNOWN_NS: KNOWN_PREFIX}

UNKNOWN_NS_1 = "http://unknown.example.org/vocab#"
UNKNOWN_NS_2 = "http://another.unknown.org/ns#"


class TestShortenStrictMode:
    def test_raises_for_unknown_namespace(self):
        """SHORTEN_STRICT raises ShortenStrictException for unknown namespace."""
        predicate = URIRef(f"{UNKNOWN_NS_1}someProp")
        with pytest.raises(ShortenStrictException):
            handle_vocab_uri_shorten_strict(predicate, PREFIXES)

    def test_known_namespace_returns_shortened(self):
        """SHORTEN_STRICT uses pre-declared prefix for known namespaces."""
        predicate = URIRef(f"{KNOWN_NS}Concept")
        result = handle_vocab_uri_shorten_strict(predicate, PREFIXES)
        assert result == "skos__Concept"

    def test_handle_vocab_uri_dispatch_shorten_strict(self):
        """handle_vocab_uri dispatches to strict mode for SHORTEN_STRICT strategy."""
        predicate = URIRef(f"{UNKNOWN_NS_1}someProp")
        with pytest.raises(ShortenStrictException):
            handle_vocab_uri({}, predicate, PREFIXES, HANDLE_VOCAB_URI_STRATEGY.SHORTEN_STRICT)


class TestShortenDynamicMode:
    def test_auto_generates_ns0_for_first_unknown(self):
        """SHORTEN auto-generates ns0__ prefix for first unknown namespace."""
        predicate = URIRef(f"{UNKNOWN_NS_1}localName")
        dynamic_ns_map = {}
        counter_ref = [0]
        result = handle_vocab_uri_shorten_dynamic(predicate, PREFIXES, dynamic_ns_map, counter_ref)
        assert result == "ns0__localName"

    def test_auto_generates_ns1_for_second_distinct_namespace(self):
        """SHORTEN generates ns1__ prefix for the second distinct unknown namespace."""
        predicate1 = URIRef(f"{UNKNOWN_NS_1}prop1")
        predicate2 = URIRef(f"{UNKNOWN_NS_2}prop2")
        dynamic_ns_map = {}
        counter_ref = [0]
        result1 = handle_vocab_uri_shorten_dynamic(predicate1, PREFIXES, dynamic_ns_map, counter_ref)
        result2 = handle_vocab_uri_shorten_dynamic(predicate2, PREFIXES, dynamic_ns_map, counter_ref)
        assert result1 == "ns0__prop1"
        assert result2 == "ns1__prop2"

    def test_same_namespace_reuses_prefix(self):
        """Same unknown namespace used twice gets the same nsN prefix; counter doesn't increment again."""
        predicate1 = URIRef(f"{UNKNOWN_NS_1}firstProp")
        predicate2 = URIRef(f"{UNKNOWN_NS_1}secondProp")
        dynamic_ns_map = {}
        counter_ref = [0]
        result1 = handle_vocab_uri_shorten_dynamic(predicate1, PREFIXES, dynamic_ns_map, counter_ref)
        result2 = handle_vocab_uri_shorten_dynamic(predicate2, PREFIXES, dynamic_ns_map, counter_ref)
        assert result1 == "ns0__firstProp"
        assert result2 == "ns0__secondProp"
        assert counter_ref[0] == 1  # counter only incremented once

    def test_known_namespace_uses_declared_prefix(self):
        """SHORTEN uses the pre-declared prefix for known namespaces, not nsN."""
        predicate = URIRef(f"{KNOWN_NS}Concept")
        dynamic_ns_map = {}
        counter_ref = [0]
        result = handle_vocab_uri_shorten_dynamic(predicate, PREFIXES, dynamic_ns_map, counter_ref)
        assert result == "skos__Concept"
        assert counter_ref[0] == 0  # counter should not have incremented

    def test_handle_vocab_uri_dispatch_shorten_dynamic(self):
        """handle_vocab_uri dispatches to dynamic mode for SHORTEN strategy."""
        predicate = URIRef(f"{UNKNOWN_NS_1}myProp")
        dynamic_ns_map = {}
        counter_ref = [0]
        result = handle_vocab_uri(
            {}, predicate, PREFIXES,
            HANDLE_VOCAB_URI_STRATEGY.SHORTEN,
            dynamic_ns_map=dynamic_ns_map,
            counter_ref=counter_ref,
        )
        assert result == "ns0__myProp"


class TestEnumValues:
    def test_shorten_value_matches_n10s(self):
        """SHORTEN enum value matches n10s canonical string."""
        assert HANDLE_VOCAB_URI_STRATEGY.SHORTEN.value == "SHORTEN"

    def test_shorten_strict_value_matches_n10s(self):
        """SHORTEN_STRICT enum value matches n10s canonical string."""
        assert HANDLE_VOCAB_URI_STRATEGY.SHORTEN_STRICT.value == "SHORTEN_STRICT"
