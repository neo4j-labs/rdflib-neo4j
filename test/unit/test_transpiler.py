"""Unit tests for the SPARQL-to-Cypher transpiler.

Uses HANDLE_VOCAB_URI_STRATEGY.IGNORE so resolved names are plain local parts
(e.g. ``name``, ``Person``), keeping assertions readable without prefix noise.

Each test covers one algebra node type.  Tests assert on the rendered Cypher
*string* — the DSL renders deterministically so this is stable.
"""
from __future__ import annotations

import pytest

from rdflib_neo4j import Neo4jStoreConfig
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY
from rdflib_neo4j.sparql.transpiler import translate, Transpiler, collect_subject_vars
from rdflib.plugins.sparql import prepareQuery


# ── shared fixture ────────────────────────────────────────────────────────────

FOAF = "http://xmlns.com/foaf/0.1/"
EX   = "http://www.example.org/indiv/"


def _config() -> Neo4jStoreConfig:
    """Minimal config using IGNORE strategy for readable property names."""
    return Neo4jStoreConfig(
        custom_prefixes={"foaf": FOAF},
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
    )


def _translate(sparql: str) -> tuple[str, dict]:
    return translate(sparql, _config())


# ── helper ────────────────────────────────────────────────────────────────────

def cypher_of(sparql: str) -> str:
    cypher, _ = _translate(sparql)
    # strip the CYPHER 25 header for easier assertions
    lines = cypher.splitlines()
    body = "\n".join(l for l in lines if l != "CYPHER 25")
    return body.strip()


# ── collect_subject_vars ──────────────────────────────────────────────────────

def test_collect_subject_vars_simple():
    q = prepareQuery(f"SELECT ?p WHERE {{ ?p <{FOAF}name> ?name }}")
    svars = collect_subject_vars(q.algebra)
    assert "p" in svars
    assert "name" not in svars  # object-only variable


def test_collect_subject_vars_relationship():
    q = prepareQuery(f"""
        SELECT ?p ?f WHERE {{
          ?p <{FOAF}knows> ?f .
          ?f <{FOAF}name> ?name .
        }}""")
    svars = collect_subject_vars(q.algebra)
    assert "p" in svars
    assert "f" in svars   # also a subject
    assert "name" not in svars


# ── BGP — type only (label) ───────────────────────────────────────────────────

def test_bgp_type_only():
    cypher = cypher_of(f"""
        SELECT ?p WHERE {{ ?p a <{FOAF}Person> }}
    """)
    assert "MATCH (p:Person)" in cypher
    assert "RETURN p" in cypher


# ── BGP — property variable binding ──────────────────────────────────────────

def test_bgp_property_binding():
    cypher = cypher_of(f"""
        SELECT ?name WHERE {{
          ?p a <{FOAF}Person> ;
             <{FOAF}name> ?name .
        }}
    """)
    assert "MATCH (p:Person)" in cypher
    assert "p.`name` AS name" in cypher


# ── BGP — literal constraint ──────────────────────────────────────────────────

def test_bgp_literal_constraint():
    cypher = cypher_of(f"""
        SELECT ?p WHERE {{
          ?p <{FOAF}name> "Alice" .
        }}
    """)
    assert 'WHERE p.`name` = "Alice"' in cypher


# ── BGP — relationship pattern ────────────────────────────────────────────────

def test_bgp_relationship():
    cypher = cypher_of(f"""
        SELECT ?p ?f WHERE {{
          ?p <{FOAF}knows> ?f .
          ?f a <{FOAF}Person> .
        }}
    """)
    assert "MATCH (p:Resource)" in cypher or "MATCH (p:" in cypher
    assert "MATCH (f:Person)" in cypher
    assert "MATCH (p)-[:knows]->(f)" in cypher


# ── BGP — fixed-URI relationship target ──────────────────────────────────────

def test_bgp_fixed_uri_target():
    cypher, params = _translate(f"""
        SELECT ?p WHERE {{
          ?p <{FOAF}knows> <{EX}Alice> .
        }}
    """)
    body = "\n".join(l for l in cypher.splitlines() if l != "CYPHER 25")
    assert "knows" in body
    # The URI should appear as a parameter
    assert any(str(EX + "Alice") == v for v in params.values())


# ── Filter ────────────────────────────────────────────────────────────────────

def test_filter_comparison():
    cypher = cypher_of(f"""
        SELECT ?name WHERE {{
          ?p a <{FOAF}Person> ;
             <{FOAF}name>  ?name ;
             <{FOAF}age>   ?age .
          FILTER(?age > 18)
        }}
    """)
    assert "WHERE p.`age` > 18" in cypher


def test_filter_string_equality():
    cypher = cypher_of(f"""
        SELECT ?p WHERE {{
          ?p <{FOAF}name> ?name .
          FILTER(?name = "Alice")
        }}
    """)
    assert 'WHERE p.`name` = "Alice"' in cypher


def test_filter_and():
    cypher = cypher_of(f"""
        SELECT ?p WHERE {{
          ?p <{FOAF}age> ?age ; <{FOAF}score> ?score .
          FILTER(?age > 18 && ?score > 50)
        }}
    """)
    assert "AND" in cypher
    assert "p.`age` > 18" in cypher
    assert "p.`score` > 50" in cypher


def test_filter_bound():
    cypher = cypher_of(f"""
        SELECT ?p WHERE {{
          ?p <{FOAF}name> ?name .
          FILTER(BOUND(?name))
        }}
    """)
    assert "IS NOT NULL" in cypher


def test_filter_regex():
    cypher = cypher_of(f"""
        SELECT ?p WHERE {{
          ?p <{FOAF}name> ?name .
          FILTER(REGEX(?name, "^Al"))
        }}
    """)
    assert "=~" in cypher


# ── Project — SELECT projection ───────────────────────────────────────────────

def test_project_multiple_vars():
    cypher = cypher_of(f"""
        SELECT ?name ?age WHERE {{
          ?p a <{FOAF}Person> ;
             <{FOAF}name> ?name ;
             <{FOAF}age>  ?age .
        }}
    """)
    assert "p.`name` AS name" in cypher
    assert "p.`age` AS age" in cypher


# ── Join ──────────────────────────────────────────────────────────────────────

def test_join_two_bgps():
    """Two BGPs sharing a variable — joined automatically."""
    cypher = cypher_of(f"""
        SELECT ?name ?dept WHERE {{
          ?p a <{FOAF}Person> ; <{FOAF}name> ?name .
          ?p <{FOAF}member> ?dept .
        }}
    """)
    assert "MATCH (p:Person)" in cypher
    assert "p.`name` AS name" in cypher
    assert "p.`member` AS dept" in cypher


# ── LeftJoin (OPTIONAL) ───────────────────────────────────────────────────────

def test_left_join_property():
    """OPTIONAL on a property variable — both vars projected, email may be null."""
    cypher = cypher_of(f"""
        SELECT ?name ?email WHERE {{
          ?p a <{FOAF}Person> ; <{FOAF}name> ?name .
          OPTIONAL {{ ?p <{FOAF}mbox> ?email }}
        }}
    """)
    # Property variables in optional are naturally null-if-absent via property access
    assert "name" in cypher
    assert "email" in cypher


def test_left_join_relationship():
    """OPTIONAL on a relationship pattern where ?f is also a subject (node var).

    When ?f is used as a subject elsewhere the transpiler knows it's a node
    and emits an OPTIONAL MATCH.  Without that structural hint the variable
    falls back to property-access semantics (null if absent) — which still
    satisfies the OPTIONAL contract for callers that check for null.
    """
    cypher = cypher_of(f"""
        SELECT ?p ?f ?fname WHERE {{
          ?p a <{FOAF}Person> .
          OPTIONAL {{
            ?p <{FOAF}knows> ?f .
            ?f <{FOAF}name> ?fname .
          }}
        }}
    """)
    # ?f IS a subject (of ?f foaf:name ?fname) so collect_subject_vars picks it up
    assert "OPTIONAL" in cypher
    assert "fname" in cypher


# ── Union ─────────────────────────────────────────────────────────────────────

def test_union():
    cypher = cypher_of(f"""
        SELECT ?name WHERE {{
          {{ ?p <{FOAF}name> ?name . FILTER(?name = "Alice") }}
          UNION
          {{ ?p <{FOAF}name> ?name . FILTER(?name = "Bob") }}
        }}
    """)
    assert "UNION" in cypher


# ── Extend (BIND) ────────────────────────────────────────────────────────────

def test_extend_ucase():
    cypher = cypher_of(f"""
        SELECT ?p ?upper WHERE {{
          ?p <{FOAF}name> ?name .
          BIND(UCASE(?name) AS ?upper)
        }}
    """)
    assert "toUpper" in cypher
    assert "upper" in cypher


def test_extend_if():
    cypher = cypher_of(f"""
        SELECT ?p ?label WHERE {{
          ?p <{FOAF}name> ?name .
          BIND(IF(?name = "Alice", "friend", "stranger") AS ?label)
        }}
    """)
    assert "CASE WHEN" in cypher
    assert '"friend"' in cypher
    assert "label" in cypher


# ── Slice (LIMIT / SKIP) ──────────────────────────────────────────────────────

def test_slice_limit():
    cypher = cypher_of(f"""
        SELECT ?p WHERE {{ ?p a <{FOAF}Person> }} LIMIT 10
    """)
    assert "LIMIT 10" in cypher


def test_slice_offset():
    cypher = cypher_of(f"""
        SELECT ?p WHERE {{ ?p a <{FOAF}Person> }} OFFSET 5 LIMIT 10
    """)
    assert "SKIP 5" in cypher
    assert "LIMIT 10" in cypher


# ── OrderBy ──────────────────────────────────────────────────────────────────

def test_order_by_asc():
    cypher = cypher_of(f"""
        SELECT ?name WHERE {{
          ?p a <{FOAF}Person> ; <{FOAF}name> ?name .
        }} ORDER BY ?name
    """)
    assert "ORDER BY" in cypher
    assert "name" in cypher


def test_order_by_desc():
    cypher = cypher_of(f"""
        SELECT ?name WHERE {{
          ?p a <{FOAF}Person> ; <{FOAF}name> ?name .
        }} ORDER BY DESC(?name)
    """)
    assert "ORDER BY" in cypher
    assert "DESC" in cypher


# ── Distinct ─────────────────────────────────────────────────────────────────

def test_distinct():
    cypher = cypher_of(f"""
        SELECT DISTINCT ?name WHERE {{
          ?p <{FOAF}name> ?name .
        }}
    """)
    assert "DISTINCT" in cypher


# ── Group / Aggregates ────────────────────────────────────────────────────────

def test_group_count():
    cypher = cypher_of(f"""
        SELECT ?dept (COUNT(?p) AS ?cnt) WHERE {{
          ?p <{FOAF}member> ?dept .
        }} GROUP BY ?dept
    """)
    assert "count(" in cypher.lower()
    assert "WITH" in cypher


def test_group_avg():
    cypher = cypher_of(f"""
        SELECT ?dept (AVG(?age) AS ?avgAge) WHERE {{
          ?p <{FOAF}member> ?dept ; <{FOAF}age> ?age .
        }} GROUP BY ?dept
    """)
    assert "avg(" in cypher.lower()


def test_group_concat():
    cypher = cypher_of(f"""
        SELECT ?dept (GROUP_CONCAT(?name; SEPARATOR=",") AS ?names) WHERE {{
          ?p <{FOAF}member> ?dept ; <{FOAF}name> ?name .
        }} GROUP BY ?dept
    """)
    assert "reduce(" in cypher.lower()
    assert "collect(" in cypher.lower()


# ── Minus ─────────────────────────────────────────────────────────────────────

def test_minus():
    cypher = cypher_of(f"""
        SELECT ?name WHERE {{
          ?p <{FOAF}name> ?name .
          MINUS {{ ?p <{FOAF}blocked> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> }}
        }}
    """)
    assert "NOT EXISTS" in cypher


# ── Values ────────────────────────────────────────────────────────────────────

def test_values_unbound():
    cypher = cypher_of(f"""
        SELECT ?name WHERE {{
          VALUES ?name {{ "Alice" "Bob" }}
          ?p <{FOAF}name> ?name .
        }}
    """)
    # Unbound VALUES → UNWIND or IN
    assert ("UNWIND" in cypher) or ("IN" in cypher)


def test_values_bound():
    cypher = cypher_of(f"""
        SELECT ?p WHERE {{
          ?p <{FOAF}name> ?name .
          VALUES ?name {{ "Alice" "Bob" }}
        }}
    """)
    # Bound VALUES → IN predicate
    assert "IN" in cypher


# ── Combined: type + filter + order + limit ───────────────────────────────────

def test_full_select():
    cypher = cypher_of(f"""
        SELECT ?name ?age WHERE {{
          ?p a <{FOAF}Person> ;
             <{FOAF}name> ?name ;
             <{FOAF}age>  ?age .
          FILTER(?age >= 18)
        }} ORDER BY ?name LIMIT 20
    """)
    assert "MATCH (p:Person)" in cypher
    assert "p.`age` >= 18" in cypher or "p.`age`>= 18" in cypher
    assert "ORDER BY" in cypher
    assert "LIMIT 20" in cypher
    assert "p.`name` AS name" in cypher
    assert "p.`age` AS age" in cypher


# ── Relationship chain ────────────────────────────────────────────────────────

def test_two_hop_relationship():
    cypher = cypher_of(f"""
        SELECT ?p ?fname WHERE {{
          ?p <{FOAF}knows> ?f .
          ?f <{FOAF}name> ?fname .
        }}
    """)
    assert "knows" in cypher
    assert "fname" in cypher
