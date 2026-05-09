"""Unit tests for rdflib_neo4j.sparql.cypher_builder.

Exercises the full typed DSL:
- Parameter (returned by add_param / add_named_param)
- Predicate hierarchy: RawPredicate, AndPredicate, OrPredicate, NotPredicate,
  Comparison, IsNull, IsNotNull, InPredicate
- Subquery expressions: ExistsExpression, NotExistsExpression, CountExpression,
  CollectExpression  (built via q.subquery() + q.*_subquery())
- Clause types: MatchClause, OptionalCallClause, WithClause, ReturnClause
- CypherQuery builder: match, where, optional_call, with_, return_, union
- Shared param namespace via subquery()
- UNION param collision safety
- Idempotency
"""

from rdflib_neo4j.sparql.cypher_builder import (
    AliasExpression,
    AndPredicate,
    AnonNode,
    CallClause,
    CollectExpression,
    Comparison,
    CountExpression,
    CypherQuery,
    ExistsExpression,
    FunctionCall,
    InPredicate,
    IsNotNull,
    IsNull,
    LabelAnd,
    LabelAtom,
    LabelNot,
    LabelOr,
    ListExpression,
    MatchClause,
    NodePattern,
    NotExistsExpression,
    NotPredicate,
    OptionalCallClause,
    OrderItem,
    OrPredicate,
    Parameter,
    PathPattern,
    Property,
    PropertyAccess,
    QPPPattern,
    RawExpression,
    RawPredicate,
    ReduceExpression,
    RelSegment,
    ReturnClause,
    StringLiteral,
    UnwindClause,
    Var,
    WithClause,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def raw(s: str) -> RawExpression:
    return RawExpression(s)

def pred(s: str) -> RawPredicate:
    return RawPredicate(s)


# ── 1. Simple MATCH + RETURN ──────────────────────────────────────────────────

def test_simple_match_return():
    q = CypherQuery()
    cypher, params = q.match("(n:Resource)").return_("n").render()
    assert "MATCH (n:Resource)" in cypher
    assert "RETURN n" in cypher
    assert params == {}


def test_cypher_25_header():
    q = CypherQuery()
    cypher, _ = q.match("(n)").return_("n").render()
    assert cypher.startswith("CYPHER 25")


# ── 2. Parameter ──────────────────────────────────────────────────────────────

def test_add_param_returns_parameter():
    q = CypherQuery()
    p = q.add_param("http://example.org/Alice")
    assert isinstance(p, Parameter)
    assert str(p) == "$p0"
    assert p.name == "p0"


def test_param_in_fstring():
    q = CypherQuery()
    p = q.add_param("Alice")
    assert f"(n {{name: {p}}})" == "(n {name: $p0})"


def test_param_counter_increments():
    q = CypherQuery()
    refs = [q.add_param(i) for i in range(5)]
    assert [str(r) for r in refs] == ["$p0", "$p1", "$p2", "$p3", "$p4"]


def test_add_named_param():
    q = CypherQuery()
    p = q.add_named_param("uri", "http://example.org/")
    assert isinstance(p, Parameter)
    assert str(p) == "$uri"
    cypher, params = q.match(f"(n {{uri: {p}}})").return_("n").render()
    assert "$uri" in cypher
    assert params["uri"] == "http://example.org/"


def test_same_value_twice_gets_distinct_params():
    q = CypherQuery()
    p1 = q.add_param("v")
    p2 = q.add_param("v")
    assert p1.name != p2.name


def test_values_never_in_query_string():
    q = CypherQuery()
    secret = "super_secret_value"
    p = q.add_param(secret)
    cypher, _ = q.match(f"(n {{x: {p}}})").return_("n").render()
    assert secret not in cypher
    assert str(p) in cypher


# ── 3. Predicate types ────────────────────────────────────────────────────────

def test_raw_predicate():
    p = RawPredicate("n.active = true")
    assert str(p) == "n.active = true"
    assert isinstance(p, RawPredicate)


def test_comparison_predicate():
    q = CypherQuery()
    age = q.add_param(18)
    cmp = Comparison(raw("n.age"), ">=", age)
    assert str(cmp) == "n.age >= $p0"


def test_and_predicate():
    p1 = RawPredicate("n.active = true")
    p2 = RawPredicate("n.age > 18")
    combined = p1.and_(p2)
    assert isinstance(combined, AndPredicate)
    assert str(combined) == "n.active = true AND n.age > 18"


def test_and_predicate_multiple():
    p1 = RawPredicate("a")
    p2 = RawPredicate("b")
    p3 = RawPredicate("c")
    combined = AndPredicate(p1, p2, p3)
    assert str(combined) == "a AND b AND c"


def test_or_predicate():
    p1 = RawPredicate("n.type = 'X'")
    p2 = RawPredicate("n.type = 'Y'")
    combined = p1.or_(p2)
    assert isinstance(combined, OrPredicate)
    assert str(combined) == "(n.type = 'X') OR (n.type = 'Y')"


def test_not_predicate():
    p = RawPredicate("n.deleted = true")
    negated = p.not_()
    assert isinstance(negated, NotPredicate)
    assert str(negated) == "NOT (n.deleted = true)"


def test_predicate_composition():
    """p1 AND (p2 OR p3).not_()"""
    p1 = RawPredicate("n.active = true")
    p2 = RawPredicate("n.role = 'admin'")
    p3 = RawPredicate("n.role = 'mod'")
    composed = p1.and_(p2.or_(p3).not_())
    assert "AND" in str(composed)
    assert "NOT" in str(composed)
    assert "OR" in str(composed)


def test_is_null():
    expr = IsNull(raw("n.email"))
    assert str(expr) == "n.email IS NULL"
    assert isinstance(expr, IsNull)


def test_is_not_null():
    expr = IsNotNull(raw("n.email"))
    assert str(expr) == "n.email IS NOT NULL"


def test_in_predicate():
    q = CypherQuery()
    lst = q.add_param(["a", "b", "c"])
    expr = InPredicate(raw("n.type"), lst)
    assert str(expr) == "n.type IN $p0"


# ── 4. Subquery expressions ───────────────────────────────────────────────────

def test_exists_returns_typed_expression():
    from rdflib_neo4j.sparql.cypher_builder import Expression
    q = CypherQuery()
    inner = q.subquery().match("(n)-[:KNOWS]->(m)")
    expr = q.exists_subquery(inner)
    assert isinstance(expr, ExistsExpression)
    assert isinstance(expr, Expression)


def test_exists_is_a_predicate():
    q = CypherQuery()
    inner = q.subquery().match("(n)-[:KNOWS]->(m)")
    expr = q.exists_subquery(inner)
    # ExistsExpression is both a SubqueryExpression and a Predicate
    from rdflib_neo4j.sparql.cypher_builder import Predicate
    assert isinstance(expr, Predicate)


def test_not_exists_returns_typed_expression():
    q = CypherQuery()
    inner = q.subquery().match("(n)-[:BLOCKED]->(m)")
    expr = q.not_exists_subquery(inner)
    assert isinstance(expr, NotExistsExpression)
    assert str(expr).startswith("NOT EXISTS {")


def test_count_returns_typed_expression():
    q = CypherQuery()
    inner = q.subquery().match("(n)-[:KNOWS]->(m)").return_("m")
    expr = q.count_subquery(inner)
    assert isinstance(expr, CountExpression)
    assert str(expr).startswith("COUNT {")


def test_collect_returns_typed_expression():
    q = CypherQuery()
    inner = q.subquery().match("(n)-[:TAGGED]->(t)").return_("t.name")
    expr = q.collect_subquery(inner)
    assert isinstance(expr, CollectExpression)
    assert str(expr).startswith("COLLECT {")


def test_exists_composes_with_and():
    """ExistsExpression is a Predicate, so it composes with .and_()."""
    q = CypherQuery()
    inner = q.subquery().match("(n)-[:KNOWS]->(m)")
    age = q.add_param(30)
    combined = q.exists_subquery(inner).and_(Comparison(raw("n.age"), ">", age))
    assert isinstance(combined, AndPredicate)
    assert "EXISTS {" in str(combined)
    assert "n.age > $p0" in str(combined)


def test_not_exists_composes_with_or():
    q = CypherQuery()
    i1 = q.subquery().match("(n)-[:A]->(m)")
    i2 = q.subquery().match("(n)-[:B]->(m)")
    combined = q.not_exists_subquery(i1).or_(q.not_exists_subquery(i2))
    assert isinstance(combined, OrPredicate)
    assert str(combined).count("NOT EXISTS") == 2


# ── 5. Shared param namespace ─────────────────────────────────────────────────

def test_subquery_shares_root_counter():
    q = CypherQuery()
    p0 = q.add_param("outer_first")
    inner = q.subquery()
    p1 = inner.add_param("inner_val")
    p2 = q.add_param("outer_third")
    assert str(p0) == "$p0"
    assert str(p1) == "$p1"
    assert str(p2) == "$p2"
    assert q._params == {"p0": "outer_first", "p1": "inner_val", "p2": "outer_third"}


def test_inner_param_visible_in_outer_render():
    q = CypherQuery()
    inner = q.subquery()
    ref = inner.add_param("active")
    inner.match("(n)-[:KNOWS]->(m)").where(Comparison(raw("m.status"), "=", ref))
    cypher, params = (
        q.match("(n:Node)")
         .where(q.exists_subquery(inner))
         .return_("n")
         .render()
    )
    assert "$p0" in cypher
    assert params["p0"] == "active"


def test_optional_call_inner_param_in_outer():
    q = CypherQuery()
    inner = q.subquery()
    ref = inner.add_param("http://tag.example/")
    inner.match(f"(n)-[:HAS_TAG]->(t {{uri: {ref}}})").return_("t")
    cypher, params = (
        q.match("(n:Post)")
         .optional_call(["n"], inner)
         .return_("n", "t")
         .render()
    )
    assert "$p0" in cypher
    assert params["p0"] == "http://tag.example/"


# ── 6. WHERE with typed predicates ───────────────────────────────────────────

def test_where_with_raw_predicate():
    q = CypherQuery()
    cypher, _ = q.match("(n)").where(pred("n.active = true")).return_("n").render()
    assert "WHERE n.active = true" in cypher


def test_where_composed_with_and():
    """Compose predicates with .and_() before passing to where() — one WhereClause."""
    q = CypherQuery()
    p1 = q.add_param("Alice")
    p2 = q.add_param(True)
    cypher, params = (
        q.match("(n:Person)")
         .where(Comparison(raw("n.name"), "=", p1).and_(Comparison(raw("n.active"), "=", p2)))
         .return_("n")
         .render()
    )
    assert "AND" in cypher
    assert "n.name = $p0" in cypher
    assert "n.active = $p1" in cypher


def test_where_separate_calls_emit_separate_clauses():
    """Two where() calls produce two WhereClause entries in the pipeline."""
    q = CypherQuery()
    p1 = q.add_param("Alice")
    p2 = q.add_param(True)
    cypher, _ = (
        q.match("(n:Person)")
         .where(Comparison(raw("n.name"), "=", p1))
         .where(Comparison(raw("n.active"), "=", p2))
         .return_("n")
         .render()
    )
    assert cypher.count("WHERE") == 2


def test_match_inline_where():
    q = CypherQuery()
    p = q.add_param("Alice")
    cypher, _ = (
        q.match(f"(n:Resource {{uri: {p}}})", where=RawPredicate("n.active = true"))
         .return_("n")
         .render()
    )
    assert "WHERE n.active = true" in cypher
    assert "$p0" in cypher


# ── 7. CALL and OPTIONAL CALL ────────────────────────────────────────────────

def test_call_basic():
    q = CypherQuery()
    inner = q.subquery().match("(n)-[:KNOWS]->(m)").return_("m")
    cypher, _ = (
        q.match("(n:Person)")
         .call(["n"], inner)
         .return_("n", "m")
         .render()
    )
    assert "CALL (n) {" in cypher
    assert "OPTIONAL CALL" not in cypher
    assert "RETURN n, m" in cypher


def test_optional_call_basic():
    q = CypherQuery()
    inner = q.subquery()
    inner.match("(article)-[:HAS_AUTHOR]->(author:Person)").return_("author")
    cypher, _ = (
        q.match("(article:Article)")
         .optional_call(["article"], inner)
         .return_("article", "author")
         .render()
    )
    assert "OPTIONAL CALL (article) {" in cypher
    assert "MATCH (article)-[:HAS_AUTHOR]->(author:Person)" in cypher
    assert "RETURN article, author" in cypher


def test_optional_call_multiple_imports():
    q = CypherQuery()
    inner = q.subquery().match("(a)-[:KNOWS]->(b)").return_("b")
    cypher, _ = (
        q.match("(a:Person)").match("(b:Person)")
         .optional_call(["a", "b"], inner)
         .return_("a", "b")
         .render()
    )
    assert "OPTIONAL CALL (a, b) {" in cypher


def test_call_and_optional_call_ordering_preserved():
    """CALL and OPTIONAL CALL interleaved must appear in declaration order."""
    q = CypherQuery()
    i1 = q.subquery().match("(n)-[:A]->(a)").return_("a")
    i2 = q.subquery().match("(n)-[:B]->(b)").return_("b")
    i3 = q.subquery().match("(n)-[:C]->(c)").return_("c")
    cypher, _ = (
        q.match("(n:Node)")
         .call(["n"], i1)
         .optional_call(["n", "a"], i2)
         .call(["n", "b"], i3)
         .return_("n", "a", "b", "c")
         .render()
    )
    call_pos = cypher.index("CALL (n)")
    opt_pos = cypher.index("OPTIONAL CALL")
    call2_pos = cypher.rindex("CALL (n, b)")
    assert call_pos < opt_pos < call2_pos


# ── 8. WITH chain ─────────────────────────────────────────────────────────────

def test_with_basic():
    q = CypherQuery()
    cypher, _ = (
        q.match("(n:Person)")
         .with_("n", "n.name AS name")
         .return_("name")
         .render()
    )
    assert "WITH n, n.name AS name" in cypher


def test_with_typed_where():
    q = CypherQuery()
    p = q.add_param(18)
    cypher, params = (
        q.match("(n:Person)")
         .with_("n", where=Comparison(raw("n.age"), ">=", p))
         .return_("n")
         .render()
    )
    assert "WHERE n.age >= $p0" in cypher
    assert params["p0"] == 18


def test_with_collect_expression():
    q = CypherQuery()
    inner = q.subquery().match("(n)-[:TAGGED]->(t)").return_("t.name")
    coll = q.collect_subquery(inner)
    cypher, _ = (
        q.match("(n:Article)")
         .with_("n", coll)
         .return_("n.title AS title")
         .render()
    )
    assert "COLLECT {" in cypher
    assert "WITH n," in cypher


def test_with_distinct():
    q = CypherQuery()
    cypher, _ = (
        q.match("(n:Person)")
         .with_("n.name AS name", distinct=True)
         .return_("name")
         .render()
    )
    assert "WITH DISTINCT n.name AS name" in cypher


def test_with_order_by_skip_limit():
    q = CypherQuery()
    cypher, _ = (
        q.match("(n:Person)")
         .with_("n", AliasExpression(PropertyAccess("n", "age"), "age"),
                order_by=[OrderItem(Var("age"), ascending=False)], skip=5, limit=10)
         .return_("n")
         .render()
    )
    assert "ORDER BY age DESC" in cypher
    assert "SKIP 5" in cypher
    assert "LIMIT 10" in cypher


def test_with_aggregation_then_where():
    """WITH with aggregate expression followed by WHERE (HAVING equivalent)."""
    q = CypherQuery()
    inner = q.subquery().match("(n)-[:KNOWS]->(m)").return_("m")
    cnt = q.count_subquery(inner)
    threshold = q.add_param(5)
    cypher, params = (
        q.match("(n:Person)")
         .with_("n", cnt, where=Comparison(RawExpression("cnt"), ">", threshold))
         .return_("n.name AS name", "cnt")
         .render()
    )
    assert "COUNT {" in cypher
    assert "WHERE cnt > $p0" in cypher
    assert params["p0"] == 5


def test_with_all_modifiers():
    q = CypherQuery()
    cypher, _ = (
        q.match("(n:Item)")
         .with_(AliasExpression(PropertyAccess("n", "category"), "cat"),
                AliasExpression(FunctionCall("count", RawExpression("*")), "cnt"),
                distinct=True, order_by=[OrderItem(Var("cnt"), ascending=False)],
                skip=0, limit=20, where=RawPredicate("cnt > 1"))
         .return_("cat", "cnt")
         .render()
    )
    assert "WITH DISTINCT" in cypher
    assert "ORDER BY cnt DESC" in cypher
    assert "SKIP 0" in cypher
    assert "LIMIT 20" in cypher
    assert "WHERE cnt > 1" in cypher


# ── 9. RETURN modifiers ───────────────────────────────────────────────────────

def test_return_distinct():
    q = CypherQuery()
    cypher, _ = q.match("(n:Tag)").return_("n.name", distinct=True).render()
    assert "RETURN DISTINCT n.name" in cypher


def test_return_order_skip_limit():
    q = CypherQuery()
    cypher, _ = (
        q.match("(n)")
         .return_("n", order_by=[OrderItem(PropertyAccess("n", "age"), ascending=False)], skip=10, limit=5)
         .render()
    )
    assert "ORDER BY n.age DESC" in cypher
    assert "SKIP 10" in cypher
    assert "LIMIT 5" in cypher


def test_return_expression_item():
    q = CypherQuery()
    inner = q.subquery().match("(n)-[:KNOWS]->(m)").return_("m")
    cnt = q.count_subquery(inner)
    cypher, _ = q.match("(n:Person)").return_(cnt).render()
    assert "COUNT {" in cypher


# ── 10. UNION / UNION ALL ─────────────────────────────────────────────────────

def test_union_basic():
    q1 = CypherQuery()
    q1.match("(n:Person)").return_("n.name AS name")
    q2 = CypherQuery()
    q2.match("(n:Robot)").return_("n.name AS name")
    q1.union(q2)
    cypher, _ = q1.render()
    assert "UNION" in cypher and "UNION ALL" not in cypher
    assert "MATCH (n:Person)" in cypher
    assert "MATCH (n:Robot)" in cypher


def test_union_all():
    q1 = CypherQuery()
    q1.match("(n:A)").return_("n")
    q2 = CypherQuery()
    q2.match("(n:B)").return_("n")
    q1.union(q2, all=True)
    assert "UNION ALL" in q1.render_str()


def test_union_no_param_collision():
    q1 = CypherQuery()
    p1 = q1.add_param("left_value")
    q1.match(f"(n:Left {{x: {p1}}})").return_("n")

    q2 = CypherQuery()
    p2 = q2.add_param("right_value")
    q2.match(f"(n:Right {{x: {p2}}})").return_("n")

    q1.union(q2)
    cypher, params = q1.render()
    assert "left_value" not in cypher
    assert "right_value" not in cypher
    assert len(params) == 2
    assert set(params.values()) == {"left_value", "right_value"}
    assert len(set(params.keys())) == 2


# ── 11. Idempotency ───────────────────────────────────────────────────────────

def test_render_idempotent():
    q = CypherQuery()
    p = q.add_param("test")
    q.match(f"(n {{x: {p}}})").where(pred("n.active = true")).return_("n", limit=10)
    assert q.render() == q.render()


def test_render_idempotent_union():
    q1 = CypherQuery()
    q1.match(f"(n:Left {{x: {q1.add_param('lv')}}})").return_("n")
    q2 = CypherQuery()
    q2.match(f"(n:Right {{x: {q2.add_param('rv')}}})").return_("n")
    q1.union(q2)
    r1 = q1.render()
    r2 = q1.render()
    assert r1 == r2


# ── 12. Complex multi-feature query ──────────────────────────────────────────

def test_complex_query():
    """Realistic SPARQL-like query using most DSL features."""
    q = CypherQuery()
    uri = q.add_param("http://example.org/topic/AI")
    min_score = q.add_param(0.8)

    call_inner = q.subquery()
    call_inner.match("(article)-[:HAS_AUTHOR]->(author:Person)").return_("author")

    exists_inner = q.subquery().match("(article)-[:TAGGED]->(t:Tag {name: 'AI'})")
    cnt_inner = q.subquery().match("(article)-[:HAS_CITATION]->(c)").return_("c")

    cypher, params = (
        q.match(f"(topic:Topic {{uri: {uri}}})")
         .match("(article:Article)-[:ABOUT]->(topic)")
         .optional_call(["article"], call_inner)
         .where(
             q.exists_subquery(exists_inner)
              .and_(Comparison(raw("article.score"), ">", min_score))
         )
         .with_("article", "author", q.count_subquery(cnt_inner))
         .return_(
             "article.title AS title",
             "author.name AS author",
             distinct=True,
             order_by=[OrderItem(PropertyAccess("article", "score"), ascending=False)],
             skip=0,
             limit=25,
         )
         .render()
    )

    assert "CYPHER 25" in cypher
    assert "EXISTS {" in cypher
    assert "AND article.score > $p1" in cypher
    assert "COUNT {" in cypher
    assert "OPTIONAL CALL (article) {" in cypher
    assert "RETURN DISTINCT" in cypher
    assert "ORDER BY article.score DESC" in cypher
    assert "LIMIT 25" in cypher
    assert "http://example.org/topic/AI" not in cypher
    assert params["p0"] == "http://example.org/topic/AI"
    assert params["p1"] == 0.8


# ── 13. render_str ────────────────────────────────────────────────────────────

def test_render_str():
    q = CypherQuery()
    s = q.match("(n)").return_("n").render_str()
    assert isinstance(s, str)
    assert "MATCH (n)" in s


# ── 14. Dataclass sanity ──────────────────────────────────────────────────────

def test_match_clause_defaults():
    m = MatchClause("(n:Person)")
    assert m.where is None


def test_return_clause_defaults():
    r = ReturnClause([RawExpression("n")])
    assert r.distinct is False
    assert r.order_by == []
    assert r.skip is None
    assert r.limit is None


def test_with_clause_defaults():
    w = WithClause([RawExpression("n")])
    assert w.where is None


def test_call_clause_dataclass():
    q = CypherQuery()
    inner = q.subquery().match("(a)-[r]->(b)").return_("r")
    c = CallClause(["a", "b"], inner)
    assert c.imports == ["a", "b"]


def test_optional_call_clause_dataclass():
    q = CypherQuery()
    inner = q.subquery().match("(a)-[r]->(b)").return_("r")
    oc = OptionalCallClause(["a", "b"], inner)
    assert oc.imports == ["a", "b"]


# ── 15. LabelExpr ─────────────────────────────────────────────────────────────

def test_label_atom():
    assert LabelAtom("Person")._cypher() == "Person"
    assert str(LabelAtom("Person")) == "Person"


def test_label_and_two():
    assert LabelAnd(LabelAtom("A"), LabelAtom("B"))._cypher() == "A&B"


def test_label_and_operator():
    assert (LabelAtom("A") & LabelAtom("B"))._cypher() == "A&B"


def test_label_or_two():
    assert LabelOr(LabelAtom("A"), LabelAtom("B"))._cypher() == "(A|B)"


def test_label_or_operator():
    assert (LabelAtom("A") | LabelAtom("B"))._cypher() == "(A|B)"


def test_label_or_single():
    assert LabelOr(LabelAtom("A"))._cypher() == "A"


def test_label_not():
    assert LabelNot(LabelAtom("Robot"))._cypher() == "!Robot"


def test_label_not_operator():
    assert (~LabelAtom("Robot"))._cypher() == "!Robot"


def test_label_complex_expression():
    # (A|B)&!C
    expr = (LabelAtom("A") | LabelAtom("B")) & ~LabelAtom("C")
    assert expr._cypher() == "(A|B)&!C"


# ── 16. NodePattern ───────────────────────────────────────────────────────────

def test_node_pattern_basic():
    n = Var("n")
    assert NodePattern(n, ["Person"])._cypher() == "(n:Person)"


def test_node_pattern_multi_label_list():
    n = Var("n")
    assert NodePattern(n, ["Person", "Employee"])._cypher() == "(n:Person&Employee)"


def test_node_pattern_label_expr_or():
    n = Var("n")
    expr = LabelAtom("Person") | LabelAtom("Robot")
    assert NodePattern(n, expr)._cypher() == "(n:(Person|Robot))"


def test_node_pattern_label_expr_not():
    n = Var("n")
    expr = LabelAtom("Resource") & ~LabelAtom("Deprecated")
    assert NodePattern(n, expr)._cypher() == "(n:Resource&!Deprecated)"


def test_node_pattern_empty_labels_raises():
    import pytest
    with pytest.raises(ValueError, match="at least one label"):
        NodePattern(Var("n"), [])


def test_node_pattern_with_props():
    q = CypherQuery()
    p = q.add_param("http://example.org/Alice")
    n = Var("n")
    node = NodePattern(n, ["Resource"], props={"uri": p})
    assert node._cypher() == "(n:Resource {uri: $p0})"


def test_node_pattern_with_where():
    n = Var("n")
    node = NodePattern(n, ["Person"], where=RawPredicate("n.age > 18"))
    assert node._cypher() == "(n:Person WHERE n.age > 18)"


def test_node_pattern_in_match():
    q = CypherQuery()
    n = Var("n")
    cypher, _ = q.match(NodePattern(n, ["Resource"])).return_("n").render()
    assert "MATCH (n:Resource)" in cypher


# ── 17. AnonNode ──────────────────────────────────────────────────────────────

def test_anon_node_empty():
    assert AnonNode()._cypher() == "()"


def test_anon_node_with_var():
    assert AnonNode(Var("m"))._cypher() == "(m)"


def test_anon_node_with_label():
    assert AnonNode(Var("m"), LabelAtom("Person"))._cypher() == "(m:Person)"


def test_anon_node_with_where():
    assert AnonNode(Var("m"), where=RawPredicate("m.x > 5"))._cypher() == "(m WHERE m.x > 5)"


# ── 18. RelSegment ────────────────────────────────────────────────────────────

def test_rel_segment_basic_out():
    assert RelSegment(types=["KNOWS"])._cypher() == "-[:KNOWS]->"


def test_rel_segment_in():
    assert RelSegment(direction="<-", types=["KNOWS"])._cypher() == "<-[:KNOWS]-"


def test_rel_segment_both():
    assert RelSegment(direction="--", types=["KNOWS"])._cypher() == "-[:KNOWS]-"


def test_rel_segment_multi_type():
    assert RelSegment(types=["R1", "R2"])._cypher() == "-[:R1|R2]->"


def test_rel_segment_with_var():
    assert RelSegment(types=["KNOWS"], var=Var("r"))._cypher() == "-[r:KNOWS]->"


def test_rel_segment_any_type():
    assert RelSegment()._cypher() == "-[]->"


def test_rel_segment_star_hops():
    assert RelSegment(types=["R"], hops=(0, None))._cypher() == "-[:R*]->"


def test_rel_segment_bounded_hops():
    assert RelSegment(types=["R"], hops=(1, 5))._cypher() == "-[:R*1..5]->"


def test_rel_segment_min_only_hops():
    assert RelSegment(types=["R"], hops=(1, None))._cypher() == "-[:R*1..]->"


def test_rel_segment_with_where():
    r = RelSegment(types=["KNOWS"], var=Var("r"), where=RawPredicate("r.since > 2020"))
    assert r._cypher() == "-[r:KNOWS WHERE r.since > 2020]->"


# ── 19. PathPattern ───────────────────────────────────────────────────────────

def test_path_pattern_single_node():
    n = Var("n")
    assert PathPattern(NodePattern(n, ["Person"]))._cypher() == "(n:Person)"


def test_path_pattern_one_hop():
    n, m = Var("n"), Var("m")
    path = PathPattern(NodePattern(n, ["Person"])).rel(RelSegment(types=["KNOWS"]), AnonNode(m))
    assert path._cypher() == "(n:Person)-[:KNOWS]->(m)"


def test_path_pattern_multi_hop():
    n, m, o = Var("n"), Var("m"), Var("o")
    path = (PathPattern(NodePattern(n, ["Person"]))
            .rel(RelSegment(types=["KNOWS"]), AnonNode(m))
            .rel(RelSegment(types=["LIKES"]), AnonNode(o)))
    assert path._cypher() == "(n:Person)-[:KNOWS]->(m)-[:LIKES]->(o)"


def test_path_pattern_in_match():
    q = CypherQuery()
    n, m = Var("n"), Var("m")
    path = PathPattern(NodePattern(n, ["Person"])).rel(RelSegment(types=["KNOWS"]), AnonNode(m))
    cypher, _ = q.match(path).return_("n", "m").render()
    assert "MATCH (n:Person)-[:KNOWS]->(m)" in cypher


def test_path_pattern_incoming():
    n, m = Var("n"), Var("m")
    path = PathPattern(NodePattern(n, ["Person"])).rel(
        RelSegment(direction="<-", types=["FOLLOWS"]), AnonNode(m)
    )
    assert path._cypher() == "(n:Person)<-[:FOLLOWS]-(m)"


# ── 20. QPPPattern ────────────────────────────────────────────────────────────

def test_qpp_pattern_unbounded():
    a, b = Var("a"), Var("b")
    inner = PathPattern(AnonNode(a)).rel(RelSegment(types=["R"]), AnonNode(b))
    assert QPPPattern(inner, min_=1)._cypher() == "((a)-[:R]->(b)){1,}"


def test_qpp_pattern_bounded():
    a, b = Var("a"), Var("b")
    inner = PathPattern(AnonNode(a)).rel(RelSegment(types=["R"]), AnonNode(b))
    assert QPPPattern(inner, min_=0, max_=5)._cypher() == "((a)-[:R]->(b)){0,5}"


def test_qpp_pattern_in_match():
    q = CypherQuery()
    s, e = Var("s"), Var("e")
    a, b = Var("a"), Var("b")
    inner = PathPattern(AnonNode(a)).rel(RelSegment(types=["LINK"]), AnonNode(b))
    qpp = QPPPattern(inner, min_=1, max_=3)
    path = PathPattern(NodePattern(s, ["Resource"])).rel(qpp, AnonNode(e))
    cypher, _ = q.match(path).return_("s", "e").render()
    assert "MATCH (s:Resource)((a)-[:LINK]->(b)){1,3}(e)" in cypher


# ── 21. Typed expression building blocks ─────────────────────────────────────

def test_var():
    assert Var("name")._cypher() == "name"
    assert str(Var("cnt")) == "cnt"
    assert f"{Var('n')}" == "n"


def test_property_access():
    n = Var("n")
    assert PropertyAccess(n, Property("Node", "name"))._cypher() == "n.name"
    article = Var("article")
    assert str(PropertyAccess(article, Property("Article", "score"))) == "article.score"


def test_alias_expression():
    n = Var("n")
    assert AliasExpression(PropertyAccess(n, Property("Node", "name")), "name")._cypher() == "n.name AS name"
    assert AliasExpression(Var("cnt"), "count")._cypher() == "cnt AS count"


def test_function_call_no_args():
    assert FunctionCall("rand")._cypher() == "rand()"


def test_function_call_one_arg():
    assert FunctionCall("toUpper", PropertyAccess("n", "name"))._cypher() == "toUpper(n.name)"


def test_function_call_multi_arg():
    assert FunctionCall("substring", Var("s"), RawExpression("0"), RawExpression("3"))._cypher() == "substring(s, 0, 3)"


def test_function_call_distinct():
    assert FunctionCall("count", Var("n"), distinct=True)._cypher() == "count(DISTINCT n)"


def test_function_call_star():
    assert FunctionCall("count", RawExpression("*"))._cypher() == "count(*)"


def test_order_item_asc():
    assert OrderItem(Var("name"))._cypher() == "name ASC"


def test_order_item_desc():
    assert OrderItem(PropertyAccess("n", "age"), ascending=False)._cypher() == "n.age DESC"


def test_order_item_in_return():
    q = CypherQuery()
    cypher, _ = (
        q.match(NodePattern("n", ["Person"]))
         .return_(AliasExpression(PropertyAccess("n", "name"), "name"),
                  order_by=[OrderItem(Var("name"))])
         .render()
    )
    assert "ORDER BY name ASC" in cypher


# ── 22. SPARQL example queries — fully typed ──────────────────────────────────
#
# Each test corresponds to one of the 10 worked examples in findings/sparql-to-cypher.md
# Built entirely from typed DSL nodes — no raw strings except escape-hatch RawExpression.

def test_sparql_ex1_simple_property_lookup():
    """SELECT ?name WHERE { <alice> foaf:name ?name }"""
    q = CypherQuery()
    uri = q.add_param("http://example.org/alice")
    cypher, params = (
        q.match(NodePattern("s", ["Resource"], props={"uri": uri}))
         .return_(AliasExpression(PropertyAccess("s", "name"), "name"))
         .render()
    )
    assert "MATCH (s:Resource {uri: $p0})" in cypher
    assert "RETURN s.name AS name" in cypher
    assert params == {"p0": "http://example.org/alice"}


def test_sparql_ex2_type_and_filter():
    """SELECT ?person ?name WHERE { ?person a foaf:Person ; foaf:name ?n ; foaf:age ?a FILTER(?a < 40) }"""
    q = CypherQuery()
    threshold = q.add_param(40)
    cypher, params = (
        q.match(NodePattern("person", ["Person"]),
                where=Comparison(PropertyAccess("person", "age"), "<", threshold))
         .return_(AliasExpression(Var("person"), "person"),
                  AliasExpression(PropertyAccess("person", "name"), "name"))
         .render()
    )
    assert "MATCH (person:Person)" in cypher
    assert "WHERE person.age < $p0" in cypher
    assert "RETURN person AS person, person.name AS name" in cypher
    assert params == {"p0": 40}


def test_sparql_ex3_relationship_join():
    """SELECT ?person ?org WHERE { ?person a foaf:Person ; ex:worksAt ?org }"""
    q = CypherQuery()
    path = PathPattern(NodePattern("person", ["Person"])).rel(
        RelSegment(types=["WORKS_AT"]),
        NodePattern("org", ["Organization"]),
    )
    cypher, _ = (
        q.match(path)
         .return_(AliasExpression(Var("person"), "person"),
                  AliasExpression(Var("org"), "org"),
                  AliasExpression(PropertyAccess("org", "name"), "orgName"))
         .render()
    )
    assert "MATCH (person:Person)-[:WORKS_AT]->(org:Organization)" in cypher
    assert "RETURN person AS person, org AS org, org.name AS orgName" in cypher


def test_sparql_ex4_optional_relationship():
    """SELECT ?person ?name ?employer WHERE { ?person foaf:name ?name OPTIONAL { ?person ex:worksAt ?employer } }"""
    q = CypherQuery()
    inner = q.subquery()
    inner.match(
        PathPattern(AnonNode("person")).rel(
            RelSegment(types=["WORKS_AT"]),
            NodePattern("employer", ["Organization"]),
        )
    ).return_(AliasExpression(Var("employer"), "employer"))
    cypher, _ = (
        q.match(NodePattern("person", ["Person"]))
         .optional_call(["person"], inner)
         .return_(AliasExpression(Var("person"), "person"),
                  AliasExpression(PropertyAccess("person", "name"), "name"),
                  AliasExpression(Var("employer"), "employer"))
         .render()
    )
    assert "MATCH (person:Person)" in cypher
    assert "OPTIONAL CALL (person) {" in cypher
    assert "MATCH (person)-[:WORKS_AT]->(employer:Organization)" in cypher
    assert "employer AS employer" in cypher


def test_sparql_ex5_optional_with_filter():
    """OPTIONAL { ?person foaf:knows ?colleague FILTER(?colleague != ?person) }"""
    q = CypherQuery()
    inner = q.subquery()
    inner.match(
        PathPattern(AnonNode("person")).rel(
            RelSegment(types=["KNOWS"]),
            NodePattern("colleague", ["Person"]),
        ),
        where=Comparison(Var("colleague"), "<>", Var("person")),
    ).return_(AliasExpression(Var("colleague"), "colleague"))
    cypher, _ = (
        q.match(NodePattern("person", ["Person"]))
         .optional_call(["person"], inner)
         .return_(AliasExpression(Var("person"), "person"),
                  AliasExpression(Var("colleague"), "colleague"))
         .render()
    )
    assert "OPTIONAL CALL (person) {" in cypher
    assert "MATCH (person)-[:KNOWS]->(colleague:Person)" in cypher
    assert "WHERE colleague <> person" in cypher


def test_sparql_ex6_union():
    """{ ?e a foaf:Person ; foaf:name ?n } UNION { ?e a ex:Organization ; ex:orgName ?n }"""
    q1 = CypherQuery()
    q1.match(NodePattern("entity", ["Person"])).return_(
        AliasExpression(Var("entity"), "entity"),
        AliasExpression(PropertyAccess("entity", "name"), "name"),
    )
    q2 = CypherQuery()
    q2.match(NodePattern("entity", ["Organization"])).return_(
        AliasExpression(Var("entity"), "entity"),
        AliasExpression(PropertyAccess("entity", "orgName"), "name"),
    )
    q1.union(q2)
    cypher, _ = q1.render()
    assert "MATCH (entity:Person)" in cypher
    assert "MATCH (entity:Organization)" in cypher
    assert "UNION" in cypher
    assert "entity.name AS name" in cypher
    assert "entity.orgName AS name" in cypher


def test_sparql_ex7_distinct_order_limit():
    """SELECT DISTINCT ?name ORDER BY ?name LIMIT 10 OFFSET 20"""
    q = CypherQuery()
    cypher, _ = (
        q.match(NodePattern("s", ["Person"]))
         .return_(
             AliasExpression(PropertyAccess("s", "name"), "name"),
             distinct=True,
             order_by=[OrderItem(Var("name"))],
             skip=20,
             limit=10,
         )
         .render()
    )
    assert "MATCH (s:Person)" in cypher
    assert "RETURN DISTINCT s.name AS name" in cypher
    assert "ORDER BY name ASC" in cypher
    assert "SKIP 20" in cypher
    assert "LIMIT 10" in cypher


def test_sparql_ex8_bind_extend():
    """BIND(UCASE(?name) AS ?upperName)"""
    q = CypherQuery()
    cypher, _ = (
        q.match(NodePattern("person", ["Person"]))
         .with_(
             AliasExpression(Var("person"), "person"),
             AliasExpression(PropertyAccess("person", "name"), "name"),
             AliasExpression(
                 FunctionCall("toUpper", PropertyAccess("person", "name")),
                 "upperName",
             ),
         )
         .return_(
             AliasExpression(Var("person"), "person"),
             AliasExpression(Var("name"), "name"),
             AliasExpression(Var("upperName"), "upperName"),
         )
         .render()
    )
    assert "toUpper(person.name) AS upperName" in cypher
    assert "RETURN person AS person, name AS name, upperName AS upperName" in cypher


def test_sparql_ex9_aggregation_having():
    """SELECT ?dept COUNT(?person) AVG(?age) HAVING count > 5 ORDER BY count DESC"""
    q = CypherQuery()
    threshold = q.add_param(5)
    cypher, params = (
        q.match(NodePattern("person", ["Person"]))
         .with_(
             AliasExpression(PropertyAccess("person", "department"), "dept"),
             AliasExpression(FunctionCall("count", Var("person")), "cnt"),
             AliasExpression(FunctionCall("avg", PropertyAccess("person", "age")), "avgAge"),
             where=Comparison(Var("cnt"), ">", threshold),
         )
         .return_(
             AliasExpression(Var("dept"), "dept"),
             AliasExpression(Var("cnt"), "cnt"),
             AliasExpression(Var("avgAge"), "avgAge"),
             order_by=[OrderItem(Var("cnt"), ascending=False)],
         )
         .render()
    )
    assert "WITH person.department AS dept" in cypher
    assert "count(person) AS cnt" in cypher
    assert "avg(person.age) AS avgAge" in cypher
    assert "WHERE cnt > $p0" in cypher
    assert "ORDER BY cnt DESC" in cypher
    assert params == {"p0": 5}


def test_sparql_ex10_property_path_one_or_more():
    """?person foaf:knows+ ?reachable  →  QPP {1,} inline path"""
    q = CypherQuery()
    person, reachable = Var("person"), Var("reachable")
    n, m = Var("n"), Var("m")
    qpp = QPPPattern(
        PathPattern(AnonNode(n)).rel(RelSegment(types=["KNOWS"]), AnonNode(m)),
        min_=1,
    )
    path = (
        PathPattern(NodePattern(person, ["Person"]))
        .rel(qpp, NodePattern(reachable, ["Person"]))
    )
    cypher, _ = (
        q.match(path)
         .return_(
             AliasExpression(person, "person"),
             AliasExpression(reachable, "reachable"),
         )
         .render()
    )
    assert "MATCH (person:Person)((n)-[:KNOWS]->(m)){1,}(reachable:Person)" in cypher
    assert "RETURN person AS person, reachable AS reachable" in cypher


# ── 22. UnwindClause ──────────────────────────────────────────────────────────

def test_unwind_clause_render():
    q = CypherQuery()
    values = q.add_param(["Alice", "Bob"])
    cypher, params = q.unwind(values, "name").match("(n)").return_("n").render()
    assert "UNWIND $p0 AS name" in cypher
    assert params == {"p0": ["Alice", "Bob"]}


def test_string_literal():
    assert StringLiteral("adult")._cypher() == '"adult"'
    assert str(StringLiteral("hello")) == '"hello"'


def test_string_literal_escaping():
    assert StringLiteral('say "hi"')._cypher() == r'"say \"hi\""'
    assert StringLiteral("back\\slash")._cypher() == '"back\\\\slash"'


def test_list_expression():
    expr = ListExpression(StringLiteral("Alice"), StringLiteral("Bob"))
    assert expr._cypher() == '["Alice", "Bob"]'


# ── SPARQL examples 11-22 ────────────────────────────────────────────────────

def test_sparql_ex11_sequence_path():
    """?person foaf:knows/ex:worksAt ?org  →  chained .rel() hops"""
    q = CypherQuery()
    person, mid, org = Var("person"), Var("mid"), Var("org")
    path = (
        PathPattern(NodePattern(person, ["Person"]))
        .rel(RelSegment(types=["KNOWS"]), AnonNode(mid))
        .rel(RelSegment(types=["WORKS_AT"]), NodePattern(org, ["Organization"]))
    )
    cypher, _ = (
        q.match(path)
         .return_(AliasExpression(person, "person"), AliasExpression(org, "org"))
         .render()
    )
    assert "MATCH (person:Person)-[:KNOWS]->(mid)-[:WORKS_AT]->(org:Organization)" in cypher
    assert "RETURN person AS person, org AS org" in cypher


def test_sparql_ex12_alternative_path():
    """?person (foaf:knows|foaf:friendOf) ?other  →  RelSegment multi-type"""
    q = CypherQuery()
    person, other = Var("person"), Var("other")
    path = (
        PathPattern(NodePattern(person, ["Person"]))
        .rel(RelSegment(types=["KNOWS", "FRIEND_OF"]), AnonNode(other))
    )
    cypher, _ = q.match(path).return_(AliasExpression(person, "person"), AliasExpression(other, "other")).render()
    assert "MATCH (person:Person)-[:KNOWS|FRIEND_OF]->(other)" in cypher


def test_sparql_ex13_inverse_path():
    """?person ^foaf:follows ?follower  →  direction='<-'"""
    q = CypherQuery()
    person, follower = Var("person"), Var("follower")
    path = (
        PathPattern(NodePattern(person, ["Person"]))
        .rel(RelSegment(direction="<-", types=["FOLLOWS"]), AnonNode(follower))
    )
    cypher, _ = q.match(path).return_(AliasExpression(follower, "follower")).render()
    assert "MATCH (person:Person)<-[:FOLLOWS]-(follower)" in cypher


def test_sparql_ex14_zero_or_more_path():
    """?person foaf:knows* ?reachable  →  QPP {0,}"""
    q = CypherQuery()
    person, reachable = Var("person"), Var("reachable")
    n, m = Var("n"), Var("m")
    qpp = QPPPattern(
        PathPattern(AnonNode(n)).rel(RelSegment(types=["KNOWS"]), AnonNode(m)),
        min_=0,
    )
    path = (
        PathPattern(NodePattern(person, ["Person"]))
        .rel(qpp, NodePattern(reachable, ["Person"]))
    )
    cypher, _ = (
        q.match(path)
         .return_(AliasExpression(person, "person"), AliasExpression(reachable, "reachable"))
         .render()
    )
    assert "MATCH (person:Person)((n)-[:KNOWS]->(m)){0,}(reachable:Person)" in cypher


def test_sparql_ex15_filter_exists():
    """FILTER(EXISTS { ?person foaf:knows ?anyone })  →  ExistsExpression"""
    q = CypherQuery()
    person, anyone = Var("person"), Var("anyone")

    exists_inner = q.subquery()
    exists_inner.match(
        PathPattern(AnonNode(person)).rel(RelSegment(types=["KNOWS"]), AnonNode(anyone))
    )

    cypher, _ = (
        q.match(NodePattern(person, ["Person"]), where=q.exists_subquery(exists_inner))
         .return_(AliasExpression(person, "person"))
         .render()
    )
    assert "MATCH (person:Person)" in cypher
    assert "WHERE EXISTS { MATCH (person)-[:KNOWS]->(anyone) }" in cypher
    assert "RETURN person AS person" in cypher


def test_sparql_ex16_minus_not_exists():
    """MINUS { ?person ex:blocked ?_ }  →  NOT EXISTS subquery"""
    q = CypherQuery()
    person = Var("person")

    minus_inner = q.subquery()
    minus_inner.match(
        PathPattern(AnonNode(person)).rel(RelSegment(types=["BLOCKED"]), AnonNode())
    )

    cypher, _ = (
        q.match(NodePattern(person, ["Person"]))
         .where(q.not_exists_subquery(minus_inner))
         .return_(AliasExpression(person, "person"))
         .render()
    )
    assert "MATCH (person:Person)" in cypher
    assert "WHERE NOT EXISTS { MATCH (person)-[:BLOCKED]->() }" in cypher
    assert "RETURN person AS person" in cypher


def test_sparql_ex17_subselect_call():
    """SELECT ?dept (COUNT(*) AS ?deptCount) ... GROUP BY ?dept  →  correlated CALL (dept) {}"""
    q = CypherQuery()
    person, dept, dept_count = Var("person"), Var("dept"), Var("deptCount")
    x = Var("x")

    q.match(NodePattern(person, ["Person"])) \
     .with_(
         AliasExpression(person, "person"),
         AliasExpression(PropertyAccess(person, Property("Person", "department")), "dept"),
     )

    sub = q.subquery()
    sub.match(
        NodePattern(x, ["Person"]),
        where=Comparison(PropertyAccess(x, Property("Person", "department")), "=", dept),
    ) \
      .with_(AliasExpression(FunctionCall("count", RawExpression("*")), "deptCount")) \
      .return_(AliasExpression(dept_count, "deptCount"))

    cypher, _ = (
        q.call(["dept"], sub)
         .return_(AliasExpression(person, "person"),
                  AliasExpression(dept, "dept"),
                  AliasExpression(dept_count, "deptCount"))
         .render()
    )
    assert "WITH person AS person, person.department AS dept" in cypher
    assert "CALL (dept) {" in cypher
    assert "WHERE x.department = dept" in cypher
    assert "count(*) AS deptCount" in cypher
    assert "RETURN person AS person, dept AS dept, deptCount AS deptCount" in cypher


def test_sparql_ex18_values_bound_var_in_predicate():
    """VALUES ?name { ... } where ?name is BGP-bound  →  IN predicate, cardinality 1"""
    q = CypherQuery()
    names = q.add_param(["Alice", "Bob", "Charlie"])
    person = Var("person")
    cypher, params = (
        q.match(NodePattern(person, ["Person"]),
                where=InPredicate(PropertyAccess(person, Property("Person", "name")), names))
         .return_(AliasExpression(person, "person"))
         .render()
    )
    assert "MATCH (person:Person)" in cypher
    assert "WHERE person.name IN $p0" in cypher
    assert "RETURN person AS person" in cypher
    assert params == {"p0": ["Alice", "Bob", "Charlie"]}


def test_sparql_ex18b_values_unbound_var_unwind():
    """VALUES ?x { 1 2 3 } where ?x unbound  →  UNWIND row-source"""
    q = CypherQuery()
    values = q.add_param([1, 2, 3])
    cypher, params = (
        q.unwind(values, "x")
         .return_(AliasExpression(Var("x"), "x"))
         .render()
    )
    assert "UNWIND $p0 AS x" in cypher
    assert "RETURN x AS x" in cypher
    assert params == {"p0": [1, 2, 3]}


def test_sparql_ex19_multi_union():
    """Three-branch UNION  →  chained .union() calls"""
    entity = Var("entity")

    q1 = CypherQuery()
    q1.match(NodePattern(entity, ["Person"])).return_(
        AliasExpression(entity, "entity"),
        AliasExpression(StringLiteral("person"), "type"),
    )
    q2 = CypherQuery()
    q2.match(NodePattern(entity, ["Organization"])).return_(
        AliasExpression(entity, "entity"),
        AliasExpression(StringLiteral("org"), "type"),
    )
    q3 = CypherQuery()
    q3.match(NodePattern(entity, ["Bot"])).return_(
        AliasExpression(entity, "entity"),
        AliasExpression(StringLiteral("bot"), "type"),
    )
    q1.union(q2)
    q2.union(q3)
    cypher, _ = q1.render()
    assert "MATCH (entity:Person)" in cypher
    assert "MATCH (entity:Organization)" in cypher
    assert cypher.count("UNION") >= 2


def test_sparql_ex20_cross_element_predicates():
    """Relationship property + inline WHERE on path node  →  RelSegment(var=...) + NodePattern(where=...)"""
    q = CypherQuery()
    threshold = q.add_param("2020-01-01")
    person, colleague, rel = Var("person"), Var("colleague"), Var("rel")
    since = PropertyAccess(rel, Property("KNOWS", "since"))

    path = (
        PathPattern(NodePattern(person, ["Person"]))
        .rel(
            RelSegment(types=["KNOWS"], var=rel),
            NodePattern(colleague, ["Person"],
                        where=Comparison(colleague, "<>", person)),
        )
    )
    cypher, params = (
        q.match(path, where=Comparison(since, ">", threshold))
         .return_(
             AliasExpression(person, "person"),
             AliasExpression(colleague, "colleague"),
             AliasExpression(since, "since"),
         )
         .render()
    )
    assert "MATCH (person:Person)-[rel:KNOWS]->(colleague:Person WHERE colleague <> person)" in cypher
    assert "WHERE rel.since > $p0" in cypher
    assert "RETURN person AS person, colleague AS colleague, rel.since AS since" in cypher
    assert params == {"p0": "2020-01-01"}


def test_sparql_ex21_full_aggregates():
    """SUM, MIN, GROUP_CONCAT → reduce(s="", n IN collect(...) | CASE WHEN s="" THEN n ELSE s+sep+n END)"""
    from rdflib_neo4j.sparql.cypher_builder import ReduceExpression, WhenClause, CaseExpression
    q = CypherQuery()
    person = Var("person")
    s, n = Var("s"), Var("n")
    names_list = FunctionCall("collect", PropertyAccess(person, Property("Person", "name")))
    group_concat = ReduceExpression(
        acc="s", init=StringLiteral(""),
        var="n", list_=names_list,
        body=CaseExpression(
            WhenClause(Comparison(s, "=", StringLiteral("")), n),
            else_=RawExpression('s + "," + n'),
        ),
    )
    cypher, _ = (
        q.match(NodePattern(person, ["Person"]))
         .with_(
             AliasExpression(PropertyAccess(person, Property("Person", "department")), "dept"),
             AliasExpression(FunctionCall("sum", PropertyAccess(person, Property("Person", "salary"))), "totalSalary"),
             AliasExpression(FunctionCall("min", PropertyAccess(person, Property("Person", "age"))), "minAge"),
             AliasExpression(group_concat, "names"),
         )
         .return_(
             AliasExpression(Var("dept"), "dept"),
             AliasExpression(Var("totalSalary"), "totalSalary"),
             AliasExpression(Var("minAge"), "minAge"),
             AliasExpression(Var("names"), "names"),
         )
         .render()
    )
    assert "sum(person.salary) AS totalSalary" in cypher
    assert "min(person.age) AS minAge" in cypher
    assert 'reduce(s = "", n IN collect(person.name) |' in cypher
    assert 'CASE WHEN s = "" THEN n ELSE s + "," + n END) AS names' in cypher


def test_sparql_ex22_regex_filter():
    """FILTER(REGEX(?name, "^Alice"))  →  =~ operator"""
    q = CypherQuery()
    pattern = q.add_param("(?i)^Alice")
    person = Var("person")
    name = PropertyAccess(person, Property("Person", "name"))
    cypher, params = (
        q.match(NodePattern(person, ["Person"]),
                where=Comparison(name, "=~", pattern))
         .return_(AliasExpression(person, "person"),
                  AliasExpression(name, "name"))
         .render()
    )
    assert "WHERE person.name =~ $p0" in cypher
    assert "RETURN person AS person, person.name AS name" in cypher
    assert params == {"p0": "(?i)^Alice"}


# ── SPARQL examples 23-24: DATATYPE and IF/CASE ───────────────────────────────

def test_sparql_ex23_datatype_is_type():
    """FILTER(DATATYPE(?age) = xsd:integer)  →  IS :: INTEGER"""
    from rdflib_neo4j.sparql.cypher_builder import IsTypePredicate
    q = CypherQuery()
    person = Var("person")
    age = PropertyAccess(person, Property("Person", "age"))
    cypher, _ = (
        q.match(NodePattern(person, ["Person"]), where=IsTypePredicate(age, "INTEGER"))
         .return_(AliasExpression(person, "person"), AliasExpression(age, "age"))
         .render()
    )
    assert "WHERE person.age IS :: INTEGER" in cypher
    assert "RETURN person AS person, person.age AS age" in cypher


def test_is_type_predicate_union():
    """isNumeric(?x)  →  IS :: INTEGER | FLOAT"""
    from rdflib_neo4j.sparql.cypher_builder import IsTypePredicate
    x = Var("x")
    assert IsTypePredicate(x, "INTEGER | FLOAT")._cypher() == "x IS :: INTEGER | FLOAT"


def test_sparql_ex24_if_case_single_branch():
    """IF(?age >= 18, "adult", "minor")  →  CASE WHEN … THEN … ELSE … END"""
    from rdflib_neo4j.sparql.cypher_builder import CaseExpression, WhenClause
    q = CypherQuery()
    threshold = q.add_param(18)
    person = Var("person")
    age = PropertyAccess(person, Property("Person", "age"))
    category = CaseExpression(
        WhenClause(Comparison(age, ">=", threshold), StringLiteral("adult")),
        else_=StringLiteral("minor"),
    )
    cypher, params = (
        q.match(NodePattern(person, ["Person"]))
         .with_(
             AliasExpression(person, "person"),
             AliasExpression(age, "age"),
             AliasExpression(category, "category"),
         )
         .return_(AliasExpression(Var("person"), "person"),
                  AliasExpression(Var("category"), "category"))
         .render()
    )
    assert 'CASE WHEN person.age >= $p0 THEN "adult" ELSE "minor" END AS category' in cypher
    assert params == {"p0": 18}


def test_case_expression_multi_branch():
    """Nested IF → multi-branch CASE WHEN"""
    from rdflib_neo4j.sparql.cypher_builder import CaseExpression, WhenClause
    q = CypherQuery()
    p90, p70 = q.add_param(90), q.add_param(70)
    score = PropertyAccess(Var("s"), Property("Exam", "score"))
    grade = CaseExpression(
        WhenClause(Comparison(score, ">", p90), StringLiteral("A")),
        WhenClause(Comparison(score, ">", p70), StringLiteral("B")),
        else_=StringLiteral("C"),
    )
    assert grade._cypher() == 'CASE WHEN s.score > $p0 THEN "A" WHEN s.score > $p1 THEN "B" ELSE "C" END'


def test_case_expression_no_else():
    """CASE with no ELSE → null when no branch matches"""
    from rdflib_neo4j.sparql.cypher_builder import CaseExpression, WhenClause
    x = Var("x")
    expr = CaseExpression(WhenClause(IsNull(x), RawExpression("0")))
    assert expr._cypher() == "CASE WHEN x IS NULL THEN 0 END"
