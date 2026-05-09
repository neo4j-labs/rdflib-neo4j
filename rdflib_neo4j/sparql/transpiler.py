"""SPARQL-to-Cypher transpiler.

Walks the rdflib SPARQL algebra AST and emits a :class:`CypherQuery` using
the typed Cypher 25 DSL in ``cypher_builder``.

Public entry points
-------------------
``translate(sparql, config)``  — convenience function for the common case.
``Transpiler(config).translate_algebra(algebra)``  — when you already have the
    algebra (e.g. from ``rdflib.plugins.sparql.prepareQuery``).
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from rdflib import Variable, URIRef, Literal, RDF, BNode
from rdflib.plugins.sparql.parserutils import CompValue

from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.sparql.mapping import MappingContext
from rdflib_neo4j.sparql.cypher_builder import (
    AliasExpression,
    AnonNode,
    AndPredicate,
    CaseExpression,
    CypherQuery,
    Expression,
    FunctionCall,
    IsNotNull,
    LabelAtom,
    ListExpression,
    MatchClause,
    NodePattern,
    NotPredicate,
    OrderItem,
    OrPredicate,
    PathPattern,
    Predicate,
    RawExpression,
    RawPredicate,
    ReduceExpression,
    RelSegment,
    StringLiteral,
    Var,
    WhenClause,
    WhereClause,
    Comparison,
    InPredicate,
)

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

# Maps SPARQL variable name → Cypher Expression for accessing its value.
VarMap = dict[str, Expression]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class TranslationError(Exception):
    pass


class UnsupportedAlgebraNode(TranslationError):
    def __init__(self, node_name: str) -> None:
        super().__init__(f"Unsupported SPARQL algebra node: {node_name!r}")
        self.node_name = node_name


# ---------------------------------------------------------------------------
# Helpers — algebra analysis
# ---------------------------------------------------------------------------

def collect_subject_vars(node: Any) -> set[str]:
    """Recursively collect variable names that appear as triple subjects."""
    if not isinstance(node, CompValue):
        return set()
    subjects: set[str] = set()
    if node.name == "BGP":
        for (s, _p, _o) in node.get("triples", []):
            if isinstance(s, Variable):
                subjects.add(str(s))
    for key in node.keys():
        val = node[key]
        if isinstance(val, CompValue):
            subjects |= collect_subject_vars(val)
        elif isinstance(val, (list, tuple)):
            for item in val:
                if isinstance(item, CompValue):
                    subjects |= collect_subject_vars(item)
    return subjects


def collect_all_vars(node: Any) -> set[str]:
    """Recursively collect ALL variable names (subjects and objects) from triples."""
    if not isinstance(node, CompValue):
        return set()
    vars_: set[str] = set()
    if node.name == "BGP":
        for (s, _p, o) in node.get("triples", []):
            if isinstance(s, Variable):
                vars_.add(str(s))
            if isinstance(o, Variable):
                vars_.add(str(o))
    for key in node.keys():
        val = node[key]
        if isinstance(val, CompValue):
            vars_ |= collect_all_vars(val)
        elif isinstance(val, (list, tuple)):
            for item in val:
                if isinstance(item, CompValue):
                    vars_ |= collect_all_vars(item)
    return vars_


# ---------------------------------------------------------------------------
# Helpers — term conversion
# ---------------------------------------------------------------------------

def _literal_to_expr(lit: Literal, query: CypherQuery) -> Expression:
    """Convert an rdflib ``Literal`` to a Cypher ``Expression``."""
    val = lit.toPython()
    if isinstance(val, Decimal):
        val = float(val)
    if isinstance(val, bool):
        return RawExpression("true" if val else "false")
    if isinstance(val, int):
        return RawExpression(str(val))
    if isinstance(val, float):
        return RawExpression(str(val))
    if isinstance(val, str):
        return StringLiteral(val)
    return query.add_param(val)


# ---------------------------------------------------------------------------
# Transpiler
# ---------------------------------------------------------------------------

class Transpiler:
    """Walks the rdflib SPARQL algebra and produces a :class:`CypherQuery`."""

    def __init__(self, config: Neo4jStoreConfig) -> None:
        self.mapping = MappingContext(config)

    # ── Public entry points ─────────────────────────────────────────────────

    def translate_algebra(self, algebra: CompValue, *, cypher_version_prefix: bool = True) -> tuple[str, dict]:
        """Translate a full algebra tree. Returns ``(cypher_string, params)``.

        Pass ``cypher_version_prefix=False`` to omit the ``CYPHER 25`` version annotation.
        """
        subject_vars = collect_subject_vars(algebra)
        query = CypherQuery()
        final_query, _ = self._translate(algebra, subject_vars, query, {})
        return final_query.render(cypher_version_prefix=cypher_version_prefix)

    # ── Internal dispatch ───────────────────────────────────────────────────

    def _translate(
        self,
        node: CompValue,
        subject_vars: set[str],
        query: CypherQuery,
        var_map: VarMap,
    ) -> tuple[CypherQuery, VarMap]:
        """Dispatch on algebra node name."""
        dispatch = {
            "SelectQuery": self._select_query,
            "Project":     self._project,
            "Filter":      self._filter,
            "BGP":         self._bgp,
            "Join":        self._join,
            "LeftJoin":    self._left_join,
            "Union":       self._union,
            "Extend":      self._extend,
            "Slice":       self._slice,
            "OrderBy":     self._order_by,
            "Distinct":    self._distinct,
            "AggregateJoin": self._aggregate_join,
            "ToMultiSet":  self._to_multi_set,
            "Minus":       self._minus,
            "values":      self._values,
        }
        handler = dispatch.get(node.name)
        if handler is None:
            raise UnsupportedAlgebraNode(node.name)
        return handler(node, subject_vars, query, var_map)

    # ── SelectQuery / Project ───────────────────────────────────────────────

    def _select_query(self, node, subject_vars, query, var_map):
        """Peel off outer Slice/Distinct wrappers, then delegate to Project."""
        inner = node["p"]
        outer_limit = outer_skip = None
        outer_distinct = False

        while isinstance(inner, CompValue) and inner.name in (
            "Slice", "Distinct", "Reduced"
        ):
            if inner.name == "Slice":
                start = inner.get("start")
                length = inner.get("length")
                outer_skip = int(start) if start else None
                outer_limit = int(length) if length else None
            elif inner.name in ("Distinct", "Reduced"):
                outer_distinct = True
            inner = inner["p"]

        if isinstance(inner, CompValue) and inner.name == "Project":
            return self._project(
                inner, subject_vars, query, var_map,
                extra_limit=outer_limit,
                extra_skip=outer_skip,
                extra_distinct=outer_distinct,
            )
        return self._translate(inner, subject_vars, query, var_map)

    def _make_return_items(self, pv: list, var_map: VarMap) -> list:
        items = []
        for var in pv:
            name = str(var)
            expr = var_map.get(name, Var(name))
            if isinstance(expr, Var) and str(expr) == name:
                items.append(f"{name}")
            else:
                items.append(AliasExpression(expr, name))
        return items

    def _project(
        self, node, subject_vars, query, var_map,
        extra_limit=None, extra_skip=None, extra_distinct=False,
    ):
        # Peel off inner wrappers (OrderBy between Project and BGP)
        inner = node["p"]
        order_conditions = []

        while isinstance(inner, CompValue) and inner.name in (
            "OrderBy", "Distinct", "Reduced"
        ):
            if inner.name == "OrderBy":
                order_conditions = inner.get("expr", [])
            elif inner.name in ("Distinct", "Reduced"):
                extra_distinct = True
            inner = inner["p"]

        pv: list[Variable] = node["PV"]

        # Union: each branch needs its own RETURN so column names match.
        if isinstance(inner, CompValue) and inner.name == "Union":
            q1 = CypherQuery()
            q2 = CypherQuery()
            q1, vm1 = self._translate(inner["p1"], subject_vars, q1, {})
            q2, vm2 = self._translate(inner["p2"], subject_vars, q2, {})
            combined_vm = {**vm2, **vm1}

            order_items: list[OrderItem] = []
            for cond in order_conditions:
                expr = self._sparql_expr(cond["expr"], combined_vm, q1)
                asc = cond.get("order") != "DESC"
                order_items.append(OrderItem(expr, ascending=asc))

            q1.return_(
                *self._make_return_items(pv, vm1),
                distinct=extra_distinct,
                order_by=order_items if order_items else None,
                skip=extra_skip,
                limit=extra_limit,
            )
            q2.return_(*self._make_return_items(pv, vm2))
            q1.union(q2)
            return q1, combined_vm

        query, var_map = self._translate(inner, subject_vars, query, var_map)

        # Resolve order-by expressions now that var_map is populated
        order_items = []
        for cond in order_conditions:
            expr = self._sparql_expr(cond["expr"], var_map, query)
            asc = cond.get("order") != "DESC"
            order_items.append(OrderItem(expr, ascending=asc))

        query.return_(
            *self._make_return_items(pv, var_map),
            distinct=extra_distinct,
            order_by=order_items if order_items else None,
            skip=extra_skip,
            limit=extra_limit,
        )
        return query, var_map

    # ── Filter ──────────────────────────────────────────────────────────────

    def _filter(self, node, subject_vars, query, var_map):
        query, var_map = self._translate(node["p"], subject_vars, query, var_map)
        expr = node["expr"]
        if not (isinstance(expr, CompValue) and expr.name == "TrueFilter"):
            pred = self._sparql_expr(expr, var_map, query)
            query.where(pred)
        return query, var_map

    # ── BGP ─────────────────────────────────────────────────────────────────

    def _bgp(
        self,
        node,
        subject_vars: set[str],
        query: CypherQuery,
        var_map: VarMap,
        optional: bool = False,
    ) -> tuple[CypherQuery, VarMap]:
        """Translate a Basic Graph Pattern.

        *optional* makes all emitted ``MATCH`` clauses ``OPTIONAL MATCH``.
        """
        triples = node.get("triples", [])
        var_map = dict(var_map)  # copy — don't mutate caller

        # ── Collect per-subject metadata ────────────────────────────────────
        node_labels: dict[str, list[str]] = {}   # subject_var → labels
        where_preds: list[Predicate] = []
        # (from_var, rel_type_or_None, to: str|tuple, rel_var_or_None)
        # rel_type_or_None=None means a variable predicate; rel_var_or_None carries the var name.
        rel_patterns: list[tuple[str, Any, Any, Any]] = []

        def _ensure_labels(v: str) -> None:
            if v not in node_labels and v not in var_map:
                node_labels[v] = []

        for (s, p, o) in triples:
            if isinstance(s, Variable):
                s_name = str(s)
                _ensure_labels(s_name)
            else:
                s_name = None  # fixed URI subject — handled below

            if p == RDF.type:
                # rdf:type → label
                if isinstance(s, Variable) and isinstance(o, URIRef):
                    label = self.mapping.resolve(o)
                    node_labels.setdefault(s_name, []).append(label)
                # Variable type (?p a ?type) is unsupported for now
            elif isinstance(p, Variable):
                # Variable predicate: ?s ?p ?o — bind relationship to ?p
                p_name = str(p)
                if isinstance(o, Variable) and s_name is not None:
                    o_name = str(o)
                    _ensure_labels(o_name)
                    rel_patterns.append((s_name, None, o_name, p_name))
            elif isinstance(o, Literal) and not isinstance(p, Variable):
                if isinstance(s, Variable):
                    prop = self.mapping.resolve(p)
                    lit_expr = _literal_to_expr(o, query)
                    prop_access = RawExpression(f"{s_name}.`{prop}`")
                    where_preds.append(Comparison(prop_access, "=", lit_expr))
            elif isinstance(o, Variable) and not isinstance(p, Variable):
                o_name = str(o)
                prop = self.mapping.resolve(p)
                if o_name in subject_vars:
                    # Relationship: both endpoints are graph nodes
                    _ensure_labels(o_name)
                    rel_patterns.append((s_name, prop, o_name, None))
                else:
                    if isinstance(s, Variable):
                        prop_expr = RawExpression(f"{s_name}.`{prop}`")
                        if o_name in var_map:
                            # Variable already bound (e.g. from VALUES/UNWIND) — add join predicate
                            where_preds.append(Comparison(prop_expr, "=", var_map[o_name]))
                        else:
                            # New property variable binding — mandatory unless optional
                            var_map[o_name] = prop_expr
                            if not optional:
                                where_preds.append(IsNotNull(prop_expr))
            elif isinstance(o, URIRef):
                # Object is a fixed URI → relationship to a known node
                if isinstance(s, Variable):
                    prop = self.mapping.resolve(p)
                    uri_param = query.add_param(str(o))
                    rel_patterns.append((s_name, prop, ("uri", str(o), uri_param), None))
            elif isinstance(o, BNode):
                pass  # blank-node objects skipped

            # Fixed URI subject
            if isinstance(s, URIRef):
                s_uri = str(s)
                uri_param = query.add_param(s_uri)
                if s_uri not in var_map:
                    # Synthesise an alias for the fixed-URI subject
                    alias = "_s" + str(id(s))[-4:]
                    props = {self.mapping.uri_key: uri_param}
                    np = NodePattern(Var(alias), ["Resource"], props=props)
                    if optional:
                        query.optional_match(np)
                    else:
                        query.match(np)
                    var_map[s_uri] = Var(alias)

        # When optional, nodes that are only relationship targets get their label
        # inlined into the path pattern instead of a separate OPTIONAL MATCH.
        # Two separate OPTIONAL MATCHes are independent in Cypher and produce a
        # cartesian product; combining them into one pattern is correlated.
        if optional:
            rel_target_only = {
                to for (_, _, to, _rv) in rel_patterns
                if isinstance(to, str) and to not in var_map
            }
        else:
            rel_target_only = set()

        # ── Emit MATCH for node variables ───────────────────────────────────
        for var_name, labels in node_labels.items():
            if var_name in var_map:
                continue  # already matched in outer scope
            if var_name in rel_target_only:
                var_map[var_name] = Var(var_name)  # will be bound via rel pattern
                continue
            np = NodePattern(
                Var(var_name),
                labels if labels else ["Resource"],
            )
            if optional:
                query.optional_match(np)
            else:
                query.match(np)
            var_map[var_name] = Var(var_name)

        # ── Emit MATCH for relationship patterns ────────────────────────────
        for (from_var, rel_type, to_target, rel_var) in rel_patterns:
            from_node = AnonNode(Var(from_var))
            if isinstance(to_target, tuple):
                # Fixed-URI endpoint: (:Resource {uri: $p0})
                _kind, _uri, uri_param = to_target
                to_node = AnonNode(
                    label_expr=LabelAtom("Resource"),
                    props={self.mapping.uri_key: uri_param},
                )
            else:
                # Variable endpoint — inline label when optional to avoid cartesian product
                if optional and to_target in rel_target_only:
                    labels = node_labels.get(to_target, [])
                    label_expr = LabelAtom(labels[0]) if labels else LabelAtom("Resource")
                    to_node = AnonNode(Var(to_target), label_expr=label_expr)
                else:
                    to_node = AnonNode(Var(to_target))
                var_map.setdefault(to_target, Var(to_target))

            if rel_var is not None:
                # Variable predicate (?s ?p ?o) — unnamed type, bind to p
                rel_seg = RelSegment(var=Var(rel_var), types=[])
                var_map[rel_var] = Var(rel_var)
            else:
                rel_seg = RelSegment(types=[rel_type])
            path = PathPattern(from_node).rel(rel_seg, to_node)
            if optional:
                query.optional_match(path)
            else:
                query.match(path)

        # ── Append WHERE predicates ─────────────────────────────────────────
        for pred in where_preds:
            query.where(pred)

        return query, var_map

    # ── Join ────────────────────────────────────────────────────────────────

    def _join(self, node, subject_vars, query, var_map):
        """Translate Join — translate both sub-patterns sharing the same query."""
        query, var_map = self._translate(node["p1"], subject_vars, query, var_map)
        query, var_map = self._translate(node["p2"], subject_vars, query, var_map)
        return query, var_map

    # ── LeftJoin (OPTIONAL) ─────────────────────────────────────────────────

    def _left_join(self, node, subject_vars, query, var_map):
        """Translate LeftJoin — mandatory p1, optional p2."""
        query, var_map = self._translate(node["p1"], subject_vars, query, var_map)
        query, var_map = self._translate_optional(
            node["p2"], subject_vars, query, var_map
        )
        # Handle the LeftJoin condition (usually TrueFilter for plain OPTIONAL)
        expr = node.get("expr")
        if expr and not (isinstance(expr, CompValue) and expr.name == "TrueFilter"):
            pred = self._sparql_expr(expr, var_map, query)
            query.where(pred)
        return query, var_map

    def _translate_optional(self, node, subject_vars, query, var_map):
        """Translate a subtree with OPTIONAL MATCH semantics."""
        if node.name == "BGP":
            return self._bgp(node, subject_vars, query, var_map, optional=True)
        if node.name == "Join":
            query, var_map = self._translate_optional(
                node["p1"], subject_vars, query, var_map
            )
            query, var_map = self._translate_optional(
                node["p2"], subject_vars, query, var_map
            )
            return query, var_map
        if node.name == "Filter":
            query, var_map = self._translate_optional(
                node["p"], subject_vars, query, var_map
            )
            expr = node["expr"]
            if not (isinstance(expr, CompValue) and expr.name == "TrueFilter"):
                pred = self._sparql_expr(expr, var_map, query)
                query.where(pred)
            return query, var_map
        # Complex optional — fall back to OPTIONAL CALL subquery
        return self._optional_call(node, subject_vars, query, var_map)

    def _optional_call(self, node, subject_vars, query, var_map):
        """Translate a complex optional via OPTIONAL CALL {} subquery."""
        # Variables already in scope that the optional might reference
        outer_vars = list(var_map.keys())
        imports = [v for v in outer_vars if isinstance(var_map[v], Var)]

        inner = query.subquery()
        inner, inner_var_map = self._translate(node, subject_vars, inner, var_map)

        # New variables introduced by the optional
        new_vars = [v for v in inner_var_map if v not in var_map]

        # Build RETURN for the inner subquery
        return_items = [
            AliasExpression(inner_var_map[v], v) for v in new_vars
        ]
        if return_items:
            inner.return_(*return_items)

        query.optional_call(imports, inner)

        # After the OPTIONAL CALL, new vars are accessible as plain Var
        var_map = dict(var_map)
        for v in new_vars:
            var_map[v] = Var(v)
        return query, var_map

    # ── Union ───────────────────────────────────────────────────────────────

    def _union(self, node, subject_vars, query, var_map):
        """Translate Union — two separate queries joined with UNION."""
        # Each branch needs its own query + RETURN
        q1 = CypherQuery()
        q2 = CypherQuery()
        q1, vm1 = self._translate(node["p1"], subject_vars, q1, {})
        q2, vm2 = self._translate(node["p2"], subject_vars, q2, {})
        # RETURN is added later by the enclosing Project; pass through combined
        # var_map (p1 wins on conflict)
        combined_vm = {**vm2, **vm1}
        q1.union(q2)
        return q1, combined_vm

    # ── Extend (BIND) ───────────────────────────────────────────────────────

    def _extend(self, node, subject_vars, query, var_map):
        """Translate Extend (BIND expr AS var)."""
        query, var_map = self._translate(node["p"], subject_vars, query, var_map)
        var = node["var"]
        var_name = str(var)
        expr = self._sparql_expr(node["expr"], var_map, query)
        var_map = dict(var_map)
        var_map[var_name] = expr
        return query, var_map

    # ── Slice / OrderBy / Distinct ───────────────────────────────────────────
    # These nodes are unwrapped by _project before translating the inner tree.
    # The handlers below are kept for the rare case where they appear *below*
    # a Project (shouldn't happen in well-formed algebra but be safe).

    def _slice(self, node, subject_vars, query, var_map):
        return self._translate(node["p"], subject_vars, query, var_map)

    def _order_by(self, node, subject_vars, query, var_map):
        return self._translate(node["p"], subject_vars, query, var_map)

    def _distinct(self, node, subject_vars, query, var_map):
        return self._translate(node["p"], subject_vars, query, var_map)

    # ── Group / AggregateJoin ───────────────────────────────────────────────

    def _aggregate_join(self, node, subject_vars, query, var_map):
        """Translate AggregateJoin wrapping a Group.

        Pattern:
          MATCH …
          WITH group_var_expr AS group_var, agg(…) AS __agg_N__
          RETURN … (added by enclosing Project via Extend renames)
        """
        group_node = node["p"]  # the Group node
        assert group_node.name == "Group"

        # Translate the inner pattern (BGP / Filter / etc.)
        query, var_map = self._translate(
            group_node["p"], subject_vars, query, var_map
        )
        var_map = dict(var_map)

        # ── Group-by variables ───────────────────────────────────────────────
        group_vars: list[Variable] = group_node.get("expr", [])
        # Map: group var name → aliased expression name (the same name, properly
        # exposed so downstream Extend/Project can reference it)
        group_var_names: set[str] = {str(v) for v in group_vars}

        with_items: list[Expression] = []

        # Emit group-by vars as aliased expressions in WITH
        for gv in group_vars:
            gv_name = str(gv)
            expr = var_map.get(gv_name, Var(gv_name))
            with_items.append(AliasExpression(expr, gv_name))
            var_map[gv_name] = Var(gv_name)  # after WITH, accessible as Var

        # ── Aggregate expressions ────────────────────────────────────────────
        for agg in node["A"]:
            res_var = str(agg["res"])
            agg_vars_name = str(agg["vars"]) if isinstance(agg["vars"], Variable) else None

            # Aggregate_Sample over a group-by var → just re-alias the group var
            if agg.name == "Aggregate_Sample" and agg_vars_name in group_var_names:
                # The group-by var is already exposed above; map res → same alias
                var_map[res_var] = var_map.get(agg_vars_name, Var(agg_vars_name))
                continue

            cypher_agg = self._translate_aggregate(agg, var_map, query)
            with_items.append(AliasExpression(cypher_agg, res_var))
            var_map[res_var] = Var(res_var)

        query.with_(*with_items)
        return query, var_map

    def _translate_aggregate(
        self, agg: CompValue, var_map: VarMap, query: CypherQuery
    ) -> Expression:
        name = agg.name
        distinct = bool(agg.get("distinct"))

        def _arg() -> Expression:
            return var_map.get(str(agg["vars"]), Var(str(agg["vars"])))

        if name == "Aggregate_Count":
            arg = agg["vars"]
            if arg == "*":
                return FunctionCall("count", RawExpression("*"))
            return FunctionCall("count", _arg(), distinct=distinct)
        if name == "Aggregate_Sum":
            return FunctionCall("sum", _arg(), distinct=distinct)
        if name == "Aggregate_Avg":
            return FunctionCall("avg", _arg(), distinct=distinct)
        if name == "Aggregate_Min":
            return FunctionCall("min", _arg(), distinct=distinct)
        if name == "Aggregate_Max":
            return FunctionCall("max", _arg(), distinct=distinct)
        if name == "Aggregate_Sample":
            # SAMPLE is not native Cypher; head(collect(x)) picks an arbitrary non-null value
            return FunctionCall("head", FunctionCall("collect", _arg()))
        if name == "Aggregate_GroupConcat":
            sep = str(agg.get("separator", ","))
            item = _arg()
            items_list = FunctionCall("collect", item)
            body = CaseExpression(
                WhenClause(
                    Comparison(Var("__s"), "=", StringLiteral("")),
                    Var("__n"),
                ),
                else_=RawExpression(f'__s + {StringLiteral(sep)} + __n'),
            )
            return ReduceExpression(
                acc="__s",
                init=StringLiteral(""),
                var="__n",
                list_=items_list,
                body=body,
            )
        return FunctionCall(f"/* unknown agg {name} */", _arg())

    # ── ToMultiSet ──────────────────────────────────────────────────────────

    def _to_multi_set(self, node, subject_vars, query, var_map):
        """ToMultiSet wraps a SelectQuery — just delegate."""
        return self._translate(node["p"], subject_vars, query, var_map)

    # ── Minus ───────────────────────────────────────────────────────────────

    def _minus(self, node, subject_vars, query, var_map):
        """Translate MINUS.

        - Pure property filter → negate the predicate directly (NOT pred).
        - Graph pattern → NOT EXISTS { MATCH ... RETURN ... } (full Cypher subquery).
        """
        query, var_map = self._translate(node["p1"], subject_vars, query, var_map)
        inner = query.subquery()
        # Treat all variables in the MINUS body as node vars so relationships are MATCHed
        minus_svars = subject_vars | collect_all_vars(node["p2"])
        inner, inner_vm = self._translate(node["p2"], minus_svars, inner, var_map)

        has_match = any(isinstance(c, MatchClause) for c in inner._clauses)
        if not has_match:
            # Pure property filter — use null-safe negation.
            # NOT (pred) is null when the property is absent, wrongly excluding the row.
            # NOT coalesce(pred, false) treats missing property as non-matching (SPARQL semantics).
            where_preds = [c.predicate for c in inner._clauses if isinstance(c, WhereClause)]
            if where_preds:
                combined: Predicate = where_preds[0]
                for p in where_preds[1:]:
                    combined = combined.and_(p)
                query.where(RawPredicate(f"NOT coalesce({combined}, false)"))
        else:
            # Graph pattern — full Cypher subquery with RETURN.
            new_vars = [v for v in inner_vm if v not in var_map]
            ret = [AliasExpression(inner_vm[v], v) for v in new_vars] or [RawExpression("1")]
            inner.return_(*ret)
            query.where(query.not_exists_subquery(inner))
        return query, var_map

    # ── Values ──────────────────────────────────────────────────────────────

    def _values(self, node, subject_vars, query, var_map):
        """Translate VALUES clause.

        The rdflib algebra uses ``res`` — a list of ``{Variable: term}`` dicts.

        Strategy:
          * Single variable already bound in var_map → IN predicate (no new rows).
          * Single variable not yet bound → UNWIND parameter list.
          * Multiple variables → UNWIND list of maps.
        """
        from rdflib_neo4j.sparql.cypher_builder import UnwindClause

        res: list[dict] = node.get("res", [])
        if not res:
            return query, var_map

        # Collect all variables from the bindings
        variables: list[Variable] = list({k for row in res for k in row.keys()})

        def _val(term):
            if isinstance(term, Literal):
                return term.toPython()
            if term is not None:
                return str(term)
            return None

        if len(variables) == 1:
            var = variables[0]
            var_name = str(var)
            values = [_val(row.get(var)) for row in res if row.get(var) is not None]

            if var_name in var_map:
                # Already bound — use IN predicate
                param = query.add_param(values)
                query.where(InPredicate(var_map[var_name], param))
            else:
                # Unbound — UNWIND the value list
                param = query.add_param(values)
                query._clauses.append(UnwindClause(param, var_name))
                var_map = dict(var_map)
                var_map[var_name] = Var(var_name)
        else:
            # Multi-variable: UNWIND list of maps
            rows = [
                {str(v): _val(row.get(v)) for v in variables}
                for row in res
            ]
            param = query.add_param(rows)
            alias = "_row"
            query._clauses.append(UnwindClause(param, alias))
            var_map = dict(var_map)
            for v in variables:
                var_map[str(v)] = RawExpression(f"{alias}.{v}")

        return query, var_map

    # ── Expression translation ──────────────────────────────────────────────

    def _sparql_expr(
        self, expr: Any, var_map: VarMap, query: CypherQuery
    ) -> Expression:
        """Convert any rdflib SPARQL expression/term to a Cypher Expression.

        Predicates (``Comparison``, ``AndPredicate``, …) are also
        ``Expression`` subclasses, so the return type is unified.
        """
        # ── Plain rdflib terms ──────────────────────────────────────────────
        if isinstance(expr, Variable):
            name = str(expr)
            return var_map.get(name, Var(name))
        if isinstance(expr, Literal):
            return _literal_to_expr(expr, query)
        if isinstance(expr, URIRef):
            return query.add_param(str(expr))
        if isinstance(expr, bool):
            return RawExpression("true" if expr else "false")
        if isinstance(expr, (int, float)):
            return RawExpression(str(expr))
        if not isinstance(expr, CompValue):
            return RawExpression(str(expr))

        name = expr.name

        # ── Comparisons / logical ───────────────────────────────────────────
        if name == "RelationalExpression":
            left = self._sparql_expr(expr["expr"], var_map, query)
            right = self._sparql_expr(expr["other"], var_map, query)
            return Comparison(left, expr["op"], right)
        if name == "ConditionalAndExpression":
            # expr = first operand; other = list of remaining operands
            result: Expression = self._sparql_expr(expr["expr"], var_map, query)
            for e in expr.get("other", []):
                result = AndPredicate(result, self._sparql_expr(e, var_map, query))
            return result
        if name == "ConditionalOrExpression":
            result = self._sparql_expr(expr["expr"], var_map, query)
            for e in expr.get("other", []):
                result = OrPredicate(result, self._sparql_expr(e, var_map, query))
            return result
        if name == "UnaryNot":
            return NotPredicate(self._sparql_expr(expr["expr"], var_map, query))

        # ── Builtins — predicates ───────────────────────────────────────────
        if name == "Builtin_BOUND":
            v = self._sparql_expr(expr["arg"], var_map, query)
            return IsNotNull(v)
        if name == "Builtin_REGEX":
            text = self._sparql_expr(expr["text"], var_map, query)
            pat_term = expr["pattern"]
            flags_str = str(expr["flags"]) if "flags" in expr else ""
            if isinstance(pat_term, Literal):
                # SPARQL REGEX is partial match; Cypher =~ is full match — wrap with .*
                raw = str(pat_term)
                if not raw.startswith("^"):
                    raw = ".*" + raw
                # ends with unescaped $ means start-anchor is already set
                if not (raw.endswith("$") and not raw.endswith("\\$")):
                    raw = raw + ".*"
                if flags_str:
                    raw = f"(?{flags_str}){raw}"
                pattern = StringLiteral(raw)
            else:
                # Dynamic pattern — cannot rewrite at compile time; emit as-is
                pattern = self._sparql_expr(pat_term, var_map, query)
            return RawPredicate(f"{text} =~ {pattern}")
        if name == "TrueFilter":
            return RawPredicate("true")

        # ── Builtins — string ───────────────────────────────────────────────
        if name == "Builtin_STR":
            return FunctionCall("toString", self._sparql_expr(expr["arg"], var_map, query))
        if name == "Builtin_UCASE":
            return FunctionCall("toUpper", self._sparql_expr(expr["arg"], var_map, query))
        if name == "Builtin_LCASE":
            return FunctionCall("toLower", self._sparql_expr(expr["arg"], var_map, query))
        if name == "Builtin_STRLEN":
            return FunctionCall("size", self._sparql_expr(expr["arg"], var_map, query))
        if name == "Builtin_SUBSTR":
            s = self._sparql_expr(expr["arg"], var_map, query)
            start = self._sparql_expr(expr["start"], var_map, query)
            args = [s, start]
            if "length" in expr:
                args.append(self._sparql_expr(expr["length"], var_map, query))
            return FunctionCall("substring", *args)
        if name == "Builtin_STRSTARTS":
            s = self._sparql_expr(expr["arg"], var_map, query)
            prefix = self._sparql_expr(expr["arg2"], var_map, query)
            return RawPredicate(f"{s} STARTS WITH {prefix}")
        if name == "Builtin_STRENDS":
            s = self._sparql_expr(expr["arg"], var_map, query)
            suffix = self._sparql_expr(expr["arg2"], var_map, query)
            return RawPredicate(f"{s} ENDS WITH {suffix}")
        if name == "Builtin_CONTAINS":
            s = self._sparql_expr(expr["arg"], var_map, query)
            sub = self._sparql_expr(expr["arg2"], var_map, query)
            return RawPredicate(f"{s} CONTAINS {sub}")
        if name == "Builtin_CONCAT":
            parts = [self._sparql_expr(a, var_map, query) for a in expr["arg"]]
            if not parts:
                return StringLiteral("")
            result = parts[0]
            for p in parts[1:]:
                result = RawExpression(f"{result} + {p}")
            return result

        # ── Builtins — numeric ──────────────────────────────────────────────
        if name == "Builtin_ABS":
            return FunctionCall("abs", self._sparql_expr(expr["arg"], var_map, query))
        if name == "Builtin_CEIL":
            return FunctionCall("ceil", self._sparql_expr(expr["arg"], var_map, query))
        if name == "Builtin_FLOOR":
            return FunctionCall("floor", self._sparql_expr(expr["arg"], var_map, query))
        if name == "Builtin_ROUND":
            return FunctionCall("round", self._sparql_expr(expr["arg"], var_map, query))

        # ── Builtins — control flow ─────────────────────────────────────────
        if name == "Builtin_IF":
            cond = self._sparql_expr(expr["arg1"], var_map, query)
            then = self._sparql_expr(expr["arg2"], var_map, query)
            else_ = self._sparql_expr(expr["arg3"], var_map, query)
            when = WhenClause(cond if isinstance(cond, Predicate) else RawPredicate(str(cond)), then)
            return CaseExpression(when, else_=else_)
        if name == "Builtin_COALESCE":
            parts = [self._sparql_expr(a, var_map, query) for a in expr["arg"]]
            return FunctionCall("coalesce", *parts)

        # ── Arithmetic ──────────────────────────────────────────────────────
        if name == "AdditiveExpression":
            result = self._sparql_expr(expr["expr"], var_map, query)
            for op, other in zip(expr.get("op", []), expr.get("other", [])):
                rhs = self._sparql_expr(other, var_map, query)
                result = RawExpression(f"{result} {op} {rhs}")
            return result
        if name == "MultiplicativeExpression":
            result = self._sparql_expr(expr["expr"], var_map, query)
            for op, other in zip(expr.get("op", []), expr.get("other", [])):
                rhs = self._sparql_expr(other, var_map, query)
                result = RawExpression(f"{result} {op} {rhs}")
            return result
        if name == "UnaryMinus":
            return RawExpression(f"-{self._sparql_expr(expr['expr'], var_map, query)}")
        if name == "UnaryPlus":
            return self._sparql_expr(expr["expr"], var_map, query)

        # ── Unsupported — emit a comment marker rather than crashing ────────
        return RawExpression(f"/* unsupported SPARQL expr: {name} */ null")


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------

def translate(sparql: str, config: Neo4jStoreConfig, *, cypher_version_prefix: bool = True) -> tuple[str, dict]:
    """Translate a SPARQL SELECT query string to a Cypher query.

    Returns ``(cypher_string, params)`` ready to pass to the Neo4j driver.
    Pass ``cypher_version_prefix=False`` to omit the ``CYPHER 25`` version annotation.
    """
    from rdflib.plugins.sparql import prepareQuery
    algebra = prepareQuery(sparql).algebra
    return Transpiler(config).translate_algebra(algebra, cypher_version_prefix=cypher_version_prefix)
