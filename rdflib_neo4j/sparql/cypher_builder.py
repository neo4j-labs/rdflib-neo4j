"""Cypher query builder for SPARQL-to-Cypher transpilation.

Typed DSL — every node is a concrete type; rendering is a separate pass.

Expression hierarchy  (value nodes)
------------------------------------
Expression
  Parameter               — $pN placeholder
  RawExpression           — escape hatch for raw Cypher fragments
  StringLiteral           — "value"  properly escaped string constant
  ListExpression          — [e1, e2, ...]  inline list literal
  ReduceExpression        — reduce(acc = init, var IN list | body)  (SPARQL GROUP_CONCAT)
  CaseExpression          — CASE WHEN c THEN r [...] [ELSE e] END  (SPARQL IF → CASE)
  NodePattern             — (var:LabelExpr {props} [WHERE pred])  also a Pattern
  AnonNode                — () or (var) intermediate node          also a Pattern
  SubqueryExpression
    CountExpression       — COUNT { <inner> }   (numeric)
    CollectExpression     — COLLECT { <inner> }  (list)
    ExistsExpression      — EXISTS { <inner> }   (boolean → also Predicate)
    NotExistsExpression   — NOT EXISTS { <inner> }  (boolean → also Predicate)

Predicate(Expression)  (boolean-valued; used in WHERE slots)
-------------------------------------------------------------
  RawPredicate            — escape hatch
  AndPredicate            — p1 AND p2 [AND ...]
  OrPredicate             — (p1) OR (p2) [OR ...]
  NotPredicate            — NOT (p)
  Comparison              — left op right
  IsNull / IsNotNull      — expr IS [NOT] NULL
  InPredicate             — expr IN list_expr
  IsTypePredicate         — expr IS :: TypeExpr  (Cypher 25 type check; SPARQL DATATYPE)
  ExistsExpression        — also a Predicate
  NotExistsExpression     — also a Predicate

Pattern  (structural nodes for MATCH)
--------------------------------------
Pattern
  NodePattern             — (var:LabelExpr {props} [WHERE pred])  also an Expression
  AnonNode                — () or (var) [with label/props/WHERE]   also an Expression
  PathPattern             — chain: (n)-[:R]->(m)-[:S]->(o); QPP inline via .rel(QPPPattern)
  QPPPattern              — ((inner)){min,max}  Quantified Path Pattern

LabelExpr  (Cypher 25 label predicates inside node patterns)
-------------------------------------------------------------
  LabelAtom               — single label:  Person
  LabelAnd                — AND:            A&B  (operator: &)
  LabelOr                 — OR:             (A|B)  (operator: |)
  LabelNot                — NOT:            !A   (operator: ~)

RelSegment  (one relationship hop inside a PathPattern)
--------------------------------------------------------
  direction "->" / "<-" / "--", types, optional var/props/WHERE, optional hops

Clause hierarchy  (pipeline stages of a query)
-----------------------------------------------
Clause  (abstract, self-rendering via _render_lines())
  MatchClause             — MATCH pattern [WHERE predicate]
  WhereClause             — standalone WHERE predicate (between reading clauses)
  CallClause              — CALL (imports) { inner }  (inner join / SubSelect)
  OptionalCallClause      — OPTIONAL CALL (imports) { inner }  (left join)
  UnwindClause            — UNWIND expr AS var  (VALUES → UNWIND translation)
  WithClause              — WITH items [WHERE predicate]
  ReturnClause            — RETURN [DISTINCT] items [ORDER BY] [SKIP] [LIMIT]

CypherQuery
-----------
  _clauses: list[Clause]  — single ordered pipeline
  _union: Optional[tuple[CypherQuery, bool]]

  subquery() → child sharing root param namespace
  add_param / add_named_param → Parameter
  match / where / call / optional_call / unwind / with_ / return_ / union
  exists_subquery / not_exists_subquery / count_subquery / collect_subquery
  render() → (cypher_str, params_dict)   idempotent
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, Union


# ═══════════════════════════════════════════════════════════════════════════════
# Expression base
# ═══════════════════════════════════════════════════════════════════════════════

class Expression(ABC):
    """Base for all DSL value nodes.

    Subclasses implement ``_cypher()``; ``__str__`` / ``__format__`` delegate
    so expressions compose naturally inside f-strings.
    """

    @abstractmethod
    def _cypher(self) -> str: ...

    def __str__(self) -> str:
        return self._cypher()

    def __format__(self, format_spec: str) -> str:
        return self._cypher()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._cypher()!r})"


class RawExpression(Expression):
    """Escape hatch — wraps a raw Cypher fragment as an Expression."""

    def __init__(self, cypher: str) -> None:
        self._value = cypher

    def _cypher(self) -> str:
        return self._value


class StringLiteral(Expression):
    """A properly escaped Cypher string constant: ``"value"``.

    Prefer this over ``RawExpression('"value"')`` — it handles backslash and
    quote escaping automatically::

        StringLiteral("adult")     → "adult"
        StringLiteral('say "hi"')  → "say \\"hi\\""
    """

    def __init__(self, value: str) -> None:
        self.value = value

    def _cypher(self) -> str:
        escaped = self.value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'


# ═══════════════════════════════════════════════════════════════════════════════
# Typed expression building blocks
# ═══════════════════════════════════════════════════════════════════════════════

class Var(Expression):
    """A typed reference to a bound Cypher variable: ``n``, ``name``, ``cnt``."""

    def __init__(self, name: str) -> None:
        self.name = name

    def _cypher(self) -> str:
        return self.name


class Property:
    """A typed property reference carrying both schema label context and Neo4j key.

    The ``label`` is the Neo4j node/relationship label — used by the transpiler's
    MappingIndex to resolve the RDF predicate to the correct storage key.  Only
    ``name`` (the actual Neo4j property key) appears in rendered Cypher.

    :param label: The owning label/type (e.g. ``"Person"``).
    :param name:  The Neo4j property key (e.g. ``"age"``).
    """

    def __init__(self, label: str, name: str) -> None:
        self.label = label
        self.name = name

    def __str__(self) -> str:
        return self.name

    def __format__(self, format_spec: str) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"Property({self.label!r}, {self.name!r})"


class PropertyAccess(Expression):
    """``var.prop`` — typed property access on a node or relationship variable.

    :param var:  The :class:`Var` holding the node/relationship.
    :param prop: The :class:`Property` describing the schema context and key.
    """

    def __init__(self, var: Var, prop: Property) -> None:
        self.var = var
        self.prop = prop

    def _cypher(self) -> str:
        return f"{self.var}.{self.prop}"


class ListExpression(Expression):
    """``[e1, e2, ...]`` — an inline list literal or list of expressions.

    Typical use: building a literal list for ``UNWIND``::

        ListExpression(RawExpression('"Alice"'), RawExpression('"Bob"'))
        # → ["Alice", "Bob"]

    For parameterised lists pass the whole list as a :class:`Parameter` to
    :meth:`CypherQuery.add_param` instead — only use this class for inline literals.
    """

    def __init__(self, *elements: Expression) -> None:
        self.elements = elements

    def _cypher(self) -> str:
        return "[" + ", ".join(str(e) for e in self.elements) + "]"


@dataclass
class ReduceExpression(Expression):
    """``reduce(acc = init, var IN list | body)`` — fold over a list.

    Maps SPARQL ``GROUP_CONCAT(?x; separator=sep)`` to the native Cypher form::

        names = FunctionCall("collect", PropertyAccess(person, Property("Person", "name")))
        ReduceExpression(
            acc="s", init=StringLiteral(""),
            var="n", list_=names,
            body=CaseExpression(
                WhenClause(Comparison(Var("s"), "=", StringLiteral("")), Var("n")),
                else_=RawExpression('s + "," + n'),
            ),
        )
        # → reduce(s = "", n IN collect(person.name) |
        #       CASE WHEN s = "" THEN n ELSE s + "," + n END)

    :param acc:   Accumulator variable name (bare identifier string).
    :param init:  Initial accumulator value.
    :param var:   Iteration variable name (bare identifier string).
    :param list_: The list expression to iterate over.
    :param body:  Expression for each step — may reference both ``acc`` and ``var``.
    """

    acc: str
    init: Expression
    var: str
    list_: Expression
    body: Expression

    def _cypher(self) -> str:
        return f"reduce({self.acc} = {self.init}, {self.var} IN {self.list_} | {self.body})"


@dataclass
class WhenClause:
    """A single ``WHEN condition THEN result`` branch inside a :class:`CaseExpression`."""

    condition: "Predicate"
    result: Expression

    def _cypher(self) -> str:
        return f"WHEN {self.condition} THEN {self.result}"


class CaseExpression(Expression):
    """``CASE WHEN c1 THEN r1 [WHEN c2 THEN r2 ...] [ELSE default] END``

    Maps SPARQL ``IF(cond, then, else)`` → single-branch CASE, and
    ``COALESCE``-style multi-branch conditionals → multi-branch CASE::

        # SPARQL: IF(?age >= 18, "adult", "minor")
        CaseExpression(WhenClause(Comparison(age, ">=", p18), raw('"adult"')),
                       else_=raw('"minor"'))

        # Multi-branch: IF(?score > 90, "A", IF(?score > 70, "B", "C"))
        CaseExpression(
            WhenClause(Comparison(score, ">", p90), raw('"A"')),
            WhenClause(Comparison(score, ">", p70), raw('"B"')),
            else_=raw('"C"'),
        )

    :param whens:  One or more :class:`WhenClause` branches (evaluated in order).
    :param else_:  Default expression; rendered as ``ELSE …``.  If omitted,
                   Cypher returns ``null`` when no branch matches.
    """

    def __init__(self, *whens: WhenClause, else_: Optional[Expression] = None) -> None:
        self.whens = whens
        self.else_ = else_

    def _cypher(self) -> str:
        parts = ["CASE"]
        parts.extend(w._cypher() for w in self.whens)
        if self.else_ is not None:
            parts.append(f"ELSE {self.else_}")
        parts.append("END")
        return " ".join(parts)


class AliasExpression(Expression):
    """``expr AS alias`` — used in WITH and RETURN items."""

    def __init__(self, expr: Expression, alias: str) -> None:
        self.expr = expr
        self.alias = alias

    def _cypher(self) -> str:
        return f"{self.expr} AS {self.alias}"


class FunctionCall(Expression):
    """``name(arg1, arg2, ...)`` — any Cypher built-in or APOC function call.

    Pass an optional ``distinct=True`` for aggregates like ``count(DISTINCT n)``.
    """

    def __init__(self, name: str, *args: Expression, distinct: bool = False) -> None:
        self.name = name
        self.args = args
        self.distinct = distinct

    def _cypher(self) -> str:
        arg_str = ", ".join(str(a) for a in self.args)
        if self.distinct:
            arg_str = f"DISTINCT {arg_str}"
        return f"{self.name}({arg_str})"


@dataclass
class OrderItem:
    """A typed ORDER BY item: ``expr [ASC|DESC]``.

    :param expr: The expression to sort by.
    :param ascending: ``True`` (default) = ASC, ``False`` = DESC.
    """

    expr: Expression
    ascending: bool = True

    def _cypher(self) -> str:
        direction = "ASC" if self.ascending else "DESC"
        return f"{self.expr} {direction}"

    def __str__(self) -> str:
        return self._cypher()


# ═══════════════════════════════════════════════════════════════════════════════
# Parameter
# ═══════════════════════════════════════════════════════════════════════════════

class Parameter(Expression):
    """A ``$name`` placeholder for a bound query parameter."""

    def __init__(self, name: str) -> None:
        self.name = name

    def _cypher(self) -> str:
        return f"${self.name}"


# ═══════════════════════════════════════════════════════════════════════════════
# Predicate — boolean-valued Expression
# ═══════════════════════════════════════════════════════════════════════════════

class Predicate(Expression, ABC):
    """A boolean-valued expression for use in WHERE slots.

    Supports fluent composition::

        p1.and_(p2).or_(p3.not_())
    """

    def and_(self, *others: "Predicate") -> "AndPredicate":
        return AndPredicate(self, *others)

    def or_(self, *others: "Predicate") -> "OrPredicate":
        return OrPredicate(self, *others)

    def not_(self) -> "NotPredicate":
        return NotPredicate(self)


class RawPredicate(Predicate):
    """Escape hatch — wraps a raw Cypher string as a Predicate."""

    def __init__(self, cypher: str) -> None:
        self._value = cypher

    def _cypher(self) -> str:
        return self._value


class AndPredicate(Predicate):
    """``p1 AND p2 [AND ...]``"""

    def __init__(self, *operands: Predicate) -> None:
        self._operands = operands

    def _cypher(self) -> str:
        return " AND ".join(str(o) for o in self._operands)


class OrPredicate(Predicate):
    """``(p1) OR (p2) [OR ...]`` — operands parenthesised to preserve precedence."""

    def __init__(self, *operands: Predicate) -> None:
        self._operands = operands

    def _cypher(self) -> str:
        return " OR ".join(f"({o})" for o in self._operands)


class NotPredicate(Predicate):
    """``NOT (p)``"""

    def __init__(self, operand: Predicate) -> None:
        self._operand = operand

    def _cypher(self) -> str:
        return f"NOT ({self._operand})"


class Comparison(Predicate):
    """``left op right`` — op is a Cypher comparison operator string."""

    def __init__(self, left: Expression, op: str, right: Expression) -> None:
        self._left = left
        self._op = op
        self._right = right

    def _cypher(self) -> str:
        return f"{self._left} {self._op} {self._right}"


class IsNull(Predicate):
    """``expr IS NULL``"""

    def __init__(self, expr: Expression) -> None:
        self._expr = expr

    def _cypher(self) -> str:
        return f"{self._expr} IS NULL"


class IsNotNull(Predicate):
    """``expr IS NOT NULL``"""

    def __init__(self, expr: Expression) -> None:
        self._expr = expr

    def _cypher(self) -> str:
        return f"{self._expr} IS NOT NULL"


class InPredicate(Predicate):
    """``expr IN list_expr``"""

    def __init__(self, expr: Expression, list_expr: Expression) -> None:
        self._expr = expr
        self._list = list_expr

    def _cypher(self) -> str:
        return f"{self._expr} IN {self._list}"


class IsTypePredicate(Predicate):
    """``expr IS :: TypeExpr`` — Cypher 25 type predicate.

    Maps SPARQL ``DATATYPE(?x) = xsd:T`` checks to the native Cypher 25 syntax.
    ``type_expr`` is a raw Cypher 25 type expression string, e.g.:

    - ``"INTEGER"``               → ``x IS :: INTEGER``
    - ``"INTEGER | FLOAT"``       → ``x IS :: INTEGER | FLOAT``  (isNumeric)
    - ``"STRING NOT NULL"``       → ``x IS :: STRING NOT NULL``
    - ``"DATE | LOCAL DATETIME"`` → ``x IS :: DATE | LOCAL DATETIME``

    Use ``FunctionCall("valueType", expr)`` to retrieve the type name as a string.
    """

    def __init__(self, expr: Expression, type_expr: str) -> None:
        self._expr = expr
        self._type_expr = type_expr

    def _cypher(self) -> str:
        return f"{self._expr} IS :: {self._type_expr}"


# ═══════════════════════════════════════════════════════════════════════════════
# Subquery expressions
# ═══════════════════════════════════════════════════════════════════════════════

class SubqueryExpression(Expression, ABC):
    """Base for expression-form subqueries (EXISTS/NOT EXISTS/COUNT/COLLECT)."""

    _KEYWORD: str

    def __init__(self, inner: "CypherQuery") -> None:
        self.inner = inner

    def _cypher(self) -> str:
        return f"{self._KEYWORD} {{ {self.inner._render_body()} }}"


class ExistsExpression(SubqueryExpression, Predicate):
    """``EXISTS { <inner> }`` — boolean, usable directly as a Predicate."""
    _KEYWORD = "EXISTS"


class NotExistsExpression(SubqueryExpression, Predicate):
    """``NOT EXISTS { <inner> }`` — boolean, usable directly as a Predicate."""
    _KEYWORD = "NOT EXISTS"


class CountExpression(SubqueryExpression):
    """``COUNT { <inner> }`` — numeric."""
    _KEYWORD = "COUNT"


class CollectExpression(SubqueryExpression):
    """``COLLECT { <inner> }`` — list."""
    _KEYWORD = "COLLECT"


# ═══════════════════════════════════════════════════════════════════════════════
# Pattern base
# ═══════════════════════════════════════════════════════════════════════════════

class Pattern(ABC):
    """Base for structural MATCH pattern types.

    Like :class:`Expression`, implements ``__str__`` / ``__format__`` so
    patterns embed naturally in f-strings.
    """

    @abstractmethod
    def _cypher(self) -> str: ...

    def __str__(self) -> str:
        return self._cypher()

    def __format__(self, format_spec: str) -> str:
        return self._cypher()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._cypher()!r})"


# ═══════════════════════════════════════════════════════════════════════════════
# Label expressions — Cypher 25 boolean label predicates
# ═══════════════════════════════════════════════════════════════════════════════

class LabelExpr(ABC):
    """Boolean algebra over node labels (Cypher 25 / GQL label predicates).

    Compose with Python operators::

        person = LabelAtom("Person")
        admin  = LabelAtom("Admin")
        robot  = LabelAtom("Robot")

        person & admin             # LabelAnd  → Person&Admin
        person | robot             # LabelOr   → (Person|Robot)
        ~robot                     # LabelNot  → !Robot
        (person | admin) & ~robot  # complex   → (Person|Admin)&!Robot
    """

    @abstractmethod
    def _cypher(self) -> str: ...

    def __str__(self) -> str:
        return self._cypher()

    def __and__(self, other: "LabelExpr") -> "LabelAnd":
        return LabelAnd(self, other)

    def __or__(self, other: "LabelExpr") -> "LabelOr":
        return LabelOr(self, other)

    def __invert__(self) -> "LabelNot":
        return LabelNot(self)


class LabelAtom(LabelExpr):
    """Single label: ``Person``"""

    def __init__(self, name: str) -> None:
        self.name = name

    def _cypher(self) -> str:
        return self.name


class LabelAnd(LabelExpr):
    """Conjunction: ``A&B[&C...]``"""

    def __init__(self, *parts: LabelExpr) -> None:
        self.parts = parts

    def _cypher(self) -> str:
        return "&".join(p._cypher() for p in self.parts)


class LabelOr(LabelExpr):
    """Disjunction: ``(A|B[|C...])`` — always parenthesised for safe nesting."""

    def __init__(self, *parts: LabelExpr) -> None:
        self.parts = parts

    def _cypher(self) -> str:
        if len(self.parts) == 1:
            return self.parts[0]._cypher()
        return "(" + "|".join(p._cypher() for p in self.parts) + ")"


class LabelNot(LabelExpr):
    """Negation: ``!A``"""

    def __init__(self, operand: LabelExpr) -> None:
        self.operand = operand

    def _cypher(self) -> str:
        return f"!{self.operand._cypher()}"


def _labels_to_expr(labels: list[str]) -> LabelExpr:
    atoms = [LabelAtom(lbl) for lbl in labels]
    return atoms[0] if len(atoms) == 1 else LabelAnd(*atoms)


# ═══════════════════════════════════════════════════════════════════════════════
# Node patterns
# ═══════════════════════════════════════════════════════════════════════════════

class NodePattern(Pattern, Expression):
    """A labelled node pattern: ``(var:LabelExpr {props} [WHERE predicate])``.

    At least one label is required for anchor nodes — Neo4j needs a label to
    use an index for the starting point of a MATCH.

    Pass ``labels`` as a ``list[str]`` (simple) or a :class:`LabelExpr` for
    boolean label algebra (Cypher 25).  The optional ``where`` generates an
    inline ``WHERE`` clause inside the node parens, valid in QPP contexts.

    :param var: Typed :class:`Var` reference for this node.
    :param labels: One or more node labels or a :class:`LabelExpr` (required).
    :param props: Property constraints as ``{key: Expression}``.
    :param where: Inline predicate (QPP-context WHERE inside the node parens).
    """

    def __init__(
        self,
        var: Var,
        labels: Union[list[str], LabelExpr],
        props: Optional[dict[str, Expression]] = None,
        where: Optional["Predicate"] = None,
    ) -> None:
        if isinstance(labels, list):
            if not labels:
                raise ValueError(
                    f"NodePattern '{var}' must have at least one label — "
                    "unlabelled anchor nodes prevent index use."
                )
            self._label_expr: LabelExpr = _labels_to_expr(labels)
        else:
            self._label_expr = labels
        self.var = var
        self.props = props or {}
        self.where = where

    def _cypher(self) -> str:
        label_str = f":{self._label_expr._cypher()}"
        props_str = ""
        if self.props:
            kv = ", ".join(f"{k}: {v}" for k, v in self.props.items())
            props_str = f" {{{kv}}}"
        where_str = f" WHERE {self.where}" if self.where else ""
        return f"({self.var}{label_str}{props_str}{where_str})"


class AnonNode(Pattern, Expression):
    """An anonymous or intermediate node pattern: ``()`` / ``(n)`` / ``(n WHERE ...)``.

    Unlike :class:`NodePattern`, labels are optional.  Use for intermediate hops
    in path patterns and QPP inner patterns where no anchor label is needed.

    :param var: Optional typed :class:`Var`; omit for a fully anonymous ``()`` node.
    :param label_expr: Optional :class:`LabelExpr`.
    :param props: Property constraints.
    :param where: Inline predicate (QPP-context WHERE).
    """

    def __init__(
        self,
        var: Optional[Var] = None,
        label_expr: Optional[LabelExpr] = None,
        props: Optional[dict[str, Expression]] = None,
        where: Optional["Predicate"] = None,
    ) -> None:
        self.var = var
        self.label_expr = label_expr
        self.props = props or {}
        self.where = where

    def _cypher(self) -> str:
        var_str = f"{self.var}" if self.var is not None else ""
        label_str = f":{self.label_expr._cypher()}" if self.label_expr else ""
        props_str = ""
        if self.props:
            kv = ", ".join(f"{k}: {v}" for k, v in self.props.items())
            props_str = f" {{{kv}}}"
        where_str = f" WHERE {self.where}" if self.where else ""
        return f"({var_str}{label_str}{props_str}{where_str})"


# ═══════════════════════════════════════════════════════════════════════════════
# Relationship segment and path patterns
# ═══════════════════════════════════════════════════════════════════════════════

_NodeLike = Union[NodePattern, AnonNode]

_DIR_OUT  = "->"
_DIR_IN   = "<-"
_DIR_BOTH = "--"


@dataclass
class RelSegment:
    """One relationship hop in a path pattern.

    Renders as ``-[var:TYPE1|TYPE2 {props} WHERE pred]->`` (direction varies).

    :param direction: ``"->"`` outgoing, ``"<-"`` incoming, ``"--"`` undirected.
    :param types: Relationship type names (``["KNOWS"]`` → ``:KNOWS``). Empty = any.
    :param var: Optional variable bound to the relationship.
    :param props: Property constraints.
    :param where: Inline predicate (QPP-context WHERE inside the rel brackets).
    :param hops: Variable-length quantifier ``(min, max)``.  ``None`` = single hop.
                 ``(0, None)`` → ``*``, ``(1, None)`` → ``*1..``, ``(0, 5)`` → ``*..5``.
    """

    direction: str = _DIR_OUT
    types: list[str] = field(default_factory=list)
    var: Optional[Var] = None
    props: dict[str, Expression] = field(default_factory=dict)
    where: Optional[Predicate] = None
    hops: Optional[tuple[Optional[int], Optional[int]]] = None

    def _cypher(self) -> str:
        var_str = f"{self.var}" if self.var is not None else ""
        type_str = ":" + "|".join(self.types) if self.types else ""
        hops_str = self._hops_str()
        props_str = ""
        if self.props:
            kv = ", ".join(f"{k}: {v}" for k, v in self.props.items())
            props_str = f" {{{kv}}}"
        where_str = f" WHERE {self.where}" if self.where else ""
        inner = f"{var_str}{type_str}{hops_str}{props_str}{where_str}"
        rel_body = f"[{inner}]"
        if self.direction == _DIR_OUT:
            return f"-{rel_body}->"
        if self.direction == _DIR_IN:
            return f"<-{rel_body}-"
        return f"-{rel_body}-"

    def _hops_str(self) -> str:
        if self.hops is None:
            return ""
        min_, max_ = self.hops
        if (min_ is None or min_ == 0) and max_ is None:
            return "*"
        min_s = "" if min_ is None else str(min_)
        max_s = "" if max_ is None else str(max_)
        return f"*{min_s}..{max_s}"


_PathSegment = Union[RelSegment, "QPPPattern"]


class PathPattern(Pattern):
    """A chain of nodes and relationships: ``(a)-[:R]->(b)-[:S]->(c)`` etc.

    Supports both plain relationship hops (:class:`RelSegment`) and Quantified
    Path Patterns (:class:`QPPPattern`) as segments between nodes::

        PathPattern(anchor).rel(RelSegment(types=["KNOWS"]), mid)
        PathPattern(anchor).rel(QPPPattern(inner, min_=1), endpoint)
        # → (anchor)-[:KNOWS]->(mid)
        # → (anchor)((inner)){1,}(endpoint)

    :param start: First node in the chain (:class:`NodePattern` or :class:`AnonNode`).
    """

    def __init__(self, start: _NodeLike) -> None:
        self._nodes: list[_NodeLike] = [start]
        self._rels: list[_PathSegment] = []

    def rel(self, segment: _PathSegment, target: _NodeLike) -> "PathPattern":
        """Extend the path by one segment (relationship or QPP) and target node."""
        self._rels.append(segment)
        self._nodes.append(target)
        return self

    def _cypher(self) -> str:
        parts: list[str] = [self._nodes[0]._cypher()]
        for rel, node in zip(self._rels, self._nodes[1:]):
            parts.append(rel._cypher())
            parts.append(node._cypher())
        return "".join(parts)


@dataclass
class QPPPattern(Pattern):
    """Quantified Path Pattern: ``((inner)){min,max}`` (Cypher 25).

    Wraps a :class:`PathPattern` with a ``{min,max}`` repetition quantifier::

        QPPPattern(inner, min_=1)          # → (path){1,}
        QPPPattern(inner, min_=0, max_=5)  # → (path){0,5}

    :param inner: The :class:`PathPattern` to quantify.
    :param min_: Minimum repetitions (default 1).
    :param max_: Maximum repetitions; ``None`` = unbounded.
    """

    inner: PathPattern
    min_: int = 1
    max_: Optional[int] = None

    def _cypher(self) -> str:
        min_s = str(self.min_)
        max_s = "" if self.max_ is None else str(self.max_)
        return f"({self.inner._cypher()}){{{min_s},{max_s}}}"


# ═══════════════════════════════════════════════════════════════════════════════
# Clause hierarchy — pipeline stages of a CypherQuery
# ═══════════════════════════════════════════════════════════════════════════════

class Clause(ABC):
    """A single pipeline stage in a Cypher query.

    Each concrete subclass knows how to render itself as a list of Cypher lines
    via ``_render_lines()``.  CypherQuery._clauses is a flat ordered list of
    these; rendering just concatenates the lines.
    """

    @abstractmethod
    def _render_lines(self) -> list[str]: ...


@dataclass
class MatchClause(Clause):
    """``MATCH pattern [WHERE predicate]``"""

    pattern: Union[NodePattern, PathPattern, QPPPattern, str]
    where: Optional[Predicate] = None

    def _render_lines(self) -> list[str]:
        lines = [f"MATCH {self.pattern}"]
        if self.where is not None:
            lines.append(f"WHERE {self.where}")
        return lines


@dataclass
class WhereClause(Clause):
    """Standalone ``WHERE predicate`` between reading clauses."""

    predicate: Predicate

    def _render_lines(self) -> list[str]:
        return [f"WHERE {self.predicate}"]


@dataclass
class CallClause(Clause):
    """``CALL (imports) { inner }`` correlated subquery — inner join semantics."""

    imports: list[str]
    inner: "CypherQuery"

    def _render_lines(self) -> list[str]:
        imp = ", ".join(self.imports)
        lines = [f"CALL ({imp}) {{"]
        for line in self.inner._render_body().splitlines():
            lines.append(f"  {line}")
        lines.append("}")
        return lines


@dataclass
class OptionalCallClause(Clause):
    """``OPTIONAL CALL (imports) { inner }`` correlated subquery — left join semantics."""

    imports: list[str]
    inner: "CypherQuery"

    def _render_lines(self) -> list[str]:
        imp = ", ".join(self.imports)
        lines = [f"OPTIONAL CALL ({imp}) {{"]
        for line in self.inner._render_body().splitlines():
            lines.append(f"  {line}")
        lines.append("}")
        return lines


@dataclass
class UnwindClause(Clause):
    """``UNWIND expr AS var``

    Primary use: SPARQL ``VALUES ?x { v1 v2 }`` → ``UNWIND $p0 AS x`` (inline data).
    Also used for any list expansion (collect + unwind pattern).

    :param expr: The list expression to unwind (typically a :class:`Parameter`).
    :param var:  The variable name each element is bound to.
    """

    expr: Expression
    var: str

    def _render_lines(self) -> list[str]:
        return [f"UNWIND {self.expr} AS {self.var}"]


@dataclass
class WithClause(Clause):
    """``WITH [DISTINCT] items [ORDER BY ...] [SKIP n] [LIMIT n] [WHERE predicate]``"""

    items: list[Expression]
    distinct: bool = False
    order_by: list[OrderItem] = field(default_factory=list)
    skip: Optional[int] = None
    limit: Optional[int] = None
    where: Optional[Predicate] = None

    def _render_lines(self) -> list[str]:
        keyword = "WITH DISTINCT" if self.distinct else "WITH"
        lines = [f"{keyword} " + ", ".join(str(i) for i in self.items)]
        if self.order_by:
            lines.append("ORDER BY " + ", ".join(str(o) for o in self.order_by))
        if self.skip is not None:
            lines.append(f"SKIP {self.skip}")
        if self.limit is not None:
            lines.append(f"LIMIT {self.limit}")
        if self.where is not None:
            lines.append(f"WHERE {self.where}")
        return lines


@dataclass
class ReturnClause(Clause):
    """``RETURN [DISTINCT] items [ORDER BY ...] [SKIP n] [LIMIT n]``"""

    items: list[Expression]
    distinct: bool = False
    order_by: list[OrderItem] = field(default_factory=list)
    skip: Optional[int] = None
    limit: Optional[int] = None

    def _render_lines(self) -> list[str]:
        keyword = "RETURN DISTINCT" if self.distinct else "RETURN"
        lines = [f"{keyword} " + ", ".join(str(i) for i in self.items)]
        if self.order_by:
            lines.append("ORDER BY " + ", ".join(str(o) for o in self.order_by))
        if self.skip is not None:
            lines.append(f"SKIP {self.skip}")
        if self.limit is not None:
            lines.append(f"LIMIT {self.limit}")
        return lines


# ═══════════════════════════════════════════════════════════════════════════════
# CypherQuery — the builder
# ═══════════════════════════════════════════════════════════════════════════════

# Convenience alias for items that accept either typed Expression or bare string.
_ExprOrStr = Union[Expression, str]


class CypherQuery:
    """Builds a single Cypher 25 query as an ordered pipeline of :class:`Clause` objects.

    Usage::

        q = CypherQuery()
        uri = q.add_param("http://example.org/Alice")   # Parameter
        age = q.add_param(30)
        inner = q.subquery().match(f"(n)-[:KNOWS]->(m {{uri: {uri}}})")
        cypher, params = (
            q.match(f"(n:Resource {{uri: {uri}}})")
             .where(q.exists_subquery(inner).and_(Comparison(RawExpression("n.age"), ">", age)))
             .return_("n.name AS name")
             .render()
        )

    All values are bound via :meth:`add_param`; the rendered string contains
    only ``$name`` placeholders, never raw values.

    Child queries created via :meth:`subquery` share the root param namespace;
    :meth:`add_param` anywhere in the tree allocates from the same counter
    and writes to the same dict.
    """

    _PARAM_PREFIX = "p"

    def __init__(self, _param_source: Optional["CypherQuery"] = None) -> None:
        self._param_source = _param_source
        if _param_source is None:
            self._params: dict[str, Any] = {}
            self._param_counter: int = 0
        self._clauses: list[Clause] = []
        self._union: Optional[tuple["CypherQuery", bool]] = None

    # ── Param namespace ─────────────────────────────────────────────────────

    @property
    def _root(self) -> "CypherQuery":
        q = self
        while q._param_source is not None:
            q = q._param_source
        return q

    def add_param(self, value: Any) -> Parameter:
        """Bind *value* and return a :class:`Parameter`."""
        root = self._root
        name = f"{self._PARAM_PREFIX}{root._param_counter}"
        root._param_counter += 1
        root._params[name] = value
        return Parameter(name)

    def add_named_param(self, name: str, value: Any) -> Parameter:
        """Bind *value* under an explicit *name* and return a :class:`Parameter`."""
        self._root._params[name] = value
        return Parameter(name)

    def subquery(self) -> "CypherQuery":
        """Return a child query sharing this query's param namespace.

        Use for building inner queries passed to subquery expression helpers
        and :meth:`call` / :meth:`optional_call`.
        """
        return CypherQuery(_param_source=self._root)

    # ── Clause pipeline builders ─────────────────────────────────────────────

    def match(
        self,
        pattern: Union[str, NodePattern, PathPattern, QPPPattern],
        where: Optional[Predicate] = None,
    ) -> "CypherQuery":
        """Append a :class:`MatchClause`."""
        self._clauses.append(MatchClause(pattern, where))
        return self

    def where(self, predicate: Predicate) -> "CypherQuery":
        """Append a standalone :class:`WhereClause`."""
        self._clauses.append(WhereClause(predicate))
        return self

    def call(self, imports: list[str], inner: "CypherQuery") -> "CypherQuery":
        """Append a :class:`CallClause` (inner join)."""
        self._clauses.append(CallClause(imports, inner))
        return self

    def optional_call(self, imports: list[str], inner: "CypherQuery") -> "CypherQuery":
        """Append an :class:`OptionalCallClause` (left join)."""
        self._clauses.append(OptionalCallClause(imports, inner))
        return self

    def unwind(self, expr: _ExprOrStr, var: str) -> "CypherQuery":
        """Append an :class:`UnwindClause`.

        Typical use — SPARQL ``VALUES ?x { v1 v2 }``::

            names = q.add_param(["Alice", "Bob"])
            q.unwind(names, "name").match(...)
        """
        e = expr if isinstance(expr, Expression) else RawExpression(expr)
        self._clauses.append(UnwindClause(e, var))
        return self

    def with_(
        self,
        *items: _ExprOrStr,
        distinct: bool = False,
        order_by: Optional[list[OrderItem]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        where: Optional[Predicate] = None,
    ) -> "CypherQuery":
        """Append a :class:`WithClause`."""
        self._clauses.append(WithClause(
            [i if isinstance(i, Expression) else RawExpression(i) for i in items],
            distinct,
            order_by if order_by is not None else [],
            skip,
            limit,
            where,
        ))
        return self

    def return_(
        self,
        *items: _ExprOrStr,
        distinct: bool = False,
        order_by: Optional[list[OrderItem]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> "CypherQuery":
        """Append (or replace) the :class:`ReturnClause`.  Always kept last."""
        clause = ReturnClause(
            [i if isinstance(i, Expression) else RawExpression(i) for i in items],
            distinct,
            order_by if order_by is not None else [],
            skip,
            limit,
        )
        for i in range(len(self._clauses) - 1, -1, -1):
            if isinstance(self._clauses[i], ReturnClause):
                self._clauses[i] = clause
                return self
        self._clauses.append(clause)
        return self

    def union(self, other: "CypherQuery", all: bool = False) -> "CypherQuery":  # noqa: A002
        """Chain *other* as a ``UNION [ALL]``."""
        self._union = (other, all)
        return self

    # ── Subquery expression factories ────────────────────────────────────────

    def exists_subquery(self, inner: "CypherQuery") -> ExistsExpression:
        return ExistsExpression(inner)

    def not_exists_subquery(self, inner: "CypherQuery") -> NotExistsExpression:
        return NotExistsExpression(inner)

    def count_subquery(self, inner: "CypherQuery") -> CountExpression:
        return CountExpression(inner)

    def collect_subquery(self, inner: "CypherQuery") -> CollectExpression:
        return CollectExpression(inner)

    # ── Rendering ───────────────────────────────────────────────────────────

    def _build_clauses(self) -> list[str]:
        """Render the clause pipeline to a flat list of Cypher line strings."""
        lines: list[str] = []
        for clause in self._clauses:
            lines.extend(clause._render_lines())
        return lines

    def _render_body(self) -> str:
        """Compact single-line rendering for use inside subquery braces."""
        return " ".join(self._build_clauses())

    def _render_parts(self, param_offset: int = 0) -> tuple[list[str], dict[str, Any]]:
        """Render with ``CYPHER 25`` header and optional param remapping for UNION."""
        root = self._root

        if param_offset == 0:
            params = dict(root._params)
            remap: dict[str, str] = {}
        else:
            params = {}
            remap = {}
            for old_name, val in root._params.items():
                if old_name.startswith(self._PARAM_PREFIX):
                    try:
                        idx = int(old_name[len(self._PARAM_PREFIX):])
                        new_name = f"{self._PARAM_PREFIX}{idx + param_offset}"
                        remap[old_name] = new_name
                        params[new_name] = val
                    except ValueError:
                        params[old_name] = val
                else:
                    params[old_name] = val

        def fix(s: str) -> str:
            for old, new in remap.items():
                s = s.replace(f"${old}", f"${new}")
            return s

        clauses = self._build_clauses()
        parts = ["CYPHER 25"] + ([fix(c) for c in clauses] if remap else clauses)
        return parts, params

    def _render_union_tail(self, offset: int) -> tuple[str, str, dict[str, Any]]:
        """Recursively render this branch (and any further chained unions).

        Returns ``(separator, body_lines_joined, params)``.
        Follows the full ``._union`` chain so three-branch+ UNIONs render correctly.
        """
        other, use_all = self._union  # type: ignore[misc]
        other_parts, other_params = other._render_parts(param_offset=offset)
        body = "\n".join(other_parts[1:] if other_parts[0] == "CYPHER 25" else other_parts)
        sep = "UNION ALL" if use_all else "UNION"

        if other._union:
            next_offset = offset + other._root._param_counter
            next_sep, next_body, next_params = other._render_union_tail(next_offset)
            body = body + "\n" + next_sep + "\n" + next_body
            other_params = {**other_params, **next_params}

        return sep, body, other_params

    def render(self, *, cypher_version_prefix: bool = True) -> tuple[str, dict[str, Any]]:
        """Return ``(cypher_string, params_dict)``.  Idempotent.

        Pass ``cypher_version_prefix=False`` to omit the ``CYPHER 25`` version annotation,
        e.g. when the caller needs the bare query body for test assertions.
        """
        parts, params = self._render_parts(param_offset=0)
        if not cypher_version_prefix and parts and parts[0] == "CYPHER 25":
            parts = parts[1:]
        cypher = "\n".join(parts)

        if self._union:
            sep, body, other_params = self._render_union_tail(self._root._param_counter)
            full = cypher + "\n" + sep + "\n" + body
            return full, {**params, **other_params}

        return cypher, params

    def render_str(self, *, cypher_version_prefix: bool = True) -> str:
        """Return only the Cypher string (params discarded)."""
        return self.render(cypher_version_prefix=cypher_version_prefix)[0]
