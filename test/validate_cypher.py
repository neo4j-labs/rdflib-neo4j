"""Validate all Cypher queries from the DSL unit tests and findings doc via EXPLAIN.

Usage:
    python3 test/validate_cypher.py

Reads NEO4J_URI_LOCAL / NEO4J_USER_LOCAL / NEO4J_PWD_LOCAL from the environment.
Exits non-zero if any query fails to parse.
"""
from __future__ import annotations

import os
import re
import sys
import textwrap

from neo4j import GraphDatabase
from neo4j.exceptions import CypherSyntaxError


# ── connection ────────────────────────────────────────────────────────────────

URI  = os.environ.get("NEO4J_URI_LOCAL",  "bolt://localhost:7687")
USER = os.environ.get("NEO4J_USER_LOCAL", "neo4j")
PWD  = os.environ.get("NEO4J_PWD_LOCAL",  "")

if not PWD:
    print("ERROR: NEO4J_PWD_LOCAL not set", file=sys.stderr)
    sys.exit(1)


# ── query extraction: run the DSL unit tests and capture rendered queries ─────

def collect_from_unit_tests() -> list[tuple[str, str]]:
    """Run each test_sparql_ex* function and return (test_name, cypher) pairs."""
    import importlib.util, types

    spec = importlib.util.spec_from_file_location(
        "test_cypher_builder",
        "test/unit/test_cypher_builder.py",
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]

    results: list[tuple[str, str]] = []
    for name in dir(mod):
        if not name.startswith("test_sparql_ex"):
            continue
        fn = getattr(mod, name)
        # The test functions call .render() and assert — we monkey-patch
        # CypherQuery.render to capture the output before assertions run.
        captured: list[str] = []
        from rdflib_neo4j.sparql.cypher_builder import CypherQuery
        original_render = CypherQuery.render

        def patched_render(self, _cap=captured, _orig=original_render):
            result = _orig(self)
            _cap.append(result[0])
            return result

        CypherQuery.render = patched_render  # type: ignore[method-assign]
        try:
            fn()
        except AssertionError:
            pass  # assertions may fail for wrong reasons; we only need the cypher
        except Exception as e:
            print(f"  [SKIP] {name}: setup error — {e}")
        finally:
            CypherQuery.render = original_render  # type: ignore[method-assign]

        for cypher in captured:
            results.append((name, cypher))

    return results


def collect_from_findings() -> list[tuple[str, str]]:
    """Extract ```cypher ... ``` blocks from sparql-to-cypher.md."""
    findings = "findings/sparql-to-cypher.md"
    if not os.path.exists(findings):
        findings = "../../../findings/sparql-to-cypher.md"
    text = open(findings).read()
    blocks = re.findall(r"```cypher\n(.*?)```", text, re.DOTALL)
    return [(f"findings[{i+1}]", b.strip()) for i, b in enumerate(blocks)]


# ── EXPLAIN validation ────────────────────────────────────────────────────────

def explain(driver, cypher: str) -> str | None:
    """Return None on success, error message on failure."""
    # Replace $params with literals so EXPLAIN doesn't complain about missing params
    body = re.sub(r"\$\w+", "null", cypher).strip()
    if not body:
        return None
    # Insert EXPLAIN after the optional CYPHER version header
    body = re.sub(r"^(CYPHER \d+)\s*\n", r"\1\nEXPLAIN ", body, count=1)
    if not re.match(r"^\s*(CYPHER \d+|EXPLAIN)\b", body):
        body = "EXPLAIN " + body
    try:
        with driver.session() as session:
            session.run(body).consume()
        return None
    except CypherSyntaxError as e:
        return str(e).split("\n")[0]
    except Exception as e:
        return f"{type(e).__name__}: {e}"


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    try:
        driver.verify_connectivity()
    except Exception as e:
        print(f"Cannot connect to {URI}: {e}", file=sys.stderr)
        return 1

    print(f"Connected to {URI}\n")

    queries: list[tuple[str, str]] = []
    queries += collect_from_unit_tests()
    queries += collect_from_findings()

    passed = failed = skipped = 0
    errors: list[tuple[str, str, str]] = []

    for label, cypher in queries:
        err = explain(driver, cypher)
        if err is None:
            print(f"  OK  {label}")
            passed += 1
        else:
            print(f"  FAIL {label}")
            errors.append((label, cypher, err))
            failed += 1

    driver.close()

    print(f"\n{passed} passed, {failed} failed, {skipped} skipped")

    if errors:
        print("\n── Failures ──────────────────────────────────────────────────")
        for label, cypher, err in errors:
            print(f"\n{label}:")
            print(textwrap.indent(cypher[:400], "  "))
            print(f"  Error: {err}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
