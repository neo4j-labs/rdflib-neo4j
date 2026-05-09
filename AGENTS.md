# rdflib-neo4j — Agent Instructions

## What this repo is

A Python rdflib `Store` plugin for Neo4j. Currently write-only (import only). The goal is to bring it to feature parity with [neosemantics (n10s)](https://github.com/neo4j-labs/neosemantics) where a client-side implementation is feasible.

## Where the plan lives

`findings/` contains the full analysis and implementation plan:

- `findings/gap-analysis.md` — what rdflib-neo4j currently has vs n10s, with addressability classification
- `findings/implementation-plan.md` — step-by-step build plan with file targets, Cypher patterns, and acceptance criteria
- `findings/n10s-roadmap.md` — what the n10s team is fixing in parallel (blocking dependencies noted)

The neosemantics source analysis (7 detailed findings files) lives at `/Users/mh/d/java/neo/neosemantics/findings/` and is the reference for n10s behaviour.

## GitHub issues

Issues #50–#66 map directly to `implementation-plan.md` steps. Each issue has full implementation detail, code sketches, Cypher patterns, and acceptance criteria. Check the issue comments — several have blocking dependency notes added after the roadmap review.

## Critical path — what to work on

**Start immediately** (no n10s dependency):
- **#50** — Rename `SHORTEN` → `SHORTEN_STRICT`, add dynamic SHORTEN with nsN prefix generation
- **#51** — Persist `_GraphConfig` and `_NsPrefDef` to Neo4j (interop with n10s)
- **#53** — Blank node → `bnode://` URI mapping
- **#54** — Implement `remove()` triple deletion
- **#55** — Implement `triples()` / `__len__()` (highest value gap — unlocks `serialize()` + SPARQL)
- **#56** — `handleRDFTypes` NODES and LABELS_AND_NODES modes
- **#65** — Preview / dry-run mode
- **#66** — In-process node cache

**Write path can proceed; read path waits:**
- **#52** — XSD datatype coercion: write path now; `OffsetDateTime` read path waits for n10s `fix/166-timezone-datetime`

**Wait for n10s Phase 5 branches to merge:**
- **#57** — OWL ontology import (monitor n10s `fix/324-chebi-owl-import`)
- **#58** — Inference helpers (wait for n10s `feat/271-inference-isTypeRel`)
- **#59/#60** — SHACL / GRAPH TYPE (wait for n10s `feat/233`, `feat/261`)
- **#64** — RDF-Star (wait for n10s `fix/265` AND `fix/192` — both required)

## Key source facts (confirmed against n10s source)

- `_NsPrefDef` is a **single node** — each prefix is a property (`{owl: "http://...", rdf: "http://..."}`)
- `handleRDFTypes` has **three modes only**: `LABELS`, `NODES`, `LABELS_AND_NODES` (no IGNORE)
- `applyNeo4jNaming` is a real `_GraphConfig` field (missing from original rdflib-neo4j) — uppercases rel types, capitalizes labels
- URI strategy canonical strings (stored in `_GraphConfig`): `SHORTEN`, `SHORTEN_STRICT`, `IGNORE`, `MAP`, `KEEP`
- Always write `_GraphConfig` with `SET gc += $params` (not `=`) to preserve unknown fields

## Coding conventions

- Run tests: `pytest test/` (testcontainers spins up Neo4j automatically in CI)
- Local integration tests: `RUN_TEST_LOCALLY=true pytest test/integration/` (requires running Neo4j + `NEO4J_URI_LOCAL=bolt://localhost`, `NEO4J_USER_LOCAL=neo4j`, `NEO4J_PWD_LOCAL=password` env vars)
- **pytest installed via Homebrew** on this machine — use `pytest` directly (not `python -m pytest`)
- Lint: `ruff check rdflib_neo4j/ test/` (add `ruff` to dev dependencies)
- One issue per branch: `fix/<issue>-<slug>` or `feat/<issue>-<slug>`
- One PR per issue — never batch multiple issues into one PR

## Pre-commit checklist (before every commit)

1. `ruff check rdflib_neo4j/ test/` — lint must pass
2. `pytest test/` — unit + integration tests must be green (run in background if testcontainers slow)
3. Stage **only files added/updated/removed for that task** — never `git add -A` or `git add .`
4. Show diff and proposed commit message to user — never auto-execute `git commit`
5. don't add co-authored by to commit messages

## Worktree usage

Use `EnterWorktree` for parallel issue lanes (see `PLAN.md` for safe parallel sets). Branches that
touch the same files (`Neo4jStore.py`, `Neo4jTriple.py`, `utils.py`) must be sequential to avoid
merge conflicts. Safe parallel starters: `#66` (cache) + `#53` (blank nodes) + `#65` (preview).

## Where the plan lives

`PLAN.md` in the project root tracks step-by-step progress. Update status and PR links as work
completes. Check `PLAN.md` first at the start of every session to resume from the right step.

## Notebook authoring conventions

When generating or editing `.ipynb` notebooks:

- **Always add cell IDs** (`"id": "<short-slug>"` field on every cell — 8–12 lowercase alphanum, e.g. `"id": "setup-01"`). Jupyter 4.5+ requires unique cell IDs; notebooks without them are harder to diff and edit programmatically.
- **Markdown cells** should use H2 (`##`) for top-level sections inside the notebook, H3 (`###`) for subsections.
- **Code cells** — one logical concept per cell; keep them runnable in isolation where possible.
- **Output cells** — clear stale outputs before committing (`jupyter nbconvert --clear-output`) so diffs are meaningful; only keep outputs that serve as documentation (expected results).
- **Assertions in demo notebooks** — add `assert` statements after key result cells to catch regressions when the notebook is re-run. This replaces a full test suite for notebook correctness.

## Running Notebooks locally with dev build

For running the notebook locally against the current source without touching the notebook code, install the package in editable mode into your .venv:

cd ~/d/python/rdflib-neo4j   # already on feat/61-sparql-transpiler
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"       # editable install — imports live source directly
uv pip install jupyter ipywidgets

Then register the venv as a Jupyter kernel so the notebook picks it up automatically:

pip install ipykernel
uv run python -m ipykernel install --user --name rdflib-neo4j --display-name "rdflib-neo4j (local)"

Open the notebook, select the rdflib-neo4j (local) kernel from the kernel picker, and any import rdflib_neo4j will resolve to your local source. Changes you make to the source are reflected immediately — no reinstall needed.