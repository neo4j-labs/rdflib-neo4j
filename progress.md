# DuckDB Bulk Import Progress

## Status

Branch: `feat/duckdb-bulk-import-experiment`

Worktree: `.claude/worktrees/duckdb-bulk-import-experiment`

Current phase: initial rdflib-backed correctness prototype.

## Task Tracker

| Task | Status | Notes |
| --- | --- | --- |
| Planning artifact | Done | `docs/duckdb-bulk-import-plan.md` created and iterated. |
| Progress tracker | In progress | This file tracks resumable state. |
| rdflib-backed DuckDB staging | Done | Raw RDF triples staged into DuckDB with batched inserts. |
| Mapping / property facts | In progress | Basic URI mapping, language projection, bnodes, labels, properties, and relationships implemented. |
| Dynamic pivot projection | Done | DuckDB dynamic `PIVOT` over discovered node attributes. |
| Parquet export | In progress | Node and deduplicated relationship Parquet files plus metadata export. Headers/import command still pending. |
| Small public RDF fixture | Done | CLI test writes a small Turtle fixture locally. |
| Unit tests | In progress | Covers staging, mapping, language projection, pivoting, relationship dedupe, metadata, and CLI smoke path. |
| Integration tests | Pending | End-to-end fixture -> DuckDB -> Parquet. |
| Native ingestion benchmark | Pending | After correctness prototype. |

## Resume Notes

- Keep rdflib ingestion replaceable. Projection code should consume only staged
  DuckDB tables.
- Default aggregation mode is `ANY`; strict `FIRST` / `LAST` require optional
  `source_order`.
- Language projection maps tagged attributes to `<name>_<lang>` when enabled.
- Current prototype CLI: `rdflib-neo4j-bulk-prototype INPUT --format turtle --output out`.
- Tested with `python3 -m pytest test/bulk/test_duckdb_bulk_prototype.py -q -p no:cacheprovider`.
- Do not commit until objectives are reached, tests are green, and docs are
  updated.
