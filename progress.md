# DuckDB Bulk Import Progress

## Status

Branch: `feat/duckdb-bulk-import-experiment`

Worktree: `.claude/worktrees/duckdb-bulk-import-experiment`

Current phase: streaming ingestion — rdflib sink + pyoxigraph backend wired, benchmark in progress.

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
| Unit tests | Done | 9/9 green: staging, mapping, language projection, pivoting, relationship dedupe, metadata, CLI, oxigraph backend. |
| Integration tests | Pending | End-to-end fixture -> DuckDB -> Parquet. |
| Native ingestion benchmark | In progress | rdflib streaming sink done; oxigraph backend done; benchmark harness done. |
| ChEBI rdflib baseline | Done | 44m29s, 16.4 GiB max RSS, 8.83M triples; too memory-heavy due to `Graph.parse()`. |
| Progress logging | Done | Phase timing + per-100k rows rate to stderr; --no-progress flag. |
| Native parser wrapper | Done | `rdflib_neo4j/bulk/ingest.py` — streaming rdflib Store sink + pyoxigraph backend. |

## Streaming Ingestion (Work Package 3 + 4)

### New module: `rdflib_neo4j/bulk/ingest.py`

Two streaming backends, both producing the same staging schema:

| Backend | How | Compatibility |
| --- | --- | --- |
| `rdflib` | Custom rdflib Store sink — `add()` batches directly into DuckDB; no `Graph.parse()` materialisation | Very broad; handles DTD entities in XML, non-standard BCP47 lang tags |
| `oxigraph` | `pyoxigraph.parse()` Rust streaming parser; never materialises a graph | Stricter: rejects DTD entity declarations in RDF/XML, enforces BCP47 |

### CLI changes

```
--parser rdflib|oxigraph   (default: rdflib)
--no-progress              suppress stderr progress output
--db                       default is now <output>/staging.duckdb (on-disk)
--batch-size               default raised to 100,000
```

### Compatibility findings (from benchmark on real datasets)

| Dataset | rdflib | oxigraph | Issue |
| --- | --- | --- | --- |
| pokepedia.xml (RDF/XML, DTD entities) | OK | ERROR | oxrdfxml does not resolve `<!ENTITY ...>` declarations |
| SWAPI-WD-data.ttl (Wikidata Turtle) | OK | ERROR | Non-standard BCP47 tags (`@be-x-old`, `@sr-EC`) fail strict parser |
| chebi.owl (RDF/XML, no DTD entities) | OK (28.2s/200k triples probe) | OK (benchmarking) | No issues expected |

**Conclusion**: oxigraph is the preferred backend for well-formed data (ChEBI, schema.org, standard ontologies). rdflib remains the safe fallback for real-world data with legacy XML entities or lenient language tags.

### Benchmark results — parse/stage phase on ChEBI (8.83M triples, 774 MB RDF/XML)

| Backend | Triples | Time | Rate | RSS delta | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| rdflib (streaming) | 200k probe | ~94s | ~2,134/s | low | full run pending |
| oxigraph | 200k probe | ~99s | ~2,023/s | low | full run pending |
| duckdb_rdf | 8,825,356 | 8.2s | **1,075,984/s** | 453 MiB | pure SQL, no Python loop |

**Key insight**: the `duckdb_rdf` backend is 500× faster than the Python-loop backends because it runs entirely within DuckDB's C++ engine — no Python per-row overhead. This is the path to 1M+ triples/sec for large datasets.

**Why rdflib ≈ oxigraph at 2k/s**: both are bottlenecked by the Python per-row loop + `executemany`. Parser speed is irrelevant at this stage — the DuckDB insert layer is the bottleneck.

**duckdb_rdf limitation**: the extension cannot distinguish plain string literals from IRI objects when neither `object_datatype` nor `object_lang` is set. A URI-heuristic (starts_with `http://`, `https://`, `urn:`, `_:`) is applied. Reliable for standard ontologies; documented as a known limitation.

## Resume Notes

- Keep rdflib ingestion replaceable. Projection code should consume only staged
  DuckDB tables.
- Default aggregation mode is `ANY`; strict `FIRST` / `LAST` require optional
  `source_order`.
- Language projection maps tagged attributes to `<name>_<lang>` when enabled.
- Current prototype module command: `python3 -m rdflib_neo4j.bulk.cli INPUT --format turtle --output out`
- Default DB is now on-disk: `<output>/staging.duckdb`
- Tests: `.venv-bench/bin/python -m pytest -v -p no:cacheprovider`
- Benchmark: `.venv-bench/bin/python bench/run_benchmark.py`
- ChEBI baseline used DuckDB 1.5.2 and `neo4j-rust-ext==6.2.0.0` via uv on Python 3.13.
- Do not commit until objectives are reached, tests are green, and docs are updated.

## Next Steps

1. Record ChEBI benchmark results (oxigraph vs rdflib streaming — parse/stage phase).
2. Run full pipeline (parse + project + export) on ChEBI with oxigraph backend.
3. Compare total runtime and peak RSS against the 44m29s / 16.4 GiB baseline.
4. Decide: adopt oxigraph as default for ChEBI-class inputs?
5. Add headers/import command generation (work package 12).
