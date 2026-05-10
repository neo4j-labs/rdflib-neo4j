#!/usr/bin/env python3
"""Benchmark ingestion backends against reference RDF datasets.

Usage:
    python bench/run_benchmark.py [--datasets PATHS...] [--parsers rdflib oxigraph]

Each (dataset, parser) combination is run once. The script prints a summary
table with triples, wall-clock time, throughput, and peak RSS.

Requirements: resource (stdlib), pyoxigraph, duckdb
"""
from __future__ import annotations

import argparse
import resource
import sys
import time
from pathlib import Path

# Add project root so we can import without installing.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

from rdflib_neo4j.bulk.ingest import BACKENDS, ingest

DATA_DIR = Path.home() / "d" / "data" / "rdf"

DATASETS = {
    "pokepedia.xml": (str(DATA_DIR / "pokepedia.xml"), "xml"),
    "SWAPI-WD.ttl": (str(DATA_DIR / "SWAPI-WD-data.ttl"), "turtle"),
    "chebi.owl": (str(DATA_DIR / "chebi.owl"), "xml"),
}

_STAGING_DDL = """
CREATE TABLE rdf_triples (
    source_order UBIGINT,
    subject VARCHAR NOT NULL,
    predicate VARCHAR NOT NULL,
    object_kind VARCHAR NOT NULL,
    object_value VARCHAR NOT NULL,
    datatype VARCHAR,
    lang VARCHAR
)
"""


def _rss_mb() -> float:
    """Return current RSS in MiB (macOS: ru_maxrss is bytes; Linux: kilobytes)."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / (1024 * 1024)
    return rss / 1024


def run_one(path: str, rdf_format: str, backend: str, db_path: str) -> dict:
    conn = duckdb.connect(db_path)
    try:
        conn.execute(_STAGING_DDL)
        rss_before = _rss_mb()
        t0 = time.monotonic()
        triples = ingest(path, rdf_format, conn, backend=backend, batch_size=100_000, progress=True)
        elapsed = time.monotonic() - t0
        rss_after = _rss_mb()
        db_size_mb = Path(db_path).stat().st_size / (1024 * 1024) if Path(db_path).exists() else 0
    finally:
        conn.close()
    return {
        "triples": triples,
        "elapsed_s": elapsed,
        "triples_per_s": triples / elapsed if elapsed > 0 else 0,
        "rss_delta_mb": rss_after - rss_before,
        "db_size_mb": db_size_mb,
    }


def _fmt_row(name, backend, r: dict | str) -> str:
    if isinstance(r, str):
        return f"  {name:<22} {backend:<10} ERROR: {r}"
    db_info = f"  DB {r.get('db_size_mb', 0):>6.0f} MiB" if r.get("db_size_mb") else ""
    return (
        f"  {name:<22} {backend:<10} "
        f"{r['triples']:>10,}  "
        f"{r['elapsed_s']:>7.1f}s  "
        f"{r['triples_per_s']:>10,.0f}/s  "
        f"{r['rss_delta_mb']:>7.0f} MiB"
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--datasets", nargs="+", help="Override dataset paths (name=path:format)")
    ap.add_argument("--parsers", nargs="+", choices=list(BACKENDS), default=list(BACKENDS))
    args = ap.parse_args()

    datasets = dict(DATASETS)
    if args.datasets:
        datasets = {}
        for entry in args.datasets:
            name, rest = entry.split("=", 1)
            path, fmt = rest.rsplit(":", 1)
            datasets[name] = (path, fmt)

    print(f"\n{'Dataset':<22} {'Backend':<10} {'Triples':>10}  {'Time':>8}  {'Rate':>10}  {'RSS delta':>10}")
    print("-" * 80)

    for name, (path, fmt) in datasets.items():
        if not Path(path).exists():
            print(f"  {name:<22} (skipped — file not found: {path})")
            continue
        for backend in args.parsers:
            db_path = str(DATA_DIR / f"bench_{Path(name).stem}_{backend}.duckdb")
            Path(db_path).unlink(missing_ok=True)
            try:
                result = run_one(path, fmt, backend, db_path)
            except Exception as exc:
                result = str(exc)
            finally:
                Path(db_path).unlink(missing_ok=True)
            print(_fmt_row(name, backend, result))

    print()


if __name__ == "__main__":
    main()
