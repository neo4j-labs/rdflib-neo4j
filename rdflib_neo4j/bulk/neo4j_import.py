"""Rewrite bulk-export Parquet files into neo4j-admin format and optionally run the import."""
from __future__ import annotations

import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import duckdb

from rdflib_neo4j.bulk.utils import mem_stat as _mem_stat

# rows_per_file sentinel — negative means no splitting (each label stays one file).
_NO_SPLIT = -1


def _prop_cols(con: duckdb.DuckDBPyConnection, path: str) -> list[str]:
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()]
    # Exclude pipeline-internal columns: `primary_label` is conveyed to neo4j-admin via the
    # filename (e.g. Person.parquet → --nodes=Person=...), not as a data column. `labels` (the
    # full raw RDF type array) is omitted here and preserved in the export Parquet for post-import
    # label materialisation via Cypher (e.g. CALL apoc.create.addLabels).
    # `:LABEL` (when present) is intentionally kept — neo4j-admin reads it as per-row
    # multi-label assignment (primary + subClassOf ancestors, semicolon-separated).
    return [c for c in cols if c not in ("uri", "primary_label", "labels")]


def _row_count(con: duckdb.DuckDBPyConnection, path: str) -> int:
    return con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]


def rewrite_nodes(
    con: duckdb.DuckDBPyConnection,
    src: Path,
    dst_dir: Path,
    label: str,
    rows_per_file: int = _NO_SPLIT,
) -> tuple[int, list[Path]]:
    """
    Rewrite a node Parquet file with neo4j-admin column names.

    When rows_per_file > 0 and the file exceeds that threshold, writes multiple
    part files into dst_dir/label/ for neo4j-admin parallel import.
    Returns (total_row_count, list_of_output_paths).
    """
    prop_cols = _prop_cols(con, str(src))
    prop_exprs = ", ".join(f'"{c}"' for c in prop_cols)
    # Bare :ID (no group qualifier): RDF URIs are globally unique across all entity types,
    # so a single global ID space suffices. Omitting the group avoids the per-group in-memory
    # hash table neo4j-admin would otherwise maintain for each node label.
    select = '"uri" AS ":ID"' + (f", {prop_exprs}" if prop_exprs else "")

    total = _row_count(con, str(src))
    if rows_per_file > 0 and total > rows_per_file:
        chunk_dir = dst_dir / label
        chunk_dir.mkdir(parents=True, exist_ok=True)
        n_threads = max(1, total // rows_per_file)
        con.execute(f"SET threads={n_threads}")
        # PER_THREAD_OUTPUT TRUE: DuckDB writes one Parquet part per thread. neo4j-admin
        # accepts comma-separated file lists per --nodes flag and assigns a parallel import
        # worker to each chunk, matching the number of parts we produce here.
        con.execute(
            f"COPY (SELECT {select} FROM read_parquet('{src}'))"
            f" TO '{chunk_dir}' (FORMAT PARQUET, COMPRESSION ZSTD, PER_THREAD_OUTPUT TRUE)"
        )
        con.execute("SET threads TO DEFAULT")
        parts = sorted(chunk_dir.glob("*.parquet"))
    else:
        dst = dst_dir / f"{label}.parquet"
        con.execute(
            f"COPY (SELECT {select} FROM read_parquet('{src}'))"
            f" TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        parts = [dst]

    return total, parts


def rewrite_rels(
    con: duckdb.DuckDBPyConnection,
    src: Path,
    dst_dir: Path,
    rel_type: str,
    rows_per_file: int = _NO_SPLIT,
) -> tuple[int, list[Path]]:
    """
    Rewrite a relationship Parquet file with neo4j-admin column names.

    When rows_per_file > 0 and the file exceeds that threshold, writes multiple
    part files for neo4j-admin parallel import.
    Returns (total_row_count, list_of_output_paths).
    """
    # Bare :START_ID / :END_ID (no group qualifier): same rationale as :ID above —
    # RDF URIs are globally unique, so no per-label group hash table is needed.
    select = 'source_uri AS ":START_ID", target_uri AS ":END_ID"'
    total = _row_count(con, str(src))

    if rows_per_file > 0 and total > rows_per_file:
        chunk_dir = dst_dir / rel_type
        chunk_dir.mkdir(parents=True, exist_ok=True)
        n_threads = max(1, total // rows_per_file)
        con.execute(f"SET threads={n_threads}")
        # PER_THREAD_OUTPUT TRUE: same rationale as for nodes above — produces one part per
        # thread so neo4j-admin can assign a parallel import worker to each chunk.
        con.execute(
            f"COPY (SELECT {select} FROM read_parquet('{src}'))"
            f" TO '{chunk_dir}' (FORMAT PARQUET, COMPRESSION ZSTD, PER_THREAD_OUTPUT TRUE)"
        )
        con.execute("SET threads TO DEFAULT")
        parts = sorted(chunk_dir.glob("*.parquet"))
    else:
        dst = dst_dir / f"{rel_type}.parquet"
        con.execute(
            f"COPY (SELECT {select} FROM read_parquet('{src}'))"
            f" TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        parts = [dst]

    return total, parts


def prepare_import_files(
    parquet_dir: Path,
    neo4j_dir: Path,
    rows_per_file: int = _NO_SPLIT,
    progress: bool = True,
) -> tuple[list[tuple[str, list[Path]]], list[tuple[str, list[Path]]]]:
    """
    Rewrite nodes/ and relationships/ Parquet files into neo4j_dir with renamed columns.

    Large files (> rows_per_file rows) are split into parallel chunks for neo4j-admin
    parallelism. Returns (node_entries, rel_entries) — each entry is
    (label_or_type, [path, ...]).
    """
    nodes_src = parquet_dir / "nodes"
    rels_src = parquet_dir / "relationships"
    nodes_dst = neo4j_dir / "nodes"
    rels_dst = neo4j_dir / "relationships"
    nodes_dst.mkdir(parents=True, exist_ok=True)
    rels_dst.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(":memory:")
    node_entries: list[tuple[str, list[Path]]] = []
    rel_entries: list[tuple[str, list[Path]]] = []

    if progress:
        print("[neo4j-import] rewriting node Parquet files...", file=sys.stderr)
    for src in sorted(nodes_src.glob("*.parquet")):
        label = src.stem
        count, parts = rewrite_nodes(con, src, nodes_dst, label, rows_per_file)
        node_entries.append((label, parts))
        if progress:
            chunk_note = f"  → {len(parts)} chunks" if len(parts) > 1 else ""
            print(f"[neo4j-import]   {label}: {count:,} rows{chunk_note}", file=sys.stderr)

    if progress:
        print("[neo4j-import] rewriting relationship Parquet files...", file=sys.stderr)
    for src in sorted(rels_src.glob("*.parquet")):
        rel_type = src.stem
        count, parts = rewrite_rels(con, src, rels_dst, rel_type, rows_per_file)
        rel_entries.append((rel_type, parts))
        if progress:
            chunk_note = f"  → {len(parts)} chunks" if len(parts) > 1 else ""
            print(f"[neo4j-import]   {rel_type}: {count:,} rows{chunk_note}", file=sys.stderr)

    con.close()
    return node_entries, rel_entries


def build_import_command(
    neo4j_admin: str,
    database: str,
    node_entries: list[tuple[str, list[Path]]],
    rel_entries: list[tuple[str, list[Path]]],
    overwrite: bool = True,
    skip_bad_relationships: bool = True,
    skip_duplicate_nodes: bool = True,
) -> list[str]:
    cmd = [
        neo4j_admin, "database", "import", "full",
        "--input-type=parquet",
    ]
    if overwrite:
        cmd.append("--overwrite-destination=true")
    if skip_bad_relationships:
        cmd.append("--skip-bad-relationships=true")
    if skip_duplicate_nodes:
        cmd.append("--skip-duplicate-nodes=true")
    for label, paths in node_entries:
        files = ",".join(str(p) for p in paths)
        cmd.append(f"--nodes={label}={files}")
    for rel_type, paths in rel_entries:
        files = ",".join(str(p) for p in paths)
        cmd.append(f"--relationships={rel_type}={files}")
    cmd.append(database)
    return cmd


def run_neo4j_import(
    parquet_dir: str,
    database: str,
    neo4j_admin: Optional[str] = None,
    overwrite: bool = True,
    skip_bad_relationships: bool = True,
    skip_duplicate_nodes: bool = True,
    rows_per_file: int = _NO_SPLIT,
    dry_run: bool = False,
    progress: bool = True,
    optional: bool = False,
) -> int:
    """
    Full neo4j import flow: rewrite Parquet columns, build the admin command, optionally execute.

    Rewrites files into <parquet_dir>/neo4j/nodes/ and <parquet_dir>/neo4j/relationships/.
    Large files (> rows_per_file rows) are split into parallel chunks so neo4j-admin
    can use multiple import workers per label/type.
    Returns the neo4j-admin exit code (0 on dry-run).

    When optional=True (used by --stage all), a missing neo4j-admin binary is not an
    error — the function prints the manual import hint and returns 0.
    """
    parquet_path = Path(parquet_dir)
    neo4j_dir = parquet_path / "neo4j"

    # Auto-detect neo4j-admin:
    #   1. PATH lookup  (covers standalone installs, Linux packages, Homebrew)
    #   2. $NEO4J_HOME/bin/neo4j-admin  (common when Neo4j is unpacked from a tarball)
    #   3. Common well-known locations as a last resort
    if neo4j_admin is None:
        import os as _os
        neo4j_admin = shutil.which("neo4j-admin")
        if neo4j_admin is None:
            neo4j_home = _os.environ.get("NEO4J_HOME")
            if neo4j_home:
                candidate = Path(neo4j_home) / "bin" / "neo4j-admin"
                if candidate.exists():
                    neo4j_admin = str(candidate)
        if neo4j_admin is None:
            for candidate in [
                "/usr/local/bin/neo4j-admin",
                "/opt/homebrew/bin/neo4j-admin",
                "/usr/bin/neo4j-admin",
            ]:
                if Path(candidate).exists():
                    neo4j_admin = candidate
                    break
    if neo4j_admin is None:
        if optional:
            print(
                "[neo4j-import] neo4j-admin not found — skipping import.\n"
                "  Install neo4j-admin or set --neo4j-admin to run the import manually.",
                file=sys.stderr,
            )
            return 0
        print(
            "[neo4j-import] ERROR: neo4j-admin not found. "
            "Set --neo4j-admin to the binary path.",
            file=sys.stderr,
        )
        return 1

    t0 = time.monotonic()
    node_entries, rel_entries = prepare_import_files(
        parquet_path, neo4j_dir, rows_per_file=rows_per_file, progress=progress
    )

    cmd = build_import_command(
        neo4j_admin, database, node_entries, rel_entries,
        overwrite=overwrite,
        skip_bad_relationships=skip_bad_relationships,
        skip_duplicate_nodes=skip_duplicate_nodes,
    )

    cmd_str = " \\\n  ".join(shlex.quote(part) for part in cmd)
    print(f"\n# neo4j-admin import command:\n{cmd_str}\n", file=sys.stderr)

    if dry_run:
        print("[neo4j-import] dry-run: skipping execution", file=sys.stderr)
        return 0

    if progress:
        print("[neo4j-import] running neo4j-admin...", file=sys.stderr)

    result = subprocess.run(cmd, text=True)
    if progress:
        print(
            f"[neo4j-import] done  exit={result.returncode}  {time.monotonic() - t0:.1f}s{_mem_stat()}",
            file=sys.stderr,
        )
    return result.returncode
