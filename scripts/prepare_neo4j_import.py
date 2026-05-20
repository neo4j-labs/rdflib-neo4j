#!/usr/bin/env python3
"""Rewrite DuckDB bulk-export Parquet files for neo4j-admin database import full.

Column rename rules:
  Nodes:  uri            -> uri:ID(Resource)
          primary_label  dropped
          labels         dropped  (label comes from filename / --nodes flag)
          <props>        kept as-is

  Rels:   source_uri     -> :START_ID(Resource)
          target_uri     -> :END_ID(Resource)

Output is written to <out_dir>/neo4j/{nodes,relationships}/.
The script also prints the complete neo4j-admin command.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def _prop_cols(con, path: str) -> list[str]:
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()]
    return [c for c in cols if c not in ("uri", "primary_label", "labels")]


def _rewrite_nodes(con, src: Path, dst: Path) -> int:
    prop_cols = _prop_cols(con, str(src))
    id_expr = '"uri:ID(Resource)"'
    prop_exprs = ", ".join(f'"{c}"' for c in prop_cols)
    select = f'"uri" AS {id_expr}' + (f", {prop_exprs}" if prop_exprs else "")
    sql = f"COPY (SELECT {select} FROM read_parquet('{src}')) TO '{dst}' (FORMAT PARQUET)"
    con.execute(sql)
    count = con.execute(f"SELECT count(*) FROM read_parquet('{dst}')").fetchone()[0]
    return count


def _rewrite_rels(con, src: Path, dst: Path) -> int:
    sql = (
        f"COPY ("
        f"  SELECT source_uri AS \":START_ID(Resource)\","
        f"         target_uri AS \":END_ID(Resource)\""
        f"  FROM read_parquet('{src}')"
        f") TO '{dst}' (FORMAT PARQUET)"
    )
    con.execute(sql)
    count = con.execute(f"SELECT count(*) FROM read_parquet('{dst}')").fetchone()[0]
    return count


def main():
    import argparse

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input_dir", help="Directory with nodes/ and relationships/ subdirs")
    ap.add_argument("--output", help="Output directory (default: <input_dir>/neo4j)")
    ap.add_argument("--database", default="chebi", help="Target Neo4j database name")
    ap.add_argument(
        "--neo4j-admin",
        default="/Users/mh/v/neo4j-enterprise-2026.04.0/bin/neo4j-admin",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="Print command only, do not import"
    )
    args = ap.parse_args()

    in_dir = Path(args.input_dir)
    out_dir = Path(args.output) if args.output else in_dir / "neo4j"

    nodes_out = out_dir / "nodes"
    rels_out = out_dir / "relationships"
    nodes_out.mkdir(parents=True, exist_ok=True)
    rels_out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(":memory:")

    node_args: list[str] = []
    print("Rewriting node files...")
    for src in sorted((in_dir / "nodes").glob("*.parquet")):
        label = src.stem
        dst = nodes_out / src.name
        count = _rewrite_nodes(con, src, dst)
        node_args.append(f"  --nodes={label}={dst}")
        print(f"  {label}: {count:,} rows -> {dst.name}")

    rel_args: list[str] = []
    print("Rewriting relationship files...")
    for src in sorted((in_dir / "relationships").glob("*.parquet")):
        rel_type = src.stem
        dst = rels_out / src.name
        count = _rewrite_rels(con, src, dst)
        rel_args.append(f"  --relationships={rel_type}={dst}")
        print(f"  {rel_type}: {count:,} rows -> {dst.name}")

    con.close()

    # Build import command
    cmd_parts = [
        f"{args.neo4j_admin} database import full",
        "  --input-type=parquet",
        "  --overwrite-destination=true",
        "  --skip-bad-relationships=true",
        "  --skip-duplicate-nodes=true",
    ] + node_args + rel_args + [f"  {args.database}"]

    cmd = " \\\n".join(cmd_parts)
    print("\n# neo4j-admin import command:")
    print(cmd)

    if not args.dry_run:
        import subprocess, shlex
        flat = " ".join(shlex.quote(p.strip()) for p in cmd_parts)
        print(f"\nRunning import...")
        result = subprocess.run(
            flat, shell=True, text=True, capture_output=False
        )
        sys.exit(result.returncode)


if __name__ == "__main__":
    main()
