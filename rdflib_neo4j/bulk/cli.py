import argparse
import sys
from pathlib import Path

from rdflib_neo4j.bulk.config import AggregationMode, BulkImportConfig
from rdflib_neo4j.bulk.ingest import BACKENDS
from rdflib_neo4j.bulk.neo4j_import import run_neo4j_import
from rdflib_neo4j.bulk.pipeline import DuckDBBulkPrototype
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="rdflib-neo4j-bulk-prototype",
        description="Convert RDF input into DuckDB-projected Neo4j bulk import Parquet files.",
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=None,
        help="RDF input file or directory of RDF files (not required for --stage build/export/export-nodes/export-rels)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output directory (required for stages that write Parquet: all, export, export-nodes, export-rels)",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="DuckDB database path (default: <output>/staging.duckdb; use :memory: to keep in RAM)",
    )
    parser.add_argument("--format", dest="rdf_format", help="RDF parser format (turtle, xml, nt, nquads, trig, n3)")
    parser.add_argument(
        "--glob",
        default=None,
        help="Glob pattern for directory input (e.g. '*.nt'). Default: auto-detect all known RDF extensions.",
    )
    parser.add_argument(
        "--parser",
        choices=list(BACKENDS),
        default="rdflib",
        help=(
            "Ingestion backend (default: rdflib). "
            "oxigraph: Rust streaming parser, strict BCP47/XML. "
            "duckdb_rdf: pure SQL via DuckDB rdf extension, fastest but uses "
            "URI-heuristic for plain-literal vs IRI disambiguation."
        ),
    )
    parser.add_argument(
        "--handle-vocab-uris",
        choices=[strategy.value for strategy in HANDLE_VOCAB_URI_STRATEGY],
        default=HANDLE_VOCAB_URI_STRATEGY.IGNORE.value,
    )
    parser.add_argument(
        "--aggregation",
        choices=[mode.value for mode in AggregationMode],
        default=AggregationMode.ANY.value,
    )
    parser.add_argument(
        "--filename-label-strip",
        default=None,
        help="Suffix to strip from the filename stem before using as label (e.g. '.na32.annot' for Affymetrix).",
    )
    parser.add_argument(
        "--filename-label",
        action="store_true",
        help=(
            "Use the input filename stem as a Neo4j label for nodes that lack an rdf:type. "
            "Applies to directory input; each file's stem becomes the label for its subjects "
            "(e.g. 'people.rdf' -> :people label). First file wins for subjects appearing in multiple files."
        ),
    )
    parser.add_argument("--language-projection", action="store_true")
    parser.add_argument("--language-filter")
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--no-progress", action="store_true", help="Suppress progress output")
    parser.add_argument(
        "--parallel",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Number of parallel worker processes for directory ingestion (duckdb_rdf only). "
            "Default: auto (CPU count). Use 1 for single-connection UNION ALL mode. "
            "Each worker converts its file slice to a temp Parquet chunk; the main process "
            "merges all chunks via DuckDB's fast Parquet reader."
        ),
    )
    parser.add_argument(
        "--export-workers",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Number of parallel worker processes for Parquet export (default: 2). "
            "Each worker opens the DB read-only and PIVOT-COPYs its assigned labels/rel-types "
            "concurrently. Each worker still uses DuckDB's internal thread pool, so don't set "
            "this too high — on large datasets (Freebase-scale) more than 2 workers can cause "
            ">200 GB combined RAM usage. Workers are further capped by available free memory "
            "(<16 GB → max 2, <8 GB → forced sequential). Use 1 for sequential export. "
            "Not supported for --db :memory:."
        ),
    )
    parser.add_argument(
        "--min-export-nodes",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Minimum node count for a label to be included in the Parquet export. "
            "Default: 0 (export all labels). Use e.g. 1000 to skip tiny fallback labels "
            "(Kp_lw, Engineering, Chess, ...) that result from domain-prefix extraction "
            "during analyze. Skipped labels are logged with their node counts."
        ),
    )
    parser.add_argument(
        "--temp-dir",
        default=None,
        help="Directory for parallel worker temp Parquet chunks (default: system temp). "
             "Set to a fast local SSD path to avoid cross-device I/O.",
    )
    parser.add_argument(
        "--drop-staging",
        action="store_true",
        help=(
            "Free staging memory/disk during build_facts: delete literal rows after "
            "property_facts are built (~70-80%% of staging), then drop the entire "
            "rdf_triples table after relationship_facts are built. "
            "Recommended for large datasets (Freebase-scale) to avoid OOM during "
            "the projection phase."
        ),
    )
    parser.add_argument(
        "--min-property-freq",
        type=int,
        default=1,
        metavar="N",
        help=(
            "Minimum number of nodes a property must appear on to be included in the output. "
            "Default: 1 (no filtering). Use e.g. 10 to drop ultra-rare properties. "
            "Applied per-global-count across all entity labels."
        ),
    )
    parser.add_argument(
        "--max-properties-per-label",
        type=int,
        default=1000,
        metavar="N",
        help=(
            "Maximum number of distinct properties to include per entity label. "
            "Default: 1000. When a label has more distinct properties than this limit "
            "(e.g. a generic 'Resource' type in Freebase), only the top-N most frequent "
            "properties (by node count) are kept. A property is kept if it ranks within "
            "the top-N for at least one of its labels."
        ),
    )
    parser.add_argument(
        "--stage",
        choices=[
            "all", "ingest", "build", "analyze",
            "export", "export-nodes", "export-rels",
            "neo4j-import", "compact",
        ],
        default="all",
        help=(
            "Pipeline stage to run (default: all = ingest→build→analyze→export). "
            "'ingest' — load RDF into staging DB only; "
            "'build' — build fact tables (node_rows, property_facts, relationship_facts); "
            "'analyze' — remap labels (smart coverage-based) + profile properties; "
            "'export' — per-label PIVOT and write Parquet; "
            "'export-nodes' / 'export-rels' — write only one side; "
            "'neo4j-import' — rewrite Parquet columns and run neo4j-admin; "
            "'compact' — copy fact tables to a new, smaller DB file (use --compact-db for dest path)."
        ),
    )
    parser.add_argument(
        "--label-map-file",
        default=None,
        metavar="PATH",
        help=(
            "Optional JSON file with custom label overrides layered on top of built-in smart defaults. "
            "Format: {\"exclude\": [glob patterns], \"map\": {\"original\": \"Canonical\"}}. "
            "Without this flag the pipeline uses its built-in exclude/map data for RDF/OWL meta-types "
            "and common ontologies (Freebase, Schema.org, DBpedia)."
        ),
    )
    parser.add_argument(
        "--min-rel-coverage",
        type=float,
        default=0.0001,
        metavar="F",
        help=(
            "Minimum fraction of total relationship triples a rel type must account for "
            "to be included in the export. Default: 0.0001 (0.01%%). "
            "Also filtered: built-in metadata predicates (notable_for, permissions, "
            "dataworld, etc.) regardless of frequency. Custom rel excludes can be added via "
            "--label-map-file JSON with a 'rel_exclude' key."
        ),
    )
    parser.add_argument(
        "--ensure-rel-per-label",
        action="store_true",
        default=True,
        help=(
            "Rescue orphaned node labels by force-including their highest-frequency "
            "non-pattern-excluded rel type. Ensures every exported node label has at "
            "least one relationship in the graph. On by default; use "
            "--no-ensure-rel-per-label to disable and only warn instead."
        ),
    )
    parser.add_argument(
        "--no-ensure-rel-per-label",
        action="store_false",
        dest="ensure_rel_per_label",
        help="Disable orphan-label rescue (see --ensure-rel-per-label).",
    )
    parser.add_argument(
        "--no-dedup-inverse-pairs",
        action="store_true",
        help=(
            "Disable automatic inverse-pair deduplication. By default, pairs of rel types "
            "with nearly identical triple counts (within 0.1%%) are assumed to be "
            "forward/reverse duplicates (common in Freebase, DBpedia); the lex-larger "
            "name is excluded. Use this flag to keep both directions."
        ),
    )
    parser.add_argument(
        "--min-label-coverage",
        type=float,
        default=0.001,
        metavar="F",
        help=(
            "Fraction of total nodes a label must appear on (globally across all labels arrays) "
            "to be selected as a primary-label candidate during analyze/remap-labels. "
            "Default: 0.001 (0.1%%). Lower → more specific labels; higher → fewer, coarser labels. "
            "Example: --min-label-coverage 0.01 targets ~100 labels on a 100M-node graph."
        ),
    )
    parser.add_argument(
        "--compact-db",
        default=None,
        metavar="PATH",
        help="Destination path for the compacted DB file (required for --stage compact).",
    )
    # neo4j-import stage options
    parser.add_argument(
        "--neo4j-admin",
        default=None,
        metavar="PATH",
        help="Path to neo4j-admin binary (default: auto-detect from PATH and common install locations).",
    )
    parser.add_argument(
        "--database",
        default="neo4j",
        help="Target Neo4j database name for import (default: neo4j).",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Do not pass --overwrite-destination to neo4j-admin (fail if database already exists).",
    )
    parser.add_argument(
        "--no-skip-bad-relationships",
        action="store_true",
        help="Do not pass --skip-bad-relationships to neo4j-admin (fail on dangling relationships).",
    )
    parser.add_argument(
        "--no-skip-duplicate-nodes",
        action="store_true",
        help="Do not pass --skip-duplicate-nodes to neo4j-admin.",
    )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=-1,
        metavar="N",
        help=(
            "For neo4j-import stage: split Parquet files larger than N rows into multiple "
            "chunks so neo4j-admin can use parallel import workers per label/type. "
            "Default: -1 (no splitting). Example: --rows-per-file 5000000 splits files "
            "with >5M rows into parallel chunks via DuckDB PER_THREAD_OUTPUT."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="For neo4j-import stage: print the neo4j-admin command but do not execute it.",
    )
    parser.add_argument(
        "--filter-cmd",
        help=(
            "Shell command piped after the decompressor (duckdb_rdf only). "
            "Executed via /bin/sh so pipes work: e.g. 'ag -v pat1 | ag -v pat2'. "
            "Use ag (Silver Searcher) for speed. "
            "Example for Affymetrix bio2rdf (drops invalid IRI lines): "
            r"""'ag -v "^<bio2rdf_dataset:" | ag -v "<[^>]*[| ][^>]*>" | ag -v '"'"'<[^>]*`[^>]*>'"'"''"""
            " (backtick must be in a single-quoted stage to avoid shell command substitution)"
        ),
    )
    args = parser.parse_args(argv)

    if args.stage in ("all", "ingest") and args.input is None:
        parser.error("'input' is required for --stage all/ingest")
    if args.stage in ("all", "export", "export-nodes", "export-rels", "neo4j-import") and args.output is None:
        parser.error("--output is required for --stage all/export/export-nodes/export-rels/neo4j-import")
    if args.stage in ("build", "analyze") and args.output is None and args.db is None:
        parser.error("--db is required for --stage build/analyze when --output is not provided")
    if args.stage == "compact" and args.compact_db is None:
        parser.error("--compact-db PATH is required for --stage compact")

    output_path = Path(args.output) if args.output else None
    # Always create the output directory when --output is given so the default staging.duckdb
    # path is valid even for --stage ingest (which writes only the DB, not Parquet files).
    if output_path is not None:
        output_path.mkdir(parents=True, exist_ok=True)
    # Default DB sits inside the output directory; explicit --db overrides this.
    db_path = args.db if args.db is not None else str(output_path / "staging.duckdb")

    config = BulkImportConfig(
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY(args.handle_vocab_uris),
        aggregation_mode=AggregationMode(args.aggregation),
        language_projection=args.language_projection,
        language_filter=args.language_filter,
    )
    filter_cmd = args.filter_cmd if args.filter_cmd else None

    prototype = DuckDBBulkPrototype(
        db_path=db_path,
        config=config,
        batch_size=args.batch_size,
        backend=args.parser,
        progress=not args.no_progress,
        filter_cmd=filter_cmd,
        filename_label=args.filename_label,
        parallel=args.parallel,
        temp_dir=args.temp_dir,
        drop_staging=args.drop_staging,
        min_property_freq=args.min_property_freq,
        max_properties_per_label=args.max_properties_per_label,
        label_map_file=args.label_map_file,
        export_workers=args.export_workers,
        min_export_nodes=args.min_export_nodes,
    )
    stage = args.stage
    try:
        if stage in ("all", "ingest"):
            input_path = Path(args.input)
            if input_path.is_dir():
                prototype.ingest_directory(
                    args.input,
                    rdf_format=args.rdf_format,
                    glob_pattern=args.glob,
                    filename_label_strip=args.filename_label_strip,
                )
            else:
                prototype.ingest_file(args.input, rdf_format=args.rdf_format)

        if stage in ("all", "build"):
            prototype.build_facts()

        if stage in ("all", "analyze"):
            prototype.remap_labels(
                label_map_file=args.label_map_file,
                min_label_coverage=args.min_label_coverage,
            )
            prototype.profile_properties()
            prototype.profile_relationships(
                rel_map_file=args.label_map_file,
                min_rel_coverage=args.min_rel_coverage,
                dedup_inverse_pairs=not args.no_dedup_inverse_pairs,
                ensure_rel_per_label=args.ensure_rel_per_label,
            )

        if stage in ("all", "export"):
            prototype.export_parquet(args.output)
        elif stage == "export-nodes":
            prototype.export_nodes(args.output)
        elif stage == "export-rels":
            prototype.export_relationships(args.output)

        if stage == "neo4j-import":
            prototype.close()  # release DB lock before running neo4j-admin
            return run_neo4j_import(
                parquet_dir=args.output,
                database=args.database,
                neo4j_admin=args.neo4j_admin,
                overwrite=not args.no_overwrite,
                skip_bad_relationships=not args.no_skip_bad_relationships,
                skip_duplicate_nodes=not args.no_skip_duplicate_nodes,
                rows_per_file=args.rows_per_file,
                dry_run=args.dry_run,
                progress=not args.no_progress,
            )

        if stage == "compact":
            prototype.compact(args.compact_db)

        return 0
    finally:
        prototype.close()


if __name__ == "__main__":
    raise SystemExit(main())
