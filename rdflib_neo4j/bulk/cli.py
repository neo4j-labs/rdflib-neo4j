import argparse
import sys
from pathlib import Path

from rdflib_neo4j.bulk.config import AggregationMode, BulkImportConfig
from rdflib_neo4j.bulk.ingest import BACKENDS
from rdflib_neo4j.bulk.pipeline import DuckDBBulkPrototype
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="rdflib-neo4j-bulk-prototype",
        description="Convert RDF input into DuckDB-projected Neo4j bulk import Parquet files.",
    )
    parser.add_argument("input", help="RDF input file or directory of RDF files")
    parser.add_argument("--output", required=True, help="Output directory")
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
        "--filter-cmd",
        help=(
            "Shell command piped after the decompressor (duckdb_rdf only). "
            "Executed via /bin/sh so pipes work: e.g. 'ag -v pat1 | ag -v pat2'. "
            "Use ag (Silver Searcher) for speed. "
            "Example for Affymetrix bio2rdf (drops invalid IRI lines): "
            r"""'ag -v "^<bio2rdf_dataset:" | ag -v "<[^>]*[| `][^>]*>"'"""
        ),
    )
    args = parser.parse_args(argv)

    # Default DB is on-disk inside the output directory to reduce memory pressure.
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)
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
    )
    try:
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
        prototype.build_facts()
        prototype.export_parquet(args.output)
        return 0
    finally:
        prototype.close()


if __name__ == "__main__":
    raise SystemExit(main())
