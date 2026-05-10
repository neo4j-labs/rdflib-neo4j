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
    parser.add_argument("input", help="RDF input file")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument(
        "--db",
        default=None,
        help="DuckDB database path (default: <output>/staging.duckdb; use :memory: to keep in RAM)",
    )
    parser.add_argument("--format", dest="rdf_format", help="RDF parser format (turtle, xml, nt, nquads, trig, n3)")
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
    parser.add_argument("--language-projection", action="store_true")
    parser.add_argument("--language-filter")
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--no-progress", action="store_true", help="Suppress progress output")
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
    prototype = DuckDBBulkPrototype(
        db_path=db_path,
        config=config,
        batch_size=args.batch_size,
        backend=args.parser,
        progress=not args.no_progress,
    )
    try:
        prototype.ingest_file(args.input, rdf_format=args.rdf_format)
        prototype.build_facts()
        prototype.export_parquet(args.output)
        return 0
    finally:
        prototype.close()


if __name__ == "__main__":
    raise SystemExit(main())
