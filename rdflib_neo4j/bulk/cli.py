import argparse

from rdflib_neo4j.bulk.config import AggregationMode, BulkImportConfig
from rdflib_neo4j.bulk.pipeline import DuckDBBulkPrototype
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="rdflib-neo4j-bulk-prototype",
        description="Convert RDF input into DuckDB-projected Neo4j bulk import Parquet files.",
    )
    parser.add_argument("input", help="RDF input file")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--db", default=":memory:", help="DuckDB database path")
    parser.add_argument("--format", dest="rdf_format", help="rdflib parser format")
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
    parser.add_argument("--batch-size", type=int, default=10000)
    args = parser.parse_args(argv)

    config = BulkImportConfig(
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY(args.handle_vocab_uris),
        aggregation_mode=AggregationMode(args.aggregation),
        language_projection=args.language_projection,
        language_filter=args.language_filter,
    )
    prototype = DuckDBBulkPrototype(
        db_path=args.db,
        config=config,
        batch_size=args.batch_size,
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
