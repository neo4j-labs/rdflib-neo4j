import json
import re
from pathlib import Path
from typing import Iterable, Optional

import duckdb
from rdflib import Graph, RDF

from rdflib_neo4j.bulk.config import AggregationMode, BulkImportConfig
from rdflib_neo4j.bulk.mapping import choose_primary_label, mapped_term, projected_property_name
from rdflib_neo4j.bulk.terms import object_parts, resource_id


class DuckDBBulkPrototype:
    def __init__(
        self,
        db_path: str = ":memory:",
        config: Optional[BulkImportConfig] = None,
        batch_size: int = 10000,
    ):
        self.db_path = db_path
        self.config = config or BulkImportConfig()
        self.batch_size = batch_size
        self.connection = duckdb.connect(db_path)
        self.initialize()

    def close(self):
        self.connection.close()

    def initialize(self):
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS rdf_triples (
                source_order UBIGINT,
                subject VARCHAR NOT NULL,
                predicate VARCHAR NOT NULL,
                object_kind VARCHAR NOT NULL,
                object_value VARCHAR NOT NULL,
                datatype VARCHAR,
                lang VARCHAR
            )
            """
        )

    def ingest_file(self, path: str, rdf_format: Optional[str] = None) -> int:
        graph = Graph()
        graph.parse(path, format=rdf_format)
        return self.ingest_triples(graph)

    def ingest_triples(self, triples: Iterable) -> int:
        rows = []
        total = 0
        for source_order, (subject, predicate, obj) in enumerate(triples, start=1):
            object_kind, object_value, datatype, lang = object_parts(obj)
            rows.append(
                (
                    source_order,
                    resource_id(subject),
                    str(predicate),
                    object_kind,
                    object_value,
                    datatype,
                    lang,
                )
            )
            if len(rows) >= self.batch_size:
                self._insert_triples(rows)
                total += len(rows)
                rows = []
        if rows:
            self._insert_triples(rows)
            total += len(rows)
        return total

    def build_facts(self):
        self._create_fact_tables()
        rows = self.connection.execute(
            "SELECT source_order, subject, predicate, object_kind, object_value, datatype, lang FROM rdf_triples"
        ).fetchall()

        resources = set()
        labels_by_uri = {}
        property_rows = []
        relationship_rows = []

        rdf_type = str(RDF.type)
        for source_order, subject, predicate, object_kind, object_value, datatype, lang in rows:
            resources.add(subject)
            if object_kind != "literal" and predicate != rdf_type:
                resources.add(object_value)

            if predicate == rdf_type and object_kind != "literal":
                labels_by_uri.setdefault(subject, set()).add(mapped_term(object_value, self.config))
            elif object_kind == "literal":
                property_name = mapped_term(predicate, self.config)
                projected_name = projected_property_name(property_name, lang, self.config)
                if projected_name:
                    property_rows.append(
                        (
                            subject,
                            property_name,
                            projected_name,
                            str(object_value),
                            datatype,
                            lang,
                            source_order,
                        )
                    )
            else:
                relationship_rows.append(
                    (
                        subject,
                        mapped_term(predicate, self.config),
                        object_value,
                        source_order,
                    )
                )

        node_rows = []
        label_rows = []
        for uri in sorted(resources):
            labels = labels_by_uri.get(uri, set())
            all_labels = ["Resource"] + sorted(labels)
            primary_label = choose_primary_label(labels, self.config)
            node_rows.append((uri, primary_label, all_labels))
            for label in all_labels:
                label_rows.append((uri, label))

        if node_rows:
            self.connection.executemany("INSERT INTO node_rows VALUES (?, ?, ?)", node_rows)
        if label_rows:
            self.connection.executemany("INSERT INTO node_labels VALUES (?, ?)", label_rows)
        if property_rows:
            self.connection.executemany("INSERT INTO property_facts VALUES (?, ?, ?, ?, ?, ?, ?)", property_rows)
        if relationship_rows:
            self.connection.executemany("INSERT INTO relationship_facts VALUES (?, ?, ?, ?)", relationship_rows)

    def profile_properties(self):
        self.connection.execute(
            """
            CREATE OR REPLACE TABLE node_property_profile AS
            SELECT
                nr.primary_label,
                pf.projected_property_name,
                count(*) AS value_count,
                count(DISTINCT pf.uri) AS subject_count,
                count(DISTINCT pf.value_type) AS type_count,
                list(DISTINCT pf.value_type ORDER BY pf.value_type) AS value_types
            FROM property_facts pf
            JOIN node_rows nr ON nr.uri = pf.uri
            GROUP BY nr.primary_label, pf.projected_property_name
            """
        )

    def pivot_nodes(self):
        self.profile_properties()
        self._create_node_property_values()
        property_count = self.connection.execute("SELECT count(*) FROM node_property_values").fetchone()[0]
        if property_count:
            self.connection.execute(
                """
                CREATE OR REPLACE TABLE node_properties_wide AS
                PIVOT (
                    SELECT uri, projected_property_name, value
                    FROM node_property_values
                )
                ON projected_property_name
                USING first(value)
                GROUP BY uri
                """
            )
            self.connection.execute(
                """
                CREATE OR REPLACE TABLE nodes_wide AS
                SELECT nr.*, npw.* EXCLUDE(uri)
                FROM node_rows nr
                LEFT JOIN node_properties_wide npw USING (uri)
                """
            )
        else:
            self.connection.execute("CREATE OR REPLACE TABLE nodes_wide AS SELECT * FROM node_rows")

    def export_parquet(self, output_dir: str):
        self.pivot_nodes()
        self.deduplicate_relationships()
        output_path = Path(output_dir)
        metadata_dir = output_path / "metadata"
        nodes_dir = output_path / "nodes"
        rels_dir = output_path / "relationships"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        nodes_dir.mkdir(parents=True, exist_ok=True)
        rels_dir.mkdir(parents=True, exist_ok=True)
        self._write_metadata(metadata_dir)

        labels = [
            row[0]
            for row in self.connection.execute("SELECT DISTINCT primary_label FROM nodes_wide ORDER BY primary_label").fetchall()
        ]
        for label in labels:
            target = nodes_dir / f"{_safe_name(label)}.parquet"
            self.connection.execute(
                f"""
                COPY (
                    SELECT *
                    FROM nodes_wide
                    WHERE primary_label = {_sql_literal(label)}
                ) TO {_sql_literal(str(target))} (FORMAT parquet)
                """
            )

        rel_types = [
            row[0]
            for row in self.connection.execute(
                "SELECT DISTINCT rel_type FROM relationship_rows ORDER BY rel_type"
            ).fetchall()
        ]
        for rel_type in rel_types:
            target = rels_dir / f"{_safe_name(rel_type)}.parquet"
            self.connection.execute(
                f"""
                COPY (
                    SELECT source_uri, target_uri
                    FROM relationship_rows
                    WHERE rel_type = {_sql_literal(rel_type)}
                ) TO {_sql_literal(str(target))} (FORMAT parquet)
                """
            )

    def counts(self):
        tables = [
            "rdf_triples",
            "node_rows",
            "property_facts",
            "relationship_facts",
        ]
        result = {}
        for table in tables:
            result[table] = self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if _table_exists(self.connection, "relationship_rows"):
            result["relationship_rows"] = self.connection.execute(
                "SELECT count(*) FROM relationship_rows"
            ).fetchone()[0]
        return result

    def deduplicate_relationships(self):
        self.connection.execute(
            """
            CREATE OR REPLACE TABLE relationship_rows AS
            SELECT source_uri, rel_type, target_uri
            FROM relationship_facts
            GROUP BY source_uri, rel_type, target_uri
            """
        )

    def _create_fact_tables(self):
        self.connection.execute("DROP TABLE IF EXISTS node_rows")
        self.connection.execute("DROP TABLE IF EXISTS node_labels")
        self.connection.execute("DROP TABLE IF EXISTS property_facts")
        self.connection.execute("DROP TABLE IF EXISTS relationship_facts")
        self.connection.execute("CREATE TABLE node_rows (uri VARCHAR, primary_label VARCHAR, labels VARCHAR[])")
        self.connection.execute("CREATE TABLE node_labels (uri VARCHAR, label VARCHAR)")
        self.connection.execute(
            """
            CREATE TABLE property_facts (
                uri VARCHAR,
                property_name VARCHAR,
                projected_property_name VARCHAR,
                value VARCHAR,
                value_type VARCHAR,
                lang VARCHAR,
                source_order UBIGINT
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE relationship_facts (
                source_uri VARCHAR,
                rel_type VARCHAR,
                target_uri VARCHAR,
                source_order UBIGINT
            )
            """
        )

    def _insert_triples(self, rows):
        self.connection.executemany(
            "INSERT INTO rdf_triples VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )

    def _write_metadata(self, metadata_dir: Path):
        (metadata_dir / "graph_config.json").write_text(
            json.dumps(
                {
                    "handleVocabUris": self.config.handle_vocab_uri_strategy.value,
                    "aggregationMode": self.config.aggregation_mode.value,
                    "languageProjection": self.config.language_projection,
                    "languageFilter": self.config.language_filter,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        (metadata_dir / "prefixes.json").write_text(
            json.dumps(self.config.all_prefixes(), indent=2, sort_keys=True) + "\n"
        )
        (metadata_dir / "counts.json").write_text(
            json.dumps(self.counts(), indent=2, sort_keys=True) + "\n"
        )
        target = metadata_dir / "primary_label_explain.parquet"
        self.connection.execute(
            f"""
            COPY (
                SELECT
                    uri,
                    primary_label,
                    labels AS all_labels,
                    CASE
                        WHEN primary_label = 'Resource' THEN 'fallback_resource'
                        ELSE 'first_non_generic_lexical_label'
                    END AS rule
                FROM node_rows
            ) TO {_sql_literal(str(target))} (FORMAT parquet)
            """
        )

    def _create_node_property_values(self):
        mode = self.config.aggregation_mode
        if mode == AggregationMode.FIRST:
            aggregate = "arg_min(value, source_order)"
        elif mode == AggregationMode.LAST:
            aggregate = "arg_max(value, source_order)"
        elif mode == AggregationMode.ARRAY:
            aggregate = "list(DISTINCT value ORDER BY value)"
        else:
            aggregate = "min(value)"

        self.connection.execute(
            f"""
            CREATE OR REPLACE TABLE node_property_values AS
            SELECT
                uri,
                projected_property_name,
                {aggregate} AS value
            FROM property_facts
            GROUP BY uri, projected_property_name
            """
        )


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return safe.strip("_") or "Resource"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _table_exists(connection, table_name: str) -> bool:
    return (
        connection.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
        > 0
    )
