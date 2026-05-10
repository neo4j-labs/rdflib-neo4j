import json
import re
import sys
import time
from pathlib import Path
from typing import Iterable, Optional

import duckdb
from rdflib import Graph, RDF

from rdflib_neo4j.bulk.config import AggregationMode, BulkImportConfig
from rdflib_neo4j.bulk.ingest import BACKENDS, ingest
from rdflib_neo4j.bulk.mapping import choose_primary_label, mapped_term, projected_property_name
from rdflib_neo4j.bulk.terms import object_parts, resource_id


class DuckDBBulkPrototype:
    def __init__(
        self,
        db_path: str = ":memory:",
        config: Optional[BulkImportConfig] = None,
        batch_size: int = 100_000,
        backend: str = "rdflib",
        progress: bool = True,
        filter_cmd: Optional[list] = None,
        filename_label: bool = False,
    ):
        self.db_path = db_path
        self.config = config or BulkImportConfig()
        self.batch_size = batch_size
        self.backend = backend
        self.progress = progress
        self.filter_cmd = filter_cmd
        self.filename_label = filename_label
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
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS subject_file_tag (
                subject VARCHAR PRIMARY KEY,
                file_tag VARCHAR NOT NULL
            )
            """
        )

    def ingest_file(self, path: str, rdf_format: Optional[str] = None) -> int:
        """Stream *path* into the staging table using the configured backend."""
        return ingest(
            path,
            rdf_format,
            self.connection,
            backend=self.backend,
            batch_size=self.batch_size,
            progress=self.progress,
            filter_cmd=self.filter_cmd,
        )

    def ingest_directory(
        self,
        directory: str,
        rdf_format: Optional[str] = None,
        glob_pattern: Optional[str] = None,
        filename_label_strip: Optional[str] = None,
    ) -> int:
        """Ingest all RDF files in *directory* into the staging table.

        Files are sorted lexicographically for deterministic ordering.
        *glob_pattern* overrides the default extension-based discovery
        (e.g. ``"*.nt"`` to restrict to N-Triples only).
        """
        from pathlib import Path as _Path

        _RDF_SUFFIXES = {
            ".nt", ".ttl", ".turtle", ".n3",
            ".xml", ".rdf", ".owl",
            ".nq", ".nquads", ".trig",
            # compressed variants
            ".nt.gz", ".ttl.gz", ".nt.bz2", ".ttl.bz2",
            ".nt.zst", ".ttl.zst", ".nt.xz", ".ttl.xz",
        }

        dir_path = _Path(directory)
        if glob_pattern:
            files = sorted(dir_path.glob(glob_pattern))
        else:
            files = sorted(
                f for f in dir_path.iterdir()
                if f.is_file() and any(f.name.lower().endswith(s) for s in _RDF_SUFFIXES)
            )

        if not files:
            raise FileNotFoundError(
                f"No RDF files found in {directory}. "
                f"Pass --glob to specify a pattern (e.g. '*.nt')."
            )

        if self.progress:
            import sys as _sys
            print(
                f"[ingest] {len(files)} files in {directory}",
                file=_sys.stderr,
            )

        total = 0
        for f in files:
            max_rowid_before = self.connection.execute(
                "SELECT coalesce(max(rowid), -1) FROM rdf_triples"
            ).fetchone()[0]
            total += self.ingest_file(str(f), rdf_format=rdf_format)
            if self.filename_label:
                stem = _Path(f).stem
                if filename_label_strip and stem.endswith(filename_label_strip):
                    stem = stem[: -len(filename_label_strip)]
                file_tag = stem
                self.connection.execute(
                    """
                    INSERT INTO subject_file_tag
                    SELECT DISTINCT subject, ?
                    FROM rdf_triples
                    WHERE rowid > ?
                    ON CONFLICT DO NOTHING
                    """,
                    [file_tag, max_rowid_before],
                )
        return total

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
        """Build projection fact tables from staged triples.

        Uses pure DuckDB SQL for IGNORE/KEEP strategies so no rows cross the
        Python boundary.  Falls back to the Python loop only when custom
        mappings or SHORTEN/MAP strategies are in use.
        """
        from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY
        if self.config.handle_vocab_uri_strategy in (
            HANDLE_VOCAB_URI_STRATEGY.IGNORE,
            HANDLE_VOCAB_URI_STRATEGY.KEEP,
        ) and not self.config.custom_mappings:
            self._build_facts_sql()
        else:
            self._build_facts_python()

    def _build_facts_sql(self):
        """All-SQL projection — no Python per-row work, no fetchall."""
        from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY

        if self.progress:
            print("[project] building fact tables (SQL)...", file=sys.stderr, flush=True)
        t0 = time.monotonic()

        keep = self.config.handle_vocab_uri_strategy == HANDLE_VOCAB_URI_STRATEGY.KEEP
        generic = ", ".join(f"'{g}'" for g in self.config.generic_labels)
        rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

        if keep:
            map_term = "predicate"
            map_obj  = "object_value"
        else:
            # IGNORE: local name after last # or /
            map_term = "regexp_replace(predicate, '^.*[/#]', '')"
            map_obj  = "regexp_replace(object_value, '^.*[/#]', '')"

        lang_filter = self.config.language_filter
        lang_filter_clause = (
            f"AND (lang IS NULL OR lang = '{lang_filter}')" if lang_filter else ""
        )

        if self.config.language_projection:
            proj_name = f"CASE WHEN lang IS NOT NULL THEN {map_term} || '_' || replace(lang, '-', '_') ELSE {map_term} END"
        else:
            proj_name = map_term

        self._create_fact_tables()

        # ---- node_labels (mapped rdf:type values) ----
        self.connection.execute(f"""
            INSERT INTO node_labels
            SELECT DISTINCT subject, {map_obj}
            FROM rdf_triples
            WHERE predicate = '{rdf_type}'
              AND object_kind IN ('iri', 'bnode')
        """)

        # ---- node_rows (one per resource, primary_label + labels array) ----
        # resources = subjects ∪ non-literal, non-type objects
        if self.filename_label:
            file_tag_join = "LEFT JOIN subject_file_tag sft ON r.uri = sft.subject"
            file_tag_primary = "COALESCE(sft.file_tag, 'Resource')"
            file_tag_labels = """
                CASE WHEN sft.file_tag IS NOT NULL
                     THEN list_prepend('Resource', list_prepend(sft.file_tag, coalesce(list(DISTINCT nl.label ORDER BY nl.label), [])))
                     ELSE list_prepend('Resource', coalesce(list(DISTINCT nl.label ORDER BY nl.label), []))
                END"""
        else:
            file_tag_join = ""
            file_tag_primary = "'Resource'"
            file_tag_labels = "list_prepend('Resource', coalesce(list(DISTINCT nl.label ORDER BY nl.label), []))"

        self.connection.execute(f"""
            INSERT INTO node_rows
            WITH all_uris AS (
                SELECT DISTINCT subject AS uri FROM rdf_triples
                UNION
                SELECT DISTINCT object_value AS uri
                FROM rdf_triples
                WHERE object_kind IN ('iri', 'bnode')
                  AND predicate != '{rdf_type}'
            )
            SELECT
                r.uri,
                COALESCE(
                    MIN(CASE WHEN nl.label NOT IN ({generic}) THEN nl.label END),
                    {file_tag_primary}
                ) AS primary_label,
                {file_tag_labels} AS labels
            FROM all_uris r
            LEFT JOIN node_labels nl ON r.uri = nl.uri
            {file_tag_join}
            GROUP BY r.uri{', sft.file_tag' if self.filename_label else ''}
        """)

        # ---- property_facts (literal objects, not rdf:type) ----
        self.connection.execute(f"""
            INSERT INTO property_facts
            SELECT
                subject,
                {map_term} AS property_name,
                {proj_name} AS projected_property_name,
                object_value,
                datatype,
                lang,
                source_order
            FROM rdf_triples
            WHERE object_kind = 'literal'
              AND predicate != '{rdf_type}'
              {lang_filter_clause}
        """)

        # ---- relationship_facts (IRI/bnode objects, not rdf:type) ----
        self.connection.execute(f"""
            INSERT INTO relationship_facts
            SELECT
                subject,
                {map_term} AS rel_type,
                object_value,
                source_order
            FROM rdf_triples
            WHERE object_kind IN ('iri', 'bnode')
              AND predicate != '{rdf_type}'
        """)

        if self.progress:
            c = self.connection
            n_nodes = c.execute("SELECT count(*) FROM node_rows").fetchone()[0]
            n_props = c.execute("SELECT count(*) FROM property_facts").fetchone()[0]
            n_rels  = c.execute("SELECT count(*) FROM relationship_facts").fetchone()[0]
            elapsed = time.monotonic() - t0
            print(
                f"[project] {n_nodes:,} nodes  {n_props:,} props  "
                f"{n_rels:,} rels  {elapsed:.1f}s",
                file=sys.stderr,
            )

    def _build_facts_python(self):
        """Python-loop projection for SHORTEN/MAP strategies with custom mappings."""
        self._create_fact_tables()
        if self.progress:
            print("[project] building fact tables (Python)...", file=sys.stderr)
        t0 = time.monotonic()
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
                        (subject, property_name, projected_name, str(object_value), datatype, lang, source_order)
                    )
            else:
                relationship_rows.append(
                    (subject, mapped_term(predicate, self.config), object_value, source_order)
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
        if self.progress:
            elapsed = time.monotonic() - t0
            print(
                f"[project] {len(node_rows):,} nodes  {len(property_rows):,} props  "
                f"{len(relationship_rows):,} rels  {elapsed:.1f}s",
                file=sys.stderr,
            )

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
        if self.progress:
            print("[export] pivoting nodes and projecting Parquet...", file=sys.stderr)
        t0 = time.monotonic()
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
            if self.progress:
                n = self.connection.execute(
                    f"SELECT count(*) FROM nodes_wide WHERE primary_label = {_sql_literal(label)}"
                ).fetchone()[0]
                print(f"[export]   nodes/{_safe_name(label)}.parquet  {n:,} rows", file=sys.stderr)

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
            if self.progress:
                n = self.connection.execute(
                    f"SELECT count(*) FROM relationship_rows WHERE rel_type = {_sql_literal(rel_type)}"
                ).fetchone()[0]
                print(f"[export]   relationships/{_safe_name(rel_type)}.parquet  {n:,} rows", file=sys.stderr)

        if self.progress:
            print(f"[export] done  {time.monotonic() - t0:.1f}s", file=sys.stderr)

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
