import fnmatch
import json
import os as _os
import re
import sys
import time
from pathlib import Path
from typing import Iterable, Optional

from rdflib_neo4j.bulk.utils import free_mem_gb as _free_mem_gb, mem_stat as _mem_stat

import duckdb
from rdflib import Graph, RDF

from rdflib_neo4j.bulk.config import AggregationMode, BulkImportConfig
from rdflib_neo4j.bulk.ingest import (
    BACKENDS,
    ingest,
    ingest_duckdb_rdf_bulk,
    ingest_duckdb_rdf_parallel,
    ingest_duckdb_rdf_split_file,
)
from rdflib_neo4j.bulk.mapping import choose_primary_label, mapped_term, projected_property_name
from rdflib_neo4j.bulk.terms import object_parts, resource_id

# ---------------------------------------------------------------------------
# Built-in label metadata — loaded from builtin_label_map.json
# ---------------------------------------------------------------------------

def _load_builtin_label_map():
    """Load exclude patterns, semantic map, and rel excludes from the bundled JSON file.

    String entries starting with '---' are treated as comments and skipped.
    """
    _json_path = Path(__file__).parent / "builtin_label_map.json"
    with open(_json_path) as _f:
        _data = json.load(_f)
    exclude = [p for p in _data.get("exclude", []) if not p.startswith("---")]
    rel_exclude = [p for p in _data.get("rel_exclude", []) if not p.startswith("---")]
    return exclude, _data.get("map", {}), rel_exclude


_BUILTIN_EXCLUDE_PATTERNS: list[str]
_BUILTIN_SEMANTIC_MAP: dict[str, str]
_BUILTIN_REL_EXCLUDE_PATTERNS: list[str]
_BUILTIN_EXCLUDE_PATTERNS, _BUILTIN_SEMANTIC_MAP, _BUILTIN_REL_EXCLUDE_PATTERNS = _load_builtin_label_map()

# Freebase-specific predicates that duplicate rdf:type semantics.
# Excluded from relationship_facts at build time alongside rdf:type.
_FREEBASE_TYPE_PREDICATES = (
    "http://rdf.freebase.com/ns/type.object.type",
    "http://rdf.freebase.com/ns/type.type.instance",
    "http://rdf.freebase.com/ns/kg.object_profile.prominent_type",
)


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
        parallel: Optional[int] = None,
        temp_dir: Optional[str] = None,
        drop_staging: bool = False,
        min_property_freq: int = 1,
        max_properties_per_label: int = 1000,
        label_map_file: Optional[str] = None,
        export_workers: Optional[int] = None,
        min_export_nodes: int = 0,
        subclass_labels: bool = True,
        subclass_label_depth: int = 5,
        min_subclass_label_coverage: float = 0.001,
    ):
        self.db_path = db_path
        self.config = config or BulkImportConfig()
        self.batch_size = batch_size
        self.backend = backend
        self.progress = progress
        self.filter_cmd = filter_cmd
        self.filename_label = filename_label
        self.parallel = parallel  # None = auto (CPU count), 1 = sequential
        self.temp_dir = temp_dir
        self.drop_staging = drop_staging
        self.min_property_freq = min_property_freq
        self.max_properties_per_label = max_properties_per_label
        # label_map_file: optional path to a JSON label mapping config.
        # When set, remap_labels() uses it to fix primary_label on an existing node_rows table.
        # Future: wire into _build_facts_sql so remapping happens at build time.
        self.label_map_file = label_map_file
        # export_workers: None = auto (cpu_count), 1 = sequential.
        # Parallel export opens read-only worker connections; not supported for :memory: DBs.
        self.export_workers = export_workers
        self.min_export_nodes = min_export_nodes
        # subclass_labels: when True (default) automatically detect rdfs:subClassOf triples
        # and use them as extra Neo4j labels on each node (stored in a :LABEL column).
        self.subclass_labels = subclass_labels
        self.subclass_label_depth = subclass_label_depth
        self.min_subclass_label_coverage = min_subclass_label_coverage
        self.connection = duckdb.connect(db_path)
        self._apply_duckdb_settings()
        self.initialize()

    def _apply_duckdb_settings(self):
        """Configure DuckDB memory, parallelism, and spill-to-disk settings.

        Called once after connect(). Sets memory_limit to 75% of physical RAM so DuckDB
        spills to disk rather than crashing on large aggregates. Threads are set
        conservatively (cpu_count // 4) because export workers open additional
        DuckDB connections and each connection uses its thread allocation.
        """
        try:
            import psutil as _ps
            total_gb = _ps.virtual_memory().total / (1024 ** 3)
            memory_limit_gb = max(4, int(total_gb * 0.75))
        except ImportError:
            memory_limit_gb = 8

        n_cpu = _os.cpu_count() or 4
        threads = max(1, n_cpu // 4)

        if self.temp_dir:
            tmp = self.temp_dir
        elif self.db_path != ":memory:":
            tmp = str(Path(self.db_path).parent / "duckdb_tmp")
        else:
            tmp = None

        self.connection.execute(f"SET memory_limit='{memory_limit_gb}GB'")
        self.connection.execute(f"SET threads={threads}")
        self.connection.execute("SET preserve_insertion_order=false")
        if tmp:
            Path(tmp).mkdir(parents=True, exist_ok=True)
            self.connection.execute(f"SET temp_directory='{tmp}'")

        if self.progress:
            print(
                f"[duckdb] memory_limit={memory_limit_gb}GB  threads={threads}"
                f"  temp={tmp or 'default'}{_mem_stat()}",
                file=sys.stderr,
            )

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
        """Stream *path* into the staging table using the configured backend.

        When the backend is duckdb_rdf and ``self.parallel`` is not 1, large NT/NQ
        files are automatically byte-split into ``self.parallel`` (or CPU-count)
        chunks and ingested in parallel for maximum throughput.  Non-line-oriented
        formats (Turtle, RDF/XML) fall back to sequential ingest automatically.
        """
        if self.backend == "duckdb_rdf" and self.parallel != 1:
            return ingest_duckdb_rdf_split_file(
                path,
                rdf_format,
                self.connection,
                n_workers=self.parallel,
                temp_dir=self.temp_dir,
                progress=self.progress,
            )
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

        can_parallel = (
            self.backend == "duckdb_rdf"
            and not self.filename_label
            and not self.filter_cmd
            and all(not str(f).endswith((".gz", ".bz2", ".xz", ".zst", ".zstd", ".7z")) for f in files)
        )
        if can_parallel:
            n_workers = self.parallel  # None → CPU count (auto), 1 → UNION ALL
            effective_workers = min(n_workers or _os.cpu_count() or 1, len(files))
            # Use UNION ALL when: explicitly requested (n_workers==1), single worker,
            # or too few files to justify process spawn overhead (< 4 files per worker).
            use_bulk = effective_workers <= 1 or len(files) < 4
            if use_bulk:
                return ingest_duckdb_rdf_bulk(
                    [str(f) for f in files],
                    rdf_format,
                    self.connection,
                    self.progress,
                )
            # Multi-process: each worker converts its slice to Parquet → fast merge.
            return ingest_duckdb_rdf_parallel(
                [str(f) for f in files],
                rdf_format,
                self.connection,
                n_workers=n_workers,
                temp_dir=self.temp_dir,
                progress=self.progress,
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

        if self.drop_staging:
            # Free literal rows — largest portion of staging (70-80% for typical RDF).
            # Only IRI/bnode object rows are still needed for relationship_facts below.
            self.connection.execute(
                "DELETE FROM rdf_triples WHERE object_kind = 'literal'"
            )
            self.connection.execute("CHECKPOINT")

        # ---- relationship_facts (IRI/bnode objects, not rdf:type) ----
        # Also exclude Freebase-specific predicates that duplicate rdf:type semantics.
        # type.object.type / type.type.instance are Freebase's own rdf:type assertions
        # stored as regular predicates — keeping them would add ~533M (~49%) redundant
        # triples that are already captured as node labels via the rdf:type path above.
        # kg.object_profile.prominent_type is a "most prominent type" index that is also
        # already reflected in the labels array and adds no traversal value.
        _fb_type_preds = ", ".join(f"'{p}'" for p in _FREEBASE_TYPE_PREDICATES)
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
              AND predicate NOT IN ({_fb_type_preds})
        """)

        if self.drop_staging:
            # All staging data is now materialised — drop the table and compact.
            self.connection.execute("DROP TABLE rdf_triples")
            self.connection.execute("CHECKPOINT")

        if self.progress:
            c = self.connection
            n_nodes = c.execute("SELECT count(*) FROM node_rows").fetchone()[0]
            n_props = c.execute("SELECT count(*) FROM property_facts").fetchone()[0]
            n_rels  = c.execute("SELECT count(*) FROM relationship_facts").fetchone()[0]
            elapsed = time.monotonic() - t0
            print(
                f"[project] {n_nodes:,} nodes  {n_props:,} props  "
                f"{n_rels:,} rels  {elapsed:.1f}s{_mem_stat()}",
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
                f"{len(relationship_rows):,} rels  {elapsed:.1f}s{_mem_stat()}",
                file=sys.stderr,
            )

    def remap_labels(
        self,
        label_map_file: Optional[str] = None,
        min_label_coverage: float = 0.001,
    ) -> None:
        """Re-derive primary_label from node_rows.labels using coverage-based selection.

        Works on an already-built DB without re-ingesting.  Algorithm:
          1. Compute global label frequency (how many nodes carry each label).
          2. Exclude ontology/meta labels (built-in list + optional file overrides).
          3. Threshold = max(total_nodes * min_label_coverage, 100).
          4. For each node: pick the *most specific* (lowest global freq) non-excluded
             label that meets the threshold.  Apply canonical rename map.
          5. If no label meets the threshold, fall back to the most specific
             non-excluded label and extract its domain prefix (e.g.
             ``film.film`` → ``Film``) so nodes still get a meaningful label.
          6. UPDATE node_rows, CHECKPOINT.

        Args:
            label_map_file: Optional JSON file with ``"exclude"`` patterns and
                ``"map"`` overrides layered on top of the built-in data.
            min_label_coverage: Fraction of total nodes a label must appear on
                globally to be considered as a primary-label candidate.
                Default 0.001 (0.1 %).  Lower → more specific labels;
                higher → fewer, coarser labels.
        """
        # ------------------------------------------------------------------ #
        # Step 1: collect all distinct labels from the DB                    #
        # ------------------------------------------------------------------ #
        # unnest(labels) returns NULL for nodes that have no rdf:type triples;
        # both the WHERE clause and the Python guard are needed: WHERE filters the
        # row before unnest, but DuckDB can still produce NULL elements from
        # unnest on a non-NULL array that contains NULLs. fnmatch(None, pat) raises.
        all_labels: list[str] = [
            row[0]
            for row in self.connection.execute(
                "SELECT DISTINCT unnest(labels) FROM node_rows WHERE labels IS NOT NULL"
            ).fetchall()
            if row[0] is not None
        ]
        if self.progress:
            print(
                f"[remap] {len(all_labels):,} distinct labels found, "
                f"coverage threshold={min_label_coverage:.3%}",
                file=sys.stderr, flush=True,
            )

        # ------------------------------------------------------------------ #
        # Step 2: resolve excludes + map (built-in + file overrides)         #
        # ------------------------------------------------------------------ #
        exclude_patterns: list[str] = list(_BUILTIN_EXCLUDE_PATTERNS) + list(self.config.generic_labels)
        map_config: dict[str, str] = dict(_BUILTIN_SEMANTIC_MAP)

        if label_map_file or self.label_map_file:
            path = label_map_file or self.label_map_file
            with open(path) as f:
                custom = json.load(f)
            exclude_patterns.extend(custom.get("exclude", []))
            # "exclude_remove" lets callers un-exclude built-in patterns (e.g. "Class"
            # for OWL ontologies where owl:Class carries domain-meaningful entities).
            for pat in custom.get("exclude_remove", []):
                try:
                    exclude_patterns.remove(pat)
                except ValueError:
                    pass
            map_config.update(custom.get("map", {}))

        resolved_excludes: set[str] = set()
        for pat in exclude_patterns:
            if "*" in pat:
                for lbl in all_labels:
                    if fnmatch.fnmatch(lbl, pat):
                        resolved_excludes.add(lbl)
            else:
                resolved_excludes.add(pat)

        resolved_map: dict[str, str] = {}
        for pat, canonical in map_config.items():
            if canonical is None:
                if "*" in pat:
                    for lbl in all_labels:
                        if fnmatch.fnmatch(lbl, pat):
                            resolved_excludes.add(lbl)
                else:
                    resolved_excludes.add(pat)
            elif "*" in pat:
                for lbl in all_labels:
                    if fnmatch.fnmatch(lbl, pat) and lbl not in resolved_excludes:
                        resolved_map[lbl] = canonical
            else:
                if pat not in resolved_excludes:
                    resolved_map[pat] = canonical

        # ------------------------------------------------------------------ #
        # Step 3: load resolved sets into DuckDB temp tables                 #
        # ------------------------------------------------------------------ #
        self.connection.execute("CREATE OR REPLACE TEMP TABLE _remap_excludes (label VARCHAR)")
        if resolved_excludes:
            self.connection.executemany(
                "INSERT INTO _remap_excludes VALUES (?)",
                [(lbl,) for lbl in resolved_excludes],
            )

        self.connection.execute(
            "CREATE OR REPLACE TEMP TABLE _remap_map (original VARCHAR, canonical VARCHAR)"
        )
        if resolved_map:
            self.connection.executemany(
                "INSERT INTO _remap_map VALUES (?, ?)", list(resolved_map.items())
            )

        if self.progress:
            print(
                f"[remap] {len(resolved_excludes):,} excluded, "
                f"{len(resolved_map):,} mapped",
                file=sys.stderr, flush=True,
            )

        # ------------------------------------------------------------------ #
        # Step 4: compute global label frequencies + threshold               #
        # ------------------------------------------------------------------ #
        total_nodes = self.connection.execute("SELECT count(*) FROM node_rows").fetchone()[0]
        # Raw Freebase types are compound (e.g. american_football.football_player) and
        # there are 16,000+ of them. Without a coverage threshold all become separate
        # node files, causing PIVOT failures. The threshold (~total * 0.001) selects
        # ~100 labels that cover most nodes; rarer labels fall back to domain extraction.
        # Floor of 1 so single-node test DBs still work.
        threshold = max(int(total_nodes * min_label_coverage), 1)
        if self.progress:
            print(
                f"[remap] total nodes={total_nodes:,}, threshold={threshold:,} nodes",
                file=sys.stderr, flush=True,
            )

        # ------------------------------------------------------------------ #
        # Step 5: pick best primary_label per node via SQL                   #
        # ------------------------------------------------------------------ #
        # Strategy (in priority order):
        #   A. Most specific (lowest freq) non-excluded label >= threshold,
        #      with canonical rename applied.
        #   B. Most specific non-excluded label (any freq), domain-extracted
        #      and/or canonically renamed — so nodes always get a meaningful label.
        #   C. 'Resource' hard fallback.
        #
        # "Most specific" = lowest global frequency: a node typed both people.person
        # (30M freq) and american_football.football_player (27K freq) gets the more
        # specific label IF it meets the threshold, else falls back to the broader one.
        #
        # Domain extraction: 'film.film' → first part before '.' → 'Film'.
        # Uses upper(left(...))||substr(...) because DuckDB has no initcap() function.
        self.connection.execute(f"""
            CREATE OR REPLACE TEMP TABLE _new_primary AS
            WITH
            lf AS (
                SELECT label, count(*) AS freq
                FROM (SELECT unnest(labels) AS label FROM node_rows)
                GROUP BY label
            ),
            scored AS (
                SELECT
                    nr.uri,
                    t.label,
                    lf.freq,
                    re.label IS NOT NULL AS is_excluded,
                    COALESCE(rm.canonical,
                        CASE
                            WHEN position('.' IN t.label) > 0
                                THEN upper(left(split_part(t.label, '.', 1), 1)) || substr(split_part(t.label, '.', 1), 2)
                            ELSE t.label
                        END
                    ) AS display_label,
                    rm.canonical IS NOT NULL AS is_mapped
                FROM node_rows nr,
                     unnest(nr.labels) AS t(label)
                LEFT JOIN lf        ON lf.label  = t.label
                LEFT JOIN _remap_excludes re ON re.label  = t.label
                LEFT JOIN _remap_map      rm ON rm.original = t.label
            ),
            best_eligible AS (
                -- Most specific non-excluded label meeting the coverage threshold
                SELECT DISTINCT ON (uri)
                    uri, display_label
                FROM scored
                WHERE NOT is_excluded AND freq >= {threshold}
                ORDER BY uri, freq ASC
            ),
            best_fallback AS (
                -- Most specific non-excluded label (any freq) — domain-extracted
                SELECT DISTINCT ON (uri)
                    uri, display_label
                FROM scored
                WHERE NOT is_excluded
                ORDER BY uri, freq ASC
            )
            SELECT
                nr.uri,
                COALESCE(e.display_label, f.display_label, 'Resource') AS new_primary
            FROM node_rows nr
            LEFT JOIN best_eligible e ON e.uri = nr.uri
            LEFT JOIN best_fallback  f ON f.uri = nr.uri
        """)

        self.connection.execute("""
            UPDATE node_rows
            SET primary_label = np.new_primary
            FROM _new_primary np
            WHERE node_rows.uri = np.uri
        """)

        for t in ("_remap_excludes", "_remap_map", "_new_primary"):
            self.connection.execute(f"DROP TABLE IF EXISTS {t}")

        if self.db_path != ":memory:":
            self.connection.execute("CHECKPOINT")

        if self.progress:
            total = self.connection.execute("SELECT count(*) FROM node_rows").fetchone()[0]
            rows = self.connection.execute("""
                SELECT primary_label, count(*) AS n,
                       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
                FROM node_rows
                GROUP BY primary_label
                ORDER BY n DESC
                LIMIT 30
            """).fetchall()
            n_labels = self.connection.execute(
                "SELECT count(DISTINCT primary_label) FROM node_rows"
            ).fetchone()[0]
            print(
                f"[remap] done — {n_labels:,} distinct labels across {total:,} nodes{_mem_stat()}",
                file=sys.stderr,
            )
            print("[remap] top 30:", file=sys.stderr)
            for label, count, pct in rows:
                print(f"[remap]   {pct:5.1f}%  {count:>12,}  {label}", file=sys.stderr)

    def compact(self, new_db_path: str) -> None:
        """Copy only the needed fact tables to a new, compact DuckDB file.

        DROP + CHECKPOINT does not shrink a DuckDB file — freed blocks stay
        allocated.  This method creates a fresh DB with only node_rows,
        property_facts, and relationship_facts, which can be significantly
        smaller when large tables (rdf_triples, node_labels) were previously
        dropped.
        """
        if self.progress:
            print(f"[compact] writing compact DB to {new_db_path} ...", file=sys.stderr)
        t0 = time.monotonic()
        dst = duckdb.connect(new_db_path)
        dst.execute(f"ATTACH '{self.db_path}' AS src (READ_ONLY)")
        tables = [
            r[0] for r in dst.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'src' ORDER BY table_name"
            ).fetchall()
        ]
        keep = {"node_rows", "property_facts", "relationship_facts"}
        for tbl in tables:
            if tbl not in keep:
                continue
            if self.progress:
                print(f"[compact]   copying {tbl}...", file=sys.stderr, flush=True)
            dst.execute(f"CREATE TABLE {tbl} AS SELECT * FROM src.{tbl}")
        dst.execute("DETACH src")
        dst.execute("CHECKPOINT")
        dst.close()
        if self.progress:
            size_mb = Path(new_db_path).stat().st_size / 1_048_576
            print(
                f"[compact] done — {size_mb:,.0f} MB  {time.monotonic()-t0:.1f}s{_mem_stat()}",
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
        if self.progress:
            # Report distinct-property counts per label so the user can pick a sensible
            # --max-properties-per-label value for the export step.
            rows = self.connection.execute(
                """
                SELECT
                    primary_label,
                    count(DISTINCT projected_property_name) AS n_props
                FROM node_property_profile
                GROUP BY primary_label
                ORDER BY n_props DESC
                """
            ).fetchall()
            if rows:
                counts = [r[1] for r in rows]
                p50 = counts[len(counts) // 2]
                p90 = counts[int(len(counts) * 0.10)]  # sorted DESC so 10% from top = p90
                p99 = counts[int(len(counts) * 0.01)]
                print(
                    f"[prop-profile] {len(rows)} labels  "
                    f"properties/label: max={counts[0]:,}  p99={p99:,}  p90={p90:,}  median={p50:,}{_mem_stat()}",
                    file=sys.stderr,
                )
                print("[prop-profile] top 15 labels by property count:", file=sys.stderr)
                for label, n in rows[:15]:
                    print(f"[prop-profile]   {n:>6,}  {label}", file=sys.stderr)
                suggested = max(counts[min(9, len(counts) - 1)], 100)
                print(
                    f"[prop-profile] suggested --max-properties-per-label: {suggested:,}"
                    f"  (top-10 label coverage)",
                    file=sys.stderr,
                )

    def build_subclass_labels(self) -> int:
        """Compute rdfs:subClassOf ancestor labels per node and store in node_subclass_labels.

        Auto-detects whether subClassOf triples exist in relationship_facts; skips silently
        when none are found (e.g. Freebase-style datasets). When found, computes transitive
        closure up to self.subclass_label_depth hops, retains only ancestor classes whose
        direct child count meets the coverage threshold, and stores the result as a
        node_subclass_labels table.  The export step JOINs this table to add a ":LABEL"
        column to node Parquet files so neo4j-admin assigns multiple labels at import time.

        Returns the number of subClassOf triples found (0 when skipped).
        """
        if not self.subclass_labels:
            return 0

        t0 = time.monotonic()

        n_subclass = self.connection.execute(
            "SELECT count(*) FROM relationship_facts WHERE rel_type = 'subClassOf'"
        ).fetchone()[0]

        if n_subclass == 0:
            if self.progress:
                print(
                    "[subclass-labels] no subClassOf triples in relationship_facts — skipping",
                    file=sys.stderr,
                )
            return 0

        depth = self.subclass_label_depth
        total_nodes = self.connection.execute("SELECT count(*) FROM node_rows").fetchone()[0]
        threshold = max(int(total_nodes * self.min_subclass_label_coverage), 1)

        if self.progress:
            print(
                f"[subclass-labels] {n_subclass:,} subClassOf triples found"
                f"  depth={'unlimited' if depth <= 0 else depth}"
                f"  coverage_threshold={threshold:,} direct children{_mem_stat()}",
                file=sys.stderr,
            )

        # Candidate ancestors: subClassOf targets with enough direct children to be
        # meaningful label categories. Filters out leaf-level classes and near-root
        # mega-classes that add noise rather than useful type information.
        self.connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE _subclass_candidates AS
            SELECT rf.target_uri AS uri
            FROM relationship_facts rf
            WHERE rf.rel_type = 'subClassOf'
              AND NOT rf.target_uri LIKE '_:%'
            GROUP BY rf.target_uri
            HAVING count(DISTINCT rf.source_uri) >= {threshold}
            """
        )
        n_candidates = self.connection.execute(
            "SELECT count(*) FROM _subclass_candidates"
        ).fetchone()[0]

        if n_candidates == 0:
            self.connection.execute("DROP TABLE IF EXISTS _subclass_candidates")
            if self.progress:
                print(
                    f"[subclass-labels] no ancestor classes meet coverage threshold"
                    f" (≥{threshold:,} direct children) — skipping",
                    file=sys.stderr,
                )
            return n_subclass

        if self.progress:
            print(
                f"[subclass-labels] {n_candidates:,} ancestor label candidates"
                f" (≥{threshold:,} direct children)",
                file=sys.stderr,
            )

        # Transitive closure via recursive CTE bounded by depth_limit.
        # UNION ALL + depth guard: faster than UNION (dedup) for acyclic ontologies.
        # OWL ontologies should be acyclic in subClassOf; depth cap is the safety net.
        depth_guard = f"AND a.depth < {depth}" if depth > 0 else ""
        self.connection.execute(
            f"""
            CREATE OR REPLACE TABLE node_subclass_labels AS
            WITH RECURSIVE anc(uri, ancestor_uri, depth) AS (
                SELECT source_uri, target_uri, 1
                FROM relationship_facts
                WHERE rel_type = 'subClassOf'
                  AND NOT target_uri LIKE '_:%'
                UNION ALL
                SELECT a.uri, rf.target_uri, a.depth + 1
                FROM anc a
                JOIN relationship_facts rf ON rf.source_uri = a.ancestor_uri
                WHERE rf.rel_type = 'subClassOf'
                  AND NOT rf.target_uri LIKE '_:%'
                  {depth_guard}
            )
            SELECT
                a.uri,
                list(DISTINCT pf.value ORDER BY pf.value) AS extra_labels
            FROM anc a
            JOIN _subclass_candidates sc ON sc.uri = a.ancestor_uri
            JOIN property_facts pf
                ON pf.uri = a.ancestor_uri
               AND pf.projected_property_name = 'label'
            GROUP BY a.uri
            """
        )
        self.connection.execute("DROP TABLE IF EXISTS _subclass_candidates")
        self.connection.execute("CHECKPOINT")

        n_labeled = self.connection.execute(
            "SELECT count(*) FROM node_subclass_labels"
        ).fetchone()[0]

        if self.progress:
            avg_row = self.connection.execute(
                "SELECT avg(len(extra_labels)) FROM node_subclass_labels"
            ).fetchone()
            avg = avg_row[0] if avg_row and avg_row[0] else 0.0
            # Show a sample of the most-used extra labels
            top = self.connection.execute(
                """
                SELECT lbl, count(*) AS n
                FROM (SELECT unnest(extra_labels) AS lbl FROM node_subclass_labels)
                GROUP BY lbl ORDER BY n DESC LIMIT 10
                """
            ).fetchall()
            print(
                f"[subclass-labels] {n_labeled:,} nodes assigned extra labels"
                f"  avg={avg:.1f}/node  {time.monotonic() - t0:.1f}s{_mem_stat()}",
                file=sys.stderr,
            )
            if top:
                print("[subclass-labels] top 10 extra labels:", file=sys.stderr)
                for lbl, n in top:
                    print(f"[subclass-labels]   {n:>8,}  {lbl}", file=sys.stderr)

        return n_subclass

    def profile_relationships(
        self,
        rel_map_file: Optional[str] = None,
        min_rel_coverage: float = 0.001,
        dedup_inverse_pairs: bool = True,
        inverse_pair_tolerance: float = 0.001,
        ensure_rel_per_label: bool = True,
    ) -> None:
        """Coverage-based relationship type filtering — mirrors remap_labels() for rels.

        Creates ``relationship_profile`` table with an ``is_excluded`` flag per rel type.
        ``deduplicate_relationships()`` reads this table to skip excluded/rare types.

        Exclusion rules (applied in order):
          1. Built-in exclude patterns (``builtin_label_map.json`` ``"rel_exclude"``).
          2. Custom patterns from ``rel_map_file`` ``"rel_exclude"`` key (optional).
          3. Coverage threshold: rel types with fewer than
             ``total_triples * min_rel_coverage`` triples are excluded.
          4. Inverse pair deduplication (when ``dedup_inverse_pairs=True``): pairs of
             rel types with triple counts within ``inverse_pair_tolerance`` of each other
             are treated as forward/reverse duplicates; the lexicographically larger name
             is excluded. Works well for Freebase/DBpedia which emit both directions of
             every relationship systematically.
        """
        t0 = time.monotonic()
        total = self.connection.execute(
            "SELECT count(*) FROM relationship_facts"
        ).fetchone()[0]
        if total == 0:
            return
        threshold = max(int(total * min_rel_coverage), 1)

        # Merge built-in + custom exclude patterns
        rel_exclude = list(_BUILTIN_REL_EXCLUDE_PATTERNS)
        if rel_map_file:
            with open(rel_map_file) as f:
                custom = json.load(f)
            rel_exclude.extend(custom.get("rel_exclude", []))
            for pat in custom.get("rel_exclude_remove", []):
                try:
                    rel_exclude.remove(pat)
                except ValueError:
                    pass

        if self.progress:
            print("[rel-profile] collecting rel types and applying exclude patterns...", file=sys.stderr)
        # Collect all distinct rel types and match against exclude globs in Python
        all_rel_types: list[str] = [
            row[0]
            for row in self.connection.execute(
                "SELECT DISTINCT rel_type FROM relationship_facts WHERE rel_type IS NOT NULL"
            ).fetchall()
        ]
        excluded: set[str] = set()
        for rel in all_rel_types:
            if any(fnmatch.fnmatch(rel, pat) for pat in rel_exclude):
                excluded.add(rel)

        if self.progress:
            print(
                f"[rel-profile] {len(excluded):,} / {len(all_rel_types):,} types pattern-excluded  ({time.monotonic() - t0:.1f}s){_mem_stat()}",
                file=sys.stderr,
            )

        # Materialise excluded set into a temp table to avoid huge IN clauses
        self.connection.execute(
            "CREATE OR REPLACE TEMP TABLE _excl_rels (rel_type VARCHAR PRIMARY KEY)"
        )
        if excluded:
            self.connection.executemany(
                "INSERT OR IGNORE INTO _excl_rels VALUES (?)",
                [(r,) for r in excluded],
            )

        self.connection.execute(
            f"""
            CREATE OR REPLACE TABLE relationship_profile AS
            WITH counts AS (
                SELECT rel_type,
                       count(*) AS triple_count,
                       count(DISTINCT source_uri) AS source_count,
                       count(DISTINCT target_uri) AS target_count
                FROM relationship_facts
                GROUP BY rel_type
            )
            SELECT
                c.rel_type,
                c.triple_count,
                c.source_count,
                c.target_count,
                -- pattern_excluded: matched a glob from builtin/custom rel_exclude lists.
                -- Distinguishes intentional metadata exclusions (never rescue) from
                -- threshold/inverse-pair exclusions (rescuable by ensure_rel_per_label).
                (ex.rel_type IS NOT NULL)                                   AS pattern_excluded,
                (ex.rel_type IS NOT NULL OR c.triple_count < {threshold})   AS is_excluded
            FROM counts c
            LEFT JOIN _excl_rels ex ON c.rel_type = ex.rel_type
            ORDER BY c.triple_count DESC
            """
        )
        self.connection.execute("DROP TABLE IF EXISTS _excl_rels")
        if self.progress:
            print(f"[rel-profile] relationship_profile built  ({time.monotonic() - t0:.1f}s){_mem_stat()}", file=sys.stderr)

        # Inverse pair deduplication: Freebase/DBpedia emit both directions of every
        # relationship (e.g. music.recording.artist ↔ music.artist.track, both ~12.4M
        # triples). Neo4j only needs one direction. Count-similarity within tolerance
        # (default 0.1%) detects such pairs; keep the lex-smaller name because Freebase
        # naming conventions put the "forward" direction first alphabetically.
        if dedup_inverse_pairs:
            tol = inverse_pair_tolerance
            inverse_excluded = self.connection.execute(
                f"""
                SELECT b.rel_type
                FROM relationship_profile a
                JOIN relationship_profile b
                  ON a.rel_type < b.rel_type
                 AND b.triple_count BETWEEN a.triple_count * {1 - tol}
                                        AND a.triple_count * {1 + tol}
                WHERE NOT a.is_excluded AND NOT b.is_excluded
                """
            ).fetchall()
            if inverse_excluded:
                pairs_excl = {row[0] for row in inverse_excluded}
                self.connection.executemany(
                    "UPDATE relationship_profile SET is_excluded = true WHERE rel_type = ?",
                    [(r,) for r in pairs_excl],
                )
                if self.progress:
                    print(
                        f"[rel-profile] {len(pairs_excl):,} inverse-pair duplicates excluded",
                        file=sys.stderr,
                    )

        kept, total_types, kept_triples, total_triples = self.connection.execute(
            """
            SELECT
                sum(CASE WHEN NOT is_excluded THEN 1 ELSE 0 END),
                count(*),
                sum(CASE WHEN NOT is_excluded THEN triple_count ELSE 0 END),
                sum(triple_count)
            FROM relationship_profile
            """
        ).fetchone()
        if self.progress:
            print(
                f"[rel-profile] {kept:,} / {total_types:,} rel types kept  "
                f"({kept_triples:,} / {total_triples:,} triples, threshold={threshold:,})",
                file=sys.stderr,
            )
            rows = self.connection.execute(
                """
                SELECT rel_type, triple_count
                FROM relationship_profile
                WHERE NOT is_excluded
                ORDER BY triple_count DESC
                LIMIT 20
                """
            ).fetchall()
            for rel, cnt in rows:
                pct = 100.0 * cnt / total_triples if total_triples else 0
                print(f"[rel-profile]   {pct:5.2f}%  {cnt:>12,}  {rel}", file=sys.stderr)

        # Orphan detection: find primary_labels with no kept relationship.
        # Set-based approach: scan relationship_facts once per direction (source + target),
        # join with the small relationship_profile to filter kept types, join with node_rows
        # to get primary_labels — then anti-join node_rows against the covered set.
        # This avoids the O(N_nodes × N_rels) correlated NOT EXISTS scan.
        if self.progress:
            print("[rel-profile] checking for orphaned node labels...", file=sys.stderr)
        t_orphan = time.monotonic()
        self.connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE _covered_labels AS
            WITH edges AS (
                SELECT source_uri AS uri, rel_type FROM relationship_facts
                UNION ALL
                SELECT target_uri AS uri, rel_type FROM relationship_facts
            )
            SELECT DISTINCT nr.primary_label
            FROM edges e
            JOIN relationship_profile rp ON rp.rel_type = e.rel_type AND NOT rp.is_excluded
            JOIN node_rows nr ON nr.uri = e.uri
            """
        )
        orphaned = self.connection.execute(
            """
            SELECT nr.primary_label, count(DISTINCT nr.uri) AS node_count
            FROM node_rows nr
            WHERE nr.primary_label NOT IN (SELECT primary_label FROM _covered_labels)
            GROUP BY nr.primary_label
            ORDER BY node_count DESC
            """
        ).fetchall()
        self.connection.execute("DROP TABLE IF EXISTS _covered_labels")
        if self.progress:
            print(
                f"[rel-profile] orphan check done  ({time.monotonic() - t_orphan:.1f}s){_mem_stat()}",
                file=sys.stderr,
            )

        if orphaned and ensure_rel_per_label:
            # After threshold + inverse-pair filtering some labels have zero rels —
            # silently producing disconnected nodes is worse than bending the threshold.
            # Rescue by force-including the highest-frequency non-pattern-excluded rel
            # for that label. Only threshold/inverse-pair excluded types are eligible;
            # pattern-excluded types are intentional metadata and must stay excluded.
            # Batch approach: join orphaned nodes (likely small set) against relationship_facts
            # via UNION to avoid the OR condition, then pick best rescue rel per label.
            orphaned_labels = {lbl for lbl, _ in orphaned}
            self.connection.execute(
                "CREATE OR REPLACE TEMP TABLE _orphaned_labels (primary_label VARCHAR PRIMARY KEY)"
            )
            self.connection.executemany(
                "INSERT OR IGNORE INTO _orphaned_labels VALUES (?)",
                [(lbl,) for lbl in orphaned_labels],
            )
            rescue_rows = self.connection.execute(
                """
                WITH orphan_edges AS (
                    SELECT nr.primary_label, rf.rel_type
                    FROM relationship_facts rf
                    JOIN node_rows nr ON nr.uri = rf.source_uri
                    WHERE nr.primary_label IN (SELECT primary_label FROM _orphaned_labels)
                    UNION ALL
                    SELECT nr.primary_label, rf.rel_type
                    FROM relationship_facts rf
                    JOIN node_rows nr ON nr.uri = rf.target_uri
                    WHERE nr.primary_label IN (SELECT primary_label FROM _orphaned_labels)
                ),
                ranked AS (
                    SELECT
                        oe.primary_label,
                        rp.rel_type,
                        rp.triple_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY oe.primary_label
                            ORDER BY rp.triple_count DESC
                        ) AS rn
                    FROM orphan_edges oe
                    JOIN relationship_profile rp ON rp.rel_type = oe.rel_type
                    WHERE rp.is_excluded AND NOT rp.pattern_excluded
                )
                SELECT primary_label, rel_type, triple_count
                FROM ranked
                WHERE rn = 1
                ORDER BY triple_count DESC
                """
            ).fetchall()
            self.connection.execute("DROP TABLE IF EXISTS _orphaned_labels")

            rescued: list[tuple[str, str, int]] = []
            for label, rel_type, cnt in rescue_rows:
                self.connection.execute(
                    "UPDATE relationship_profile SET is_excluded = false WHERE rel_type = ?",
                    [rel_type],
                )
                rescued.append((label, rel_type, cnt))

            if rescued and self.progress:
                print(
                    f"[rel-profile] rescued {len(rescued)} rel types to avoid orphan labels:",
                    file=sys.stderr,
                )
                for label, rel, cnt in rescued:
                    print(
                        f"[rel-profile]   {label}  ←  {rel}  ({cnt:,} triples)",
                        file=sys.stderr,
                    )
            rescued_labels = {r[0] for r in rescued}
            still_orphaned = [(lbl, n) for lbl, n in orphaned if lbl not in rescued_labels]
            if still_orphaned:
                print(
                    f"[rel-profile] WARNING: {len(still_orphaned)} labels remain orphaned "
                    f"(all their rel types are pattern-excluded):",
                    file=sys.stderr,
                )
                for label, n in still_orphaned[:10]:
                    print(f"[rel-profile]   {n:>12,} nodes  {label}", file=sys.stderr)
        elif orphaned:
            print(
                f"[rel-profile] WARNING: {len(orphaned)} node labels have no kept "
                f"relationships — use --ensure-rel-per-label or lower --min-rel-coverage:",
                file=sys.stderr,
            )
            for label, n in orphaned[:15]:
                print(f"[rel-profile]   {n:>12,} orphan nodes  {label}", file=sys.stderr)

    def pivot_nodes(self):
        # Skip profile_properties if the analyze stage already ran it.
        if not _table_exists(self.connection, "node_property_profile"):
            self.profile_properties()
        self._create_node_property_values()
        property_count = self.connection.execute("SELECT count(*) FROM node_property_values").fetchone()[0]
        if property_count:
            n_distinct = self.connection.execute(
                "SELECT count(DISTINCT projected_property_name) FROM node_property_values"
            ).fetchone()[0]
            # DuckDB has a default PIVOT column safety limit of 1000. Freebase has 769K
            # raw property names so the PIVOT fails unless the limit is raised to at least
            # n_distinct+100. The per-label PIVOT in _write_node_parquet sees fewer columns
            # but we raise it globally here once rather than per-label.
            self.connection.execute(f"SET pivot_limit={max(n_distinct + 100, 100_000)}")
        # Drop any leftover wide tables from a previous failed run to free disk space.
        self.connection.execute("DROP TABLE IF EXISTS node_properties_wide")
        self.connection.execute("DROP TABLE IF EXISTS nodes_wide")
        self.connection.execute("CHECKPOINT")

    def export_parquet(self, output_dir: str):
        """Full export: pivot nodes, deduplicate rels, write all Parquet files."""
        if self.progress:
            print("[export] pivoting nodes and projecting Parquet...", file=sys.stderr)
        t0 = time.monotonic()
        self.pivot_nodes()
        self.deduplicate_relationships()
        output_path = Path(output_dir)
        self._ensure_output_dirs(output_path)
        self._write_metadata(output_path / "metadata")
        self._write_node_parquet(output_path / "nodes")
        self._write_rel_parquet(output_path / "relationships")
        if self.progress:
            print(f"[export] done  {time.monotonic() - t0:.1f}s{_mem_stat()}", file=sys.stderr)

    def export_nodes(self, output_dir: str):
        """Export only node Parquet files (assumes pivot_nodes already ran or will run)."""
        if self.progress:
            print("[export] pivoting nodes...", file=sys.stderr)
        t0 = time.monotonic()
        self.pivot_nodes()
        output_path = Path(output_dir)
        self._ensure_output_dirs(output_path)
        self._write_metadata(output_path / "metadata")
        self._write_node_parquet(output_path / "nodes")
        if self.progress:
            print(f"[export] nodes done  {time.monotonic() - t0:.1f}s{_mem_stat()}", file=sys.stderr)

    def export_relationships(self, output_dir: str):
        """Export only relationship Parquet files (assumes deduplicate_relationships already ran or will run)."""
        if self.progress:
            print("[export] deduplicating relationships...", file=sys.stderr)
        t0 = time.monotonic()
        self.deduplicate_relationships()
        output_path = Path(output_dir)
        self._ensure_output_dirs(output_path)
        self._write_rel_parquet(output_path / "relationships")
        if self.progress:
            print(f"[export] relationships done  {time.monotonic() - t0:.1f}s{_mem_stat()}", file=sys.stderr)

    def _ensure_output_dirs(self, output_path: Path):
        (output_path / "metadata").mkdir(parents=True, exist_ok=True)
        (output_path / "nodes").mkdir(parents=True, exist_ok=True)
        (output_path / "relationships").mkdir(parents=True, exist_ok=True)

    def _export_workers_count(self, n_tasks: int) -> int:
        """Resolve effective worker count for parallel export.

        :memory: DBs can't be shared across processes — fall back to 1.
        Otherwise clamp to [1, n_tasks] and apply a memory-aware cap so we
        don't trigger OOM when RAM is scarce.  Default is 2 to avoid the
        200 GB+ RAM spike seen with higher concurrency on large datasets.
        """
        if self.db_path == ":memory:":
            return 1
        if self.export_workers is not None:
            n = self.export_workers
        else:
            # Default: 2 workers. Each runs DuckDB's multi-threaded PIVOT internally;
            # more than 2 concurrent PIVOTs on a large DB (e.g. Freebase) caused
            # >200 GB combined RAM usage and OOM-killed the machine.
            n = 2
        n = max(1, min(n, n_tasks))

        # Memory-aware cap (applied even when --export-workers is explicit).
        free_gb = _free_mem_gb()
        if free_gb >= 0:
            if free_gb < 8:
                if n > 1 and self.progress:
                    print(
                        f"[export] WARNING: only {free_gb:.1f} GB free — forcing sequential export (1 worker)",
                        file=sys.stderr,
                    )
                n = 1
            elif free_gb < 16 and n > 2:
                if self.progress:
                    print(
                        f"[export] WARNING: only {free_gb:.1f} GB free — capping export workers at 2",
                        file=sys.stderr,
                    )
                n = 2
        return n

    def _run_parallel_export(self, tasks, worker_fn, n_workers: int, label_for_log: str):
        """Checkpoint, close the write connection, run workers, reopen the connection.

        DuckDB does not allow opening a read-only connection to a file that already has
        an open write connection in the same process. Closing the write connection first
        releases the lock; read-only workers in separate processes can then connect.
        The connection is always reopened in the finally block so subsequent pipeline
        steps keep working.
        """
        from concurrent.futures import ProcessPoolExecutor, as_completed
        if self.progress:
            print(
                f"[export] starting {n_workers} worker(s) for {len(tasks)} {label_for_log} tasks{_mem_stat()}",
                file=sys.stderr,
            )
        self.connection.execute("CHECKPOINT")
        self.connection.close()
        try:
            with ProcessPoolExecutor(max_workers=n_workers) as executor:
                futures = {executor.submit(worker_fn, *a): a[1] for a in tasks}
                for fut in as_completed(futures):
                    name, n = fut.result()
                    if self.progress:
                        print(
                            f"[export]   {label_for_log}/{_safe_name(name)}.parquet  {n:,} rows{_mem_stat()}",
                            file=sys.stderr,
                        )
        finally:
            self.connection = duckdb.connect(self.db_path)

    def _write_node_parquet(self, nodes_dir: Path):
        # Per-label PIVOT: a global PIVOT of all nodes × all properties exhausted
        # 256 GiB temp space in testing. Pivoting one label at a time bounds peak memory
        # to the largest single label's property matrix, which is manageable.
        # preserve_insertion_order=false is required: DuckDB refuses to PIVOT large result
        # sets unless it can reorder rows internally for its hash-aggregate strategy.
        self.connection.execute("SET preserve_insertion_order=false")

        # Pre-fetch node counts per label (one query) to filter tiny labels and
        # avoid a separate count(*) query per label during the export loop.
        label_counts: dict[str, int] = dict(
            self.connection.execute(
                "SELECT primary_label, count(*) FROM node_rows GROUP BY primary_label ORDER BY primary_label"
            ).fetchall()
        )
        if self.min_export_nodes > 0:
            skipped = [(lbl, n) for lbl, n in label_counts.items() if n < self.min_export_nodes]
            if skipped and self.progress:
                print(
                    f"[export] skipping {len(skipped)} labels with < {self.min_export_nodes:,} nodes:",
                    file=sys.stderr,
                )
                for lbl, n in sorted(skipped, key=lambda x: x[1]):
                    print(f"[export]   skip  {n:>8,}  {lbl}", file=sys.stderr)
            label_counts = {lbl: n for lbl, n in label_counts.items() if n >= self.min_export_nodes}
        labels = sorted(label_counts)

        n_distinct = (
            self.connection.execute(
                "SELECT count(DISTINCT projected_property_name) FROM node_property_values"
            ).fetchone()[0]
            if _table_exists(self.connection, "node_property_values") else 0
        )
        pivot_limit = max(n_distinct + 100, 100_000)
        n_workers = self._export_workers_count(len(labels))
        threads_per_worker = max(1, (_os.cpu_count() or 1) // n_workers)

        if self.progress:
            print(
                f"[export] writing {len(labels)} node label files"
                + (f" with {n_workers} parallel workers" if n_workers > 1 else " (sequential)")
                + _mem_stat(),
                file=sys.stderr,
            )

        has_subclass = _table_exists(self.connection, "node_subclass_labels")

        if n_workers == 1:
            # Sequential: use the existing connection so :memory: and single-file DBs work.
            for label in labels:
                label_lit = _sql_literal(label)
                target_lit = _sql_literal(str(nodes_dir / f"{_safe_name(label)}.parquet"))
                has_props = self.connection.execute(
                    f"SELECT count(*) FROM node_property_values WHERE primary_label = {label_lit} LIMIT 1"
                ).fetchone()[0]
                # :LABEL column: primary_label alone, or primary_label + semicolon-joined
                # subClassOf ancestor labels when node_subclass_labels table exists.
                label_col = (
                    f"CASE WHEN sl.extra_labels IS NOT NULL"
                    f" THEN {label_lit} || ';' || list_aggregate(sl.extra_labels, 'string_agg', ';')"
                    f" ELSE {label_lit} END AS \":LABEL\""
                    if has_subclass
                    else f"{label_lit} AS \":LABEL\""
                )
                subclass_join = (
                    f"LEFT JOIN node_subclass_labels sl ON sl.uri = nr.uri"
                    if has_subclass else ""
                )
                if has_props:
                    self.connection.execute(
                        f"""
                        COPY (
                            WITH lp AS (
                                SELECT uri, projected_property_name, value
                                FROM node_property_values
                                WHERE primary_label = {label_lit}
                            ),
                            piv AS (
                                PIVOT lp ON projected_property_name USING first(value) GROUP BY uri
                            )
                            SELECT nr.uri, nr.primary_label, nr.labels, {label_col},
                                   piv.* EXCLUDE(uri)
                            FROM node_rows nr
                            LEFT JOIN piv ON nr.uri = piv.uri
                            {subclass_join}
                            WHERE nr.primary_label = {label_lit}
                        ) TO {target_lit} (FORMAT parquet, COMPRESSION ZSTD)
                        """
                    )
                else:
                    self.connection.execute(
                        f"""
                        COPY (
                            SELECT nr.uri, nr.primary_label, nr.labels, {label_col}
                            FROM node_rows nr
                            {subclass_join}
                            WHERE nr.primary_label = {label_lit}
                        ) TO {target_lit} (FORMAT parquet, COMPRESSION ZSTD)
                        """
                    )
                if self.progress:
                    print(f"[export]   nodes/{_safe_name(label)}.parquet  {label_counts[label]:,} rows{_mem_stat()}", file=sys.stderr)
        else:
            # Parallel: close the write connection so workers can open read-only connections.
            task_args = [
                (self.db_path, label, str(nodes_dir / f"{_safe_name(label)}.parquet"),
                 pivot_limit, threads_per_worker, has_subclass)
                for label in labels
            ]
            self._run_parallel_export(task_args, _export_node_label, n_workers, "nodes")

    def _write_rel_parquet(self, rels_dir: Path):
        rel_types = [
            row[0]
            for row in self.connection.execute(
                "SELECT DISTINCT rel_type FROM relationship_rows ORDER BY rel_type"
            ).fetchall()
        ]
        n_workers = self._export_workers_count(len(rel_types))
        threads_per_worker = max(1, (_os.cpu_count() or 1) // n_workers)

        if self.progress:
            print(
                f"[export] writing {len(rel_types)} relationship type files"
                + (f" with {n_workers} parallel workers" if n_workers > 1 else " (sequential)")
                + _mem_stat(),
                file=sys.stderr,
            )

        if n_workers == 1:
            for rel_type in rel_types:
                rel_lit = _sql_literal(rel_type)
                target_lit = _sql_literal(str(rels_dir / f"{_safe_name(rel_type)}.parquet"))
                self.connection.execute(
                    f"""
                    COPY (
                        SELECT source_uri, target_uri
                        FROM relationship_rows
                        WHERE rel_type = {rel_lit}
                    ) TO {target_lit} (FORMAT parquet, COMPRESSION ZSTD)
                    """
                )
                if self.progress:
                    n = self.connection.execute(
                        f"SELECT count(*) FROM relationship_rows WHERE rel_type = {rel_lit}"
                    ).fetchone()[0]
                    print(f"[export]   relationships/{_safe_name(rel_type)}.parquet  {n:,} rows{_mem_stat()}", file=sys.stderr)
        else:
            task_args = [
                (self.db_path, rel_type, str(rels_dir / f"{_safe_name(rel_type)}.parquet"),
                 threads_per_worker)
                for rel_type in rel_types
            ]
            self._run_parallel_export(task_args, _export_rel_type, n_workers, "relationships")

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
        if _table_exists(self.connection, "relationship_profile"):
            # When profile exists (analyze stage ran), join during deduplication rather
            # than in a separate filter pass — avoids one full scan of relationship_facts
            # which can be billions of rows at Freebase scale.
            self.connection.execute(
                """
                CREATE OR REPLACE TABLE relationship_rows AS
                SELECT rf.source_uri, rf.rel_type, rf.target_uri
                FROM relationship_facts rf
                JOIN relationship_profile rp ON rp.rel_type = rf.rel_type
                WHERE NOT rp.is_excluded
                GROUP BY rf.source_uri, rf.rel_type, rf.target_uri
                """
            )
        else:
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
            ) TO {_sql_literal(str(target))} (FORMAT parquet, COMPRESSION ZSTD)
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

        min_freq = self.min_property_freq
        max_per_label = self.max_properties_per_label

        # Build an allowed-property allowlist from node_property_profile:
        # keep only properties meeting the min-frequency threshold AND
        # ranking within the top-N for at least one of their labels.
        self.connection.execute(
            f"""
            CREATE OR REPLACE TABLE _allowed_properties AS
            SELECT DISTINCT projected_property_name
            FROM (
                SELECT
                    projected_property_name,
                    row_number() OVER (
                        PARTITION BY primary_label
                        ORDER BY subject_count DESC
                    ) AS rank_within_label
                FROM node_property_profile
                WHERE subject_count >= {min_freq}
            )
            WHERE rank_within_label <= {max_per_label}
            """
        )

        if self.progress:
            allowed = self.connection.execute(
                "SELECT count(*) FROM _allowed_properties"
            ).fetchone()[0]
            total = self.connection.execute(
                "SELECT count(DISTINCT projected_property_name) FROM node_property_profile"
            ).fetchone()[0]
            dropped = total - allowed
            if dropped > 0:
                print(
                    f"[pivot] property filter: {allowed:,} of {total:,} properties kept "
                    f"(min_freq={min_freq}, max_per_label={max_per_label}, {dropped:,} dropped)",
                    file=sys.stderr,
                )
            # Warn for labels that were capped
            capped = self.connection.execute(
                f"""
                SELECT primary_label, count(DISTINCT projected_property_name) AS n_props
                FROM node_property_profile
                WHERE subject_count >= {min_freq}
                GROUP BY primary_label
                HAVING n_props > {max_per_label}
                ORDER BY n_props DESC
                LIMIT 20
                """
            ).fetchall()
            for label, n_props in capped:
                print(
                    f"[pivot]   label '{label}': {n_props:,} properties → capped to {max_per_label}",
                    file=sys.stderr,
                )

        # primary_label is baked into node_property_values so per-label PIVOT
        # export can filter with a direct equality check instead of re-joining
        # node_rows against all property rows for every label.
        #
        # We INSERT one label at a time and CHECKPOINT between labels to bound
        # the peak memory. A single monolithic GROUP BY across all nodes × all
        # properties caused >100 GB aggregate memory usage on Freebase-scale data.
        value_type = "VARCHAR[]" if mode == AggregationMode.ARRAY else "VARCHAR"
        self.connection.execute(
            f"""
            CREATE OR REPLACE TABLE node_property_values (
                uri VARCHAR,
                primary_label VARCHAR,
                projected_property_name VARCHAR,
                value {value_type}
            )
            """
        )

        labels = [
            row[0]
            for row in self.connection.execute(
                "SELECT DISTINCT primary_label FROM node_property_profile ORDER BY primary_label"
            ).fetchall()
        ]
        n_labels = len(labels)
        t_pivot_start = time.monotonic()
        for i, label in enumerate(labels, 1):
            label_lit = _sql_literal(label)
            self.connection.execute(
                f"""
                INSERT INTO node_property_values
                SELECT
                    pf.uri,
                    nr.primary_label,
                    pf.projected_property_name,
                    {aggregate} AS value
                FROM property_facts pf
                JOIN node_rows nr ON nr.uri = pf.uri
                JOIN _allowed_properties ap USING (projected_property_name)
                WHERE nr.primary_label = {label_lit}
                GROUP BY pf.uri, nr.primary_label, pf.projected_property_name
                """
            )
            # Flush to disk after each label to release aggregate memory.
            # CHECKPOINT is a no-op on :memory: databases.
            self.connection.execute("CHECKPOINT")
            if self.progress:
                elapsed = time.monotonic() - t_pivot_start
                print(
                    f"[pivot] ({i}/{n_labels}) {label}  {elapsed:.0f}s{_mem_stat()}",
                    file=sys.stderr,
                )

        self.connection.execute("DROP TABLE IF EXISTS _allowed_properties")


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return safe.strip("_") or "Resource"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _export_node_label(
    db_path: str, label: str, target: str, pivot_limit: int, threads: int,
    has_subclass: bool = False,
) -> tuple[str, int]:
    """Worker: open DB read-only, PIVOT-COPY one label to Parquet, return row count.

    Module-level so it is picklable by ProcessPoolExecutor. DuckDB's SWMR model
    allows multiple read-only connections while the main write connection is open,
    as long as the data has been CHECKPOINTed beforehand.
    """
    import duckdb as _ddb
    con = _ddb.connect(db_path, read_only=True)
    try:
        con.execute(f"SET threads={threads}")
        con.execute(f"SET pivot_limit={pivot_limit}")
        con.execute("SET preserve_insertion_order=false")
        label_lit = _sql_literal(label)
        target_lit = _sql_literal(target)
        has_props = con.execute(
            f"SELECT count(*) FROM node_property_values WHERE primary_label = {label_lit} LIMIT 1"
        ).fetchone()[0]
        # :LABEL column: primary label alone, or combined with subClassOf ancestor labels
        label_col = (
            f"CASE WHEN sl.extra_labels IS NOT NULL"
            f" THEN {label_lit} || ';' || list_aggregate(sl.extra_labels, 'string_agg', ';')"
            f" ELSE {label_lit} END AS \":LABEL\""
            if has_subclass
            else f"{label_lit} AS \":LABEL\""
        )
        subclass_join = "LEFT JOIN node_subclass_labels sl ON sl.uri = nr.uri" if has_subclass else ""
        if has_props:
            con.execute(
                f"""
                COPY (
                    WITH lp AS (
                        SELECT uri, projected_property_name, value
                        FROM node_property_values
                        WHERE primary_label = {label_lit}
                    ),
                    piv AS (
                        PIVOT lp ON projected_property_name USING first(value) GROUP BY uri
                    )
                    SELECT nr.uri, nr.primary_label, nr.labels, {label_col},
                           piv.* EXCLUDE(uri)
                    FROM node_rows nr
                    LEFT JOIN piv ON nr.uri = piv.uri
                    {subclass_join}
                    WHERE nr.primary_label = {label_lit}
                ) TO {target_lit} (FORMAT parquet, COMPRESSION ZSTD)
                """
            )
        else:
            con.execute(
                f"""
                COPY (
                    SELECT nr.uri, nr.primary_label, nr.labels, {label_col}
                    FROM node_rows nr
                    {subclass_join}
                    WHERE nr.primary_label = {label_lit}
                ) TO {target_lit} (FORMAT parquet, COMPRESSION ZSTD)
                """
            )
        n = con.execute(
            f"SELECT count(*) FROM node_rows WHERE primary_label = {label_lit}"
        ).fetchone()[0]
        return label, n
    finally:
        con.close()


def _export_rel_type(db_path: str, rel_type: str, target: str, threads: int) -> tuple[str, int]:
    """Worker: open DB read-only, COPY one rel type to Parquet, return row count."""
    import duckdb as _ddb
    con = _ddb.connect(db_path, read_only=True)
    try:
        con.execute(f"SET threads={threads}")
        rel_lit = _sql_literal(rel_type)
        target_lit = _sql_literal(target)
        con.execute(
            f"""
            COPY (
                SELECT source_uri, target_uri
                FROM relationship_rows
                WHERE rel_type = {rel_lit}
            ) TO {target_lit} (FORMAT parquet, COMPRESSION ZSTD)
            """
        )
        n = con.execute(
            f"SELECT count(*) FROM relationship_rows WHERE rel_type = {rel_lit}"
        ).fetchone()[0]
        return rel_type, n
    finally:
        con.close()


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
