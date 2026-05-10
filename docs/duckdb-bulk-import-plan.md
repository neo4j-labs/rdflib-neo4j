# DuckDB RDF Bulk Import Experiment

## Goal

Build an independent utility that converts large RDF files into Neo4j bulk-import
Parquet files without holding all per-node state in Python memory.

The utility should preserve the mapping semantics used by n10s and rdflib-neo4j,
but replace the in-memory subject aggregation step with DuckDB staging and SQL
aggregation. The first benchmark target is:

- `/Users/mh/d/java/neo/neosemantics/testdata/chebi.owl`
- RDF/XML
- 774 MB
- roughly 11M triples

Later targets are 100 GB to 1000 GB RDF inputs.

## Baseline Decisions

- Base branch: `master`
- Experiment branch: `feat/duckdb-bulk-import-experiment`
- Worktree: `.claude/worktrees/duckdb-bulk-import-experiment`
- Target Neo4j: Enterprise Edition `2026.04.0`
- Bulk importer format: Parquet via `neo4j-admin database import --input-type=parquet`
- Mapping source: n10s / rdflib-neo4j mapping semantics
- Initial relationship properties: skip, because regular RDF has no relationship
  properties. Add RDF-star relationship property support later.

## Current n10s / rdflib Label Semantics

n10s and rdflib-neo4j do not currently choose a single "main" label.

In `handleRDFTypes = LABELS`, every non-blank-object `rdf:type` triple becomes a
Neo4j label on the subject node. Every imported node also has the structural
`Resource` label and `uri` property.

For bulk import, this experiment needs one physical node row per resource. If we
emit one file per entity type, we therefore need a partitioning label, but that
label is not an n10s semantic concept. The utility will call it `primary_label`
and use it only for output partitioning, schema selection, and importer file
layout. The Neo4j node still receives all mapped RDF type labels.

## Primary Label Heuristic

The primary label selection should be deterministic, explainable, and overridable.

Input:

- All mapped `rdf:type` labels for a resource.
- Optional class hierarchy from `rdfs:subClassOf` triples in the source data.
- A configurable generic-label denylist.
- Optional explicit label priority map.

Default rules:

1. If an explicit priority list selects a candidate label, use it.
2. Remove structural and generic labels from primary-label candidates:
   `Resource`, `owl__Thing`, `rdfs__Resource`, `Entity`, and configurable values.
3. If no non-generic candidates remain, use `Resource`.
4. If one candidate remains, use it.
5. If a class hierarchy is available, prefer the "middle" class:
   - Compute depth from a generic/root ancestor.
   - Compute height to a leaf/specific descendant among the candidate classes.
   - Pick the class maximizing `min(depth, height)`.
   - Example: `Entity -> Person -> Teacher` picks `Person`.
6. If hierarchy is unavailable or tied, prefer labels whose global frequency is
   neither extremely broad nor extremely narrow.
7. Final tie-breaker: lexical label order for deterministic output.

The output should include an explanation table:

```text
uri, primary_label, all_labels, rule, score
```

This makes incorrect choices visible and gives us a path to user overrides.

## Mapping Semantics To Preserve

### Resource Identity

- URI resource subject/object -> `(:Resource {uri: full_uri})`
- Blank node -> `bnode://<id>` URI and then same `Resource(uri)` identity
- All node Parquet files use the same importer id group:
  `uri:ID(Resource)`

Using one id group keeps relationships simple even when node rows are partitioned
by primary label.

### URI Handling

Support the canonical n10s modes:

- `SHORTEN`
- `SHORTEN_STRICT`
- `IGNORE`
- `MAP`
- `KEEP`

For `SHORTEN`, dynamically allocate `ns0`, `ns1`, ... prefixes for unknown
namespaces and persist the resulting prefix map. For `SHORTEN_STRICT`, fail when
the namespace is missing.

Persist / emit:

- `_GraphConfig` equivalent metadata
- `_NsPrefDef` equivalent prefix map
- `_MapDef` / `_MapNs` compatible mapping metadata where relevant

### RDF Type Handling

Support:

- `LABELS`: add mapped class labels to the subject node.
- `NODES`: create class nodes and `rdf__type` relationships.
- `LABELS_AND_NODES`: do both.

The first implementation should focus on `LABELS`, because it is the default and
is the core case for node projection.

### Multi-Valued Properties

Support three utility-level aggregation modes:

- `ANY`: pick a deterministic value when multiple values are present and no
  array preservation is requested. This is the preferred bulk default when exact
  stream-order overwrite semantics are not required.
- `LAST`: last value by RDF stream order wins, only when a stable source order is
  available or strict n10s compatibility is explicitly requested.
- `FIRST`: first value by RDF stream order wins, only when a stable source order
  is available.
- `ARRAY`: collect values into a Neo4j-compatible homogeneous list.

n10s compatibility note: n10s `OVERWRITE` behaves as last-write-wins. If we do
not find an existing first-write-wins config in n10s/rdflib-neo4j, `FIRST` should
remain an explicit bulk-utility extension.

For `ARRAY`, preserve stream order where practical, but do not make that a hard
requirement for the default bulk path. De-duplicate according to n10s behavior.
Neo4j array properties must be homogeneous, so mixed datatype arrays need a
configured policy:

- `error`
- `stringify`
- `discard_conflicts`

### Literal Coercion

Use n10s-compatible literal conversion:

- string and langString -> string, optional `@lang`
- integer families -> integer
- decimal / float / double -> float
- boolean -> boolean
- date -> date
- dateTime -> zoned datetime where import type support allows it
- custom datatype -> string, optionally with `^^datatype`

Language handling:

- If no language projection is configured, follow n10s-compatible value handling:
  preserve language tags in values only when `keepLangTag` is enabled.
- If language projection is configured, language-tagged literal properties use
  language-suffixed property names in the form `<property>_<lang>`.
  Example: `name` with `@en` becomes `name_en`.
- Language projection can be restricted by `languageFilter`; filtered languages
  are skipped before aggregation.
- If both a plain literal and language-tagged literals exist for the same
  property, keep the plain property name for untagged values and use suffixed
  names for tagged values.

## DuckDB Staging Model

Start with a simple fact table and add dictionaries if profiling shows string
storage dominates.

Initial table:

```sql
CREATE TABLE rdf_triples (
  source_order UBIGINT, -- optional; required only for strict FIRST/LAST
  subject VARCHAR NOT NULL,
  predicate VARCHAR NOT NULL,
  object_kind VARCHAR NOT NULL, -- iri | bnode | literal
  object_value VARCHAR NOT NULL,
  datatype VARCHAR,
  lang VARCHAR
);
```

Derived tables / views:

```sql
mapped_terms(term, mapped_name, namespace, local_name, kind)
node_labels(uri, label)
node_label_sets(uri, primary_label, labels)
property_facts(uri, property_name, projected_property_name, value, value_type, lang, source_order)
relationship_facts(source_uri, rel_type, target_uri, source_order)
```

For 100 GB+ inputs, move to dictionary tables:

```sql
terms(term_id BIGINT, kind VARCHAR, value VARCHAR, namespace VARCHAR, local VARCHAR)
rdf_triples(source_order UBIGINT, subject_id BIGINT, predicate_id BIGINT, object_id BIGINT, ...)
```

## Fast Ingestion Strategy

Initial implementation can use rdflib for the fastest path to a correctness
prototype. That prototype should be intentionally structured around a narrow raw
triple staging contract so the ingester can be replaced without changing URI
mapping, pivoting, Parquet export, or Neo4j import generation.

The long-term target is aggressive bulk import at 100 GB to 1000 GB scale, where
every minute saved matters. rdflib should therefore be treated as the prototype
and correctness oracle, not as the final high-throughput ingestion path. Native /
compiled ingestion paths should be benchmarked as soon as the projection
pipeline is working.

### Option 0: rdflib Prototype Sink

First implementation path:

- Implement a DuckDB-backed rdflib sink/store that batches triples into the raw
  staging table.
- Use it for small fixtures, ChEBI correctness checks, and mapping semantics.
- Keep the sink limited to producing the same staged schema as native ingesters.
- Avoid embedding rdflib-specific term objects past the staging boundary.

Current prototype caveat:

- `DuckDBBulkPrototype.ingest_file()` still calls `Graph.parse()` first, then
  inserts triples into DuckDB. This materializes the RDFLib graph in memory and
  is not the intended streaming sink shape.
- Add regular progress output for parse/stage/project/export phases, including
  triples processed every 10k or 100k rows, elapsed time, and throughput.
- Replace `Graph.parse()` materialization with a true streaming parser/sink or a
  native concurrent parser plus DuckDB appender.

Exit criteria:

- Small fixtures match rdflib-neo4j / n10s-compatible expected output.
- ChEBI can be parsed and projected end to end, even if not yet fast enough.
- The same projection SQL works when the staged triples come from another
  ingester.

### Option A: DuckDB `rdf` Extension

First candidate:

```sql
INSTALL rdf FROM community;
LOAD rdf;

CREATE TABLE rdf_triples AS
SELECT
  row_number() OVER ()::UBIGINT AS source_order,
  subject,
  predicate,
  object,
  graph,
  language_tag AS lang,
  datatype
FROM read_rdf($path, file_type = $file_type);
```

The extension exposes `read_rdf()` as a DuckDB table function and returns:

- `subject`
- `predicate`
- `object`
- `graph`
- `language_tag`
- `datatype`

It supports Turtle, N-Triples, N-Quads, TriG, and experimental read-only
RDF/XML. It also supports glob patterns and parallel scanning across multiple
matched files.

Validation questions for this path:

- Does `read_rdf()` distinguish literal objects from IRI / blank-node objects
  strongly enough for our relationship-vs-property split? If `datatype` and
  `language_tag` are the only literal markers, plain string literals need careful
  verification.
- Is `row_number() OVER ()` stable enough for strict `FIRST` / `LAST` semantics,
  especially with parallel glob reads? If not, keep this path as the default
  high-throughput path for `ANY`, `ARRAY`, and unordered relationship imports.
  Request or patch parser offset/order exposure only if exact overwrite
  compatibility becomes a hard requirement.
- Is experimental RDF/XML support correct enough on ChEBI?
- Can prefix expansion be controlled in a way compatible with n10s
  `SHORTEN` / `SHORTEN_STRICT` semantics?

This is the highest-leverage option because it keeps parsing inside DuckDB and
avoids Python row-by-row appends.

### Option B: Rust Parser + DuckDB Appender

Second candidate:

- `oxrdfio` for a single Rust entry point over Turtle, N-Triples, N-Quads,
  TriG, RDF/XML, N3, and JSON-LD.
- `oxttl` for Turtle / TriG / N-Triples / N-Quads if we want a narrower parser.
- `oxrdfxml` for direct RDF/XML parsing.
- DuckDB Appender through Rust bindings or DuckDB's C API.

This is the preferred custom implementation route if the DuckDB `rdf` extension
does not expose enough term metadata. The Rust parser can attach an explicit
`source_order` counter before appending rows to DuckDB when strict `FIRST` /
`LAST` behavior is required.

Potential Python integration:

- CLI binary writes `staging.duckdb` directly.
- Optional Python wrapper invokes the binary and then runs projection SQL.
- Avoid per-triple Python callbacks.

### Option C: C / C++ Parser + DuckDB C API

Third candidate:

- SERD: lightweight C parser for Turtle, N-Triples, N-Quads, and TriG.
- Raptor2: mature C RDF parser with RDF/XML support via expat/libxml.
- DuckDB C Appender, which DuckDB documents as the efficient loading path and
  faster than prepared statements or individual inserts.

This route is useful if Rust RDF/XML performance is weaker than expected or if
we need a small custom DuckDB extension/table function. Prefer DuckDB's C API
over its C++ API for stability.

### Option D: Go Parser + DuckDB Appender

Lower priority.

Available Go RDF libraries look less mature for this workload:

- Some packages read full Turtle input into memory before parsing.
- Some RDF/XML packages parse into an in-memory graph.
- Stream-oriented Go RDF support appears weaker than Rust and C options.

Use Go only if a specific library proves streaming, actively maintained, and
complete enough for RDF/XML plus N-Triples/N-Quads.

### Option E: rdflib Fallback

Keep the DuckDB-backed rdflib sink for:

- Correctness comparison.
- Small fixtures.
- Formats or edge cases not handled by the native path.

Do not use it as the default path for 100 GB+ production imports.

Metrics to capture during ingestion:

- triples parsed per second
- rows inserted per second
- peak RSS
- DuckDB database size
- temp/spill size

Benchmark matrix:

| Path | Formats to test | Must preserve | Decision gate |
| --- | --- | --- | --- |
| DuckDB `rdf` extension | RDF/XML ChEBI, N-Triples | object kind, language, datatype | Default if metadata is sufficient |
| Rust `oxrdfio` CLI | RDF/XML ChEBI, N-Triples | full term model; optional source_order | Use if extension is insufficient |
| C SERD/Raptor2 | RDF/XML ChEBI, N-Triples | full term model; optional source_order | Use if Rust is slower or incomplete |
| rdflib sink | small fixtures, ChEBI prototype | correctness | First prototype and fallback only |

## Dynamic Attribute Discovery and Pivoting

The utility should discover node and relationship attributes from the staged RDF
facts, not require every property column to be declared up front.

DuckDB has two relevant mechanisms:

- Native `PIVOT` / `PIVOT_WIDER`, where the simplified syntax can dynamically
  detect output columns from distinct values in the `ON` column.
- The DuckDB `rdf` extension's `pivot_rdf()` helper, which profiles RDF shape
  first and then pivots to a wide table.

The plan should use these for discovery, but not blindly emit every discovered
predicate into one huge file.

### Attribute Discovery Flow

1. Build `property_facts` with:
   `uri`, `primary_label`, `projected_property_name`, `value`, `value_type`,
   `lang`, and `source_order`.
2. Profile discovered attributes by primary label:

   ```sql
   CREATE TABLE node_property_profile AS
   SELECT
     primary_label,
     projected_property_name,
     count(*) AS value_count,
     count(DISTINCT uri) AS subject_count,
     count(DISTINCT value_type) AS type_count,
     list(DISTINCT value_type ORDER BY value_type) AS value_types
   FROM property_facts
   GROUP BY primary_label, projected_property_name;
   ```

3. Decide the projection columns per `primary_label`:
   - Include explicitly mapped/configured properties first.
   - Include observed properties under configurable limits.
   - Drop, split, or side-table properties that would create pathological width.
   - Emit a report of included and excluded attributes.
4. Pivot per `primary_label` and aggregation mode.

### Dynamic Pivot Examples

For default single-valued `ANY` mode:

```sql
CREATE TABLE node_props_any AS
SELECT
  uri,
  primary_label,
  projected_property_name,
  min(value) AS value
FROM property_facts
GROUP BY uri, primary_label, projected_property_name;

CREATE TABLE nodes_person AS
PIVOT (
  SELECT uri, projected_property_name, value
  FROM node_props_any
  WHERE primary_label = 'Person'
)
ON projected_property_name
USING first(value)
GROUP BY uri;
```

For strict single-valued `LAST` mode:

```sql
CREATE TABLE node_props_last AS
SELECT
  uri,
  primary_label,
  projected_property_name,
  arg_max(value, source_order) AS value
FROM property_facts
GROUP BY uri, primary_label, projected_property_name;

CREATE TABLE nodes_person AS
PIVOT (
  SELECT uri, projected_property_name, value
  FROM node_props_last
  WHERE primary_label = 'Person'
)
ON projected_property_name
USING first(value)
GROUP BY uri;
```

For `FIRST`, use `arg_min(value, source_order)` in the pre-aggregation step.
For unordered `ARRAY`, pre-aggregate with `list(value)` or sorted distinct value
lists. For strict ordered `ARRAY`, use `list(value ORDER BY source_order)`.

This two-step pattern keeps the pivot simple and makes `FIRST` / `LAST` /
`ANY` / `ARRAY` semantics explicit before dynamic column generation.

### Relationship Attribute Discovery

Regular RDF relationships have no attributes, so skip relationship property
pivoting initially.

For RDF-star, use the same profile + dynamic pivot flow over:

```text
rel_id, source_primary_label, rel_type, target_primary_label,
projected_property_name, value, value_type, source_order
```

Partition the result by `(source_primary_label, rel_type, target_primary_label)`
or by `rel_type`, depending on importer ergonomics and file counts.

## Node Projection

One node row per resource.

Physical partitioning:

- `nodes/<primary_label>.parquet`

Logical labels:

- Include all mapped RDF type labels in a `labels` column.
- Include `Resource` on every row.

Importer header pattern:

```text
uri:ID(Resource),labels:LABEL,<prop columns...>
uri,labels,<prop columns...>
```

Property aggregation examples:

```sql
-- ANY mode
min(value) FILTER (WHERE property_name = 'name') AS name

-- strict LAST mode
arg_max(value, source_order) FILTER (WHERE property_name = 'name') AS name

-- strict FIRST mode
arg_min(value, source_order) FILTER (WHERE property_name = 'name') AS name

-- strict ordered ARRAY mode
list(value ORDER BY source_order) FILTER (WHERE property_name = 'name') AS name
```

Property schema options:

1. Use observed predicates per primary label.
2. Use mapping-configured properties per primary label when present.
3. Allow a max-column guard to avoid pathological ultra-wide files.

## Relationship Projection

Initial regular RDF output:

- Partition by relationship type, with optional source/target primary labels for
  file organization and statistics.
- Use a single `Resource` id group in importer headers.
- De-duplicate regular RDF relationships by:
  `(source_uri, rel_type, target_uri)`.

Importer header pattern:

```text
source_uri:START_ID(Resource),target_uri:END_ID(Resource)
source_uri,target_uri
```

Command pattern:

```text
--relationships=<REL_TYPE>=headers/rels/<rel_type>.csv,relationships/<rel_type>.parquet
```

Future RDF-star output:

- Assign deterministic relationship ids, likely hash of quoted `(s, p, o)` plus
  collision handling.
- Aggregate relationship properties by relationship id.
- Emit relationship property columns only for RDF-star files.

## Output Layout

```text
out/
  staging.duckdb
  metadata/
    graph_config.json
    prefixes.json
    mappings.json
    primary_label_explain.parquet
    import_args.txt
    schema.cypher
  headers/
    nodes/
    relationships/
  nodes/
    <primary_label>.parquet
  relationships/
    <rel_type>.parquet
```

## Running the Prototype

Run the current rdflib-backed correctness prototype from the DuckDB worktree:

```bash
cd /Users/mh/d/python/rdflib-neo4j/.claude/worktrees/duckdb-bulk-import-experiment

python3 -m rdflib_neo4j.bulk.cli path/to/file.ttl \
  --format turtle \
  --output /tmp/duckdb-bulk-out
```

If the package has been installed and the console script is available, the same
pipeline can be run as:

```bash
rdflib-neo4j-bulk-prototype path/to/file.ttl \
  --format turtle \
  --output /tmp/duckdb-bulk-out
```

For the ChEBI RDF/XML benchmark target:

```bash
python3 -m rdflib_neo4j.bulk.cli /Users/mh/d/java/neo/neosemantics/testdata/chebi.owl \
  --format xml \
  --output /tmp/chebi-bulk-out
```

The prototype writes:

- `metadata/graph_config.json`
- `metadata/prefixes.json`
- `metadata/counts.json`
- `metadata/primary_label_explain.parquet`
- `nodes/<primary_label>.parquet`
- `relationships/<rel_type>.parquet`

Quick local validation:

```bash
python3 -m pytest test/bulk -q -p no:cacheprovider
```

This path is still the rdflib-backed correctness prototype. It is useful for
fixture validation and end-to-end output checks, but large RDF/XML files such as
ChEBI may run slowly until the native ingestion path is implemented.

## Validation Plan

Small fixtures:

- Compare row-level output against existing rdflib-neo4j / n10s behavior for:
  - `SHORTEN`, `IGNORE`, `MAP`, `KEEP`
  - bnodes
  - `rdf:type` as labels
  - `ANY`, `LAST`, `FIRST`, and `ARRAY`

ChEBI benchmark:

- Run on `/Users/mh/d/java/neo/neosemantics/testdata/chebi.owl`.
- Record parse time, DuckDB size, projection time, Parquet size, and importer time.
- Verify counts:
  - raw triples
  - resources
  - label counts
  - property facts
  - relationship facts
  - node rows
  - relationship rows

Neo4j import validation:

- Import into Neo4j Enterprise `2026.04.0`.
- Create `Resource(uri)` uniqueness constraint or include schema import where
  supported.
- Validate sample resources against n10s import output.

## ChEBI Baseline

Baseline run on May 10, 2026:

```bash
env UV_CACHE_DIR=/private/tmp/uv-cache /usr/bin/time -lp \
  uv run --python /opt/homebrew/bin/python3.13 \
    --with 'rdflib==7.0.0' \
    --with 'duckdb==1.5.2' \
    --with 'neo4j-rust-ext==6.2.0.0' \
    python -m rdflib_neo4j.bulk.cli \
      /Users/mh/d/java/neo/neosemantics/testdata/chebi.owl \
      --format xml \
      --output /private/tmp/chebi-bulk-baseline-duckdb152-run1
```

Observed result:

- Input: `chebi.owl`, 774.4 MB RDF/XML.
- Runtime: 2669.42 seconds, about 44 minutes 29 seconds.
- CPU: 3204.84 user seconds + 750.25 sys seconds, about 148% CPU.
- Max RSS: 17,655,955,456 bytes, about 16.4 GiB.
- Output size: 235 MB.
- Staged triples: 8,825,356.
- Node rows: 1,217,372.
- Property facts: 4,750,708.
- Relationship facts: 2,857,565.
- Deduplicated relationship rows: 2,857,565.

Interpretation:

- This is a poor but useful baseline. It proves the projection pipeline can
  complete on ChEBI, but memory use is dominated by RDFLib `Graph.parse()`
  materializing the RDF/XML graph before DuckDB insertion.
- A streaming or native parser should be an easy improvement target for both
  runtime and memory.
- The benchmark needs progress logging so long runs report phase timing,
  triples processed, throughput, and output counts while they run.
- Also evaluate whether the same native/concurrent parser wrapper should be
  usable by rdflib-neo4j generally, not only by the bulk import prototype.

## Work Packages

1. Planning artifact and CLI skeleton.
2. rdflib-backed DuckDB prototype sink:
   - Small fixture correctness.
   - ChEBI end-to-end prototype.
   - Raw staging contract validation.
3. Native ingestion benchmark harness:
   - DuckDB `rdf` extension.
   - Rust `oxrdfio` prototype if needed.
   - rdflib fallback for correctness.
4. Add prototype progress logging:
   - Parse/stage/project/export phase timing.
   - Triples processed every 10k or 100k rows.
   - Throughput, memory, and output counts.
5. Investigate a reusable native/concurrent RDF parser wrapper for both bulk
   import and rdflib-neo4j write paths.
6. Config model for n10s-compatible mapping options.
7. Raw triple staging contract with required term-kind, datatype, language, and
   graph fields. `source_order` is optional and required only for strict
   stream-order modes.
8. URI mapping, prefix handling, literal coercion, and bnode conversion.
9. Label extraction and primary-label heuristic.
10. Node property aggregation and Parquet/header export.
11. Relationship aggregation and Parquet/header export.
12. Import command generation for Neo4j Enterprise `2026.04.0`.
13. Small fixture tests.
14. ChEBI benchmark script and result report.

## Known Failure Modes and Workarounds

### duckdb_rdf backend — compressed input

The `rdf` extension's serd parser requires seekable files.  Named pipes (FIFOs)
fail with `PipeFileSystem: SeekPosition is not implemented!`.

**Workaround**: a fsspec `DecompressingFS` registered with DuckDB intercepts
`decomp://` URLs, wraps a decompressor subprocess (`pigz`, `bzip2`, `xz`,
`zstd`, `7zz`) in a forward-only file object, and fakes `SeekPosition()` by
returning a monotonically increasing byte offset.  serd only calls this for
EOF detection, not for random access, so the fake seek is safe.  No temp file
is written to disk.

Decompressor preference order for `.gz`: `pigz` (parallel) → `zcat` → `gzip`.
For `.bz2`: `pbzip2` → `bzip2`.  For `.7z`: `7zz -mmt{N}` → `7z -mmt{N}`.

A GH issue was filed with the `duck_rdf` maintainer requesting native
decompression support via a C++ `FileHandle` wrapper using zlib (see
https://github.com/nonodename/duck_rdf/issues/36).

### duckdb_rdf backend — invalid Turtle content (real-world datasets)

serd is a strict Turtle parser.  Two classes of real-world data trigger crashes
or hard errors even with `strict_parsing=false`:

| Issue | Example | Symptom |
|---|---|---|
| Backslash escape sequences in IRIs | `<ar/معالج_IBM_BlueGene_\L\P>` | `SERD parsing error 'Invalid syntax'` |
| ISO 639-2 (3-letter) language tags | `"text"@deu` | `SERD other error` (crash) |

Both patterns appear in YAGO 3.1 Wikipedia-derived data.  `strict_parsing=false`
handles some syntax errors but cannot recover from the backslash or 3-letter
language-tag crash.

**Workaround**: pass a `--filter-cmd` that strips offending lines before serd
sees them.  Use `ag` (Silver Searcher) for speed over `grep`:

```bash
# Drop lines with backslash IRIs and 3-letter language tags
--filter-cmd 'ag -v "(<[^>]*\\[^>]*>|\"@[a-z]{3}[ .])"'
```

This drops a small fraction of triples (~0.05% backslash IRIs; variable % of
multilingual anchor-text triples for 3-letter tags).  For production use,
consider a pre-processing step that normalises language tags (ISO 639-2 →
ISO 639-1) rather than discarding them.

### neo4j-admin database import — database name positional arg broken with --input-type=parquet

In `neo4j-enterprise-2026.04.0`, specifying a database name other than the
default `neo4j` as the positional `<database>` argument fails with:

```
Unable to find the parent of the path: <name>
```

The argument is validated as a filesystem path rather than a database name when
`--input-type=parquet` is active.  This appears to be a bug in 2026.04.0.

**Workaround**: omit the database name (import into the default `neo4j`
database), then rename the database directory in `data/databases/` afterwards.

### rdflib / oxigraph backends — Python per-row bottleneck

Both backends use `executemany` to insert into DuckDB, which caps throughput
at ~2–3k triples/s due to the Python GIL and per-call overhead.  These backends
exist as correctness fallbacks for data that the `duckdb_rdf` backend cannot
parse (DTD entity declarations in RDF/XML, non-standard language tags).

**Not a bug**; for large datasets use `--parser duckdb_rdf`.

## Open Risks

- rdflib RDF/XML parsing may be too slow or memory-heavy at 100 GB+.
- DuckDB `rdf` extension RDF/XML support is marked experimental.
- DuckDB `rdf` extension may not expose enough term-kind metadata for plain
  string literals vs IRI objects.
- Parallel native scans may not preserve source order. This is acceptable for the
  default bulk path, but not for strict `FIRST` / `LAST` compatibility modes.
- Primary-label selection is heuristic and must remain overridable.
- Very wide node projections may produce poor importer behavior; add column caps
  and split policies.
- Neo4j property arrays require homogeneous types.
- Dynamic labels in Parquet import need confirmation on the target importer.
- DuckDB temp storage and sort/group-by memory need explicit configuration for
  100 GB+ runs.

## Directory / Multi-File Input

The CLI (`rdflib-neo4j-bulk-prototype`) now accepts a directory as its `input`
argument.  All files with recognised RDF extensions (`.nt`, `.ttl`, `.xml`,
`.rdf`, `.owl`, `.nq`, `.trig`, and compressed variants) are ingested
sequentially into the same staging DuckDB table, sorted lexicographically.

```bash
# Ingest an entire directory
rdflib-neo4j-bulk-prototype /path/to/DBPedia-Subset \
    --parser duckdb_rdf \
    --output /tmp/dbpedia-out

# Restrict to a specific extension pattern
rdflib-neo4j-bulk-prototype /path/to/DBPedia-Subset \
    --glob "*.nt" \
    --parser duckdb_rdf \
    --output /tmp/dbpedia-out
```

The `--glob` flag is a `Path.glob()` pattern relative to the directory.

### Open Design Decision: Filename-Based Primary Label

When loading a directory of RDF files, the primary label for nodes that lack an
`rdf:type` triple defaults to `Resource`.  Three options for using the filename
as a label hint:

**Option A — per-file `rdf:type` injection** *(preferred for simplicity)*
: Before ingesting each file, add an `rdf:type <filename-stem>` triple for
every subject appearing in the file.  Requires a pre-scan pass or a separate
staging step.  Works transparently with the existing SQL projection.

**Option B — primary_label override after projection**
: Track which `source_order` range came from each file (record first/last order
per file).  After `build_facts()`, update `primary_label` to the filename stem
for any node whose only labels are generic (`Resource`).  No pre-scan needed;
requires extending the schema with a `file_tag` column in `rdf_triples`.

**Option C — configurable filename → label mapping (CLI flag)**
: Add `--file-label-pattern` accepting a Python regex with a named group
`label` extracted from the filename stem.  Example:
`--file-label-pattern "(?P<label>[a-z]+)_types_en"` maps
`instance_types_en.nt` → `InstanceType`.  Maximum flexibility, maximum
configuration burden.

**Recommendation**: implement Option B first (minimal schema change, no
pre-scan) with a `--filename-label` flag that enables the feature.

---

## LargeRDFBench Dataset Examples

Datasets sourced from https://github.com/dice-group/LargeRDFBench

### NYT (New York Times Linked Data)

3 RDF/XML files (~38 MB total). Clean data, no filter needed.

```bash
rdflib-neo4j-bulk-prototype /Users/mh/d/data/rdf/NYT \
    --parser duckdb_rdf \
    --output /tmp/nyt-out

# With filename-based labels (locations, organizations, people):
rdflib-neo4j-bulk-prototype /Users/mh/d/data/rdf/NYT \
    --parser duckdb_rdf \
    --filename-label \
    --output /tmp/nyt-out
```

Result (filename-label): 110,185 nodes (Concept: 9,872 · Feature: 1,760 · locations: 1,959 · organizations: 3,060 · people: 5,015 · Resource: 88,519), 176,908 props, 146,567 rels — ~1 second.

### Affymetrix (Bio2RDF microarray annotations)

66 N-Triples files, ~8 GB total. Four data quality issues require filtering:
- Lines starting with `<bio2rdf_dataset:` — underscore in URI scheme, invalid per RFC 3986
- Lines containing `<http://...gi|...|...>` — pipe `|` characters in IRIs (GenBank accessions)
- Lines containing IRIs with embedded spaces (e.g. `<http://bio2rdf.org/ec:EC:1.5.99.8;  1.5.1.12>`)
- Lines containing IRIs with backtick characters (e.g. `<http://bio2rdf.org/symbol:tomQ\`b>`)

```bash
rdflib-neo4j-bulk-prototype /Users/mh/d/data/rdf/Affymetrix \
    --parser duckdb_rdf \
    --filename-label \
    --filename-label-strip ".na32.annot" \
    --filter-cmd 'ag -v "^<bio2rdf_dataset:" | ag -v "<[^>]*[| `][^>]*>"' \
    --output /tmp/affymetrix-out
```

The two-filter chain: first drops the invalid `bio2rdf_dataset:` scheme; second drops any IRI containing
pipe `|`, space, or backtick `` ` `` (all invalid per RFC 3986, all present in bio2rdf data).

Label strip `.na32.annot` converts `HG-U133_Plus_2.na32.annot` → `HG-U133_Plus_2` as the Neo4j label.

Throughput with `cat | ag` filter: ~175,000 triples/s per file (vs ~1M/s unfiltered; bottleneck is the pipeline overhead).

### DBPedia Subset

30 N-Triples files, ~7.5 GB total. Data is clean for `duckdb_rdf`.

```bash
rdflib-neo4j-bulk-prototype /Users/mh/d/data/rdf/DBPedia-Subset \
    --glob "*.nt" \
    --parser duckdb_rdf \
    --output /tmp/dbpedia-out

# Note: directory also contains dbpedia_3.5.1.owl (RDF/XML) —
# use --glob "*.nt" to exclude it, or omit --glob to include all formats.
```

### YAGO 3.1 (25 GB compressed, 2.66 billion triples)

Single `.ttl.gz` file. Two crash-causing patterns require filtering:
- Backslash escape sequences in IRIs: `<ar/word\L\P>` → SERD "Invalid syntax"  
- ISO 639-2 three-letter language tags: `"text"@deu` → SERD "other error"

```bash
rdflib-neo4j-bulk-prototype \
    /Users/mh/d/data/rdf/yago/yago3.1_entire_ttl.gz \
    --format turtle \
    --parser duckdb_rdf \
    --filter-cmd 'ag -v "(<[^>]*\\[^>]*>|\"@[a-z]{3}[ .])"' \
    --output /Users/mh/d/data/rdf/yago-bulk-out \
    --db /Users/mh/d/data/rdf/yago-bulk-out/staging.duckdb
```

Status: **in progress** — filter caught most bad lines; failed at line 383M (29 min in).
A remaining serd-crashing pattern exists in YAGO. Investigate with:
```bash
pigz -dc yago3.1_entire_ttl.gz | ag -v "(<[^>]*\\[^>]*>|\"@[a-z]{3}[ .])" | \
    sed -n '383522420,383522430p'
```

### FIBO (Financial Industry Business Ontology)

Source: https://edmconnect.edmcouncil.org/okgspecialinterestgroup/resources-sig-link/resources-sig-link-fibo-products-download

Single compressed N-Triples file, 2.1 MB gzip (~241K triples). Clean standard IRIs — no filter needed.

```bash
rdflib-neo4j-bulk-prototype /Users/mh/d/data/rdf/fibo/fibo.nt.gz \
    --format nt \
    --parser duckdb_rdf \
    --output /tmp/fibo-out
```

Result: 29,273 nodes across 70+ typed labels (Class, Exchange, Country, Currency, CodeElement, Restriction, …), 60,139 props, 60,522 rels — 1 second at 1M triples/s.

### Freebase (~30 GB compressed, 3.13 billion triples)

```bash
rdflib-neo4j-bulk-prototype \
    /Users/mh/d/data/rdf/freebase-rdf-latest.gz \
    --format nt \
    --parser duckdb_rdf \
    --output /Users/mh/d/data/rdf/freebase-out \
    --db /Users/mh/d/data/rdf/freebase-out/staging.duckdb
```

Expected: Freebase uses clean Freebase IRIs — no filter likely needed. Estimated ~2–3 hours for ingestion.

**Observed**: Python process runs at 100% CPU during ingestion — serd's single-threaded C parser is the bottleneck.
The parser runs flat-out on 30 GB of data; this is expected behaviour, not a Python overhead issue.

**Named-FIFO bypass not possible**: serd requires `SeekPosition` (for EOF detection) which kernel pipes/FIFOs
don't implement. The Python virtual FS (`DecompressingFS`) works because it fake-implements seek by returning
a monotonically increasing byte offset. Python GIL overhead is negligible: `read()` releases the GIL during
the kernel `read(2)` syscall, so the bottleneck is the subprocess decompressor + serd, not Python.

TODO: investigate multi-threaded ingestion by splitting `.gz` into chunks with `pigz --block` or ingesting
multiple files in parallel when the input is a directory.
