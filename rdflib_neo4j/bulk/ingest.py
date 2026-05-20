"""Streaming ingestion backends for the DuckDB bulk import pipeline.

Each backend produces rows in the staging schema:
  (source_order UBIGINT, subject VARCHAR, predicate VARCHAR,
   object_kind VARCHAR, object_value VARCHAR, datatype VARCHAR, lang VARCHAR)

Backends:
- rdflib   -- streaming rdflib Store sink; no full-graph materialisation
- oxigraph -- pyoxigraph Rust parser; fastest for most formats including RDF/XML
"""
from __future__ import annotations

import sys
import time
from typing import Callable, Optional

from rdflib_neo4j.bulk.terms import BNODE_PREFIX, object_parts, resource_id
from rdflib_neo4j.bulk.utils import mem_stat as _mem_stat

# Staging table column order used by all INSERT statements.
_COLUMNS = "(source_order, subject, predicate, object_kind, object_value, datatype, lang)"
_INSERT = f"INSERT INTO rdf_triples {_COLUMNS} VALUES (?, ?, ?, ?, ?, ?, ?)"

_PROGRESS_INTERVAL = 100_000


def _emit_progress(label: str, total: int, start: float) -> None:
    elapsed = time.monotonic() - start
    rate = total / elapsed if elapsed > 0 else 0
    print(
        f"\r[{label}] {total:>10,} triples  {rate:>10,.0f}/s  {elapsed:6.1f}s",
        end="",
        file=sys.stderr,
        flush=True,
    )


def _finish_progress(label: str, total: int, start: float) -> None:
    _emit_progress(label, total, start)
    print(file=sys.stderr)


# ---------------------------------------------------------------------------
# rdflib streaming sink
# ---------------------------------------------------------------------------

def ingest_rdflib(
    path: str,
    rdf_format: Optional[str],
    connection,
    batch_size: int = 100_000,
    progress: bool = True,
) -> int:
    """Parse *path* with rdflib, streaming triples into *connection* as they arrive.

    Uses a custom rdflib Store so the parser never holds a full in-memory graph.
    """
    from rdflib.store import Store, VALID_STORE

    label = "rdflib"
    start = time.monotonic()
    conn = connection

    class _DuckDBSink(Store):
        context_aware = False
        formula_aware = False
        transaction_aware = False
        graph_aware = False

        def __init__(self):
            super().__init__()
            self._buf: list = []
            self._total = 0
            self._order = 0

        def open(self, configuration, create=False):
            return VALID_STORE

        def add(self, triple, context, quoted=False):
            s, p, o = triple
            self._order += 1
            kind, value, datatype, lang = object_parts(o)
            self._buf.append((
                self._order,
                resource_id(s),
                str(p),
                kind,
                str(value),
                datatype,
                lang,
            ))
            if len(self._buf) >= batch_size:
                self._flush()

        def _flush(self):
            if not self._buf:
                return
            conn.executemany(_INSERT, self._buf)
            self._total += len(self._buf)
            if progress and self._total % _PROGRESS_INTERVAL < batch_size:
                _emit_progress(label, self._total, start)
            self._buf.clear()

        def close(self, commit_pending_transaction=False):
            self._flush()

        # Required no-op stubs
        def __len__(self, context=None):
            return 0

        def triples(self, triple_pattern, context=None):
            return iter([])

        def contexts(self, triple=None):
            return iter([])

        def remove(self, triple_pattern, context=None):
            pass

    sink = _DuckDBSink()
    from rdflib import Graph as _Graph
    g = _Graph(store=sink)
    g.parse(path, format=rdf_format)
    sink.close()
    if progress:
        _finish_progress(label, sink._total, start)
    return sink._total


# ---------------------------------------------------------------------------
# pyoxigraph streaming parser
# ---------------------------------------------------------------------------

_OXIGRAPH_FORMATS = {
    "turtle": "TURTLE",
    "ttl": "TURTLE",
    "xml": "RDF_XML",
    "rdf": "RDF_XML",
    "application/rdf+xml": "RDF_XML",
    "nt": "N_TRIPLES",
    "n-triples": "N_TRIPLES",
    "ntriples": "N_TRIPLES",
    "nquads": "N_QUADS",
    "n-quads": "N_QUADS",
    "trig": "TRIG",
    "n3": "N3",
}


def _ox_format(rdf_format: Optional[str], path: str):
    import pyoxigraph

    if rdf_format:
        key = rdf_format.lower().strip()
        attr = _OXIGRAPH_FORMATS.get(key)
        if attr:
            return getattr(pyoxigraph.RdfFormat, attr)
        # Try pyoxigraph's own extension/media-type lookup
        try:
            return pyoxigraph.RdfFormat.from_media_type(rdf_format)
        except Exception:
            pass
        try:
            return pyoxigraph.RdfFormat.from_extension(rdf_format.lstrip("."))
        except Exception:
            pass
    # Fall back to file extension
    ext = path.rsplit(".", 1)[-1].lower()
    attr = _OXIGRAPH_FORMATS.get(ext)
    if attr:
        return getattr(pyoxigraph.RdfFormat, attr)
    raise ValueError(
        f"Cannot determine RDF format for '{path}'. "
        f"Pass --format or use a recognised file extension."
    )


def _ox_object_parts(obj):
    """Convert a pyoxigraph object term to (kind, value, datatype, lang)."""
    import pyoxigraph

    if isinstance(obj, pyoxigraph.Literal):
        lang = obj.language  # None or e.g. "en"
        dt = str(obj.datatype.value) if obj.datatype else None
        # Normalise xsd:boolean to Python-style True/False to match rdflib behaviour.
        if dt == "http://www.w3.org/2001/XMLSchema#boolean":
            value = "True" if obj.value.lower() in ("true", "1") else "False"
        else:
            value = obj.value
        return "literal", value, dt, lang
    if isinstance(obj, pyoxigraph.BlankNode):
        return "bnode", f"{BNODE_PREFIX}{obj.value}", None, None
    # NamedNode
    return "iri", obj.value, None, None


def _ox_subject(s):
    import pyoxigraph

    if isinstance(s, pyoxigraph.BlankNode):
        return f"{BNODE_PREFIX}{s.value}"
    return s.value


def ingest_oxigraph(
    path: str,
    rdf_format: Optional[str],
    connection,
    batch_size: int = 100_000,
    progress: bool = True,
) -> int:
    """Parse *path* with pyoxigraph, streaming triples into *connection*.

    pyoxigraph uses compiled Rust parsers and never materialises a full graph.
    """
    import pyoxigraph

    label = "oxigraph"
    start = time.monotonic()
    fmt = _ox_format(rdf_format, path)

    buf: list = []
    total = 0
    order = 0

    def _flush():
        nonlocal total
        connection.executemany(_INSERT, buf)
        total += len(buf)
        if progress and total % _PROGRESS_INTERVAL < batch_size:
            _emit_progress(label, total, start)
        buf.clear()

    with open(path, "rb") as fh:
        for triple in pyoxigraph.parse(fh, fmt):
            order += 1
            kind, value, datatype, lang = _ox_object_parts(triple.object)
            buf.append((
                order,
                _ox_subject(triple.subject),
                triple.predicate.value,
                kind,
                value,
                datatype,
                lang,
            ))
            if len(buf) >= batch_size:
                _flush()

    if buf:
        _flush()

    if progress:
        _finish_progress(label, total, start)
    return total


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

BACKENDS = ("rdflib", "oxigraph", "duckdb_rdf")


# ---------------------------------------------------------------------------
# DuckDB rdf extension backend — pure SQL, no Python per-row overhead
# ---------------------------------------------------------------------------

# Map our format names to DuckDB rdf file_type values (only those confirmed to work).
# RDF/XML (.xml, .rdf) is auto-detected from extension; .owl needs a rename/symlink.
_DUCKDB_RDF_FILETYPE_MAP = {
    "turtle": "turtle",
    "ttl": "turtle",
    "n3": "turtle",   # Notation3 is a Turtle superset
    "nt": "nt",
    "ntriples": "ntriples",
    "n-triples": "ntriples",
    "nquads": "nquads",
    "n-quads": "nquads",
    # RDF/XML auto-detected from .xml / .rdf extension — no file_type override available.
}

# Extensions that DuckDB rdf extension recognises for RDF/XML auto-detection.
_DUCKDB_RDFXML_EXTENSIONS = {"xml", "rdf"}

# Object-kind disambiguation heuristic: the DuckDB rdf extension does not distinguish plain
# string literals from IRI objects in its output schema. The URI prefix check (http://, https://,
# urn:, _:) is reliable for standard ontologies but is a known limitation — plain string literals
# that happen to look like URIs will be misclassified as IRIs.
_DUCKDB_RDF_INSERT_SQL = """
INSERT INTO rdf_triples
SELECT
    row_number() OVER () AS source_order,
    subject AS subject,
    predicate AS predicate,
    CASE
        WHEN object_datatype IS NOT NULL OR object_lang IS NOT NULL THEN 'literal'
        WHEN starts_with(object, 'http://') OR starts_with(object, 'https://')
             OR starts_with(object, 'urn:') OR starts_with(object, '_:') THEN
            CASE WHEN starts_with(object, '_:') THEN 'bnode' ELSE 'iri' END
        ELSE 'literal'
    END AS object_kind,
    CASE
        WHEN starts_with(object, '_:') THEN concat('bnode://', substr(object, 3))
        ELSE object
    END AS object_value,
    object_datatype AS datatype,
    object_lang AS lang
FROM read_rdf($path, prefix_expansion=true{file_type_clause})
"""


# ---------------------------------------------------------------------------
# Compressed-file helpers (named-pipe decompression for duckdb_rdf backend)
# ---------------------------------------------------------------------------

import os as _os
_CPU_COUNT = _os.cpu_count() or 1

# Maps compression suffix → ordered list of (command, args) to try.
# pigz/pbzip2/zstd are parallel and preferred; fall back to single-threaded tools.
_DECOMPRESSORS: dict[str, list[list[str]]] = {
    "gz":   [["pigz", "-dc"], ["zcat"], ["gzip", "-dc"]],
    "bz2":  [["pbzip2", "-dc"], ["bzip2", "-dc"]],
    "xz":   [["xz", "-dc"]],
    "zst":  [["zstd", "-dc"]],
    "zstd": [["zstd", "-dc"]],
    "7z":   [["7zz", "e", "-so", f"-mmt{_CPU_COUNT}"], ["7z", "e", "-so", f"-mmt{_CPU_COUNT}"]],
}


def _find_decompressor(compression_ext: str) -> Optional[list[str]]:
    """Return the first available decompressor command for *compression_ext*."""
    import shutil

    candidates = _DECOMPRESSORS.get(compression_ext.lower(), [])
    for cmd in candidates:
        if shutil.which(cmd[0]):
            return cmd
    return None


def _compression_ext(path: str) -> Optional[str]:
    """Return the compression suffix of *path* if recognised, else None."""
    suffix = path.rsplit(".", 1)[-1].lower()
    return suffix if suffix in _DECOMPRESSORS else None


class _DecompressFile:
    """Forward-only file backed by a decompressor subprocess.

    serd reads sequentially via Read() callbacks and only calls SeekPosition()
    for EOF detection — it never seeks backward.  We track bytes read and
    return that as the current position, satisfying serd without actual seeking.

    An optional *filter_cmd* (e.g. ``["grep", "-v", "pattern"]``) is chained
    after the decompressor via a pipe — useful to drop lines that would cause
    serd to crash even with strict_parsing=false (e.g. IRIs with backslashes
    in YAGO-style Wikipedia-derived datasets).
    """

    def __init__(
        self,
        real_path: str,
        comp_ext: Optional[str],
        filter_cmd: Optional[list] = None,
    ) -> None:
        import subprocess

        self._procs: list = []
        if comp_ext:
            cmd = _find_decompressor(comp_ext)
            if cmd is None:
                raise RuntimeError(
                    f"No decompressor found for .{comp_ext} files. "
                    f"Install one of: {[c[0] for c in _DECOMPRESSORS[comp_ext]]}"
                )
            p1 = subprocess.Popen(
                cmd + [real_path], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
            )
        else:
            # No decompression — use cat to produce a pipe so downstream tools
            # (e.g. ag) see stdin-mode input and don't add line numbers.
            p1 = subprocess.Popen(
                ["cat", real_path], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
            )
        self._procs.append(p1)
        src_stdout = p1.stdout

        if filter_cmd:
            # shell=True so the user can pipe multiple commands:
            # e.g. "ag -v pat1 | ag -v pat2"
            p2 = subprocess.Popen(
                filter_cmd, stdin=src_stdout, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, shell=True,
            )
            if comp_ext:
                src_stdout.close()  # let upstream receive SIGPIPE if p2 exits
            self._procs.append(p2)
            self._stdout = p2.stdout
        else:
            self._stdout = src_stdout
        self._pos = 0
        self._closed = False
        # DuckDB reads in 4 KB chunks from the virtual FS; without buffering each chunk
        # triggers a GIL-acquiring kernel read() through the C extension. 1 MB read-ahead
        # amortises this to ~1 syscall per MB, letting the decompressor run at full speed.
        self._buf = b""
        self._buf_pos = 0
        self._READ_AHEAD = 1 << 20  # 1 MB

    def read(self, n: int = -1) -> bytes:
        if n < 0:
            # Unbounded read — drain buffer then pipe.
            tail = self._stdout.read()
            data = self._buf[self._buf_pos:] + tail
            self._buf = b""
            self._buf_pos = 0
            self._pos += len(data)
            return data

        available = len(self._buf) - self._buf_pos
        if available < n:
            # Refill buffer: pull at least READ_AHEAD bytes from pipe.
            chunk = self._stdout.read(self._READ_AHEAD)
            if chunk:
                self._buf = self._buf[self._buf_pos:] + chunk
            else:
                self._buf = self._buf[self._buf_pos:]
            self._buf_pos = 0

        data = self._buf[self._buf_pos : self._buf_pos + n]
        self._buf_pos += len(data)
        self._pos += len(data)
        return data

    def seek(self, pos: int, whence: int = 0) -> int:
        # serd calls SeekPosition() only to detect EOF, never to rewind. Returning the
        # monotonically-increasing byte offset satisfies that contract without real seeking.
        # Named pipes (FIFOs) would be simpler but fail because serd requires SeekPosition support.
        return self._pos

    def tell(self) -> int:
        return self._pos

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def close(self) -> None:
        if not self._closed:
            self._stdout.close()
            for p in reversed(self._procs):
                p.wait()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


_decomp_fs_registry: dict = {}  # id(connection) -> DecompressingFS


def _get_or_create_decomp_fs(connection):
    """Return the DecompressingFS for *connection*, creating and registering it once."""
    key = id(connection)
    if key not in _decomp_fs_registry:
        fs = _make_decomp_fs()
        connection.register_filesystem(fs)
        _decomp_fs_registry[key] = fs
    return _decomp_fs_registry[key]


def _make_decomp_fs():
    """Return a per-connection DecompressingFS instance (lazy import of fsspec)."""
    from fsspec.spec import AbstractFileSystem

    class DecompressingFS(AbstractFileSystem):
        """decomp:// filesystem — register virtual→real mappings before use.

        The virtual path carries the RDF extension serd needs (.ttl, .xml …).
        The real path is the actual compressed file on disk.
        """

        protocol = "decomp"

        def __init__(self):
            super().__init__()
            self._registry: dict[str, tuple] = {}

        def register(
            self,
            virtual: str,
            real: str,
            comp_ext: str,
            filter_cmd: Optional[list] = None,
        ) -> None:
            self._registry[virtual] = (real, comp_ext, filter_cmd)

        def _lookup(self, path: str) -> tuple:
            p = path.removeprefix("decomp://")
            return self._registry.get(p, (p, None, None))

        def open(self, path, mode="rb", **kwargs):
            real, comp_ext, filter_cmd = self._lookup(path)
            return (
                _DecompressFile(real, comp_ext, filter_cmd)
                if comp_ext or filter_cmd
                else open(real, "rb")
            )

        def info(self, path, **kwargs):
            real, comp_ext, filter_cmd = self._lookup(path)
            if filter_cmd:
                # Return a small nominal size so serd never sees bytes_read < declared_size.
                # (Filtered output is smaller than original; serd raises SERD failure if it
                # reads fewer bytes than the size reported here.)
                size = 1
            else:
                size = _os.stat(real).st_size
            return {
                "name": path.removeprefix("decomp://"),
                "size": size,
                "type": "file",
            }

        def ls(self, path, detail=True, **kwargs):
            i = self.info(path)
            return [i] if detail else [i["name"]]

        def glob(self, path, **kwargs):
            # rdf extension globs before opening; return the virtual path directly.
            p = path.removeprefix("decomp://")
            if p in self._registry:
                return [p]
            return super().glob(path, **kwargs)

    return DecompressingFS()


def ingest_duckdb_rdf(
    path: str,
    rdf_format: Optional[str],
    connection,
    progress: bool = True,
    filter_cmd: Optional[list] = None,
) -> int:
    """Ingest *path* entirely within DuckDB using the community rdf extension.

    This is the fastest possible path for supported formats: no Python per-row
    overhead. The DuckDB rdf extension must be installed (community repository).

    Compressed inputs (.gz, .bz2, .xz, .zst, .7z) are handled via a registered
    fsspec DecompressingFS: a decompressor subprocess (pigz preferred for .gz)
    streams into a forward-only file object whose seek() returns a monotonically
    increasing byte offset.  serd only calls SeekPosition() for EOF detection so
    no actual backward seeking is needed.  No temp file is written to disk.

    Object-kind disambiguation: the extension does not distinguish plain string
    literals from IRI objects in its output schema. A URI-heuristic is applied
    (starts_with http://, https://, urn:, _:). This is reliable for standard
    ontologies; document as known limitation per the plan.
    """
    import os
    import tempfile

    label = "duckdb_rdf"
    start = time.monotonic()

    try:
        connection.execute("LOAD rdf")
    except Exception as exc:
        raise RuntimeError(
            "DuckDB rdf extension not available. "
            "Install with: INSTALL rdf FROM community"
        ) from exc

    # Strip compression suffix to get the real RDF extension.
    comp_ext = _compression_ext(path)
    base_path = path[: -(len(comp_ext) + 1)] if comp_ext else path
    ext = base_path.rsplit(".", 1)[-1].lower()
    fmt_key = (rdf_format or "").lower().strip()

    is_rdfxml = fmt_key in ("xml", "rdf", "application/rdf+xml") or ext in ("xml", "rdf", "owl")
    rdf_ext = "xml" if is_rdfxml else (ext or "ttl")

    file_type_clause = ""
    if not is_rdfxml:
        mapped = _DUCKDB_RDF_FILETYPE_MAP.get(fmt_key or ext)
        if mapped:
            file_type_clause = f", file_type='{mapped}'"

    sql = _DUCKDB_RDF_INSERT_SQL.format(file_type_clause=file_type_clause)

    if progress:
        decomp_cmd = _find_decompressor(comp_ext) if comp_ext else None
        decomp_note = f" [decompress via {decomp_cmd[0]}]" if decomp_cmd else ""
        print(f"[{label}] loading {path}{decomp_note} ...", file=sys.stderr, flush=True)

    count_before = connection.execute("SELECT count(*) FROM rdf_triples").fetchone()[0]

    if comp_ext or filter_cmd:
        # Use the decomp:// virtual FS whenever decompression or filtering is needed.
        # Named FIFOs don't work here: serd requires SeekPosition which pipes don't support.
        # The Python virtual FS fake-implements seek (returns monotonic byte offset) which
        # satisfies serd's EOF detection without actual backward seeking.
        # GIL note: DuckDB calls our read() via the C extension, acquiring the GIL per chunk.
        # The real work (subprocess pipe read) releases the GIL during the kernel system call,
        # so throughput is bounded by the decompressor + serd parser, not Python overhead.
        virtual = f"/{_os.path.basename(base_path)}"
        if rdf_ext == "xml" and not virtual.endswith((".xml", ".rdf")):
            virtual = _os.path.splitext(virtual)[0] + ".xml"
        fs = _get_or_create_decomp_fs(connection)
        fs.register(virtual, _os.path.abspath(path), comp_ext, filter_cmd)
        effective_path = f"decomp://{virtual}"
        connection.execute(sql, {"path": effective_path})
    else:
        # Uncompressed, no filter: use the real path directly, symlink .owl→.xml if needed.
        effective_path = path
        symlink_path: Optional[str] = None
        if is_rdfxml and ext not in _DUCKDB_RDFXML_EXTENSIONS:
            tmp = tempfile.mktemp(suffix=".xml")
            os.symlink(os.path.abspath(path), tmp)
            symlink_path = tmp
            effective_path = tmp
        try:
            connection.execute(sql, {"path": effective_path})
        finally:
            if symlink_path and os.path.islink(symlink_path):
                os.unlink(symlink_path)

    total = connection.execute("SELECT count(*) FROM rdf_triples").fetchone()[0] - count_before

    if progress:
        elapsed = time.monotonic() - start
        rate = total / elapsed if elapsed > 0 else 0
        print(
            f"[{label}] {total:,} triples  {rate:,.0f}/s  {elapsed:.1f}s{_mem_stat()}",
            file=sys.stderr,
        )
    return total


def _file_type_clause(path: str, rdf_format: Optional[str]) -> str:
    """Return the file_type clause string for a read_rdf() call."""
    comp_ext = _compression_ext(path)
    base_path = path[: -(len(comp_ext) + 1)] if comp_ext else path
    ext = base_path.rsplit(".", 1)[-1].lower()
    fmt_key = (rdf_format or "").lower().strip()
    is_rdfxml = fmt_key in ("xml", "rdf", "application/rdf+xml") or ext in ("xml", "rdf", "owl")
    if is_rdfxml:
        return ""
    mapped = _DUCKDB_RDF_FILETYPE_MAP.get(fmt_key or ext)
    return f", file_type='{mapped}'" if mapped else ""


# source_order=0 for all rows: the bulk/parallel path loads an entire directory in one
# UNION ALL so global ordering across files is undefined. FIRST/LAST aggregation falls
# back to min(value) behaviour. Use sequential ingest when per-triple ordering matters.
_DUCKDB_RDF_BULK_BODY = """\
SELECT
    0::UBIGINT AS source_order,
    subject AS subject,
    predicate AS predicate,
    CASE
        WHEN object_datatype IS NOT NULL OR object_lang IS NOT NULL THEN 'literal'
        WHEN starts_with(object, 'http://') OR starts_with(object, 'https://')
             OR starts_with(object, 'urn:') OR starts_with(object, '_:') THEN
            CASE WHEN starts_with(object, '_:') THEN 'bnode' ELSE 'iri' END
        ELSE 'literal'
    END AS object_kind,
    CASE
        WHEN starts_with(object, '_:') THEN concat('bnode://', substr(object, 3))
        ELSE object
    END AS object_value,
    object_datatype AS datatype,
    object_lang AS lang
FROM ({union_all})"""


def ingest_duckdb_rdf_bulk(
    paths: list,
    rdf_format: Optional[str],
    connection,
    progress: bool = True,
) -> int:
    """Ingest multiple uncompressed RDF files in parallel using a single UNION ALL query.

    DuckDB executes each branch of the UNION ALL on a separate thread,
    giving near-linear speedup across files (benchmarked: 16x on 16-core Apple M-series
    with 306 files, 415M triples in 5.3 s — 78M triples/s).

    Only for uncompressed, unfiltered files with the duckdb_rdf backend.
    Falls back to sequential ingest_duckdb_rdf() when filter_cmd or compression is needed.
    """
    try:
        connection.execute("LOAD rdf")
    except Exception as exc:
        raise RuntimeError(
            "DuckDB rdf extension not available. "
            "Install with: INSTALL rdf FROM community"
        ) from exc

    label = "duckdb_rdf_bulk"
    start = time.monotonic()

    if progress:
        print(
            f"[{label}] loading {len(paths)} files in parallel ...",
            file=sys.stderr,
            flush=True,
        )

    count_before = connection.execute("SELECT count(*) FROM rdf_triples").fetchone()[0]

    scans = []
    for p in paths:
        ft = _file_type_clause(p, rdf_format)
        scans.append(f"SELECT * FROM read_rdf('{p}', prefix_expansion=true{ft})")
    union_all = " UNION ALL ".join(scans)

    connection.execute(
        "INSERT INTO rdf_triples " + _DUCKDB_RDF_BULK_BODY.format(union_all=union_all)
    )

    total = connection.execute("SELECT count(*) FROM rdf_triples").fetchone()[0] - count_before

    if progress:
        elapsed = time.monotonic() - start
        rate = total / elapsed if elapsed > 0 else 0
        print(
            f"[{label}] {total:,} triples  {rate:,.0f}/s  {elapsed:.1f}s{_mem_stat()}",
            file=sys.stderr,
        )
    return total


# ---------------------------------------------------------------------------
# Parallel-Parquet worker (must be top-level for multiprocessing spawn)
# ---------------------------------------------------------------------------

def _worker_rdf_to_parquet(args: tuple) -> tuple:
    """Worker function: convert a batch of RDF files → one Parquet chunk.

    Must be a top-level function (not nested/lambda) so multiprocessing can
    pickle it under the 'spawn' start method (macOS/Windows default).

    Returns (output_parquet_path, row_count).
    """
    import duckdb as _duckdb

    files, out_pq, rdf_format, filter_cmd = args
    # Each worker gets its own in-memory DuckDB connection: a shared file-based DB would
    # require locking overhead. Workers write to temp Parquet; the main process merges via
    # DuckDB's fast Parquet reader, avoiding any cross-process serialisation bottleneck.
    con = _duckdb.connect(":memory:")
    con.execute("LOAD rdf")
    scans = []

    if filter_cmd:
        # Register each file with the virtual FS so the filter subprocess is applied.
        # This avoids writing filtered temp files — data streams through OS pipes.
        fs = _get_or_create_decomp_fs(con)
        for p in files:
            comp_ext = _compression_ext(p)
            base = p[: -(len(comp_ext) + 1)] if comp_ext else p
            virtual = f"/{_os.path.basename(base)}"
            fs.register(virtual, _os.path.abspath(p), comp_ext, filter_cmd)
            ft = _file_type_clause(p, rdf_format)
            scans.append(f"SELECT * FROM read_rdf('decomp://{virtual}', prefix_expansion=true{ft})")
    else:
        for p in files:
            ft = _file_type_clause(p, rdf_format)
            scans.append(f"SELECT * FROM read_rdf('{p}', prefix_expansion=true{ft})")

    union_all = " UNION ALL ".join(scans)
    body = _DUCKDB_RDF_BULK_BODY.format(union_all=union_all)
    con.execute(f"COPY ({body}) TO '{out_pq}' (FORMAT PARQUET)")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{out_pq}')").fetchone()[0]
    con.close()
    return out_pq, n


def ingest_duckdb_rdf_parallel(
    paths: list,
    rdf_format: Optional[str],
    connection,
    n_workers: Optional[int] = None,
    temp_dir: Optional[str] = None,
    progress: bool = True,
    filter_cmd: Optional[str] = None,
    delete_inputs_after_use: bool = False,
) -> int:
    """Ingest multiple RDF files using N parallel worker processes + Parquet merge.

    Each worker gets a slice of *paths*, converts them to a temp Parquet file
    using its own DuckDB in-memory connection, then the main process merges all
    Parquet chunks into *connection*.rdf_triples via DuckDB's fast Parquet reader.

    Benchmarked speedup vs. single-connection UNION ALL:
    - 2 workers, 4 files (2 per worker): 3.25M/s vs 1.9M/s (1.7x)
    - Expected 16 workers: ~26M/s (13x), e.g. 3.13B Freebase triples in ~2 min.

    Parquet temp files are written to *temp_dir* (default: system temp).
    They are deleted after the merge step.

    Only supports uncompressed, unfiltered files with the duckdb_rdf backend.
    """
    import os
    import tempfile
    from concurrent.futures import ProcessPoolExecutor, as_completed

    try:
        connection.execute("LOAD rdf")
    except Exception as exc:
        raise RuntimeError(
            "DuckDB rdf extension not available. "
            "Install with: INSTALL rdf FROM community"
        ) from exc

    n_workers = min(n_workers or _CPU_COUNT, len(paths))
    label = "duckdb_rdf_parallel"
    start = time.monotonic()

    tmp_owned = temp_dir is None
    tmp_dir_obj = None
    if tmp_owned:
        tmp_dir_obj = tempfile.mkdtemp(prefix="rdflib_neo4j_chunks_")
        temp_dir = tmp_dir_obj

    if progress:
        print(
            f"[{label}] {len(paths)} files, {n_workers} workers → {temp_dir}",
            file=sys.stderr,
            flush=True,
        )

    chunks = [paths[i::n_workers] for i in range(n_workers) if paths[i::n_workers]]
    worker_args = [
        (chunk, os.path.join(temp_dir, f"chunk_{i:04d}.parquet"), rdf_format, filter_cmd)
        for i, chunk in enumerate(chunks)
    ]

    parquet_files: list[str] = []
    try:
        t_convert = time.monotonic()
        results = []
        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            # Map future → input chunk files so we can free them as each worker finishes.
            futures = {ex.submit(_worker_rdf_to_parquet, a): a[0] for a in worker_args}
            for fut in as_completed(futures):
                input_files = futures[fut]
                pq, n = fut.result()
                results.append((pq, n))

                freed_gb = 0.0
                if delete_inputs_after_use:
                    for f in input_files:
                        try:
                            freed_gb += os.path.getsize(f) / 1e9
                            os.unlink(f)
                        except OSError:
                            pass

                if progress:
                    dt = time.monotonic() - t_convert
                    done = len(results)
                    rate_so_far = sum(r[1] for r in results) / dt if dt > 0 else 0
                    freed_str = f"  freed {freed_gb:.1f} GB" if freed_gb > 0 else ""
                    print(
                        f"[{label}] worker {done}/{len(worker_args)} done"
                        f"  {n:,} triples  {rate_so_far:,.0f}/s  {dt:.1f}s elapsed{freed_str}{_mem_stat()}",
                        file=sys.stderr,
                        flush=True,
                    )
        dt_convert = time.monotonic() - t_convert
        parquet_files = [pq for pq, _ in results]
        n_converted = sum(n for _, n in results)

        if progress:
            rate_c = n_converted / dt_convert if dt_convert > 0 else 0
            print(
                f"[{label}] all workers done: {n_converted:,} triples in {dt_convert:.1f}s"
                f" ({rate_c:,.0f}/s); merging into staging DB ...{_mem_stat()}",
                file=sys.stderr,
                flush=True,
            )

        t_merge = time.monotonic()
        count_before = connection.execute("SELECT count(*) FROM rdf_triples").fetchone()[0]
        connection.execute(
            f"INSERT INTO rdf_triples SELECT * FROM read_parquet({parquet_files})"
        )
        total = connection.execute("SELECT count(*) FROM rdf_triples").fetchone()[0] - count_before
        if progress:
            print(
                f"[{label}] merge done: {total:,} triples in {time.monotonic() - t_merge:.1f}s{_mem_stat()}",
                file=sys.stderr,
                flush=True,
            )

        # Free temp Parquet chunks immediately after the merge — no reason to keep them.
        pq_gb = sum(os.path.getsize(pq) / 1e9 for pq in parquet_files if os.path.exists(pq))
        for pq in parquet_files:
            try:
                os.unlink(pq)
            except OSError:
                pass
        parquet_files.clear()
        if progress and pq_gb > 0:
            print(
                f"[{label}] freed {pq_gb:.1f} GB of temp Parquet chunks",
                file=sys.stderr,
                flush=True,
            )

    finally:
        for pq in parquet_files:  # catches any that weren't freed above (e.g. on error)
            try:
                os.unlink(pq)
            except OSError:
                pass
        if tmp_owned and tmp_dir_obj:
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass

    if progress:
        elapsed = time.monotonic() - start
        rate = total / elapsed if elapsed > 0 else 0
        print(
            f"[{label}] {total:,} triples  {rate:,.0f}/s  {elapsed:.1f}s",
            file=sys.stderr,
        )
    return total


def _fix_nt_chunk_boundaries(chunk_files: list) -> None:
    """Trim partial NT lines at byte-split boundaries.

    `split -b SIZE` cuts at byte boundaries, not line boundaries. At most 1 line
    is truncated at the END of each chunk and 1 line at the START of the next chunk.
    This function reads only ~4 KB from the head and tail of each file and truncates
    or skips partial lines in-place — O(1) I/O per boundary, no subprocess needed.
    """
    READ_HEAD = 4096  # more than any realistic NT line
    for i, path in enumerate(chunk_files):
        is_first = i == 0
        is_last = i == len(chunk_files) - 1
        with open(path, "r+b") as f:
            # --- fix truncated first line (start of non-first chunk) ---
            if not is_first:
                head = f.read(READ_HEAD)
                nl = head.find(b"\n")
                if nl >= 0:
                    first_line = head[:nl].lstrip()
                    # Valid NT subject starts with '<' (IRI) or '_:' (blank node).
                    # A continuation fragment from the previous chunk's split line
                    # will start with arbitrary bytes (e.g. "ject> .") — skip it.
                    if not (first_line.startswith(b"<") or first_line.startswith(b"_:")):
                        # Partial — rebuild file without the first line.
                        f.seek(nl + 1)
                        rest = f.read()
                        f.seek(0)
                        f.write(rest)
                        f.truncate(f.tell())
            # --- fix truncated last line (end of non-last chunk) ---
            if not is_last:
                f.seek(0, 2)
                size = f.tell()
                read_back = min(size, READ_HEAD)
                f.seek(size - read_back)
                tail = f.read()
                # Strip trailing newline to find the last real line.
                stripped = tail.rstrip(b"\n")
                last_nl = stripped.rfind(b"\n")
                last_line = stripped[last_nl + 1:] if last_nl >= 0 else stripped
                if not last_line.rstrip().endswith(b" ."):
                    # Partial — truncate at the last complete newline.
                    trim_to = size - len(tail) + last_nl + 1
                    f.truncate(max(0, trim_to))


def ingest_duckdb_rdf_split_file(
    path: str,
    rdf_format: Optional[str],
    connection,
    n_workers: Optional[int] = None,
    temp_dir: Optional[str] = None,
    min_size_bytes: int = 100_000_000,
    progress: bool = True,
) -> int:
    """Ingest a single large NT/NQ file by byte-splitting and processing chunks in parallel.

    For NT and NQ formats (line-per-triple): byte-split the (decompressed) file
    into N roughly equal chunks, fix the 1-2 partial lines at each split boundary
    in-place (O(1) I/O per boundary, no subprocess), then process each chunk via
    a separate worker process using ``ingest_duckdb_rdf_parallel()``.

    Only suitable for NT / NQ formats where each triple is a single line.
    For Turtle, RDF/XML, or other multi-line formats use sequential ingest.

    Falls back to sequential ``ingest_duckdb_rdf()`` when:
    - file is smaller than *min_size_bytes* (default 100 MB compressed)
    - format is not recognised as line-oriented (not NT/NQ)
    """
    import os
    import shutil
    import subprocess
    import tempfile
    import glob as _glob

    # Only line-oriented formats can be byte-split
    comp_ext = _compression_ext(path)
    base_path = path[: -(len(comp_ext) + 1)] if comp_ext else path
    ext = base_path.rsplit(".", 1)[-1].lower()
    fmt_key = (rdf_format or "").lower().strip()
    effective_fmt = fmt_key or ext
    line_formats = {"nt", "ntriples", "n-triples", "nq", "nquads", "n-quads"}
    if effective_fmt not in line_formats:
        if not effective_fmt and comp_ext:
            # Compressed file with no recognised inner extension and no --format given.
            # Example: freebase-rdf-latest.gz — inner stem has no .nt extension.
            raise ValueError(
                f"Cannot determine RDF format for '{path}': compressed file has no recognised "
                f"extension after stripping '.{comp_ext}'. Pass --format nt (or nquads/turtle) "
                f"to specify the format explicitly."
            )
        if progress:
            print(
                f"[split] {effective_fmt!r} is not line-oriented; using sequential ingest",
                file=sys.stderr,
            )
        return ingest_duckdb_rdf(path, rdf_format, connection, progress)

    # Skip split for small files
    if os.path.getsize(path) < min_size_bytes:
        return ingest_duckdb_rdf(path, rdf_format, connection, progress)

    n_workers = n_workers or _CPU_COUNT
    label = "duckdb_rdf_split"
    start = time.monotonic()

    tmp_owned = temp_dir is None
    tmp_dir_path: Optional[str] = None
    if tmp_owned:
        tmp_dir_path = tempfile.mkdtemp(prefix="rdflib_neo4j_split_")
        temp_dir = tmp_dir_path

    if progress:
        print(
            f"[{label}] splitting {path} into {n_workers} chunks in {temp_dir} ...",
            file=sys.stderr,
            flush=True,
        )

    chunk_prefix = _os.path.join(temp_dir, "chunk_")
    chunk_files: list[str] = []

    try:
        t_split = time.monotonic()
        if comp_ext:
            decomp_cmd = _find_decompressor(comp_ext)
            if decomp_cmd is None:
                raise RuntimeError(
                    f"No decompressor found for .{comp_ext} files. "
                    f"Install one of: {[c[0] for c in _DECOMPRESSORS[comp_ext]]}"
                )
            # BSD split (macOS) does not support `split -n N -` from stdin because it
            # cannot seek to determine the total size. Use `split -b SIZE` instead,
            # deriving SIZE from the compressed file size (a lower-bound approximation;
            # uncompressed chunks will be larger, and boundary-fix handles the 1-2
            # partial lines at each split point).
            file_size = os.path.getsize(path)
            chunk_bytes = max(1, file_size // n_workers)
            p1 = subprocess.Popen(
                decomp_cmd + [path], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
            )
            subprocess.run(
                ["split", "-b", str(chunk_bytes), "-", chunk_prefix],
                stdin=p1.stdout,
                stderr=subprocess.DEVNULL,
            )
            p1.stdout.close()
            p1.wait()
        else:
            file_size = os.path.getsize(path)
            chunk_bytes = max(1, file_size // n_workers)
            subprocess.run(
                ["split", "-b", str(chunk_bytes), path, chunk_prefix],
                check=True,
                stderr=subprocess.DEVNULL,
            )

        chunk_files = sorted(_glob.glob(f"{chunk_prefix}*"))
        if progress:
            dt_s = time.monotonic() - t_split
            print(
                f"[{label}] split into {len(chunk_files)} chunks in {dt_s:.1f}s",
                file=sys.stderr,
                flush=True,
            )

        # Fix the 1-2 partial lines at each byte-split boundary in-place.
        # Reads only ~4 KB from the head/tail of each chunk — no grep subprocess.
        t_fix = time.monotonic()
        _fix_nt_chunk_boundaries(chunk_files)
        if progress:
            sizes_gb = sum(_os.path.getsize(c) for c in chunk_files) / 1e9
            print(
                f"[{label}] boundary fix done in {time.monotonic() - t_fix:.1f}s"
                f"; {len(chunk_files)} chunks, {sizes_gb:.1f} GB total",
                file=sys.stderr,
                flush=True,
            )

        chunk_format = effective_fmt if effective_fmt in {"nquads", "n-quads"} else "nt"
        return ingest_duckdb_rdf_parallel(
            chunk_files,
            chunk_format,
            connection,
            n_workers=n_workers,
            temp_dir=temp_dir,
            progress=progress,
            delete_inputs_after_use=True,
        )

    finally:
        # shutil.rmtree handles everything: partial chunks written before the glob ran,
        # any chunks not yet consumed by workers, and the directory itself.
        # Worker-consumed chunks are already gone; rmtree ignores missing files.
        if tmp_owned and tmp_dir_path:
            shutil.rmtree(tmp_dir_path, ignore_errors=True)

    if progress:
        elapsed = time.monotonic() - start
        total_q = connection.execute("SELECT count(*) FROM rdf_triples").fetchone()[0]
        print(
            f"[{label}] done  {elapsed:.1f}s",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def ingest(
    path: str,
    rdf_format: Optional[str],
    connection,
    backend: str = "rdflib",
    batch_size: int = 100_000,
    progress: bool = True,
    filter_cmd: Optional[list] = None,
) -> int:
    """Ingest *path* into the ``rdf_triples`` staging table using *backend*.

    *filter_cmd* (duckdb_rdf only) is an optional shell command list piped after
    the decompressor — e.g. ``["grep", "-v", "<[^>]*\\\\[^>]*>"]`` to drop lines
    with backslash-containing IRIs (YAGO/Wikipedia datasets).
    """
    if backend == "oxigraph":
        return ingest_oxigraph(path, rdf_format, connection, batch_size, progress)
    if backend == "rdflib":
        return ingest_rdflib(path, rdf_format, connection, batch_size, progress)
    if backend == "duckdb_rdf":
        return ingest_duckdb_rdf(path, rdf_format, connection, progress, filter_cmd)
    raise ValueError(f"Unknown backend '{backend}'. Choose from: {BACKENDS}")
