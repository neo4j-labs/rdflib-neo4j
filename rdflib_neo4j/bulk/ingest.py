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
    "nt": "nt",
    "ntriples": "ntriples",
    "n-triples": "ntriples",
    "nquads": "nquads",
    "n-quads": "nquads",
    # RDF/XML auto-detected from .xml / .rdf extension — no file_type override available.
}

# Extensions that DuckDB rdf extension recognises for RDF/XML auto-detection.
_DUCKDB_RDFXML_EXTENSIONS = {"xml", "rdf"}

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
            p2 = subprocess.Popen(
                filter_cmd, stdin=src_stdout, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            if comp_ext:
                src_stdout.close()  # let upstream receive SIGPIPE if p2 exits
            self._procs.append(p2)
            self._stdout = p2.stdout
        else:
            self._stdout = src_stdout
        self._pos = 0
        self._closed = False

    def read(self, n: int = -1) -> bytes:
        data = self._stdout.read(n) if n >= 0 else self._stdout.read()
        self._pos += len(data)
        return data

    def seek(self, pos: int, whence: int = 0) -> int:
        return self._pos  # forward-only; serd only uses this for EOF detection

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
        # The virtual path carries the RDF extension serd needs (.ttl, .xml …).
        # Reuse a single DecompressingFS per connection — DuckDB only allows one
        # registration per protocol name.
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
            f"[{label}] {total:,} triples  {rate:,.0f}/s  {elapsed:.1f}s",
            file=sys.stderr,
        )
    return total


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
