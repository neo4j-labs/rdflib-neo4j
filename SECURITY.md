# Security Policy

`rdflib-neo4j` is a Neo4j Labs community project (not a supported Neo4j product).
It is an [RDFLib](https://github.com/RDFLib/rdflib) `Store` plugin that persists
RDF triples into Neo4j. RDFLib is a **direct dependency** (declared in
`setup.py` / `requirements.txt`); it is not vendored or bundled into this package.

## Reporting a vulnerability

Please open a [GitHub issue](https://github.com/neo4j-labs/rdflib-neo4j/issues)
or, for sensitive reports, email the maintainers. Because this is a Labs project,
there is no formal SLA, but reports are reviewed on a best-effort basis.

## Dependency vulnerability assessment (VEX)

The statements below describe whether known third-party advisories against the
`rdflib` dependency are *exploitable through `rdflib-neo4j`*. This is a
vendor applicability statement, not a claim that the upstream advisory is invalid.

> Note on databases: As of this writing, the open vulnerability databases
> ([OSV](https://osv.dev/), GitHub Advisory / PyPA Advisory DB) report **no
> advisories** for the `rdflib` package on PyPI. The two findings below originate
> from Sonatype's proprietary OSS Index.

### CVE-2019-7653 — Not affected

* **Severity (as reported):** Critical, CVSS 9.8
* **Status:** `not_affected` — *vulnerable code not present* and *vulnerable
  component never invoked*.
* **Rationale:** This CVE describes the Debian `python-rdflib-tools` 4.2.2-1
  package, whose CLI wrappers loaded Python modules from the current working
  directory via `python -m` (demonstrated with `rdf2dot`). It is tied to
  **RDFLib 4.2.2** and the Debian packaging scripts. `rdflib-neo4j` requires
  `rdflib >= 7.1.1`; the issue is not present in the RDFLib 7.x PyPI distribution,
  and `rdflib-neo4j` never invokes the `rdf2dot` CLI tool. A scanner flagging
  this against rdflib 7.x is matching an over-broad CPE range; it is a false
  positive for this project.

### sonatype-2021-4223 — Not affected in normal usage

* **Severity (as reported):** Medium, CVSS 6.4 (SSRF)
* **Status:** `not_affected` — *vulnerable code not in the execution path* for
  normal `rdflib-neo4j` operation.
* **Rationale:** This is a Server-Side Request Forgery class issue inherent to
  RDFLib's JSON-LD parser: parsing a JSON-LD document causes any remote
  `@context` URL to be dereferenced (RDFLib 7.6.0 still does this via
  `plugins/shared/jsonld/context.py::_fetch_context`). **There is no RDFLib
  release that removes this behavior** — it is JSON-LD spec behavior, so no
  version upgrade clears this finding.
  `rdflib-neo4j` is a write-path `Store` backend. The library itself never parses
  JSON-LD and never fetches remote contexts. The typical ingestion path is
  `graph.parse(source, format="ttl")` (Turtle / RDF-XML / N-Triples), which does
  not invoke the JSON-LD context loader.
* **User guidance:** The JSON-LD path is only reached if **you** explicitly call
  `graph.parse(..., format="json-ld")` (or let the format be auto-detected) on
  **untrusted** input. If you do, treat the input as untrusted and control
  context resolution — pre-expand the document with a restricted document loader,
  or strip/allowlist remote `@context` URLs before parsing.

## Keeping the dependency current

`rdflib-neo4j` tracks current RDFLib releases. `requirements.txt` is kept on a
recent rdflib version and `setup.py` declares a `>= 7.1.1` floor so installs pick
up upstream security and bug fixes.
