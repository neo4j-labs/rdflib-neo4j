"""Maps RDF URIs to Neo4j storage names using Neo4jStoreConfig conventions."""
from __future__ import annotations

from rdflib import URIRef

from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.utils import handle_vocab_uri


class MappingContext:
    """Resolves RDF predicate/type URIs to Neo4j property, label and rel-type names.

    Mirrors the logic in ``Neo4jStore.__create_current_subject`` — uses the
    reversed prefix dict (namespace-URI → prefix-short-name) so that
    ``handle_vocab_uri`` with the SHORTEN strategy produces ``prefix__local``
    names consistent with what the store writes.
    """

    def __init__(self, config: Neo4jStoreConfig) -> None:
        self.config = config
        # handle_vocab_uri_shorten expects {namespace_uri_str: prefix_short_name}
        self._prefixes: dict[str, str] = {
            str(v): k for k, v in config.get_prefixes().items()
        }

    def resolve(self, uri: URIRef) -> str:
        """Map a predicate or rdf:type object URI to its Neo4j storage name."""
        return handle_vocab_uri(
            self.config.custom_mappings,
            uri,
            self._prefixes,
            self.config.handle_vocab_uri_strategy,
        )

    @property
    def uri_key(self) -> str:
        """Primary-key property name on every Resource node."""
        return "uri"
