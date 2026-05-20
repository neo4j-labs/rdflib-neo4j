from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, Optional, Set

from rdflib_neo4j.config.const import (
    DEFAULT_PREFIXES,
    HANDLE_VOCAB_URI_STRATEGY,
)


class AggregationMode(Enum):
    ANY = "ANY"
    FIRST = "FIRST"
    LAST = "LAST"
    ARRAY = "ARRAY"


@dataclass
class BulkImportConfig:
    handle_vocab_uri_strategy: HANDLE_VOCAB_URI_STRATEGY = HANDLE_VOCAB_URI_STRATEGY.IGNORE
    prefixes: Dict[str, str] = field(default_factory=lambda: dict(DEFAULT_PREFIXES))
    custom_mappings: Dict[str, str] = field(default_factory=dict)
    language_projection: bool = False
    language_filter: Optional[str] = None
    aggregation_mode: AggregationMode = AggregationMode.ANY
    generic_labels: Set[str] = field(
        default_factory=lambda: {"Resource", "owl__Thing", "rdfs__Resource", "Thing", "Entity"}
    )

    def all_prefixes(self) -> Dict[str, str]:
        return dict(self.prefixes)

    def namespace_to_prefix(self) -> Dict[str, str]:
        return {namespace: prefix for prefix, namespace in self.all_prefixes().items()}

    def should_keep_language(self, lang: Optional[str]) -> bool:
        return self.language_filter is None or lang is None or lang == self.language_filter

    def with_generic_labels(self, labels: Iterable[str]) -> "BulkImportConfig":
        self.generic_labels.update(labels)
        return self
