from typing import Optional

from rdflib import URIRef

from rdflib_neo4j.bulk.config import BulkImportConfig
from rdflib_neo4j.utils import handle_vocab_uri


def mapped_term(term: str, config: BulkImportConfig) -> str:
    result = handle_vocab_uri(
        mappings={URIRef(key): value for key, value in config.custom_mappings.items()},
        predicate=URIRef(term),
        prefixes=config.namespace_to_prefix(),
        strategy=config.handle_vocab_uri_strategy,
    )
    return str(result)


def projected_property_name(property_name: str, lang: Optional[str], config: BulkImportConfig) -> Optional[str]:
    if not config.should_keep_language(lang):
        return None
    if config.language_projection and lang:
        return f"{property_name}_{lang.replace('-', '_')}"
    return property_name


def choose_primary_label(labels, config: BulkImportConfig) -> str:
    candidates = sorted(label for label in labels if label not in config.generic_labels)
    return candidates[0] if candidates else "Resource"
