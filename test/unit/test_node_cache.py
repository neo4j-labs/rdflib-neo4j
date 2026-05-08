"""
Unit tests for the in-process node cache in Neo4jStore (issue #66).

These tests do NOT require a running Neo4j instance — they mock the driver/session.

The strategy is:
  - Build a store backed by MagicMock driver/session, bypassing open().
  - Drive triples through add() + commit() and count session.run() calls.
  - A cached node is skipped → one fewer session.run call for that URI's label group.
  - Relationships always increment their own session.run calls.
"""
from unittest.mock import MagicMock
from rdflib import URIRef, Literal
from rdflib.namespace import FOAF

from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.config.const import HANDLE_VOCAB_URI_STRATEGY


def _make_store(node_cache_size=10000):
    """
    Build a Neo4jStore backed by a mock driver/session without a real Neo4j
    instance. We bypass open() so no SHOW CONSTRAINTS roundtrip is needed.
    """
    mock_driver = MagicMock()
    mock_session = MagicMock()
    mock_driver.session.return_value = mock_session

    config = Neo4jStoreConfig(
        custom_prefixes={},
        custom_mappings=[],
        multival_props_names=[],
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
        batching=True,
        batch_size=5000,
        node_cache_size=node_cache_size,
    )
    store = Neo4jStore(config=config, neo4j_driver=mock_driver)
    # Bypass open() (constraint check) by setting internal state directly
    store._Neo4jStore__open = True
    store.session = mock_session
    return store


def _flush(store, *extras):
    """
    Flush all pending buffers.

    We commit nodes (writes node_buffer to session.run) and rels separately.
    After each commit the buffers are cleared, ready for the next pass.
    """
    store.commit(commit_nodes=True)
    store.commit(commit_rels=True)


# ---------------------------------------------------------------------------
# Test 1 – duplicate URI → only ONE node MERGE written to the session
# ---------------------------------------------------------------------------

def test_cached_node_skips_node_buffer():
    """A node URI written a second time must NOT trigger another MERGE call."""
    store = _make_store()
    mock_session = store.session

    donna = URIRef("https://example.org/donna")
    other = URIRef("https://example.org/other1")

    # --- First occurrence of donna ---
    # Switch subjects to force donna into the buffer, then commit.
    store.add((donna, FOAF.name, Literal("Donna")))
    store.add((other, FOAF.name, Literal("Other")))   # flushes donna → buffer
    _flush(store)

    calls_after_first = mock_session.run.call_count  # includes node MERGE for donna

    # --- Second occurrence of donna (should be cached / skipped) ---
    other2 = URIRef("https://example.org/other2")
    store.add((donna, FOAF.age, Literal(30)))
    store.add((other2, FOAF.name, Literal("Other2")))  # flushes donna → skipped by cache
    _flush(store)

    calls_after_second = mock_session.run.call_count

    # Only other2's node MERGE should have been emitted (1 new call).
    # If donna was NOT cached, there would be 2 new calls (donna + other2).
    new_calls = calls_after_second - calls_after_first
    assert new_calls == 1, (
        f"Expected exactly 1 new session.run call (other2 only), "
        f"got {new_calls} — cache did not suppress donna's second MERGE"
    )

    # Verify donna is in the cache
    assert donna in store._node_cache


# ---------------------------------------------------------------------------
# Test 2 – relationships are NOT skipped for cached nodes
# ---------------------------------------------------------------------------

def test_relationships_not_skipped_for_cached_nodes():
    """Even if a node is cached, its outgoing relationships must still be written."""
    store = _make_store()
    mock_session = store.session

    donna = URIRef("https://example.org/donna")
    bob = URIRef("https://example.org/bob")
    carol = URIRef("https://example.org/carol")
    other = URIRef("https://example.org/flush-helper")

    # First: donna→bob relationship
    store.add((donna, FOAF.knows, bob))
    store.add((other, FOAF.name, Literal("Other")))   # flush donna
    _flush(store)

    calls_after_first_rel = mock_session.run.call_count

    # Second: donna→carol relationship; donna is now cached so node MERGE skipped,
    # but the relationship MERGE must still be written.
    store.add((donna, FOAF.knows, carol))
    store.add((other, FOAF.age, Literal(1)))           # flush donna
    _flush(store)

    calls_after_second_rel = mock_session.run.call_count

    # At least one more session.run call must have happened (for the new rel).
    assert calls_after_second_rel > calls_after_first_rel, (
        "No session.run calls detected for the second relationship — "
        "relationships must not be suppressed by the node cache"
    )


# ---------------------------------------------------------------------------
# Test 3 – cache eviction when node_cache_size is exceeded
# ---------------------------------------------------------------------------

def test_cache_eviction():
    """With cache size=2, adding a 3rd distinct URI must evict the oldest."""
    store = _make_store(node_cache_size=2)

    a = URIRef("https://example.org/a")
    b = URIRef("https://example.org/b")
    c = URIRef("https://example.org/c")
    flush = URIRef("https://example.org/flush")

    # Add a: switch to b to flush a → a cached
    store.add((a, FOAF.name, Literal("A")))
    store.add((b, FOAF.name, Literal("B")))   # flushes a → cache: {a}
    # Add b: switch to c to flush b → b cached; cache: {a, b}
    store.add((c, FOAF.name, Literal("C")))   # flushes b → cache: {a, b}
    # Add c: switch to flush helper → c cached; cache overflows to 3 → a evicted
    store.add((flush, FOAF.name, Literal("Flush")))  # flushes c → cache: {b, c}

    # a should have been evicted (it was the oldest); b and c remain
    assert a not in store._node_cache, "Oldest URI 'a' should have been evicted"
    assert b in store._node_cache
    assert c in store._node_cache
    assert len(store._node_cache) == 2


# ---------------------------------------------------------------------------
# Test 4 – node_cache_size=0 disables caching entirely
# ---------------------------------------------------------------------------

def test_cache_size_zero_disables_cache():
    """With node_cache_size=0, no URIs should ever be stored in the cache.

    Both nodes (donna + other2) must appear in the UNWIND batch on the second
    pass, proving that donna was NOT suppressed by a cache hit.
    Nodes with the same labels are batched into one UNWIND query, so we check
    the params list length rather than the session.run call count.
    """
    store = _make_store(node_cache_size=0)
    mock_session = store.session

    donna = URIRef("https://example.org/donna")
    other1 = URIRef("https://example.org/other1")
    other2 = URIRef("https://example.org/other2")

    # First occurrence of donna
    store.add((donna, FOAF.name, Literal("Donna")))
    store.add((other1, FOAF.name, Literal("Other")))   # flush donna
    _flush(store)

    calls_after_first = mock_session.run.call_count

    # Second occurrence of donna — with cache disabled, donna should flow through again
    store.add((donna, FOAF.age, Literal(30)))
    store.add((other2, FOAF.name, Literal("Other2")))  # flush donna
    _flush(store)

    # Inspect the params list of the UNWIND call(s) that appeared in the second pass.
    # Nodes with the same labels are batched into one UNWIND query; donna and other2
    # both have only the Resource label, so they share one call.
    second_pass_calls = mock_session.run.call_args_list[calls_after_first:]
    node_uris_written = []
    for call in second_pass_calls:
        if "params" in call.kwargs:
            node_uris_written.extend(p["uri"] for p in call.kwargs["params"])

    assert donna in node_uris_written, (
        "donna must appear in the second-pass UNWIND params — "
        "cache is disabled (node_cache_size=0) so it must not be suppressed"
    )
    assert other2 in node_uris_written, (
        "other2 must appear in the second-pass UNWIND params"
    )

    # The internal cache dict must remain empty when size=0
    assert len(store._node_cache) == 0, (
        "Cache must remain empty when node_cache_size=0"
    )
