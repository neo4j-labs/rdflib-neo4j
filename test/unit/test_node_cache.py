"""Unit tests for BoundedNodeCache (issue #66)."""

from rdflib_neo4j.cache import BoundedNodeCache


class TestBoundedNodeCache:
    def test_contains_returns_false_for_unseen_uri(self):
        cache = BoundedNodeCache(max_size=10)
        assert not cache.contains("http://example.org/A")

    def test_contains_returns_true_after_add(self):
        cache = BoundedNodeCache(max_size=10)
        cache.add("http://example.org/A")
        assert cache.contains("http://example.org/A")

    def test_evicts_lru_when_full(self):
        cache = BoundedNodeCache(max_size=3)
        cache.add("http://example.org/A")
        cache.add("http://example.org/B")
        cache.add("http://example.org/C")
        # Access A to make it MRU, then add D — B should be evicted
        cache.contains("http://example.org/A")
        cache.add("http://example.org/D")
        assert not cache.contains("http://example.org/B")
        assert cache.contains("http://example.org/A")
        assert cache.contains("http://example.org/C")
        assert cache.contains("http://example.org/D")

    def test_clear_empties_cache(self):
        cache = BoundedNodeCache(max_size=10)
        cache.add("http://example.org/A")
        cache.clear()
        assert not cache.contains("http://example.org/A")

    def test_disabled_cache_never_contains(self):
        cache = BoundedNodeCache(max_size=0)
        cache.add("http://example.org/A")
        assert not cache.contains("http://example.org/A")

    def test_disabled_cache_is_falsy(self):
        assert not BoundedNodeCache(max_size=0)

    def test_active_cache_is_truthy(self):
        assert BoundedNodeCache(max_size=10)

    def test_duplicate_add_does_not_grow_beyond_max(self):
        cache = BoundedNodeCache(max_size=3)
        for _ in range(5):
            cache.add("http://example.org/A")
        assert len(cache._cache) == 1


class TestNeo4jStoreConfigNodeCacheSize:
    def test_default_node_cache_size(self):
        from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
        config = Neo4jStoreConfig()
        assert config.node_cache_size == 10_000

    def test_custom_node_cache_size(self):
        from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
        config = Neo4jStoreConfig(node_cache_size=500)
        assert config.node_cache_size == 500

    def test_disabled_node_cache_size(self):
        from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
        config = Neo4jStoreConfig(node_cache_size=0)
        assert config.node_cache_size == 0
