"""In-process node cache to reduce redundant MERGE round trips on large imports."""
from collections import OrderedDict


class BoundedNodeCache:
    """LRU cache of subject URIs seen in the current import session.

    Tracks URIs that have already been flushed to Neo4j so that repeated
    references to the same subject do not generate redundant MERGE parameters
    in subsequent batches.  Size is bounded to cap memory use; the default
    of 10,000 entries costs roughly 2 MB and covers the vast majority of
    real-world ontology imports.

    Set ``max_size=0`` to disable the cache entirely (current behaviour).
    """

    def __init__(self, max_size: int = 10_000) -> None:
        self._cache: OrderedDict[str, bool] = OrderedDict()
        self._max_size = max_size

    def __bool__(self) -> bool:
        """Return True when the cache is active (max_size > 0)."""
        return self._max_size > 0

    def contains(self, uri: str) -> bool:
        """Return True if *uri* is already in the cache; promote it to MRU."""
        if not self._max_size:
            return False
        if uri in self._cache:
            self._cache.move_to_end(uri)
            return True
        return False

    def add(self, uri: str) -> None:
        """Record *uri* as seen; evict the LRU entry when the cache is full."""
        if not self._max_size:
            return
        if uri in self._cache:
            self._cache.move_to_end(uri)
        else:
            self._cache[uri] = True
            if len(self._cache) > self._max_size:
                self._cache.popitem(last=False)

    def clear(self) -> None:
        """Reset the cache (called between import sessions)."""
        self._cache.clear()
