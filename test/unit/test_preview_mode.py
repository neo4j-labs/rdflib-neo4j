"""Unit tests for Neo4jStore preview/dry-run mode."""
from unittest.mock import MagicMock, patch

from rdflib_neo4j.Neo4jStore import Neo4jStore
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig


def _make_store(preview: bool) -> Neo4jStore:
    """Create a Neo4jStore with a mocked driver/session (no real connection)."""
    config = Neo4jStoreConfig(
        auth_data={
            "uri": "bolt://localhost",
            "database": "neo4j",
            "user": "neo4j",
            "pwd": "test",
        },
        preview=preview,
    )

    mock_session = MagicMock()
    # Simulate constraint already existing so the CREATE branch is never entered
    mock_session.run.return_value = iter([{"constraint_found": True}])

    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session

    # Patch GraphDatabase.driver so __get_driver() never opens a socket
    with patch("rdflib_neo4j.Neo4jStore.GraphDatabase") as mock_gdb:
        mock_gdb.driver.return_value = mock_driver
        store = Neo4jStore(config=config)

    # After construction rdflib calls open(); re-attach fresh mocks and re-open
    store.driver = mock_driver
    store.session = mock_session
    store._Neo4jStore__open = True
    return store


def _make_preview_store() -> Neo4jStore:
    return _make_store(preview=True)


def _make_normal_store() -> Neo4jStore:
    return _make_store(preview=False)


# ---------------------------------------------------------------------------
# Config-level tests
# ---------------------------------------------------------------------------


def test_config_preview_default_is_false():
    config = Neo4jStoreConfig(
        auth_data={"uri": "bolt://localhost", "database": "neo4j", "user": "neo4j", "pwd": "test"}
    )
    assert config.preview is False


def test_config_preview_true():
    config = Neo4jStoreConfig(
        auth_data={"uri": "bolt://localhost", "database": "neo4j", "user": "neo4j", "pwd": "test"},
        preview=True,
    )
    assert config.preview is True


# ---------------------------------------------------------------------------
# Store initialisation
# ---------------------------------------------------------------------------


def test_store_preview_flag_propagated():
    store = _make_preview_store()
    assert store.preview is True


def test_store_normal_flag_propagated():
    store = _make_normal_store()
    assert store.preview is False


# ---------------------------------------------------------------------------
# get_preview_results in preview mode
# ---------------------------------------------------------------------------


def test_preview_results_initially_empty():
    store = _make_preview_store()
    assert store.get_preview_results() == []


def test_preview_results_collected_after_query():
    store = _make_preview_store()
    # Directly exercise __query_database via name-mangled accessor
    query = "UNWIND $rows AS row MERGE (n:Resource {uri: row.uri})"
    params = {"rows": [{"uri": "http://example.org/s"}]}
    store._Neo4jStore__query_database(query, params)

    results = store.get_preview_results()
    assert len(results) == 1
    assert results[0] == (query, params)


def test_preview_results_contain_str_and_dict():
    store = _make_preview_store()
    store._Neo4jStore__query_database("MATCH (n) RETURN n", {"foo": "bar"})
    q, p = store.get_preview_results()[0]
    assert isinstance(q, str)
    assert isinstance(p, dict)


def test_preview_results_accumulate_multiple_queries():
    store = _make_preview_store()
    store._Neo4jStore__query_database("QUERY_A", {"a": 1})
    store._Neo4jStore__query_database("QUERY_B", {"b": 2})
    assert len(store.get_preview_results()) == 2


def test_get_preview_results_returns_copy():
    """Mutating the returned list should not affect internal state."""
    store = _make_preview_store()
    store._Neo4jStore__query_database("Q", {})
    results = store.get_preview_results()
    results.clear()
    assert len(store.get_preview_results()) == 1


# ---------------------------------------------------------------------------
# session.run NOT called for data queries in preview mode
# ---------------------------------------------------------------------------


def test_session_run_not_called_in_preview_mode():
    store = _make_preview_store()
    # Reset mock call count after setup
    store.session.run.reset_mock()

    store._Neo4jStore__query_database("MERGE (n:Resource {uri: $uri})", {"uri": "http://example.org/x"})

    store.session.run.assert_not_called()


# ---------------------------------------------------------------------------
# Normal mode: session.run IS called
# ---------------------------------------------------------------------------


def test_session_run_called_in_normal_mode():
    store = _make_normal_store()
    store.session.run.reset_mock()

    query = "MERGE (n:Resource {uri: $uri})"
    params = {"uri": "http://example.org/x"}
    store._Neo4jStore__query_database(query, params)

    store.session.run.assert_called_once_with(query, params=params)


# ---------------------------------------------------------------------------
# Normal mode: get_preview_results is always empty
# ---------------------------------------------------------------------------


def test_normal_mode_get_preview_results_empty():
    store = _make_normal_store()
    store.session.run.reset_mock()

    store._Neo4jStore__query_database("MERGE (n:Resource {uri: $uri})", {"uri": "http://example.org/x"})

    assert store.get_preview_results() == []


# ---------------------------------------------------------------------------
# clear_preview_results
# ---------------------------------------------------------------------------


def test_clear_preview_results():
    store = _make_preview_store()
    store._Neo4jStore__query_database("Q1", {})
    store._Neo4jStore__query_database("Q2", {})
    assert len(store.get_preview_results()) == 2

    store.clear_preview_results()
    assert store.get_preview_results() == []


def test_clear_preview_results_idempotent():
    store = _make_preview_store()
    store.clear_preview_results()
    store.clear_preview_results()
    assert store.get_preview_results() == []


# ---------------------------------------------------------------------------
# Constraint-creation skipped in preview mode
# ---------------------------------------------------------------------------


def test_constraint_creation_skipped_in_preview_mode(capsys):
    """When constraint is missing and preview=True, creation should be skipped."""
    store = _make_preview_store()
    # Simulate constraint NOT found
    store.session.run.reset_mock()
    store.session.run.return_value = iter([{"constraint_found": False}])

    store._Neo4jStore__constraint_check(create=True)

    captured = capsys.readouterr()
    assert "Preview mode" in captured.out
    # Ensure the CREATE CONSTRAINT query was NOT sent via session.run
    for call in store.session.run.call_args_list:
        assert "CREATE CONSTRAINT" not in str(call)
