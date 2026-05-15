"""Shared utilities for the DuckDB bulk import pipeline."""

try:
    import psutil as _psutil
except ImportError:
    _psutil = None


def free_mem_gb() -> float:
    """Return available system memory in GiB, or -1 if psutil is unavailable."""
    if _psutil is None:
        return -1.0
    return _psutil.virtual_memory().available / (1024 ** 3)


def mem_stat() -> str:
    """Short string like '  mem:32.1g' for progress lines; empty if unavailable."""
    gb = free_mem_gb()
    if gb < 0:
        return ""
    return f"  mem:{gb:.1f}g"
