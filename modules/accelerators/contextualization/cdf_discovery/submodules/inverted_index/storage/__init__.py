"""Storage adapters for inverted index entries."""

from __future__ import annotations

from inverted_index.storage.dm_adapter import DmStorageAdapter
from inverted_index.storage.memory_adapter import MemoryStorageAdapter
from inverted_index.storage.raw_adapter import RawStorageAdapter

__all__ = ["DmStorageAdapter", "MemoryStorageAdapter", "RawStorageAdapter"]


def get_storage_adapter(storage_config: dict, client=None):
    """Factory for the configured storage backend."""
    backend = (storage_config or {}).get("backend", "raw")
    if backend in ("memory",):
        return MemoryStorageAdapter(storage_config)
    if backend == "raw":
        return RawStorageAdapter(storage_config, client)
    return DmStorageAdapter(storage_config, client)


def adapter_local_registry(adapter: object, client) -> dict | None:
    """Return in-memory registry only for offline (no CDF client) runs."""
    if client is not None:
        return None
    return getattr(adapter, "_local_registry", None)


def adapter_local_cache(adapter: object, client) -> dict | None:
    """Return in-memory partition cache only for offline (no CDF client) runs."""
    if client is not None:
        return None
    return getattr(adapter, "_local_partitions", None)
