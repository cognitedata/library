"""Local DAG parallelism: worker limits and thread-safe CDF client access."""

from __future__ import annotations

import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any, Callable, Mapping, MutableMapping, TypeVar

from threading import Lock

T = TypeVar("T")

_DEFAULT_MAX_WORKERS = 4


def resolve_max_workers(
    *,
    layer_size: int,
    override: int | None = None,
    configuration: Mapping[str, Any] | None = None,
) -> int:
    """
    Resolve pool size for a DAG layer or fan-out batch group.

    Precedence: explicit *override* → ``configuration.parameters.local_max_workers`` →
    env ``KEA_LOCAL_MAX_WORKERS`` → ``min(4, layer_size)``.
    ``1`` forces serial execution; ``0`` means auto.
    """
    size = max(1, int(layer_size))
    raw: int | None = override
    if raw is None and isinstance(configuration, Mapping):
        params = configuration.get("parameters")
        if isinstance(params, Mapping):
            pval = params.get("local_max_workers")
            if pval is not None and str(pval).strip() != "":
                try:
                    raw = int(pval)
                except (TypeError, ValueError):
                    raw = None
    if raw is None:
        env = (os.environ.get("KEA_LOCAL_MAX_WORKERS") or "").strip()
        if env:
            try:
                raw = int(env)
            except ValueError:
                raw = None
    if raw is None:
        return min(_DEFAULT_MAX_WORKERS, size)
    if raw <= 0:
        return min(_DEFAULT_MAX_WORKERS, size)
    if raw == 1:
        return 1
    return min(raw, size)


def locked_cognite_call(lock: Lock, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """Invoke *fn* while holding *lock* (shared Cognite client safety)."""
    with lock:
        return fn(*args, **kwargs)


class LockedCogniteClient:
    """Proxy that serializes attribute access on a Cognite client."""

    def __init__(self, client: Any, lock: Lock) -> None:
        self._client = client
        self._lock = lock

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._client, name)

        if not callable(attr):
            return attr

        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            with self._lock:
                return attr(*args, **kwargs)

        return _wrapped


def run_parallel(
    items: list[T],
    worker_fn: Callable[[T], Any],
    *,
    max_workers: int,
) -> list[Any]:
    """Run *worker_fn* over *items*; raise the first worker exception."""
    if max_workers <= 1 or len(items) <= 1:
        return [worker_fn(item) for item in items]
    results: list[Any] = [None] * len(items)
    index_by_item = {id(item): i for i, item in enumerate(items)}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map: dict[Future[Any], int] = {}
        for item in items:
            fut = pool.submit(worker_fn, item)
            future_map[fut] = index_by_item[id(item)]
        try:
            for fut in as_completed(future_map):
                idx = future_map[fut]
                results[idx] = fut.result()
        except Exception:
            for fut in future_map:
                fut.cancel()
            raise
    return results


def merge_shared_task_result(
    shared_data: MutableMapping[str, Any],
    shared_lock: Lock,
    *,
    task_id: str,
    summary: Mapping[str, Any],
    data: Mapping[str, Any],
    in_memory: bool,
) -> None:
    """Apply post-task handoff into *shared_data* (caller holds layer barrier)."""
    with shared_lock:
        if in_memory and "_predecessor_rows" in data:
            buffers = shared_data.get("etl_task_row_buffers")
            if not isinstance(buffers, dict):
                buffers = {}
                shared_data["etl_task_row_buffers"] = buffers
            buffers[task_id] = list(data.get("_predecessor_rows") or [])
        elif in_memory and "_predecessor_index_rows" in data:
            shared_data["_predecessor_index_rows"] = list(data.get("_predecessor_index_rows") or [])
            buffers = shared_data.get("etl_task_row_buffers")
            if not isinstance(buffers, dict):
                buffers = {}
                shared_data["etl_task_row_buffers"] = buffers
            buffers[task_id] = list(data.get("_predecessor_index_rows") or [])
        if data.get("run_id"):
            shared_data["run_id"] = data["run_id"]
