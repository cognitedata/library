"""Paginated ``instances.list`` for DM view query previews and handlers."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from .query_enumeration import resolve_page_size


@dataclass
class ListInstancesStats:
    """Metrics from a paginated ``instances.list`` walk."""

    page_count: int = 0
    instances_yielded: int = 0
    list_duration_sec: float = 0.0
    sort_property: Optional[str] = None
    limit_per_page: int = 0


def dm_node_instance_space(instance: Any) -> str:
    space = getattr(instance, "space", None)
    if space is not None and str(space).strip():
        return str(space).strip()
    dump = instance.dump() if hasattr(instance, "dump") else {}
    if isinstance(dump, dict):
        for key in ("space",):
            v = dump.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
        node = dump.get("node")
        if isinstance(node, dict):
            v = node.get("space")
            if v is not None and str(v).strip():
                return str(v).strip()
    return ""


def node_instance_id_str(instance: Any) -> str:
    space = dm_node_instance_space(instance)
    iid = getattr(instance, "instance_id", None)
    if iid is not None:
        s = str(iid).strip()
        if space:
            return f"{space}:{s}"
        return s
    ext = getattr(instance, "external_id", None)
    if ext is not None:
        return f"{space}:{ext}" if space else str(ext)
    return ""


def node_last_updated_time_ms(instance: Any) -> Optional[int]:
    raw = getattr(instance, "last_updated_time", None)
    if raw is None:
        dump = instance.dump() if hasattr(instance, "dump") else {}
        if isinstance(dump, dict):
            raw = dump.get("lastUpdatedTime")
            if raw is None:
                node = dump.get("node")
                if isinstance(node, dict):
                    raw = node.get("lastUpdatedTime")
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    try:
        if isinstance(raw, str):
            s = raw
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            return int(dt.timestamp() * 1000)
    except Exception:
        pass
    try:
        return int(raw)
    except Exception:
        return None


def _sort_property_label(sort: Any) -> Optional[str]:
    if sort is None:
        return None
    prop = getattr(sort, "property", None)
    if prop is None and isinstance(sort, dict):
        prop = sort.get("property")
    if isinstance(prop, (list, tuple)):
        return ".".join(str(p) for p in prop)
    return str(prop) if prop is not None else repr(sort)


def list_all_instances(
    client: Any,
    *,
    instance_type: str,
    space: Optional[str],
    sources: List[Any],
    filter: Any,
    limit_per_page: int = 1000,
    sort: Any = None,
    logger: Optional[Any] = None,
    progress_context: str = "",
    stats_out: Optional[ListInstancesStats] = None,
) -> Iterable[Any]:
    """Page through instances using the SDK chunk iterator."""
    t0 = time.perf_counter()
    batch_no = 0
    total = 0
    sort_label = _sort_property_label(sort)
    if stats_out is not None:
        stats_out.limit_per_page = limit_per_page
        stats_out.sort_property = sort_label
    page_size = max(1, int(limit_per_page or 1000))
    list_kwargs: Dict[str, Any] = dict(
        chunk_size=page_size,
        instance_type=instance_type,
        space=space,
        sources=sources,
        filter=filter,
        limit=None,
    )
    if sort is not None:
        list_kwargs["sort"] = sort

    instances_api = client.data_modeling.instances
    if not callable(instances_api):
        raise TypeError("client.data_modeling.instances must support chunk iteration")
    page_iter = instances_api(**list_kwargs)

    for batch in page_iter:
        if not batch:
            continue
        batch_no += 1
        n_in_page = 0
        for node in batch:
            n_in_page += 1
            total += 1
            yield node
        if logger is not None and hasattr(logger, "info"):
            ctx = f" {progress_context}" if progress_context else ""
            sort_note = f" sort={sort_label}" if sort_label else ""
            logger.info(
                "instances.list batch %s complete%s: %s instance(s) this page, %s cumulative%s",
                batch_no,
                ctx,
                n_in_page,
                total,
                sort_note,
            )
    if stats_out is not None:
        stats_out.page_count = batch_no
        stats_out.instances_yielded = total
        stats_out.list_duration_sec = round(time.perf_counter() - t0, 6)
    elif logger is not None and hasattr(logger, "info") and batch_no:
        ctx = f" {progress_context}" if progress_context else ""
        logger.info(
            "instances.list finished%s: %s page(s), %s instance(s), %.3fs%s",
            ctx,
            batch_no,
            total,
            time.perf_counter() - t0,
            f" sort={sort_label}" if sort_label else "",
        )


def resolve_list_page_size(cfg: Any, *, default: int = 1000) -> int:
    """Convenience wrapper for handlers that already have task config."""
    return resolve_page_size(cfg, default=default)
