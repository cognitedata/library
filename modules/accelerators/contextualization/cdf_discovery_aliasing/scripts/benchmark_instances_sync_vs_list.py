#!/usr/bin/env python3
"""Spike: compare ``instances.list`` vs ``instances.sync`` for a view-shaped filter.

Requires CDF credentials. Builds a minimal graph query for sync with the same view
filter used by ``build_source_view_query_filter``. Use for incremental tuning only —
production view query remains on ``instances.list``.

Example::

    python scripts/benchmark_instances_sync_vs_list.py \\
        --scope-document workflow.local.config.yaml \\
        --canvas-node-id vq_eq \\
        --max-pages 5
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

_MODULE = Path(__file__).resolve().parents[1]
_FUNCS = _MODULE / "functions"
for _p in (_FUNCS, _MODULE):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))


def _view_config_from_canvas(scope: Dict[str, Any], canvas_node_id: str) -> Dict[str, Any]:
    canvas = scope.get("canvas") if isinstance(scope.get("canvas"), dict) else {}
    for node in canvas.get("nodes") or []:
        if not isinstance(node, dict) or str(node.get("id") or "") != canvas_node_id:
            continue
        data = node.get("data") if isinstance(node.get("data"), dict) else {}
        return dict(data.get("config") or {})
    raise KeyError(canvas_node_id)


def _benchmark_list(client: Any, view_id: Any, flt: Any, instance_space: Optional[str], limit: int) -> Dict[str, Any]:
    from cdf_fn_common.incremental_scope import ListInstancesStats, list_all_instances

    stats = ListInstancesStats()
    for _ in list_all_instances(
        client,
        instance_type="node",
        space=instance_space,
        sources=[view_id],
        filter=flt,
        limit_per_page=limit,
        stats_out=stats,
    ):
        pass
    return {
        "api": "instances.list",
        "page_count": stats.page_count,
        "instances": stats.instances_yielded,
        "duration_sec": stats.list_duration_sec,
    }


def _benchmark_sync(client: Any, view_id: Any, flt: Any, limit: int) -> Dict[str, Any]:
    from cognite.client.data_classes.data_modeling.query import NodeResultSetExpression, Query, Select, SourceSelector

    t0 = time.perf_counter()
    query = Query(
        with_={
            "nodes": NodeResultSetExpression(filter=flt, limit=limit),
        },
        select={
            "nodes": Select([SourceSelector(view_id, ["externalId", "name"])]),
        },
        cursors={},
    )
    total = 0
    pages = 0
    while True:
        res = client.data_modeling.instances.sync(query)
        pages += 1
        nodes = getattr(res, "nodes", None) or []
        total += len(nodes)
        cursors = getattr(res, "cursors", None) or {}
        next_cur = cursors.get("nodes") if isinstance(cursors, dict) else None
        if not next_cur:
            break
        query.cursors = dict(cursors)
    return {
        "api": "instances.sync",
        "page_count": pages,
        "instances": total,
        "duration_sec": round(time.perf_counter() - t0, 6),
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope-document", type=Path, default=_MODULE / "workflow.local.config.yaml")
    parser.add_argument("--canvas-node-id", default="vq_eq")
    parser.add_argument("--limit-per-page", type=int, default=1000)
    parser.add_argument("--max-pages", type=int, default=0, help="Stop list after N pages (0=all)")
    args = parser.parse_args(argv)

    import yaml
    from cognite.client import CogniteClient
    from cognite.client.data_classes.data_modeling.ids import ViewId

    from cdf_fn_common.source_view_filter_build import build_source_view_query_filter

    scope = yaml.safe_load(args.scope_document.resolve().read_text(encoding="utf-8")) or {}
    cfg = _view_config_from_canvas(scope, args.canvas_node_id)
    view_id = ViewId(
        space=str(cfg.get("view_space") or "cdf_cdm"),
        external_id=str(cfg.get("view_external_id") or ""),
        version=str(cfg.get("view_version") or "v1"),
    )
    scope_view = {
        "view_space": view_id.space,
        "view_external_id": view_id.external_id,
        "view_version": view_id.version,
        "instance_space": cfg.get("instance_space"),
        "filters": cfg.get("filters") or [],
    }
    flt = build_source_view_query_filter(view_id, scope_view["filters"])
    ins = str(cfg.get("instance_space") or "").strip() or None
    if ins and ins.lower() == "all_spaces":
        ins = None

    client = CogniteClient()
    report = {
        "canvas_node_id": args.canvas_node_id,
        "view": f"{view_id.space}/{view_id.external_id}/{view_id.version}",
        "list": _benchmark_list(client, view_id, flt, ins, args.limit_per_page),
    }
    try:
        report["sync"] = _benchmark_sync(client, view_id, flt, args.limit_per_page)
    except Exception as exc:
        report["sync"] = {"error": f"{type(exc).__name__}: {exc}"}

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
