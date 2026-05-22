#!/usr/bin/env python3
"""Benchmark ``instances.list`` for a view-query filter (baseline metrics).

Requires CDF credentials in the environment. Compares default list vs incremental sort
(``node.lastUpdatedTime`` ascending) when ``--incremental`` is set.

Example::

    python scripts/benchmark_view_query_list.py \\
        --scope-document workflow.local.config.yaml \\
        --canvas-node-id vq_eq
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


def _load_scope_document(path: Path) -> Dict[str, Any]:
    import yaml

    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _view_config_from_canvas(scope: Dict[str, Any], canvas_node_id: str) -> Dict[str, Any]:
    canvas = scope.get("canvas") if isinstance(scope.get("canvas"), dict) else {}
    for node in canvas.get("nodes") or []:
        if not isinstance(node, dict):
            continue
        if str(node.get("id") or "") != canvas_node_id:
            continue
        if str(node.get("kind") or "") != "query_view":
            raise ValueError(f"Node {canvas_node_id!r} is not query_view")
        data = node.get("data") if isinstance(node.get("data"), dict) else {}
        cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
        return dict(cfg)
    raise KeyError(f"No canvas node {canvas_node_id!r}")


def _run_list_benchmark(
    client: Any,
    *,
    view_cfg: Dict[str, Any],
    incremental: bool,
    use_incremental_sort: bool,
    max_pages: Optional[int],
) -> Dict[str, Any]:
    from cognite.client.data_classes.data_modeling.ids import ViewId

    from cdf_fn_common.incremental_scope import ListInstancesStats, list_all_instances, view_query_list_sort
    from cdf_fn_common.source_view_filter_build import build_source_view_query_filter

    view_space = str(view_cfg.get("view_space") or "cdf_cdm").strip()
    view_external_id = str(view_cfg.get("view_external_id") or "").strip()
    view_version = str(view_cfg.get("view_version") or "v1").strip()
    instance_space = str(view_cfg.get("instance_space") or "").strip() or None
    batch_size = int(view_cfg.get("batch_size") or view_cfg.get("limit") or 1000)
    limit_per_page = min(1000, batch_size) if batch_size > 0 else 1000

    view_id = ViewId(space=view_space, external_id=view_external_id, version=view_version)
    scope_view = {
        "view_space": view_space,
        "view_external_id": view_external_id,
        "view_version": view_version,
        "instance_space": instance_space,
        "filters": view_cfg.get("filters") or [],
    }
    flt = build_source_view_query_filter(view_id, scope_view.get("filters") or [])

    list_space = None
    if instance_space and instance_space.lower() != "all_spaces":
        list_space = instance_space

    sort = view_query_list_sort(incremental=incremental and use_incremental_sort)
    stats = ListInstancesStats()
    for _ in list_all_instances(
        client,
        instance_type="node",
        space=list_space,
        sources=[view_id],
        filter=flt,
        limit_per_page=limit_per_page,
        sort=sort,
        stats_out=stats,
    ):
        if max_pages is not None and stats.page_count >= max_pages:
            break
    return {
        "incremental_mode": incremental,
        "use_incremental_sort": use_incremental_sort,
        "list_sort": stats.sort_property,
        "page_count": stats.page_count,
        "instances_listed": stats.instances_yielded,
        "list_duration_sec": stats.list_duration_sec,
        "limit_per_page": limit_per_page,
        "view": f"{view_space}/{view_external_id}/{view_version}",
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope-document",
        type=Path,
        default=_MODULE / "workflow.local.config.yaml",
    )
    parser.add_argument("--canvas-node-id", default="vq_eq")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Also benchmark with view_query_list_sort (lastUpdatedTime asc)",
    )
    parser.add_argument("--max-pages", type=int, default=0, help="Stop after N pages (0 = all)")
    args = parser.parse_args(argv)

    from cognite.client import CogniteClient

    scope = _load_scope_document(args.scope_document.resolve())
    view_cfg = _view_config_from_canvas(scope, args.canvas_node_id.strip())
    client = CogniteClient()
    max_pages = args.max_pages if args.max_pages > 0 else None

    runs: List[Dict[str, Any]] = []
    runs.append(
        _run_list_benchmark(
            client,
            view_cfg=view_cfg,
            incremental=False,
            use_incremental_sort=False,
            max_pages=max_pages,
        )
    )
    if args.incremental:
        runs.append(
            _run_list_benchmark(
                client,
                view_cfg=view_cfg,
                incremental=True,
                use_incremental_sort=False,
                max_pages=max_pages,
            )
        )
        runs.append(
            _run_list_benchmark(
                client,
                view_cfg=view_cfg,
                incremental=True,
                use_incremental_sort=True,
                max_pages=max_pages,
            )
        )

    print(json.dumps({"canvas_node_id": args.canvas_node_id, "runs": runs}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
