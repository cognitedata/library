#!/usr/bin/env python3
"""Audit view-query canvas nodes and recommend DM container indexes for list performance.

Reads ``canvas`` ``query_view`` nodes from a scope document and prints filter properties
plus index recommendations (BTree cursorable; composite order matches filter usage).

Example::

    python scripts/audit_view_query_dm_indexes.py --scope-document workflow.local.config.yaml
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

_MODULE = Path(__file__).resolve().parents[1]


def _collect_query_view_nodes(scope: Dict[str, Any]) -> List[Dict[str, Any]]:
    canvas = scope.get("canvas") if isinstance(scope.get("canvas"), dict) else {}
    out: List[Dict[str, Any]] = []
    for node in canvas.get("nodes") or []:
        if not isinstance(node, dict):
            continue
        if str(node.get("kind") or "") != "query_view":
            continue
        data = node.get("data") if isinstance(node.get("data"), dict) else {}
        cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
        filters = cfg.get("filters") if isinstance(cfg.get("filters"), list) else []
        filter_props: List[Dict[str, str]] = []
        for item in filters:
            if isinstance(item, dict) and item.get("target_property"):
                filter_props.append(
                    {
                        "target_property": str(item.get("target_property")),
                        "operator": str(item.get("operator") or "EQUALS"),
                        "property_scope": str(item.get("property_scope") or "view"),
                    }
                )
        incremental = bool(cfg.get("incremental_change_processing"))
        out.append(
            {
                "canvas_node_id": str(node.get("id") or ""),
                "label": str(data.get("label") or cfg.get("description") or ""),
                "view": f"{cfg.get('view_space')}/{cfg.get('view_external_id')}/{cfg.get('view_version')}",
                "incremental_change_processing": incremental,
                "filters": filter_props,
                "recommended_indexes": _recommended_indexes(filter_props, incremental=incremental),
            }
        )
    return out


def _recommended_indexes(
    filter_props: List[Dict[str, str]], *, incremental: bool
) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    view_props = [
        p["target_property"]
        for p in filter_props
        if p.get("property_scope", "view") == "view"
    ]
    if view_props:
        recs.append(
            {
                "indexType": "btree",
                "cursorable": True,
                "properties": view_props,
                "note": "Composite order should match equality filter order in queries.",
            }
        )
    if incremental:
        recs.append(
            {
                "indexType": "btree",
                "cursorable": True,
                "properties": ["(node.lastUpdatedTime via list filter)"],
                "note": "Discovery filters incremental lists by node.lastUpdatedTime (Range); "
                "ensure cursorable BTree on underlying containers or use CDF sync API for changes.",
            }
        )
    if not recs:
        recs.append(
            {
                "indexType": "btree",
                "cursorable": True,
                "properties": [],
                "note": "HasData-only listing: default list sort uses internal id; add indexes on "
                "properties you filter or sort by (see DM performance /list pitfalls).",
            }
        )
    return recs


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope-document",
        type=Path,
        default=_MODULE / "workflow.local.config.yaml",
    )
    args = parser.parse_args(argv)

    import yaml

    scope = yaml.safe_load(args.scope_document.resolve().read_text(encoding="utf-8")) or {}
    report = {
        "scope_document": str(args.scope_document),
        "query_view_nodes": _collect_query_view_nodes(scope),
        "guidance": {
            "do_not_use_instances_search_for_bulk_listing": True,
            "search_max_results": 1000,
            "list_api": "instances.list with cursor pagination",
            "incremental_list_sort": "none (API default; node.lastUpdatedTime is filter-only)",
        },
    }
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
