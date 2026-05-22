"""Object Explorer tree — node IDs and lazy children."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote

from ui.server import cdf_browse, explorer_config

TreeNodeOut = Dict[str, Any]

# Data — RAW, Data Modeling, Classic, Transformations.
_DATA_BRANCHES: Tuple[Tuple[str, str, str], ...] = (
    ("raw", "RAW", "raw"),
    ("dm", "Data Modeling", "dm"),
    ("classic", "Classic", "classic"),
    ("tx", "Transformations", "transformations"),
)

# Orchestration — workflows, pipelines, and functions.
_ORCHESTRATION_BRANCHES: Tuple[Tuple[str, str, str], ...] = (
    ("wf", "Workflows", "workflows"),
    ("ep", "Pipelines", "pipelines"),
    ("fn", "Functions", "functions"),
)

def encode_segment(seg: str) -> str:
    return quote(seg, safe="")


def decode_segment(seg: str) -> str:
    return unquote(seg)


def parse_node_id(node_id: str) -> tuple[str, List[str]]:
    """Return (kind, segments). ``node_id`` uses ``kind`` or ``kind:seg1:seg2``."""
    raw = (node_id or "connection").strip()
    if not raw:
        raw = "connection"
    parts = raw.split(":")
    kind = parts[0]
    segs = [decode_segment(p) for p in parts[1:]]
    return kind, segs


def _sort_nodes(nodes: List[TreeNodeOut], *, starred_ids: Optional[List[str]] = None) -> List[TreeNodeOut]:
    """Starred nodes first (config order), then case-insensitive label."""
    stars = starred_ids if starred_ids is not None else explorer_config.get_starred_node_ids()
    star_rank = {nid: i for i, nid in enumerate(stars)}

    def sort_key(n: TreeNodeOut) -> tuple[int, int, str]:
        nid = str(n.get("id") or "")
        if nid in star_rank:
            return (0, star_rank[nid], "")
        return (1, 0, str(n.get("label") or "").casefold())

    sorted_nodes = sorted(nodes, key=sort_key)
    star_set = set(stars)
    for n in sorted_nodes:
        if str(n.get("id") or "") in star_set:
            n["starred"] = True
    return sorted_nodes


def _node(
    *,
    id: str,
    label: str,
    kind: str,
    has_children: bool,
    open_target: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> TreeNodeOut:
    out: TreeNodeOut = {
        "id": id,
        "label": label,
        "kind": kind,
        "has_children": has_children,
    }
    if open_target:
        out["open_target"] = open_target
    if meta:
        out["meta"] = meta
    return out


def _data_model_label(row: Dict[str, str]) -> str:
    name = (row.get("name") or "").strip()
    base = f"{row['external_id']} ({row['version']})"
    return f"{name} — {base}" if name else base


def _parse_model_path(segs: List[str]) -> Optional[Tuple[str, str, str]]:
    """``model:{space}:{ext}:{ver}`` → (space, external_id, version)."""
    if len(segs) == 4 and segs[0] == "model":
        return segs[1], segs[2], segs[3]
    return None


def _model_node_id(space: str, model_ext: str, model_ver: str) -> str:
    return (
        f"dm:model:{encode_segment(space)}:"
        f"{encode_segment(model_ext)}:{encode_segment(model_ver)}"
    )


def _dm_data_model_nodes(client: Any) -> List[TreeNodeOut]:
    models = cdf_browse.dm_list_all_data_models(client, limit=500)
    return _sort_nodes(
        [
            _node(
                id=_model_node_id(m["space"], m["external_id"], m["version"]),
                label=_data_model_label(m),
                kind="dm_data_model",
                has_children=True,
                meta={**m},
            )
            for m in models
        ]
    )


def _dm_view_nodes_under_model(
    client: Any,
    *,
    space: str,
    model_ext: str,
    model_ver: str,
) -> List[TreeNodeOut]:
    base = _model_node_id(space, model_ext, model_ver)
    views = cdf_browse.dm_list_views_for_data_model(
        client, space=space, external_id=model_ext, version=model_ver
    )
    out: List[TreeNodeOut] = []
    for v in views:
        ve = encode_segment(v["external_id"])
        vv = encode_segment(v["version"])
        vs = encode_segment(v["space"])
        out.append(
            _node(
                id=f"{base}:view:{vs}:{ve}:{vv}",
                label=f"{v['external_id']} ({v['version']})",
                kind="dm_view",
                has_children=False,
                open_target={
                    "type": "dm_instances",
                    "view_space": v["space"],
                    "view_external_id": v["external_id"],
                    "view_version": v["version"],
                },
                meta={
                    **v,
                    "data_model_space": space,
                    "data_model_external_id": model_ext,
                    "data_model_version": model_ver,
                },
            )
        )
    return _sort_nodes(out)


def _raw_database_nodes(client: Any) -> List[TreeNodeOut]:
    dbs = cdf_browse.raw_list_databases(client, limit=200)
    return _sort_nodes(
        [
            _node(
                id=f"raw:db:{encode_segment(db)}",
                label=db,
                kind="raw_database",
                has_children=True,
                meta={"database": db},
            )
            for db in dbs
        ]
    )


def list_children(client: Any, node_id: str) -> List[TreeNodeOut]:
    kind, segs = parse_node_id(node_id)

    if kind == "connection":
        info = cdf_browse.connection_info(client)
        proj = info.get("project") or "CDF"
        return [
            _node(
                id="sq",
                label="Saved Queries",
                kind="folder",
                has_children=True,
                meta={"domain": "saved_queries"},
            ),
            _node(
                id="data",
                label="Data",
                kind="folder",
                has_children=True,
                meta={"domain": "data"},
            ),
            _node(
                id="orch",
                label="Orchestration",
                kind="folder",
                has_children=True,
                meta={"domain": "orchestration"},
            ),
            _node(
                id="gov",
                label="Governance",
                kind="folder",
                has_children=True,
                meta={"domain": "governance"},
            ),
            _node(
                id="connection:info",
                label=f"Project: {proj}",
                kind="connection",
                has_children=False,
                meta=info,
            ),
        ]

    if kind == "sq" and not segs:
        return _sort_nodes(
            [
                _node(
                    id=f"sq:item:{encode_segment(q['id'])}",
                    label=q["name"],
                    kind="saved_query",
                    has_children=False,
                    meta={
                        "saved_query_id": q["id"],
                        "name": q["name"],
                        "query": q["query"],
                        "limit": q.get("limit", 100),
                        "convert_to_string": q.get("convert_to_string", True),
                    },
                )
                for q in explorer_config.get_saved_queries()
            ]
        )

    if kind == "data" and not segs:
        return [
            _node(
                id=branch_id,
                label=label,
                kind="folder",
                has_children=True,
                meta={"domain": domain},
            )
            for branch_id, label, domain in _DATA_BRANCHES
        ]

    if kind == "orch" and not segs:
        return [
            _node(
                id=branch_id,
                label=label,
                kind="folder",
                has_children=True,
                meta={"domain": domain},
            )
            for branch_id, label, domain in _ORCHESTRATION_BRANCHES
        ]

    if kind == "classic":
        return _sort_nodes(
            [
                _node(
                    id=f"classic:{rid}",
                    label=label,
                    kind="classic_resource",
                    has_children=False,
                    open_target={"type": "classic_list", "resource_type": rid},
                    meta={"resource_type": rid},
                )
                for rid, label in cdf_browse.CLASSIC_RESOURCE_BRANCHES
            ]
        )

    if kind == "dm" and not segs:
        return _dm_data_model_nodes(client)

    parsed = _parse_model_path(segs)
    if kind == "dm" and parsed is not None:
        space, model_ext, model_ver = parsed
        if len(segs) == 4:
            return _dm_view_nodes_under_model(
                client, space=space, model_ext=model_ext, model_ver=model_ver
            )
        if len(segs) >= 7 and segs[4] == "view":
            return []

    if kind == "raw" and not segs:
        return _raw_database_nodes(client)

    if kind == "raw" and len(segs) >= 4 and segs[0] == "db" and segs[2] == "table":
        return []

    if kind == "tx" and not segs:
        return _sort_nodes(
            [
                _node(
                    id=f"tx:item:{encode_segment(str(t['id']))}",
                    label=t["label"],
                    kind="transformation",
                    has_children=False,
                    meta=t,
                )
                for t in cdf_browse.list_transformations(client, limit=500)
            ]
        )

    if kind == "wf" and not segs:
        return _sort_nodes(
            [
                _node(
                    id=f"wf:item:{encode_segment(str(w['external_id'] or w['label']))}",
                    label=w["label"],
                    kind="workflow",
                    has_children=False,
                    meta=w,
                )
                for w in cdf_browse.list_workflows(client, limit=500)
            ]
        )

    if kind == "fn" and not segs:
        return _sort_nodes(
            [
                _node(
                    id=f"fn:item:{encode_segment(str(f['id']))}",
                    label=f["label"],
                    kind="function",
                    has_children=False,
                    meta=f,
                )
                for f in cdf_browse.list_functions(client, limit=500)
            ]
        )

    if kind == "ep" and not segs:
        return _sort_nodes(
            [
                _node(
                    id=f"ep:item:{encode_segment(str(p['external_id'] or p['label']))}",
                    label=p["label"],
                    kind="extraction_pipeline",
                    has_children=False,
                    meta=p,
                )
                for p in cdf_browse.list_extraction_pipelines(client, limit=500)
            ]
        )

    if kind == "gov" and not segs:
        return [
            _node(
                id="gov:spaces",
                label="Spaces",
                kind="folder",
                has_children=True,
                meta={"domain": "governance_spaces"},
            ),
            _node(
                id="gov:groups",
                label="Groups",
                kind="folder",
                has_children=True,
                meta={"domain": "governance_groups"},
            ),
        ]

    if kind == "gov" and len(segs) == 1 and segs[0] == "spaces":
        return _sort_nodes(
            [
                _node(
                    id=f"gov:space:{encode_segment(s['space'])}",
                    label=s["label"],
                    kind="gov_space",
                    has_children=False,
                    meta=s,
                )
                for s in cdf_browse.list_governance_spaces(client, limit=2000, include_global=True)
            ]
        )

    if kind == "gov" and len(segs) == 1 and segs[0] == "groups":
        return _sort_nodes(
            [
                _node(
                    id=f"gov:group:{encode_segment(str(g['id']))}",
                    label=g["label"],
                    kind="gov_group",
                    has_children=False,
                    meta=g,
                )
                for g in cdf_browse.list_security_groups(client, limit=2000)
            ]
        )

    if kind == "raw" and len(segs) == 2 and segs[0] == "db":
        database = segs[1]
        tables = cdf_browse.raw_list_tables(client, database=database, limit=500)
        dbe = encode_segment(database)
        return _sort_nodes(
            [
                _node(
                    id=f"raw:db:{dbe}:table:{encode_segment(t['name'])}",
                    label=t["name"],
                    kind="raw_table",
                    has_children=False,
                    open_target={
                        "type": "raw_rows",
                        "database": database,
                        "table": t["name"],
                    },
                    meta={"database": database, "table": t["name"], "row_count": t.get("row_count")},
                )
                for t in tables
            ]
        )

    return []
