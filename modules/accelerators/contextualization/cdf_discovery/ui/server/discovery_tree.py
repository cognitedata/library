"""Discovery object tree — node IDs and lazy children."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote

from ui.server import cdf_browse, discovery_config, governance_declared
from ui.server.tree_node_ids import (
    DATA_ROOT,
    DATA_SAVED_QUERIES,
    FUSION_DM_ROOT,
    FUSION_ADMIN,
    FUSION_GROUPS,
    FUSION_INTEGRATION_ROOT,
    FUSION_ROOT,
    FUSION_SPACES,
    CONNECTION_INFO,
    CONNECTION_ROOT_CHILD_ORDER,
    EXTRACT_ROOT,
    GOVERNANCE_ROOT,
    MONITOR_ROOT,
    TRANSFORM_PIPELINE_PREFIX,
    TRANSFORM_ROOT,
    TRANSFORM_TEMPLATE_PREFIX,
    TRANSFORM_TEMPLATES,
    TRANSFORM_SCOPE,
    TRANSFORM_WORKFLOW_PREFIX,
)

TreeNodeOut = Dict[str, Any]

# Data — Saved Queries, RAW, Data Models, Classic.
_DATA_BRANCHES: Tuple[Tuple[str, str, str], ...] = (
    ("raw", "RAW", "raw"),
    ("dm", "Data Models", "dm"),
    ("classic", "Classic", "classic"),
)

# Transform — built pipeline scopes under ``transform/workflows/{scope}/``, then templates.

# Integration — under Fusion → Integration.
_INTEGRATION_BRANCHES: Tuple[Tuple[str, str, str], ...] = (
    ("workflows", "Workflows", "workflows"),
    ("pipelines", "Pipelines", "pipelines"),
    ("functions", "Functions", "functions"),
    ("transformations", "Transformations", "transformations"),
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


def _sort_connection_root_children(nodes: List[TreeNodeOut]) -> List[TreeNodeOut]:
    """Pinned order for top-level connection folders; project info stays last."""
    order = {nid: i for i, nid in enumerate(CONNECTION_ROOT_CHILD_ORDER)}
    if not nodes or any(str(n.get("id") or "") not in order for n in nodes):
        return nodes
    return sorted(nodes, key=lambda n: order[str(n.get("id") or "")])


def _sort_nodes(nodes: List[TreeNodeOut], *, starred_ids: Optional[List[str]] = None) -> List[TreeNodeOut]:
    """Starred nodes first (config order), then case-insensitive label."""
    stars = starred_ids if starred_ids is not None else discovery_config.get_starred_node_ids()
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


def _discovery_module_root():
    from ui.server.main import MODULE_ROOT

    return MODULE_ROOT


def _declared_root():
    return governance_declared.declared_root(_discovery_module_root())


_NAME_TOKEN_SPLIT = re.compile(r"[_:\-]+")


def _name_tokens(name: str) -> List[str]:
    """Split a CDF resource name into hierarchy tokens on underscores, dashes, or colons."""
    parts = [p for p in _NAME_TOKEN_SPLIT.split(name or "") if p]
    return parts if parts else ["(other)"]


def _fusion_live_node_id(branch_path: str, token_path: List[str]) -> str:
    """Node id for a live CDF path under Fusion (e.g. ``spaces`` or ``admin:groups``)."""
    branch_part = ":".join(encode_segment(s) for s in branch_path.split(":"))
    token_part = ":".join(encode_segment(t) for t in token_path)
    return f"fusion:{branch_part}:live:{token_part}"


def _fusion_cdf_hierarchy_children(
    items: List[Dict[str, Any]],
    *,
    branch_path: str,
    branch: str,
    token_path: List[str],
    name_key: str,
    leaf_kind: str,
    leaf_id_prefix: str,
    id_key: str,
) -> List[TreeNodeOut]:
    """Folder nodes for configured name-token depth; deeper suffixes appear as leaves."""
    depth = len(token_path)
    folder_depth = discovery_config.get_gov_live_token_folder_depth()
    child_tokens: set[str] = set()
    leaves: List[TreeNodeOut] = []

    for item in items:
        tokens = _name_tokens(str(item.get(name_key) or item.get("label") or ""))
        if tokens[:depth] != token_path:
            continue
        if (
            depth >= folder_depth
            or len(tokens) == depth
            or (depth == 0 and len(tokens) == 1)
        ):
            leaf_id = encode_segment(str(item[id_key]))
            leaves.append(
                _node(
                    id=f"{leaf_id_prefix}{leaf_id}",
                    label=item["label"],
                    kind=leaf_kind,
                    has_children=False,
                    meta={**item, "live_cdf": True, "fusion_branch": branch},
                )
            )
        else:
            child_tokens.add(tokens[depth])

    folder_nodes = [
        _node(
            id=_fusion_live_node_id(branch_path, token_path + [tok]),
            label=tok,
            kind="folder",
            has_children=True,
            meta={
                "domain": f"fusion_{branch}_token",
                "name_tokens": token_path + [tok],
                "live_cdf": True,
                "fusion_branch": branch,
            },
        )
        for tok in child_tokens
    ]
    return _sort_nodes(folder_nodes + leaves)


def _gov_artifact_branch_nodes(*, workspace: str, prefix: str) -> List[TreeNodeOut]:
    declared = _declared_root()
    ac_kind = "spaces" if workspace == "spaces" else "groups"
    nodes: List[TreeNodeOut] = []
    for child in governance_declared.list_artifact_tree_children(
        declared, kind=ac_kind, prefix=prefix
    ):
        rel = str(child.get("rel") or "")
        name = str(child.get("name") or rel)
        if child.get("kind") == "file":
            nid = f"gov:{workspace}:artifact:{encode_segment(rel)}"
            nodes.append(
                _node(
                    id=nid,
                    label=name,
                    kind="gov_artifact_file",
                    has_children=False,
                    meta={
                        "governance_workspace": workspace,
                        "artifact_rel": rel,
                    },
                )
            )
        else:
            nid = f"gov:{workspace}:adir:{encode_segment(rel)}"
            nodes.append(
                _node(
                    id=nid,
                    label=name,
                    kind="folder",
                    has_children=True,
                    meta={"domain": f"governance_{workspace}_artifact_dir", "artifact_prefix": rel},
                )
            )
    return nodes


def _gov_spaces_children(client: Any, segs: List[str]) -> List[TreeNodeOut]:
    if not segs:
        return _sort_nodes(_gov_artifact_branch_nodes(workspace="spaces", prefix="spaces"))

    if segs[0] == "adir" and len(segs) == 2:
        prefix = decode_segment(segs[1])
        return _sort_nodes(_gov_artifact_branch_nodes(workspace="spaces", prefix=prefix))

    return []


def _gov_groups_children(client: Any, segs: List[str]) -> List[TreeNodeOut]:
    if not segs:
        return _sort_nodes(_gov_artifact_branch_nodes(workspace="groups", prefix="auth"))

    if segs[0] == "adir" and len(segs) == 2:
        prefix = decode_segment(segs[1])
        return _sort_nodes(_gov_artifact_branch_nodes(workspace="groups", prefix=prefix))

    return []


def _fusion_spaces_children(client: Any, segs: List[str]) -> List[TreeNodeOut]:
    if not segs:
        items = cdf_browse.list_governance_spaces(client, limit=2000, include_global=True)
        return _fusion_cdf_hierarchy_children(
            items,
            branch_path="spaces",
            branch="spaces",
            token_path=[],
            name_key="space",
            leaf_kind="gov_space",
            leaf_id_prefix="fusion:space:",
            id_key="space",
        )

    if segs[0] == "live":
        token_path = [decode_segment(s) for s in segs[1:]]
        items = cdf_browse.list_governance_spaces(client, limit=2000, include_global=True)
        return _fusion_cdf_hierarchy_children(
            items,
            branch_path="spaces",
            branch="spaces",
            token_path=token_path,
            name_key="space",
            leaf_kind="gov_space",
            leaf_id_prefix="fusion:space:",
            id_key="space",
        )

    return []


def _fusion_groups_children(client: Any, segs: List[str]) -> List[TreeNodeOut]:
    if not segs:
        items = cdf_browse.list_security_groups(client, limit=2000)
        return _fusion_cdf_hierarchy_children(
            items,
            branch_path="admin:groups",
            branch="groups",
            token_path=[],
            name_key="name",
            leaf_kind="gov_group",
            leaf_id_prefix="fusion:group:",
            id_key="id",
        )

    if segs[0] == "live":
        token_path = [decode_segment(s) for s in segs[1:]]
        items = cdf_browse.list_security_groups(client, limit=2000)
        return _fusion_cdf_hierarchy_children(
            items,
            branch_path="admin:groups",
            branch="groups",
            token_path=token_path,
            name_key="name",
            leaf_kind="gov_group",
            leaf_id_prefix="fusion:group:",
            id_key="id",
        )

    return []


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
                open_target=cdf_browse.dm_instances_open_target(
                    view_space=v["space"],
                    view_external_id=v["external_id"],
                    view_version=v["version"],
                    instance_kind=v.get("instance_kind", "node"),
                ),
                meta={
                    **v,
                    "data_model_space": space,
                    "data_model_external_id": model_ext,
                    "data_model_version": model_ver,
                },
            )
        )
    return _sort_nodes(out)


def _fusion_dm_space_nodes(spaces: List[str]) -> List[TreeNodeOut]:
    return [
        _node(
            id=f"fusion:dm:space:{encode_segment(sp)}",
            label=sp,
            kind="folder",
            has_children=True,
            meta={"domain": "fusion_dm_space", "space": sp},
        )
        for sp in spaces
    ]


def _fusion_dm_view_nodes(views: List[Dict[str, Any]], *, space: str) -> List[TreeNodeOut]:
    spe = encode_segment(space)
    out: List[TreeNodeOut] = []
    for v in views:
        ve = encode_segment(v["external_id"])
        vv = encode_segment(v["version"])
        out.append(
            _node(
                id=f"fusion:dm:space:{spe}:view:{ve}:{vv}",
                label=v["label"],
                kind="fusion_dm_view",
                has_children=False,
                open_target=cdf_browse.dm_instances_open_target(
                    view_space=v["space"],
                    view_external_id=v["external_id"],
                    view_version=v["version"],
                    instance_kind=v.get("instance_kind", "node"),
                ),
                meta={**v},
            )
        )
    return out


def _fusion_dm_container_nodes(
    containers: List[Dict[str, Any]],
    *,
    views_by_container_key: Dict[tuple[str, str], Dict[str, Any]],
    space: str,
) -> List[TreeNodeOut]:
    spe = encode_segment(space)
    out: List[TreeNodeOut] = []
    for c in containers:
        ce = encode_segment(c["external_id"])
        open_target = cdf_browse.fusion_open_target_for_container(
            c, views_by_container_key
        )
        out.append(
            _node(
                id=f"fusion:dm:space:{spe}:container:{ce}",
                label=c["label"],
                kind="fusion_dm_container",
                has_children=False,
                open_target=open_target,
                meta={**c, "queryable": open_target is not None},
            )
        )
    return out


def _fusion_dm_model_nodes(
    models: List[Dict[str, str]], *, space: str
) -> List[TreeNodeOut]:
    return [
        _node(
            id=_fusion_model_node_id(space, m["external_id"], m["version"]),
            label=_data_model_label(m),
            kind="dm_data_model",
            has_children=True,
            meta={**m, "space": space},
        )
        for m in models
    ]


def _fusion_model_node_id(space: str, model_ext: str, model_ver: str) -> str:
    spe = encode_segment(space)
    return (
        f"fusion:dm:space:{spe}:model:"
        f"{encode_segment(model_ext)}:{encode_segment(model_ver)}"
    )


def _fusion_dm_view_nodes_under_model(
    client: Any,
    *,
    space: str,
    model_ext: str,
    model_ver: str,
) -> List[TreeNodeOut]:
    base = _fusion_model_node_id(space, model_ext, model_ver)
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
                open_target=cdf_browse.dm_instances_open_target(
                    view_space=v["space"],
                    view_external_id=v["external_id"],
                    view_version=v["version"],
                    instance_kind=v.get("instance_kind", "node"),
                ),
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
        return _sort_connection_root_children(
            [
                _node(
                    id=DATA_ROOT,
                    label="Data",
                    kind="folder",
                    has_children=True,
                    meta={"domain": "data"},
                ),
                _node(
                    id=FUSION_ROOT,
                    label="Fusion",
                    kind="folder",
                    has_children=True,
                    meta={"domain": "fusion"},
                ),
                _node(
                    id=GOVERNANCE_ROOT,
                    label="Governance",
                    kind="folder",
                    has_children=True,
                    meta={"domain": "governance", "governance_workspace": "scope"},
                ),
                _node(
                    id=EXTRACT_ROOT,
                    label="Extract",
                    kind="extract",
                    has_children=False,
                    meta={"domain": "extract"},
                ),
                _node(
                    id=TRANSFORM_ROOT,
                    label="Transform",
                    kind="folder",
                    has_children=True,
                    meta={"domain": "transform"},
                ),
                _node(
                    id=MONITOR_ROOT,
                    label="Monitor",
                    kind="monitor",
                    has_children=False,
                    meta={"domain": "monitor"},
                ),
                _node(
                    id=CONNECTION_INFO,
                    label=f"Project: {proj}",
                    kind="connection",
                    has_children=False,
                    meta=info,
                ),
            ]
        )

    if kind == TRANSFORM_ROOT and not segs:
        from ui.server import transform_registry

        scope_config = _node(
            id=TRANSFORM_SCOPE,
            label="Scope hierarchy",
            kind="etl_scope",
            has_children=False,
            meta={"domain": "transform_scope"},
        )
        scope_nodes = [
            _node(
                id=f"{TRANSFORM_ROOT}:{encode_segment(scope)}",
                label=scope,
                kind="folder",
                has_children=True,
                meta={"domain": "transform_built_scope", "scope_suffix": scope},
            )
            for scope in transform_registry.list_built_scope_suffixes()
        ]
        templates_folder = _node(
            id=TRANSFORM_TEMPLATES,
            label="Templates",
            kind="folder",
            has_children=True,
            meta={"domain": "transform_templates"},
        )
        return _sort_nodes([scope_config, *scope_nodes, templates_folder])

    if kind == TRANSFORM_ROOT and len(segs) == 1 and segs[0] != "templates":
        from ui.server import transform_registry

        scope_suffix = segs[0]
        return _sort_nodes(
            [
                _node(
                    id=(
                        f"{TRANSFORM_PIPELINE_PREFIX}{encode_segment(scope_suffix)}:"
                        f"{encode_segment(str(p['id']))}"
                    ),
                    label=str(p.get("label") or p["id"]),
                    kind="etl_pipeline",
                    has_children=bool(p.get("has_workflow_children")),
                    meta={**p},
                )
                for p in transform_registry.list_built_pipeline_entries(scope_suffix=scope_suffix)
            ]
        )

    if kind == TRANSFORM_ROOT and len(segs) >= 3 and segs[0] == "pipeline":
        from ui.server import transform_registry

        scope_suffix = segs[1]
        pipeline_id = segs[2]
        return _sort_nodes(
            [
                _node(
                    id=(
                        f"{TRANSFORM_WORKFLOW_PREFIX}{encode_segment(scope_suffix)}:"
                        f"{encode_segment(pipeline_id)}:{encode_segment(str(a['id']))}"
                    ),
                    label=str(a.get("label") or a["id"]),
                    kind="etl_workflow_yaml",
                    has_children=False,
                    meta={**a},
                )
                for a in transform_registry.list_pipeline_workflow_artifacts(
                    pipeline_id, scope_suffix=scope_suffix
                )
            ]
        )

    if kind == TRANSFORM_ROOT and len(segs) == 1 and segs[0] == "templates":
        from ui.server import transform_registry

        return _sort_nodes(
            [
                _node(
                    id=f"{TRANSFORM_TEMPLATE_PREFIX}{encode_segment(str(t['id']))}",
                    label=str(t.get("label") or t["id"]),
                    kind="etl_template",
                    has_children=False,
                    meta={**t},
                )
                for t in transform_registry.list_template_ids()
            ]
        )

    if kind == DATA_ROOT and segs == ["sq"]:
        return _sort_nodes(
            [
                _node(
                    id=f"{DATA_SAVED_QUERIES}:item:{encode_segment(q['id'])}",
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
                for q in discovery_config.get_saved_queries()
            ]
        )

    if kind == DATA_ROOT and not segs:
        return [
            _node(
                id=DATA_SAVED_QUERIES,
                label="Saved Queries",
                kind="folder",
                has_children=True,
                meta={"domain": "saved_queries"},
            ),
            *[
                _node(
                    id=branch_id,
                    label=label,
                    kind="folder",
                    has_children=True,
                    meta={"domain": domain},
                )
                for branch_id, label, domain in _DATA_BRANCHES
            ],
        ]

    if kind == FUSION_ROOT and not segs:
        return [
            _node(
                id=FUSION_DM_ROOT,
                label="Data Modeling",
                kind="folder",
                has_children=True,
                meta={"domain": "fusion_dm"},
            ),
            _node(
                id=FUSION_SPACES,
                label="Spaces",
                kind="folder",
                has_children=True,
                meta={"domain": "fusion_spaces"},
            ),
            _node(
                id=FUSION_ADMIN,
                label="Admin",
                kind="folder",
                has_children=True,
                meta={"domain": "fusion_admin"},
            ),
            _node(
                id=FUSION_INTEGRATION_ROOT,
                label="Integration",
                kind="folder",
                has_children=True,
                meta={"domain": "integration"},
            ),
        ]

    if kind == FUSION_ROOT and len(segs) >= 1 and segs[0] == "spaces":
        return _fusion_spaces_children(client, segs[1:])

    if kind == FUSION_ROOT and len(segs) == 1 and segs[0] == "admin":
        return [
            _node(
                id=FUSION_GROUPS,
                label="Groups",
                kind="folder",
                has_children=True,
                meta={"domain": "fusion_groups"},
            ),
        ]

    if kind == FUSION_ROOT and len(segs) >= 2 and segs[0] == "admin" and segs[1] == "groups":
        return _fusion_groups_children(client, segs[2:])

    if kind == FUSION_ROOT and len(segs) == 1 and segs[0] == "integration":
        return [
            _node(
                id=f"{FUSION_INTEGRATION_ROOT}:{branch_id}",
                label=label,
                kind="folder",
                has_children=True,
                meta={"domain": domain},
            )
            for branch_id, label, domain in _INTEGRATION_BRANCHES
        ]

    if kind == FUSION_ROOT and len(segs) == 2 and segs[0] == "integration":
        branch = segs[1]
        if branch == "transformations":
            return _sort_nodes(
                [
                    _node(
                        id=f"{FUSION_INTEGRATION_ROOT}:transformations:item:{encode_segment(str(t['id']))}",
                        label=t["label"],
                        kind="transformation",
                        has_children=False,
                        meta=t,
                    )
                    for t in cdf_browse.list_transformations(client, limit=500)
                ]
            )
        if branch == "workflows":
            return _sort_nodes(
                [
                    _node(
                        id=f"{FUSION_INTEGRATION_ROOT}:workflows:item:{encode_segment(str(w['external_id'] or w['label']))}",
                        label=w["label"],
                        kind="workflow",
                        has_children=False,
                        meta=w,
                    )
                    for w in cdf_browse.list_workflows(client, limit=500)
                ]
            )
        if branch == "functions":
            return _sort_nodes(
                [
                    _node(
                        id=f"{FUSION_INTEGRATION_ROOT}:functions:item:{encode_segment(str(f['id']))}",
                        label=f["label"],
                        kind="function",
                        has_children=False,
                        meta=f,
                    )
                    for f in cdf_browse.list_functions(client, limit=500)
                ]
            )
        if branch == "pipelines":
            return _sort_nodes(
                [
                    _node(
                        id=f"{FUSION_INTEGRATION_ROOT}:pipelines:item:{encode_segment(str(p['external_id'] or p['label']))}",
                        label=p["label"],
                        kind="extraction_pipeline",
                        has_children=False,
                        meta=p,
                    )
                    for p in cdf_browse.list_extraction_pipelines(client, limit=500)
                ]
            )

    if kind == FUSION_ROOT and len(segs) == 1 and segs[0] == "dm":
        return [
            _node(
                id="fusion:dm:nodes",
                label="All nodes",
                kind="fusion_dm_all",
                has_children=False,
                open_target={"type": "fusion_dm_all", "entity": "nodes"},
                meta={"entity": "nodes"},
            ),
            _node(
                id="fusion:dm:edges",
                label="All edges",
                kind="fusion_dm_all",
                has_children=False,
                open_target={"type": "fusion_dm_all", "entity": "edges"},
                meta={"entity": "edges"},
            ),
            _node(
                id="fusion:dm:system",
                label="System spaces",
                kind="folder",
                has_children=True,
                meta={"domain": "fusion_dm_system"},
            ),
            _node(
                id="fusion:dm:spaces",
                label="Spaces",
                kind="folder",
                has_children=True,
                meta={"domain": "fusion_dm_spaces"},
            ),
        ]

    if kind == FUSION_ROOT and len(segs) == 2 and segs[0] == "dm" and segs[1] == "system":
        system_spaces, _user = cdf_browse.fusion_partition_spaces(client, limit=2000)
        return _sort_nodes(_fusion_dm_space_nodes(system_spaces))

    if kind == FUSION_ROOT and len(segs) == 2 and segs[0] == "dm" and segs[1] == "spaces":
        _system, user_spaces = cdf_browse.fusion_partition_spaces(client, limit=2000)
        return _sort_nodes(_fusion_dm_space_nodes(user_spaces))

    if kind == FUSION_ROOT and len(segs) >= 3 and segs[0] == "dm" and segs[1] == "space":
        space = decode_segment(segs[2])
        spe = encode_segment(space)
        if len(segs) == 3:
            return [
                _node(
                    id=f"fusion:dm:space:{spe}:views",
                    label="Views",
                    kind="folder",
                    has_children=True,
                    meta={"domain": "fusion_dm_views", "space": space},
                ),
                _node(
                    id=f"fusion:dm:space:{spe}:containers",
                    label="Containers",
                    kind="folder",
                    has_children=True,
                    meta={"domain": "fusion_dm_containers", "space": space},
                ),
                _node(
                    id=f"fusion:dm:space:{spe}:models",
                    label="Data models",
                    kind="folder",
                    has_children=True,
                    meta={"domain": "fusion_dm_models", "space": space},
                ),
            ]
        if len(segs) == 4 and segs[3] == "views":
            include_global = cdf_browse.is_system_dm_space(space)
            views = cdf_browse.fusion_list_views_in_space(
                client, space, include_global=include_global
            )
            return _sort_nodes(_fusion_dm_view_nodes(views, space=space))
        if len(segs) == 4 and segs[3] == "containers":
            include_global = cdf_browse.is_system_dm_space(space)
            views = cdf_browse.fusion_list_views_in_space(
                client, space, include_global=include_global
            )
            views_by_container = cdf_browse.fusion_view_by_container_lookup(views)
            containers = cdf_browse.fusion_list_containers_in_space(
                client, space, include_global=include_global
            )
            return _sort_nodes(
                _fusion_dm_container_nodes(
                    containers, views_by_container_key=views_by_container, space=space
                )
            )
        if len(segs) == 4 and segs[3] == "models":
            models = cdf_browse.dm_list_data_models(
                client, space=space, limit=500, include_global=cdf_browse.is_system_dm_space(space)
            )
            return _sort_nodes(_fusion_dm_model_nodes(models, space=space))
        if len(segs) == 6 and segs[3] == "model":
            return _fusion_dm_view_nodes_under_model(
                client,
                space=space,
                model_ext=decode_segment(segs[4]),
                model_ver=decode_segment(segs[5]),
            )

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

    if kind == "gov" and not segs:
        return [
            _node(
                id="gov:spaces",
                label="Spaces",
                kind="folder",
                has_children=True,
                meta={"domain": "governance_spaces", "governance_workspace": "spaces"},
            ),
            _node(
                id="gov:groups",
                label="Groups",
                kind="folder",
                has_children=True,
                meta={"domain": "governance_groups", "governance_workspace": "groups"},
            ),
        ]

    if kind == "gov" and len(segs) >= 1 and segs[0] == "spaces":
        return _gov_spaces_children(client, segs[1:])

    if kind == "gov" and len(segs) >= 1 and segs[0] == "groups":
        return _gov_groups_children(client, segs[1:])

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
