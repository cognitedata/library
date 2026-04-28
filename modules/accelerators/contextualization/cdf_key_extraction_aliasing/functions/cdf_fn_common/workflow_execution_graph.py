"""
Declarative macro execution graph (Kahn-style process network) for cdf_key_extraction_aliasing.

``workflow.execution.graph.yaml`` may be refreshed by ``build_scopes.py --force`` from
``compiled_workflow`` IR. Validation compares an :class:`ExecutionGraph` derived from that IR
to the generated ``WorkflowVersion`` document (task ``externalId`` / ``dependsOn``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set

import yaml

# Optional metadata for docs / tooling (inputs/outputs are logical; CDF uses RAW + configuration).
FUNCTION_CHANNEL_METADATA: Mapping[str, Mapping[str, Any]] = {
    "kea__incremental_state": {
        "outputs": ["cohort RAW rows", "run_id in task data"],
        "inputs": ["workflow.input.configuration", "run_all", "compiled_workflow"],
    },
    "kea__key_extraction": {
        "inputs": ["run_id", "configuration", "key-extraction RAW parameters", "compiled_workflow"],
        "outputs": ["key-extraction RAW rows", "FOREIGN_KEY_REFERENCES_JSON", "DOCUMENT_REFERENCES_JSON"],
    },
    "kea__reference_index": {
        "inputs": ["configuration", "source RAW db/table", "run_id", "compiled_workflow"],
        "outputs": ["reference index RAW rows"],
    },
    "kea__aliasing": {
        "inputs": ["configuration", "key-extraction RAW or in-memory mirror", "compiled_workflow"],
        "outputs": ["tag-aliasing RAW rows"],
    },
    "kea__alias_persistence": {
        "inputs": ["aliasing RAW", "configuration", "optional FK from key-extraction RAW", "compiled_workflow"],
        "outputs": ["DM instance updates"],
    },
}


@dataclass(frozen=True)
class ExecutionGraphEdge:
    from_id: str
    to_id: str
    channel: str


@dataclass
class ExecutionGraph:
    schema_version: int
    description: str
    nodes: List[str]
    node_roles: Dict[str, str]
    edges: List[ExecutionGraphEdge]

    def depends_on_map(self) -> Dict[str, List[str]]:
        """For each node id, list of predecessor ids (from edges)."""
        m: Dict[str, List[str]] = {n: [] for n in self.nodes}
        for e in self.edges:
            if e.to_id in m:
                m[e.to_id].append(e.from_id)
        for k in m:
            m[k] = sorted(set(m[k]))
        return m


def default_execution_graph_path(module_root: Optional[Path] = None) -> Path:
    """Path to workflow.execution.graph.yaml next to workflow templates."""
    if module_root is None:
        here = Path(__file__).resolve()
        module_root = here.parents[2]
    return module_root / "workflow_template" / "workflow.execution.graph.yaml"


def load_execution_graph(path: Path) -> ExecutionGraph:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Execution graph must be a mapping: {path}")
    sv = int(raw.get("schemaVersion") or 0)
    if sv < 1:
        raise ValueError(f"Unsupported schemaVersion in {path}")
    desc = str(raw.get("description") or "").strip()
    nodes_raw = raw.get("nodes") or []
    if not isinstance(nodes_raw, list):
        raise ValueError(f"nodes must be a list: {path}")
    node_ids: List[str] = []
    roles: Dict[str, str] = {}
    for n in nodes_raw:
        if not isinstance(n, dict):
            raise ValueError(f"Invalid node entry in {path}")
        nid = str(n.get("id") or "").strip()
        if not nid:
            raise ValueError(f"Node missing id in {path}")
        node_ids.append(nid)
        r = str(n.get("role") or "").strip()
        if r:
            roles[nid] = r
    edges_raw = raw.get("edges") or []
    if not isinstance(edges_raw, list):
        raise ValueError(f"edges must be a list: {path}")
    edges: List[ExecutionGraphEdge] = []
    node_set = set(node_ids)
    for e in edges_raw:
        if not isinstance(e, dict):
            raise ValueError(f"Invalid edge entry in {path}")
        fr = str(e.get("from") or "").strip()
        to = str(e.get("to") or "").strip()
        ch = str(e.get("channel") or "").strip()
        if not fr or not to:
            raise ValueError(f"Edge missing from/to in {path}")
        if fr not in node_set or to not in node_set:
            raise ValueError(f"Edge references unknown node: {fr} -> {to} in {path}")
        edges.append(ExecutionGraphEdge(from_id=fr, to_id=to, channel=ch or "unspecified"))
    return ExecutionGraph(
        schema_version=sv,
        description=desc,
        nodes=node_ids,
        node_roles=roles,
        edges=edges,
    )


def validate_execution_graph(graph: ExecutionGraph) -> List[str]:
    """Return human-readable errors; empty if valid."""
    errors: List[str] = []
    seen: Set[str] = set()
    for n in graph.nodes:
        if n in seen:
            errors.append(f"Duplicate node id: {n}")
        seen.add(n)
    # Cycle detection (DFS)
    adj: MutableMapping[str, List[str]] = {n: [] for n in graph.nodes}
    for e in graph.edges:
        adj[e.from_id].append(e.to_id)

    visited: Set[str] = set()
    stack: Set[str] = set()

    def dfs(u: str) -> bool:
        visited.add(u)
        stack.add(u)
        for v in adj.get(u, []):
            if v not in visited:
                if dfs(v):
                    return True
            elif v in stack:
                return True
        stack.remove(u)
        return False

    for n in graph.nodes:
        if n not in visited:
            if dfs(n):
                errors.append("Graph contains a cycle")
                break
    return errors


def depends_on_from_workflow_version(wv: Mapping[str, Any]) -> Dict[str, List[str]]:
    """
    Map **task** ``externalId`` -> list of dependency task ``externalId`` values from WorkflowVersion YAML.

    Canvas-driven versions use stable per-task ids (e.g. ``kea__key_extraction``) distinct from
    Cognite Function ``externalId`` (``parameters.function.externalId``).
    """
    wd = wv.get("workflowDefinition") or {}
    tasks = wd.get("tasks") or []
    out: Dict[str, List[str]] = {}
    if not isinstance(tasks, list):
        return out
    for t in tasks:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("externalId") or "").strip()
        if not tid:
            continue
        deps: List[str] = []
        for d in t.get("dependsOn") or []:
            if isinstance(d, dict):
                did = str(d.get("externalId") or "").strip()
                if did:
                    deps.append(did)
        out[tid] = sorted(set(deps))
    return out


def depends_on_from_execution_graph(graph: ExecutionGraph) -> Dict[str, List[str]]:
    m = graph.depends_on_map()
    return {n: sorted(m.get(n, [])) for n in graph.nodes}


def validate_template_workflow_version_matches_execution_graph(
    module_root: Path,
    workflow_version_template_path: Path,
) -> List[str]:
    """Compare workflow.execution.graph.yaml to workflow.template.WorkflowVersion.yaml."""
    graph = load_execution_graph(default_execution_graph_path(module_root))
    wv = load_workflow_version_yaml(workflow_version_template_path)
    return compare_graph_to_workflow_version(graph, wv)


def compare_graph_to_workflow_version(
    graph: ExecutionGraph,
    workflow_version: Mapping[str, Any],
) -> List[str]:
    """
    Assert task set and dependsOn match between execution graph and WorkflowVersion template.
    """
    errors: List[str] = []
    g_deps = depends_on_from_execution_graph(graph)
    w_deps = depends_on_from_workflow_version(workflow_version)
    g_nodes = set(graph.nodes)
    w_nodes = set(w_deps.keys())
    if g_nodes != w_nodes:
        only_g = sorted(g_nodes - w_nodes)
        only_w = sorted(w_nodes - g_nodes)
        if only_g:
            errors.append(f"Nodes only in execution graph: {only_g}")
        if only_w:
            errors.append(f"Nodes only in WorkflowVersion: {only_w}")
    for n in sorted(g_nodes & w_nodes):
        if g_deps.get(n) != w_deps.get(n):
            errors.append(
                f"dependsOn mismatch for {n}: graph={g_deps.get(n)} workflow={w_deps.get(n)}"
            )
    return errors


def load_workflow_version_yaml(path: Path) -> Dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"WorkflowVersion must be a mapping: {path}")
    return raw


_FN_TO_ROLE: Dict[str, str] = {
    "fn_dm_incremental_state_update": "incremental_cohort",
    "fn_dm_key_extraction": "key_extraction",
    "fn_dm_reference_index": "reference_index",
    "fn_dm_aliasing": "aliasing",
    "fn_dm_alias_persistence": "alias_persistence",
}


def compiled_workflow_structural_signature(cw: Mapping[str, Any]) -> tuple:
    """Stable tuple for DAG equality (task ids, function ids, sorted depends_on)."""
    tasks = cw.get("tasks") if isinstance(cw, dict) else None
    if not isinstance(tasks, list):
        return ()
    rows: List[tuple] = []
    for t in tasks:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id") or "").strip()
        fn = str(t.get("function_external_id") or "").strip()
        deps_raw = t.get("depends_on")
        deps = [str(x) for x in deps_raw] if isinstance(deps_raw, list) else []
        rows.append((tid, fn, tuple(sorted(deps))))
    return tuple(sorted(rows))


def execution_graph_from_compiled_workflow(cw: Mapping[str, Any]) -> ExecutionGraph:
    """Build :class:`ExecutionGraph` from ``compiled_workflow`` tasks and optional ``channels``."""
    tasks = cw.get("tasks") if isinstance(cw, dict) else None
    if not isinstance(tasks, list) or not tasks:
        return ExecutionGraph(
            schema_version=1,
            description="Empty compiled_workflow",
            nodes=[],
            node_roles={},
            edges=[],
        )

    ch_map: Dict[tuple, str] = {}
    channels = cw.get("channels")
    if isinstance(channels, list):
        for ch in channels:
            if not isinstance(ch, dict):
                continue
            fr = str(ch.get("from") or "").strip()
            to = str(ch.get("to") or "").strip()
            cname = str(ch.get("channel") or "").strip()
            if fr and to:
                ch_map[(fr, to)] = cname or f"{fr}_to_{to}"

    node_ids: List[str] = []
    node_roles: Dict[str, str] = {}
    seen: Set[str] = set()
    for t in tasks:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id") or "").strip()
        if not tid or tid in seen:
            continue
        seen.add(tid)
        node_ids.append(tid)
        fn = str(t.get("function_external_id") or "").strip()
        role = _FN_TO_ROLE.get(fn, "")
        if role:
            node_roles[tid] = role

    edges: List[ExecutionGraphEdge] = []
    node_set = set(node_ids)
    for t in tasks:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id") or "").strip()
        if not tid:
            continue
        deps_raw = t.get("depends_on")
        if not isinstance(deps_raw, list):
            continue
        for d in deps_raw:
            ds = str(d).strip()
            if not ds or ds not in node_set or tid not in node_set:
                continue
            channel = ch_map.get((ds, tid), f"{ds}_to_{tid}")
            edges.append(ExecutionGraphEdge(from_id=ds, to_id=tid, channel=channel))

    desc = (
        "Cognite workflow task nodes from compiled_workflow IR; edges follow depends_on "
        "(CDF does not pass payloads between tasks)."
    )
    return ExecutionGraph(
        schema_version=int(cw.get("schemaVersion") or 1),
        description=desc,
        nodes=node_ids,
        node_roles=node_roles,
        edges=edges,
    )


def execution_graph_to_mapping(graph: ExecutionGraph) -> Dict[str, Any]:
    """Serialize graph to a YAML-root mapping (``schemaVersion``, ``nodes``, ``edges``)."""
    nodes_out: List[Dict[str, Any]] = []
    for nid in graph.nodes:
        entry: Dict[str, Any] = {"id": nid}
        r = graph.node_roles.get(nid, "")
        if r:
            entry["role"] = r
        nodes_out.append(entry)
    edges_out = [
        {"from": e.from_id, "to": e.to_id, "channel": e.channel} for e in graph.edges
    ]
    return {
        "schemaVersion": graph.schema_version,
        "description": graph.description,
        "nodes": nodes_out,
        "edges": edges_out,
    }


EXECUTION_GRAPH_FILE_HEADER = (
    "# IR-derived Kahn-style execution graph for cdf_key_extraction_aliasing.\n"
    "# Regenerated by ``python module.py build --force`` from ``compiled_workflow``.\n"
    "# Channel semantics: see workflow_channel_contracts.md\n"
)


def dump_execution_graph_yaml_for_compiled_workflow(
    module_root: Path,
    cw: Mapping[str, Any],
    *,
    dry_run: bool,
) -> None:
    """Write ``workflow_template/workflow.execution.graph.yaml`` from *cw* (no-op when *dry_run*)."""
    graph = execution_graph_from_compiled_workflow(cw)
    errs = validate_execution_graph(graph)
    if errs:
        raise ValueError(
            "compiled_workflow yields invalid execution graph: " + "; ".join(errs)
        )
    path = default_execution_graph_path(module_root)
    body = yaml.safe_dump(
        execution_graph_to_mapping(graph),
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
    )
    if dry_run:
        return
    path.write_text(EXECUTION_GRAPH_FILE_HEADER + body, encoding="utf-8")


def validate_compiled_workflow_matches_workflow_version_document(
    workflow_version: Mapping[str, Any],
    cw: Mapping[str, Any],
) -> List[str]:
    """Compare :func:`execution_graph_from_compiled_workflow` to *workflow_version* tasks."""
    graph = execution_graph_from_compiled_workflow(cw)
    return compare_graph_to_workflow_version(graph, workflow_version)
