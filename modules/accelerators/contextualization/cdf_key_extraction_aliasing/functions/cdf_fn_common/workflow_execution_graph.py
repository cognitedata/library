"""
Declarative macro execution graph (Kahn-style process network) for cdf_key_extraction_aliasing.

SSOT: workflow_template/workflow.execution.graph.yaml
Aligned with workflow.template.WorkflowVersion.yaml task dependsOn.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set

import yaml

# Optional metadata for docs / tooling (inputs/outputs are logical; CDF uses RAW + configuration).
FUNCTION_CHANNEL_METADATA: Mapping[str, Mapping[str, Any]] = {
    "fn_dm_incremental_state_update": {
        "outputs": ["cohort RAW rows", "run_id in task data"],
        "inputs": ["workflow.input.configuration", "run_all"],
    },
    "fn_dm_key_extraction": {
        "inputs": ["run_id", "configuration", "key-extraction RAW parameters"],
        "outputs": ["key-extraction RAW rows", "FOREIGN_KEY_REFERENCES_JSON", "DOCUMENT_REFERENCES_JSON"],
    },
    "fn_dm_reference_index": {
        "inputs": ["configuration", "source RAW db/table", "run_id"],
        "outputs": ["reference index RAW rows"],
    },
    "fn_dm_aliasing": {
        "inputs": ["configuration", "key-extraction RAW or in-memory mirror"],
        "outputs": ["tag-aliasing RAW rows"],
    },
    "fn_dm_alias_persistence": {
        "inputs": ["aliasing RAW", "configuration", "optional FK from key-extraction RAW"],
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
    Map function externalId -> list of dependency externalIds from WorkflowVersion YAML.
    """
    wd = wv.get("workflowDefinition") or {}
    tasks = wd.get("tasks") or []
    out: Dict[str, List[str]] = {}
    if not isinstance(tasks, list):
        return out
    for t in tasks:
        if not isinstance(t, dict):
            continue
        params = t.get("parameters") or {}
        fn = params.get("function") or {}
        ext = str(fn.get("externalId") or t.get("externalId") or "").strip()
        if not ext:
            continue
        deps: List[str] = []
        for d in t.get("dependsOn") or []:
            if isinstance(d, dict):
                did = str(d.get("externalId") or "").strip()
                if did:
                    deps.append(did)
        out[ext] = sorted(set(deps))
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
