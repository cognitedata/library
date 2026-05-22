"""Cascade disable/enable for canvas pipeline nodes (UI graph editing; compile ignores ``cascade_disabled``)."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, FrozenSet, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

from .canvas_dag import (
    JOIN_TARGET_HANDLE_LEFT,
    JOIN_TARGET_HANDLE_RIGHT,
    PASS_THROUGH_KINDS,
    STRUCTURAL_KINDS,
    _JOIN_INPUT_SOURCE_KINDS,
    _KIND_FN,
)

# Pipeline kinds that compile to tasks (exclude auto-inserted cleanup).
EXECUTABLE_PIPELINE_KINDS: FrozenSet[str] = frozenset(
    k for k in _KIND_FN if k != "discovery_raw_cleanup"
)

QUERY_KINDS: FrozenSet[str] = frozenset({"query_view", "query_raw", "query_classic", "query_sql"})

# Kinds the user can toggle + that participate in cascade.
DISABLEABLE_KINDS: FrozenSet[str] = EXECUTABLE_PIPELINE_KINDS | frozenset({"subgraph"})


def _kind(n: Mapping[str, Any]) -> str:
    return str(n.get("kind") or "").strip()


def is_canvas_node_enabled(n: Mapping[str, Any]) -> bool:
    return n.get("enabled", True) is not False


def is_canvas_node_cascade_disabled(n: Mapping[str, Any]) -> bool:
    return n.get("cascade_disabled") is True


def build_canvas_adjacency(
    nodes: Sequence[Mapping[str, Any]],
    edges: Sequence[Mapping[str, Any]],
) -> Tuple[Dict[str, Mapping[str, Any]], Dict[str, List[str]], Dict[str, List[str]]]:
    by_id: Dict[str, Mapping[str, Any]] = {}
    rev_adj: Dict[str, List[str]] = defaultdict(list)
    fwd_adj: Dict[str, List[str]] = defaultdict(list)
    for n in nodes:
        if not isinstance(n, dict):
            continue
        nid = str(n.get("id") or "").strip()
        if nid:
            by_id[nid] = n
    for e in edges:
        if not isinstance(e, dict):
            continue
        s = str(e.get("source") or "").strip()
        t = str(e.get("target") or "").strip()
        if s and t and s in by_id and t in by_id:
            rev_adj[t].append(s)
            fwd_adj[s].append(t)
    for nid in by_id:
        rev_adj.setdefault(nid, [])
        fwd_adj.setdefault(nid, [])
    return by_id, dict(rev_adj), dict(fwd_adj)


def _is_effectively_enabled(
    nid: str,
    n: Mapping[str, Any],
    *,
    disabled_ids: Set[str],
    treat_as_enabled: Set[str],
) -> bool:
    if nid in treat_as_enabled:
        return True
    if nid in disabled_ids:
        return False
    return is_canvas_node_enabled(n)


def _executable_ids_from_canvas(
    by_id: Mapping[str, Mapping[str, Any]],
    *,
    disabled_ids: Set[str],
    treat_as_enabled: Optional[Set[str]] = None,
) -> Set[str]:
    extra = treat_as_enabled or set()
    out: Set[str] = set()
    for nid, n in by_id.items():
        if not _is_effectively_enabled(nid, n, disabled_ids=disabled_ids, treat_as_enabled=extra):
            continue
        if _kind(n) in EXECUTABLE_PIPELINE_KINDS:
            out.add(nid)
    return out


def _is_disabled_pass_through_node(
    n: Mapping[str, Any],
    *,
    disabled_ids: Set[str],
) -> bool:
    nid = str(n.get("id") or "").strip()
    if nid and nid in disabled_ids:
        return True
    k = _kind(n)
    if k == "subgraph" and not is_canvas_node_enabled(n):
        return True
    if k in EXECUTABLE_PIPELINE_KINDS and not is_canvas_node_enabled(n):
        return True
    return False


def collect_executable_ancestors(
    start_id: str,
    *,
    by_id: Mapping[str, Mapping[str, Any]],
    rev_adj: Mapping[str, List[str]],
    disabled_ids: Set[str],
    treat_as_enabled: Optional[Set[str]] = None,
) -> Set[str]:
    """Enabled executable canvas node ids that must finish before ``start_id``."""
    extra = treat_as_enabled or set()
    executable_ids = _executable_ids_from_canvas(
        by_id, disabled_ids=disabled_ids, treat_as_enabled=extra
    )
    out: Set[str] = set()
    stack = list(rev_adj.get(start_id, []))
    visited: Set[str] = set()
    while stack:
        cur = stack.pop()
        if cur in visited:
            continue
        visited.add(cur)
        if cur in executable_ids:
            out.add(cur)
            continue
        if cur not in by_id:
            continue
        n = by_id[cur]
        k = _kind(n)
        if k in PASS_THROUGH_KINDS | STRUCTURAL_KINDS:
            stack.extend(rev_adj.get(cur, []))
            continue
        if k == "subgraph" and is_canvas_node_enabled(n):
            stack.extend(rev_adj.get(cur, []))
            continue
        if _is_disabled_pass_through_node(n, disabled_ids=disabled_ids):
            stack.extend(rev_adj.get(cur, []))
            continue
    return out


def _resolve_enabled_executable_canvas_id(
    canvas_id: str,
    *,
    by_id: Mapping[str, Mapping[str, Any]],
    rev_adj: Mapping[str, List[str]],
    disabled_ids: Set[str],
    treat_as_enabled: Optional[Set[str]] = None,
) -> Optional[str]:
    extra = treat_as_enabled or set()
    executable_ids = _executable_ids_from_canvas(
        by_id, disabled_ids=disabled_ids, treat_as_enabled=extra
    )
    stack = [canvas_id]
    visited: Set[str] = set()
    while stack:
        cur = stack.pop()
        if cur in visited:
            continue
        visited.add(cur)
        if cur in executable_ids:
            return cur
        if cur not in by_id:
            continue
        n = by_id[cur]
        k = _kind(n)
        if k in PASS_THROUGH_KINDS | STRUCTURAL_KINDS:
            stack.extend(rev_adj.get(cur, []))
            continue
        if k == "subgraph" and is_canvas_node_enabled(n):
            stack.extend(rev_adj.get(cur, []))
            continue
        if _is_disabled_pass_through_node(n, disabled_ids=disabled_ids):
            stack.extend(rev_adj.get(cur, []))
            continue
    return None


def is_entry_executable_node(
    node_id: str,
    *,
    by_id: Mapping[str, Mapping[str, Any]],
    rev_adj: Mapping[str, List[str]],
    disabled_ids: Set[str],
    treat_as_enabled: Optional[Set[str]] = None,
) -> bool:
    extra = treat_as_enabled or set()
    """Query nodes that only need start/source_view upstream (``depends_on`` may be empty)."""
    n = by_id.get(node_id)
    if n is None or _kind(n) not in QUERY_KINDS:
        return False
    if collect_executable_ancestors(
        node_id,
        by_id=by_id,
        rev_adj=rev_adj,
        disabled_ids=disabled_ids,
        treat_as_enabled=extra,
    ):
        return False
    stack = list(rev_adj.get(node_id, []))
    visited: Set[str] = set()
    while stack:
        cur = stack.pop()
        if cur in visited:
            continue
        visited.add(cur)
        if cur not in by_id:
            continue
        k = _kind(cur_n := by_id[cur])
        if k in EXECUTABLE_PIPELINE_KINDS and _is_effectively_enabled(
            cur, cur_n, disabled_ids=disabled_ids, treat_as_enabled=extra
        ):
            return False
        if k in PASS_THROUGH_KINDS | STRUCTURAL_KINDS | {"subgraph"}:
            stack.extend(rev_adj.get(cur, []))
            continue
        if _is_disabled_pass_through_node(cur_n, disabled_ids=disabled_ids):
            stack.extend(rev_adj.get(cur, []))
            continue
        return False
    return True


def join_has_executable_input(
    join_id: str,
    *,
    edges: Sequence[Mapping[str, Any]],
    by_id: Mapping[str, Mapping[str, Any]],
    rev_adj: Mapping[str, List[str]],
    disabled_ids: Set[str],
    treat_as_enabled: Optional[Set[str]] = None,
) -> bool:
    extra = treat_as_enabled or set()
    """True when at least one join handle resolves an enabled executable (parallel branch may stay)."""
    left_ok = False
    right_ok = False
    for e in edges:
        if not isinstance(e, dict):
            continue
        if str(e.get("target") or "").strip() != join_id:
            continue
        src = str(e.get("source") or "").strip()
        if not src:
            continue
        pred = by_id.get(src)
        if pred is None:
            continue
        if _kind(pred) not in _JOIN_INPUT_SOURCE_KINDS:
            continue
        if not _resolve_enabled_executable_canvas_id(
            src,
            by_id=by_id,
            rev_adj=rev_adj,
            disabled_ids=disabled_ids,
            treat_as_enabled=extra,
        ):
            continue
        th = str(e.get("target_handle") or "").strip()
        if th == JOIN_TARGET_HANDLE_LEFT:
            left_ok = True
        elif th == JOIN_TARGET_HANDLE_RIGHT:
            right_ok = True
    return left_ok or right_ok


def has_valid_executable_upstream(
    node_id: str,
    *,
    edges: Sequence[Mapping[str, Any]],
    by_id: Mapping[str, Mapping[str, Any]],
    rev_adj: Mapping[str, List[str]],
    disabled_ids: Set[str],
    treat_as_enabled: Optional[Set[str]] = None,
) -> bool:
    extra = treat_as_enabled or set()
    n = by_id.get(node_id)
    if n is None:
        return True
    k = _kind(n)
    if k not in DISABLEABLE_KINDS:
        return True
    if k == "join":
        return join_has_executable_input(
            node_id,
            edges=edges,
            by_id=by_id,
            rev_adj=rev_adj,
            disabled_ids=disabled_ids,
            treat_as_enabled=extra,
        )
    ancestors = collect_executable_ancestors(
        node_id,
        by_id=by_id,
        rev_adj=rev_adj,
        disabled_ids=disabled_ids,
        treat_as_enabled=extra,
    )
    if ancestors:
        return True
    return is_entry_executable_node(
        node_id,
        by_id=by_id,
        rev_adj=rev_adj,
        disabled_ids=disabled_ids,
        treat_as_enabled=extra,
    )


def should_cascade_disable(
    node_id: str,
    *,
    edges: Sequence[Mapping[str, Any]],
    by_id: Mapping[str, Mapping[str, Any]],
    rev_adj: Mapping[str, List[str]],
    disabled_ids: Set[str],
) -> bool:
    n = by_id.get(node_id)
    if n is None:
        return False
    if _kind(n) not in DISABLEABLE_KINDS:
        return False
    if node_id in disabled_ids:
        return False
    if not is_canvas_node_enabled(n):
        return False
    return not has_valid_executable_upstream(
        node_id,
        edges=edges,
        by_id=by_id,
        rev_adj=rev_adj,
        disabled_ids=disabled_ids,
    )


def _canvas_graph(canvas: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], List[Mapping[str, Any]]]:
    nodes_raw = canvas.get("nodes")
    edges_raw = canvas.get("edges")
    nodes = nodes_raw if isinstance(nodes_raw, list) else []
    edges = edges_raw if isinstance(edges_raw, list) else []
    return nodes, edges


def cascade_disable_ids(
    canvas: Mapping[str, Any],
    root_ids: Set[str],
) -> Set[str]:
    """
    Node ids to disable when ``root_ids`` are turned off (includes ``root_ids``).

    BFS downstream; repeatedly disables orphans until stable.
    """
    nodes, edges = _canvas_graph(canvas)
    by_id, rev_adj, fwd_adj = build_canvas_adjacency(nodes, edges)
    disabled = set(root_ids)
    queue = [nid for nid in root_ids if nid in by_id]
    while queue:
        nid = queue.pop(0)
        for tgt in fwd_adj.get(nid, []):
            if tgt in disabled:
                continue
            if should_cascade_disable(
                tgt,
                edges=edges,
                by_id=by_id,
                rev_adj=rev_adj,
                disabled_ids=disabled,
            ):
                disabled.add(tgt)
                queue.append(tgt)
    return disabled


def _effective_disabled_ids(
    by_id: Mapping[str, Mapping[str, Any]],
    *,
    treat_as_enabled: Set[str],
) -> Set[str]:
    out: Set[str] = set()
    for nid, n in by_id.items():
        if nid in treat_as_enabled:
            continue
        if not is_canvas_node_enabled(n):
            out.add(nid)
    return out


def cascade_enable_ids(
    canvas: Mapping[str, Any],
    root_ids: Set[str],
    *,
    pending_enable: Optional[Set[str]] = None,
) -> Set[str]:
    """
    Cascade-marked node ids to re-enable after ``root_ids`` are turned on (excludes roots).

    Only nodes with ``cascade_disabled``; does not enable manually disabled nodes.

    Pass ``pending_enable`` when roots are not yet enabled on ``canvas`` but will be
  (e.g. ``apply_canvas_node_enablement_patch`` before writing nodes).
    """
    nodes, edges = _canvas_graph(canvas)
    by_id, rev_adj, fwd_adj = build_canvas_adjacency(nodes, edges)
    pending = pending_enable or set()
    to_enable: Set[str] = set()
    extra_enabled: Set[str] = set()
    for nid in root_ids:
        if nid not in by_id:
            continue
        if nid in pending or is_canvas_node_enabled(by_id[nid]):
            extra_enabled.add(nid)
    queue = list(extra_enabled)
    while queue:
        nid = queue.pop(0)
        for tgt in fwd_adj.get(nid, []):
            if tgt in to_enable or tgt in root_ids:
                continue
            n = by_id.get(tgt)
            if n is None:
                continue
            if is_canvas_node_enabled(n):
                continue
            if not is_canvas_node_cascade_disabled(n):
                continue
            dis = _effective_disabled_ids(by_id, treat_as_enabled=extra_enabled)
            if has_valid_executable_upstream(
                tgt,
                edges=edges,
                by_id=by_id,
                rev_adj=rev_adj,
                disabled_ids=dis,
                treat_as_enabled=extra_enabled,
            ):
                to_enable.add(tgt)
                extra_enabled.add(tgt)
                queue.append(tgt)
    return to_enable


def _inner_canvas_from_subgraph(n: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    data = n.get("data")
    if not isinstance(data, dict):
        return None
    inner = data.get("inner_canvas")
    return inner if isinstance(inner, dict) else None


def _disable_inner_canvas_for_disabled_frame(inner: Mapping[str, Any]) -> Mapping[str, Any]:
    nodes_raw = inner.get("nodes")
    nodes = nodes_raw if isinstance(nodes_raw, list) else []
    out_nodes: List[Mapping[str, Any]] = []
    for n in nodes:
        if not isinstance(n, dict):
            out_nodes.append(n)
            continue
        k = _kind(n)
        if k not in DISABLEABLE_KINDS:
            out_nodes.append(n)
            continue
        if not is_canvas_node_enabled(n) and not is_canvas_node_cascade_disabled(n):
            out_nodes.append(n)
            continue
        patched = dict(n)
        patched["enabled"] = False
        patched["cascade_disabled"] = True
        out_nodes.append(patched)
    return _sync_all_subgraph_inner_enablement({**dict(inner), "nodes": out_nodes})


def _reconcile_inner_canvas_enablement(inner: Mapping[str, Any]) -> Mapping[str, Any]:
    nodes_raw = inner.get("nodes")
    edges_raw = inner.get("edges")
    nodes: List[Mapping[str, Any]] = [
        dict(n) if isinstance(n, dict) else n for n in (nodes_raw if isinstance(nodes_raw, list) else [])
    ]
    edges = edges_raw if isinstance(edges_raw, list) else []
    cleared: List[Mapping[str, Any]] = []
    for n in nodes:
        if not isinstance(n, dict):
            cleared.append(n)
            continue
        if is_canvas_node_cascade_disabled(n) and not is_canvas_node_enabled(n):
            out = dict(n)
            out.pop("enabled", None)
            out.pop("cascade_disabled", None)
            cleared.append(out)
        else:
            cleared.append(n)
    nodes = cleared
    changed = True
    while changed:
        changed = False
        by_id, rev_adj, _ = build_canvas_adjacency(nodes, edges)
        disabled_ids = {nid for nid, nn in by_id.items() if not is_canvas_node_enabled(nn)}
        for nid, nn in by_id.items():
            if not is_canvas_node_enabled(nn):
                continue
            if _kind(nn) not in DISABLEABLE_KINDS:
                continue
            if should_cascade_disable(
                nid,
                edges=edges,
                by_id=by_id,
                rev_adj=rev_adj,
                disabled_ids=disabled_ids,
            ):
                patched = dict(nn)
                patched["enabled"] = False
                patched["cascade_disabled"] = True
                nodes = [patched if isinstance(x, dict) and str(x.get("id") or "").strip() == nid else x for x in nodes]
                changed = True
    return _sync_all_subgraph_inner_enablement({**dict(inner), "nodes": nodes})


def _sync_all_subgraph_inner_enablement(canvas: Mapping[str, Any]) -> Mapping[str, Any]:
    nodes_raw = canvas.get("nodes")
    if not isinstance(nodes_raw, list):
        return canvas
    return {
        **dict(canvas),
        "nodes": [sync_subgraph_inner_enablement(n) for n in nodes_raw],
    }


def sync_subgraph_inner_enablement(n: Mapping[str, Any]) -> Mapping[str, Any]:
    if _kind(n) != "subgraph":
        return n
    inner = _inner_canvas_from_subgraph(n)
    if not inner:
        return n
    nodes_raw = inner.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return n
    next_inner = (
        _reconcile_inner_canvas_enablement(inner)
        if is_canvas_node_enabled(n)
        else _disable_inner_canvas_for_disabled_frame(inner)
    )
    out = dict(n)
    data = dict(n.get("data") or {})
    data["inner_canvas"] = next_inner
    out["data"] = data
    return out


def apply_canvas_node_enablement_patch(
    canvas: MutableMapping[str, Any],
    *,
    root_id: str,
    enabled: Optional[bool] = None,
) -> Tuple[MutableMapping[str, Any], Set[str]]:
    """
    Apply manual disable/enable on ``root_id`` plus cascade side-effects.

    Returns patched canvas and the set of *other* node ids affected by cascade (for UI hints).
    """
    nodes_raw = canvas.get("nodes")
    if not isinstance(nodes_raw, list):
        return canvas, set()
    by_id, _, _ = build_canvas_adjacency(nodes_raw, _canvas_graph(canvas)[1])
    if root_id not in by_id:
        return canvas, set()

    root_node = by_id[root_id]
    turn_on = enabled if enabled is not None else not is_canvas_node_enabled(root_node)

    manual_disable_ids: Set[str] = set()
    manual_enable_ids: Set[str] = set()
    cascade_enable_only_ids: Set[str] = set()
    cascade_hint_ids: Set[str] = set()

    if turn_on:
        manual_enable_ids.add(root_id)
        cascade_enable_only_ids = cascade_enable_ids(
            canvas, {root_id}, pending_enable={root_id}
        )
        cascade_hint_ids = cascade_enable_only_ids
    else:
        manual_disable_ids = cascade_disable_ids(canvas, {root_id})
        cascade_hint_ids = manual_disable_ids - {root_id}

    def patch_node(n: Mapping[str, Any]) -> Mapping[str, Any]:
        if not isinstance(n, dict):
            return n
        nid = str(n.get("id") or "").strip()
        if not nid:
            return n
        if turn_on and (nid in manual_enable_ids or nid in cascade_enable_only_ids):
            out = dict(n)
            out.pop("enabled", None)
            out.pop("cascade_disabled", None)
            return out
        if nid in manual_disable_ids:
            out = dict(n)
            out["enabled"] = False
            if nid == root_id:
                out.pop("cascade_disabled", None)
            else:
                out["cascade_disabled"] = True
            return out
        return n

    canvas = dict(canvas)
    canvas["nodes"] = [sync_subgraph_inner_enablement(patch_node(n)) for n in nodes_raw]
    return canvas, cascade_hint_ids
