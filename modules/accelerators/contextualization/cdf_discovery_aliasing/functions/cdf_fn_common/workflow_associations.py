"""Top-level ``associations`` (v1 scope): source-view → extraction bindings.

Mirrors the UI layer in ``ui/src/components/flow/workflowScopeAssociations.ts``.
Canvas edges compile into ``associations``; engines gate extraction rules by these pairs.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set, Tuple

KIND_SOURCE_VIEW_TO_EXTRACTION = "source_view_to_extraction"


def coerce_association_source_view_index(idx: Any) -> Optional[int]:
    """Normalize ``source_view_index`` from YAML/JSON (int, float, or numeric string)."""
    if idx is None or isinstance(idx, bool):
        return None
    if isinstance(idx, int):
        return int(idx)
    if isinstance(idx, float) and float(idx).is_integer():
        return int(idx)
    if isinstance(idx, str):
        s = idx.strip()
        if not s:
            return None
        try:
            v = float(s)
        except (TypeError, ValueError):
            return None
        if v.is_integer():
            return int(v)
    return None


def parse_source_view_to_extraction_pairs(doc: Mapping[str, Any]) -> List[Tuple[int, str]]:
    """Return sorted deduped (source_view_index, extraction_rule_name) pairs."""
    raw = doc.get("associations")
    if not isinstance(raw, list):
        return []
    out: List[Tuple[int, str]] = []
    seen: Set[str] = set()
    for row in raw:
        if not isinstance(row, dict):
            continue
        if str(row.get("kind") or "").strip() != KIND_SOURCE_VIEW_TO_EXTRACTION:
            continue
        idx = row.get("source_view_index")
        name = row.get("extraction_rule_name")
        i = coerce_association_source_view_index(idx)
        if i is None or not isinstance(name, str) or not name.strip():
            continue
        rn = name.strip()
        k = f"{i}\0{rn}"
        if k in seen:
            continue
        seen.add(k)
        out.append((i, rn))
    out.sort(key=lambda t: (t[0], t[1]))
    return out


def validate_workflow_associations(doc: Mapping[str, Any]) -> List[str]:
    """Return human-readable errors; empty if valid (unknown kinds ignored)."""
    errors: List[str] = []
    raw = doc.get("associations")
    if raw is None:
        return errors
    if not isinstance(raw, list):
        errors.append("associations must be a list when present")
        return errors

    svs = doc.get("source_views")
    n_sv = len(svs) if isinstance(svs, list) else 0

    ke = doc.get("key_extraction")
    names: Set[str] = set()
    if isinstance(ke, dict):
        kcfg = ke.get("config")
        if isinstance(kcfg, dict):
            data = kcfg.get("data")
            if isinstance(data, dict):
                rules = data.get("extraction_rules")
                if isinstance(rules, list):
                    for r in rules:
                        if isinstance(r, dict) and r.get("name") is not None:
                            nm = str(r.get("name")).strip()
                            if nm:
                                names.add(nm)

    for i, row in enumerate(raw):
        if not isinstance(row, dict):
            errors.append(f"associations[{i}] must be a mapping")
            continue
        kind = str(row.get("kind") or "").strip()
        if kind != KIND_SOURCE_VIEW_TO_EXTRACTION:
            continue
        idx = row.get("source_view_index")
        rn = row.get("extraction_rule_name")
        ii = coerce_association_source_view_index(idx)
        if ii is None or not isinstance(rn, str) or not rn.strip():
            errors.append(
                f"associations[{i}] ({kind}): source_view_index and extraction_rule_name required"
            )
            continue
        if n_sv and (ii < 0 or ii >= n_sv):
            errors.append(
                f"associations[{i}]: source_view_index {ii} out of range (source_views length {n_sv})"
            )
        rnm = rn.strip()
        if names and rnm not in names:
            errors.append(
                f"associations[{i}]: unknown extraction_rule_name {rnm!r} "
                "(not found in key_extraction.config.data.extraction_rules)"
            )
    return errors


def collect_source_view_to_extraction_pairs_from_canvas_dict(
    canvas: Mapping[str, Any],
) -> List[Tuple[int, str]]:
    """
    Parse a serialized ``WorkflowCanvasDocument`` (nodes + edges) into association pairs.

    Used by ``scripts/compile_canvas_associations.py`` (Python-side compile step).
    """
    nodes_raw = canvas.get("nodes")
    edges_raw = canvas.get("edges")
    if not isinstance(nodes_raw, list) or not isinstance(edges_raw, list):
        return []

    by_id: Dict[str, Dict[str, Any]] = {}
    for n in nodes_raw:
        if isinstance(n, dict) and n.get("id") is not None:
            by_id[str(n["id"])] = n

    def is_data_edge(e: Mapping[str, Any]) -> bool:
        k = e.get("kind")
        return k not in ("sequence", "parallel_group")

    def ref_num(data: Any, key: str) -> Optional[int]:
        if not isinstance(data, dict):
            return None
        ref = data.get("ref")
        if not isinstance(ref, dict):
            return None
        v = ref.get(key)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return int(v) if float(v).is_integer() else None
        if isinstance(v, str) and v.strip():
            try:
                return int(float(v))
            except ValueError:
                return None
        return None

    def ref_str(data: Any, key: str) -> Optional[str]:
        if not isinstance(data, dict):
            return None
        ref = data.get("ref")
        if not isinstance(ref, dict):
            return None
        v = ref.get(key)
        if v is None:
            return None
        s = str(v).strip()
        return s or None

    out: List[Tuple[int, str]] = []
    seen: Set[str] = set()
    for e in edges_raw:
        if not isinstance(e, dict) or not is_data_edge(e):
            continue
        src = by_id.get(str(e.get("source") or ""))
        tgt = by_id.get(str(e.get("target") or ""))
        if not src or not tgt:
            continue
        if str(src.get("kind") or "") != "source_view":
            continue
        if str(tgt.get("kind") or "") != "extraction":
            continue
        data_s = src.get("data")
        data_t = tgt.get("data")
        sv_idx = ref_num(data_s, "source_view_index")
        rule_name = ref_str(data_t, "extraction_rule_name")
        if sv_idx is None or sv_idx < 0 or not rule_name:
            continue
        k = f"{sv_idx}\0{rule_name}"
        if k in seen:
            continue
        seen.add(k)
        out.append((sv_idx, rule_name))
    out.sort(key=lambda t: (t[0], t[1]))
    return out


def merge_source_view_to_extraction_rows_into_doc(
    doc: MutableMapping[str, Any],
    pairs: List[Tuple[int, str]],
) -> None:
    """Replace ``source_view_to_extraction`` rows; keep other association kinds."""
    if not pairs and "associations" not in doc:
        return
    raw = doc.get("associations")
    other: List[Any] = []
    if isinstance(raw, list):
        for x in raw:
            if not isinstance(x, dict):
                other.append(x)
                continue
            if str(x.get("kind") or "").strip() != KIND_SOURCE_VIEW_TO_EXTRACTION:
                other.append(x)
    rows = [
        {
            "kind": KIND_SOURCE_VIEW_TO_EXTRACTION,
            "source_view_index": int(i),
            "extraction_rule_name": str(n).strip(),
        }
        for i, n in pairs
    ]
    doc["associations"] = other + rows


def apply_canvas_dict_to_scope_associations(
    canvas: Mapping[str, Any],
    scope_doc: MutableMapping[str, Any],
) -> None:
    """Update *scope_doc* ``associations`` from canvas edges (source_view → extraction)."""
    pairs = collect_source_view_to_extraction_pairs_from_canvas_dict(canvas)
    merge_source_view_to_extraction_rows_into_doc(scope_doc, pairs)
