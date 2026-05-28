"""Expand cohort row lists to staging tables."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping

from cdf_fn_common.etl_annotation_map.classic_rows import classic_annotation_row_from_hit
from cdf_fn_common.etl_annotation_map.cohort_hit import read_detect_hit_from_cohort_row
from cdf_fn_common.etl_annotation_map.dm_rows import dm_annotation_row_from_hit


def expand_cohort_rows_to_dm_rows(
    rows: List[Mapping[str, Any]],
    cfg: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    space = str(cfg.get("annotation_space") or "discovery-annotations")
    status = str(cfg.get("default_status") or "Suggested")
    out: List[Dict[str, Any]] = []
    for row in rows:
        cols = row.get("columns") if isinstance(row.get("columns"), dict) else {}
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        hit = read_detect_hit_from_cohort_row(cols, props)
        if hit is None:
            continue
        out.extend(
            dm_annotation_row_from_hit(hit, annotation_space=space, default_status=status)
        )
    return out


def expand_cohort_rows_to_classic_rows(
    rows: List[Mapping[str, Any]],
    cfg: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        cols = row.get("columns") if isinstance(row.get("columns"), dict) else {}
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        hit = read_detect_hit_from_cohort_row(cols, props)
        if hit is None:
            continue
        out.append(classic_annotation_row_from_hit(hit))
    return out
