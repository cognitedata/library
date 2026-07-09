#!/usr/bin/env python3
"""One-shot migration: modular submodules layout + fn_discovery_{module}_* renames."""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

MODULE_ROOT = Path(__file__).resolve().parents[1]

FN_RENAMES: dict[str, str] = {
    "fn_etl_view_query": "fn_discovery_etl_view_query",
    "fn_etl_raw_query": "fn_discovery_etl_raw_query",
    "fn_etl_classic_query": "fn_discovery_etl_classic_query",
    "fn_etl_records_query": "fn_discovery_etl_records_query",
    "fn_etl_sql_query": "fn_discovery_etl_sql_query",
    "fn_etl_transform": "fn_discovery_etl_transform",
    "fn_etl_filter": "fn_discovery_etl_filter",
    "fn_etl_score": "fn_discovery_etl_score",
    "fn_etl_join": "fn_discovery_etl_join",
    "fn_etl_merge": "fn_discovery_etl_merge",
    "fn_etl_build_index": "fn_discovery_etl_build_index",
    "fn_etl_view_save": "fn_discovery_etl_view_save",
    "fn_etl_raw_save": "fn_discovery_etl_raw_save",
    "fn_etl_records_save": "fn_discovery_etl_records_save",
    "fn_etl_stream_save": "fn_discovery_etl_stream_save",
    "fn_etl_classic_save": "fn_discovery_etl_classic_save",
    "fn_etl_raw_cleanup": "fn_discovery_etl_raw_cleanup",
    "fn_etl_workflow_fanout_plan": "fn_discovery_etl_fanout_plan",
    "fn_etl_file_annotation": "fn_discovery_etl_file_annotation",
    "fn_etl_file_annotation_launch": "fn_discovery_etl_file_annotation_launch",
    "fn_etl_file_annotation_finalize": "fn_discovery_etl_file_annotation_finalize",
    "fn_etl_file_annotation_barrier": "fn_discovery_etl_file_annotation_barrier",
    "fn_idx_build_metadata": "fn_discovery_idx_build_metadata",
    "fn_idx_build_annotations": "fn_discovery_idx_build_annotations",
    "fn_idx_target_driven": "fn_discovery_idx_target_driven",
    "fn_idx_handle_subscription": "fn_discovery_idx_handle_subscription",
    "fn_idx_handle_source_metadata": "fn_discovery_idx_handle_source_metadata",
    "fn_idx_build_watermark_incremental": "fn_discovery_idx_build_watermark_incremental",
    "fn_idx_score": "fn_discovery_idx_score",
    "fn_idx_deltas": "fn_discovery_idx_deltas",
    "fn_idx_upsert_detections": "fn_discovery_idx_upsert_detections",
    "fn_idx_index_metadata_instance": "fn_discovery_idx_index_metadata_instance",
    "fn_idx_virtual_tags": "fn_discovery_idx_virtual_tags",
}

SKIP_DIRS = {".git", "node_modules", "__pycache__", ".cache", "build"}
TEXT_EXTENSIONS = {".py", ".yaml", ".yml", ".md", ".ts", ".tsx", ".json", ".toml", ".txt", ".ini", ".css", ".html"}


def _replace_in_file(path: Path) -> bool:
    if path.suffix not in TEXT_EXTENSIONS:
        return False
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return False
    original = text
    for old_id, new_id in sorted(FN_RENAMES.items(), key=lambda x: -len(x[0])):
        text = text.replace(old_id, new_id)
    if text != original:
        path.write_text(text, encoding="utf-8")
        return True
    return False


def main() -> int:
    count = 0
    for path in MODULE_ROOT.rglob("*"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.is_file() and _replace_in_file(path):
            count += 1
    print(f"renamed references in {count} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
