"""Scheduled incremental index builds with persisted watermark cursor."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from inverted_index.build import build_diagram_annotation_index, build_metadata_index
from inverted_index.config import SOURCE_INDEX_CONFIG
from inverted_index.config_loader import build_runtime_config
from inverted_index.raw_ops import create_table_if_not_exists
from inverted_index.storage import get_storage_adapter

logger = logging.getLogger(__name__)

_WATERMARK_ROW_KEY = "global"


def _watermark_cfg(runtime_config: dict | None) -> dict:
    runtime = runtime_config or build_runtime_config()
    src_cfg = runtime.get("source_index_config") or SOURCE_INDEX_CONFIG
    return src_cfg.get("watermark") or {}


def read_watermark(client: Any, cfg: dict | None = None) -> datetime | None:
    if not client:
        return None
    resolved = cfg or {}
    raw_db = resolved.get("raw_database", "db_contextualization_idx")
    table = resolved.get("state_table", "index_build_state")
    try:
        row = client.raw.rows.retrieve(raw_db, table, _WATERMARK_ROW_KEY)
        cols = getattr(row, "columns", None) or {}
        value = cols.get("WATERMARK_AT")
        if not value:
            return None
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def write_watermark(client: Any, watermark: datetime, cfg: dict | None = None) -> None:
    if not client:
        return
    resolved = cfg or {}
    raw_db = resolved.get("raw_database", "db_contextualization_idx")
    table = resolved.get("state_table", "index_build_state")
    create_table_if_not_exists(client, raw_db, table)
    client.raw.rows.insert(
        db_name=raw_db,
        table_name=table,
        row={
            _WATERMARK_ROW_KEY: {
                "RECORD_KIND": "index_build_watermark",
                "WATERMARK_AT": watermark.astimezone(timezone.utc).isoformat(),
            }
        },
    )


def run_watermark_incremental_build(
    client: Any,
    *,
    dry_run: bool = False,
    runtime_config: dict | None = None,
    force_full_lookback: bool = False,
) -> dict:
    runtime = runtime_config or build_runtime_config()
    src_cfg = runtime.get("source_index_config") or SOURCE_INDEX_CONFIG
    if not src_cfg.get("enabled", True):
        return {"status": "skipped", "reason": "source_index_disabled"}

    wm_cfg = _watermark_cfg(runtime)
    if not wm_cfg.get("enabled", True):
        return {"status": "skipped", "reason": "watermark_disabled"}

    now = datetime.now(timezone.utc)
    watermark_before = None if force_full_lookback else read_watermark(client, wm_cfg)
    if watermark_before is None:
        lookback = int(wm_cfg.get("initial_lookback_seconds", 3600))
        watermark_before = now - timedelta(seconds=lookback)

    storage_cfg = runtime.get("storage_config") or {}
    storage_adapter = None
    if not dry_run and client is not None:
        storage_adapter = get_storage_adapter(storage_cfg, client)

    metadata_result = build_metadata_index(
        client,
        index_field_config=runtime.get("index_field_config"),
        scope_config=runtime.get("scope_config"),
        storage_config=storage_cfg,
        instance_spaces=runtime.get("instance_spaces"),
        filter_updated_after=watermark_before,
        dry_run=dry_run,
        storage_adapter=storage_adapter,
        virtual_tag_creation_config=runtime.get("virtual_tag_creation_config"),
    )

    annotation_result = build_diagram_annotation_index(
        client,
        annotation_config=runtime.get("annotation_index_config"),
        scope_config=runtime.get("scope_config"),
        storage_config=storage_cfg,
        instance_spaces=runtime.get("instance_spaces"),
        filter_updated_after=watermark_before,
        dry_run=dry_run,
        storage_adapter=storage_adapter,
        virtual_tag_creation_config=runtime.get("virtual_tag_creation_config"),
    )

    watermark_after = now
    if not dry_run and client:
        write_watermark(client, watermark_after, wm_cfg)

    return {
        "status": "ok",
        "trigger": "watermark_incremental",
        "dry_run": dry_run,
        "watermark_before": watermark_before.isoformat(),
        "watermark_after": watermark_after.isoformat(),
        "metadata": metadata_result,
        "annotations": annotation_result,
    }
