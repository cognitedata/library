"""CDF handler: incremental diagram detection index writes."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.fn_runtime import (  # noqa: E402
    require_client,
    resolve_handler_payload,
    storage_adapter_for,
)
from inverted_index.incremental import upsert_diagram_detections  # noqa: E402


def handle(data: dict[str, Any] | None = None, client: Any = None) -> dict[str, Any]:
    resolved = resolve_handler_payload(data)
    payload = resolved["payload"]
    overrides = resolved["overrides"]
    client = require_client(client)
    adapter = storage_adapter_for(
        client,
        overrides["storage_config"],
        dry_run=overrides["dry_run"],
    )
    detection_mode = payload.get("detection_mode")
    if detection_mode is not None and detection_mode not in ("standard", "pattern"):
        return {"error": "detection_mode must be standard or pattern"}
    write_mode = str(payload.get("write_mode", "replace"))
    return upsert_diagram_detections(
        client,
        payload.get("detections") or [],
        detection_mode=detection_mode,
        write_mode=write_mode,  # type: ignore[arg-type]
        file_external_id=payload.get("file_external_id") or payload.get("file_id"),
        file_space=str(payload.get("file_space") or payload.get("space") or "cdf_cdm"),
        annotations=payload.get("annotations") or [],
        scope_config=overrides["scope_config"],
        storage_config=overrides["storage_config"],
        annotation_config=overrides["annotation_config"],
        dry_run=overrides["dry_run"],
        storage_adapter=adapter,
        virtual_tag_creation_config=overrides.get("virtual_tag_creation_config"),
    )
