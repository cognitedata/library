"""Tests for compiled-task merge into handler ``data``."""

from __future__ import annotations

import sys
from pathlib import Path

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.task_runtime import merge_compiled_task_into_data  # noqa: E402


def test_merge_payload_config_replaces_empty_dict() -> None:
    """Empty ``data["config"]`` must not block canvas ``payload.config`` (incremental flags, view ids)."""
    data: dict = {
        "task_id": "kea__vq",
        "compiled_workflow": {
            "tasks": [
                {
                    "id": "kea__vq",
                    "payload": {
                        "config": {
                            "view_external_id": "CogniteAsset",
                            "incremental_change_processing": True,
                        }
                    },
                }
            ]
        },
        "config": {},
    }
    merge_compiled_task_into_data(data)
    assert data["config"]["view_external_id"] == "CogniteAsset"
    assert data["config"]["incremental_change_processing"] is True
