"""Tests for workflow_task_policy."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
for _p in (str(_FUNCS), str(_MODULE_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from cdf_fn_common.workflow_compile.codegen import build_workflow_version_document  # noqa: E402
from cdf_fn_common.workflow_task_policy import (  # noqa: E402
    DEFAULT_TASK_RETRIES,
    discovery_task_workflow_policy,
)


def test_cleanup_uses_skip_task() -> None:
    pol = discovery_task_workflow_policy("fn_dm_discovery_raw_cleanup")
    assert pol["onFailure"] == "skipTask"
    assert pol["retries"] == DEFAULT_TASK_RETRIES


def test_query_uses_abort_workflow() -> None:
    pol = discovery_task_workflow_policy("fn_dm_view_query")
    assert pol["onFailure"] == "abortWorkflow"


def test_codegen_cleanup_task_has_skip_task() -> None:
    cw = {
        "tasks": [
            {
                "id": "kea__discovery_raw_cleanup",
                "function_external_id": "fn_dm_discovery_raw_cleanup",
                "depends_on": ["kea__ii"],
            }
        ]
    }
    doc = build_workflow_version_document(
        workflow_external_id="wf_test",
        version="v5",
        compiled_workflow=cw,
    )
    tasks = doc["workflowDefinition"]["tasks"]
    cleanup = next(t for t in tasks if t["externalId"] == "kea__discovery_raw_cleanup")
    assert cleanup["onFailure"] == "skipTask"
    assert cleanup["retries"] == 3
