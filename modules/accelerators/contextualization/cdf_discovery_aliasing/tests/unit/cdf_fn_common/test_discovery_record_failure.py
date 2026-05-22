"""Tests for discovery_record_failure."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
for _p in (str(_FUNCS), str(_MODULE_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from cdf_fn_common.discovery_record_failure import (  # noqa: E402
    EntityFailureRecorder,
    mark_cohort_entity_failed,
    record_entity_processing_failure,
)
from cdf_fn_common.incremental_scope import (  # noqa: E402
    NODE_INSTANCE_ID_COLUMN,
    WORKFLOW_STATUS_COLUMN,
    WORKFLOW_STATUS_FAILED,
)


def test_mark_cohort_entity_failed_sets_status() -> None:
    client = MagicMock()
    cols = {NODE_INSTANCE_ID_COLUMN: "sp:abc", "ENTITY": "x"}
    mark_cohort_entity_failed(client, "db", "tbl", "rk1", cols)
    client.raw.rows.insert.assert_called_once()
    _args, kwargs = client.raw.rows.insert.call_args
    row = kwargs.get("row") or _args[2] if len(_args) > 2 else kwargs["row"]
    assert row["rk1"][WORKFLOW_STATUS_COLUMN] == WORKFLOW_STATUS_FAILED


def test_record_entity_processing_failure_increments_attempt() -> None:
    client = MagicMock()
    recorder = EntityFailureRecorder(
        client=client,
        raw_db="db",
        raw_table="tbl",
        kd_backend=None,
        attempt_by_node={"sp:n1": 2},
    )
    cols = {NODE_INSTANCE_ID_COLUMN: "sp:n1", "EXTERNAL_ID": "ext1"}
    assert record_entity_processing_failure(
        recorder, row_key="rk", cols=cols, error_message="transform err"
    )
    assert recorder.entities_failed == 1
    client.raw.rows.insert.assert_called()
