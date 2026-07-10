"""Unit tests for deployed diagram jsonMapping orchestration."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_TRANSFORM_ROOT = Path(__file__).resolve().parents[2]
_FUNCTIONS = _TRANSFORM_ROOT / "functions"
_SHARED = _TRANSFORM_ROOT.parent.parent / "shared"
for p in (_TRANSFORM_ROOT, _FUNCTIONS, _SHARED):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from cdf_fn_common.etl_json_mapping_orchestration import etl_handle_json_mapping  # noqa: E402


@patch("cdf_fn_common.etl_json_mapping_orchestration.materialize_json_mapping_output_to_cohort")
@patch("cdf_fn_common.etl_json_mapping_orchestration.run_json_mapping_kuiper")
@patch("cdf_fn_common.etl_json_mapping_orchestration.prepare_local_json_mapping_input")
@patch("cdf_fn_common.etl_json_mapping_orchestration.require_pipeline_run_key")
def test_json_mapping_orchestration_materializes_diagram_rows(
    mock_run_id: MagicMock,
    mock_prepare: MagicMock,
    mock_kuiper: MagicMock,
    mock_materialize: MagicMock,
) -> None:
    mock_run_id.return_value = "run-1"
    mock_prepare.return_value = {"rows": [{"annotation_external_id": "f1_p1_0"}]}
    mock_kuiper.return_value = [{"annotation_external_id": "f1_p1_0"}]
    mock_materialize.return_value = 1

    client = MagicMock()
    data = {
        "task_id": "map_annotations_dm",
        "config": {
            "mapper_kind": "diagram_detect_to_dm",
            "annotation_space": "discovery-annotations",
            "input": {"rows": "${finalize_annotations.output}"},
        },
        "source_task_id": "completion_barrier",
    }
    summary = etl_handle_json_mapping("fn_discovery_etl_json_mapping", data, client, None)

    assert summary["status"] == "ok"
    assert summary["cohort_rows_written"] == 1
    assert summary["source_task_id"] == "finalize_annotations"
    mock_prepare.assert_called_once()
    assert mock_prepare.call_args.kwargs["source_task_id"] == "finalize_annotations"
    mock_materialize.assert_called_once()
