from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[2]
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)


def test_fn_idx_build_metadata_handler_dry_run() -> None:
    from fn_idx_build_metadata.handler import handle

    result = handle({"dry_run": True}, client=MagicMock())
    assert "processed" in result
    assert result.get("entries_created", 0) >= 0


def test_fn_idx_target_driven_requires_instance_id() -> None:
    from fn_idx_target_driven.handler import handle

    result = handle({}, client=MagicMock())
    assert result.get("error") == "instance_external_id or instance_external_ids is required"


def test_fn_idx_score_requires_file_id() -> None:
    from fn_idx_score.handler import handle

    result = handle({}, client=MagicMock())
    assert result.get("error") == "file_external_id is required"


def test_fn_idx_deltas_both_modes() -> None:
    from fn_idx_deltas.handler import handle

    client = MagicMock()
    with __import__("unittest.mock", fromlist=["patch"]).patch(
        "fn_idx_deltas.handler.get_pattern_not_in_standard_delta",
        return_value=[{"term": "A"}],
    ), __import__("unittest.mock", fromlist=["patch"]).patch(
        "fn_idx_deltas.handler.get_standard_not_in_pattern_delta",
        return_value=[{"term": "B"}],
    ):
        result = handle(
            {"file_external_id": "FILE_1", "delta_mode": "both"},
            client=client,
        )
    assert "missing_tags" in result
    assert "pattern_feedback" in result


def test_fn_idx_upsert_detections_dry_run() -> None:
    from fn_idx_upsert_detections.handler import handle

    result = handle(
        {
            "dry_run": True,
            "detection_mode": "pattern",
            "file_external_id": "FILE_1",
            "detections": [
                {
                    "file_external_id": "FILE_1",
                    "text": "P-101A",
                    "page": 1,
                    "bbox": [0.1, 0.2, 0.3, 0.4],
                }
            ],
        },
        client=MagicMock(),
    )
    assert result.get("candidate_entries", 0) >= 1
    assert result.get("write_mode") == "replace"


def test_fn_idx_index_metadata_instance_requires_instance_id() -> None:
    from fn_idx_index_metadata_instance.handler import handle

    result = handle({"view_external_id": "CogniteEquipment"}, client=MagicMock())
    assert result.get("error") == "instance_external_id or instance_external_ids is required"


def test_fn_idx_index_metadata_instance_requires_view() -> None:
    from fn_idx_index_metadata_instance.handler import handle

    result = handle({"instance_external_id": "EQ-1"}, client=MagicMock())
    assert result.get("error") == "view_external_id or incoming_view_key is required"


def test_fn_idx_index_metadata_instance_multiple_ids() -> None:
    from fn_idx_index_metadata_instance.handler import handle

    with __import__("unittest.mock", fromlist=["patch"]).patch(
        "fn_idx_index_metadata_instance.handler.build_metadata_index_for_instance_ids",
        return_value={"processed": 2, "instance_external_ids": ["EQ-1", "EQ-2"]},
    ) as batch:
        result = handle(
            {
                "dry_run": True,
                "instance_external_ids": ["EQ-1", "EQ-2"],
                "view_external_id": "CogniteEquipment",
            },
            client=MagicMock(),
        )
    batch.assert_called_once()
    assert result["processed"] == 2


def test_fn_idx_virtual_tags_requires_scope() -> None:
    from fn_idx_virtual_tags.handler import handle

    with __import__("unittest.mock", fromlist=["patch"]).patch(
        "fn_idx_virtual_tags.handler.run_virtual_tag_creation",
        side_effect=ValueError("virtual-tags requires --all-scopes or at least one --scope-key"),
    ):
        result = handle({"dry_run": True}, client=MagicMock())
    assert "error" in result


def test_fn_idx_virtual_tags_dry_run() -> None:
    from fn_idx_virtual_tags.handler import handle

    with __import__("unittest.mock", fromlist=["patch"]).patch(
        "fn_idx_virtual_tags.handler.run_virtual_tag_creation",
        return_value={"terms_processed": 1, "leaf_assets": 1, "dry_run": True},
    ):
        result = handle(
            {"dry_run": True, "all_scopes": True},
            client=MagicMock(),
        )
    assert result["terms_processed"] == 1
