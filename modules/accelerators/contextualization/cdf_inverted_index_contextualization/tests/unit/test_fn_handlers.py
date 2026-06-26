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
