"""Tests for unique pipeline run id generation."""

from __future__ import annotations

import re
import sys
from pathlib import Path

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.discovery_query_shared import (  # noqa: E402
    new_pipeline_run_id,
    resolve_run_id,
)

_RUN_ID_RE = re.compile(
    r"^\d{8}T\d{6}\.\d{6}Z-[0-9a-f]{12}$"
)


def test_new_pipeline_run_id_format() -> None:
    rid = new_pipeline_run_id()
    assert _RUN_ID_RE.match(rid), rid


def test_new_pipeline_run_id_unique_on_rapid_calls() -> None:
    ids = {new_pipeline_run_id() for _ in range(100)}
    assert len(ids) == 100


def test_resolve_run_id_uses_generator_when_missing() -> None:
    rid = resolve_run_id({})
    assert _RUN_ID_RE.match(rid), rid


def test_resolve_run_id_preserves_explicit() -> None:
    assert resolve_run_id({"run_id": "operator-run-1"}) == "operator-run-1"
