"""Kuiper evaluation for jsonMapping (cognite-kuiper)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TRANSFORM_ROOT = Path(__file__).resolve().parents[1]
_FUNCTIONS = _TRANSFORM_ROOT / "functions"
for p in (_TRANSFORM_ROOT, _FUNCTIONS):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from cdf_fn_common.kuiper_eval import (  # noqa: E402
    evaluate_kuiper_expression,
    kuiper_available,
)
from cdf_fn_common.etl_annotation_map.kuiper_templates import (  # noqa: E402
    run_json_mapping_kuiper,
)


pytestmark = pytest.mark.skipif(not kuiper_available(), reason="cognite-kuiper not installed")


def test_evaluate_kuiper_expression_arithmetic():
    out = evaluate_kuiper_expression(
        '{"theAnswer": input.numericValue + 27}',
        {"numericValue": 15},
    )
    assert out == {"theAnswer": 42}


def test_run_json_mapping_passthrough_rows():
    cfg = {"mapper_kind": "custom", "expression": "input.rows"}
    out = run_json_mapping_kuiper(cfg, {"rows": [{"id": 1}]})
    assert out == [{"id": 1}]
