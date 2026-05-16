"""Tests for ``output_template`` substitution and delimiter cleanup."""

from __future__ import annotations

import sys
from pathlib import Path

from typing import Mapping

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from fn_dm_transform.engine.field_template import apply_output_template  # noqa: E402


@pytest.mark.parametrize(
    ("template", "field_values", "expected"),
    [
        (
            "{unit}-{tag}",
            {"unit": "", "tag": "P-1234"},
            "P-1234",
        ),
        (
            "{unit}-{tag}",
            {"unit": None, "tag": "P-1234"},
            "P-1234",
        ),
        (
            "{a}-{b}",
            {"a": "A", "b": ""},
            "A",
        ),
        (
            "{a}-{b}-{c}",
            {"a": "x", "b": "", "c": "y"},
            "x-y",
        ),
        (
            "{a}-{b}",
            {"a": "", "b": ""},
            "",
        ),
        (
            "prefix-{x}-suffix",
            {"x": "mid"},
            "prefix-mid-suffix",
        ),
        (
            "{a}__{b}",
            {"a": "", "b": "z"},
            "z",
        ),
    ],
)
def test_apply_output_template_strips_extraneous_delimiters(
    template: str,
    field_values: Mapping[str, object],
    expected: str,
) -> None:
    assert apply_output_template(template, field_values) == expected
