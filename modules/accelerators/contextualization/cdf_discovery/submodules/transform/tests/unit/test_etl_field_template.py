"""Tests for ``output_template`` substitution and delimiter cleanup."""

from __future__ import annotations

from typing import Mapping

import pytest

from cdf_fn_common.etl_transform.field_template import apply_output_template


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
