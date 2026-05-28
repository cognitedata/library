from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.etl_transform.row_pipeline import validate_transform_config  # noqa: E402
from cdf_fn_common.etl_transform.transform_steps import (  # noqa: E402
    validate_transform_pipeline_config,
)


def test_validate_transform_config_requires_output_field() -> None:
    with pytest.raises(ValueError, match="output_field is required"):
        validate_transform_config(
            {
                "handler_id": "trim_whitespace",
                "trim_whitespace": {"mode": "ends_only"},
                "fields": [{"field_name": "name"}],
            }
        )


def test_validate_transform_pipeline_config_checks_each_step() -> None:
    with pytest.raises(ValueError, match=r"step\[1\].*output_field is required"):
        validate_transform_pipeline_config(
            {
                "execution": {"mode": "ordered"},
                "steps": [
                    {
                        "handler_id": "trim_whitespace",
                        "trim_whitespace": {"mode": "ends_only"},
                        "output_field": "draft",
                        "fields": [{"field_name": "name"}],
                    },
                    {
                        "handler_id": "trim_whitespace",
                        "trim_whitespace": {"mode": "ends_only"},
                        "fields": [{"field_name": "name"}],
                    },
                ],
            }
        )
