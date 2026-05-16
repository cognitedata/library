"""Tests for cdf_fn_common.resolve_run_all."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.run_all import (
    resolve_run_all,
)


@pytest.mark.parametrize(
    "params_val,data,expected",
    [
        (False, None, False),
        (True, None, True),
        (False, {}, False),
        (True, {}, True),
        (False, {"run_all": True}, True),
        (True, {"run_all": False}, False),
    ],
)
def test_resolve_run_all(params_val, data, expected):
    params = SimpleNamespace(run_all=params_val)
    assert resolve_run_all(params, data) is expected
