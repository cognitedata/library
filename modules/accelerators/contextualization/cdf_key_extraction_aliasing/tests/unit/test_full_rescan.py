"""Tests for cdf_fn_common.resolve_full_rescan."""

from types import SimpleNamespace

import pytest

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.full_rescan import (
    resolve_full_rescan,
)


@pytest.mark.parametrize(
    "params_val,data,expected",
    [
        (False, None, False),
        (True, None, True),
        (False, {}, False),
        (True, {}, True),
        (False, {"full_rescan": True}, True),
        (True, {"full_rescan": False}, False),
    ],
)
def test_resolve_full_rescan(params_val, data, expected):
    params = SimpleNamespace(full_rescan=params_val)
    assert resolve_full_rescan(params, data) is expected
