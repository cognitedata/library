"""Unit tests for transient Cognite API retry."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.cognite_retry import (  # noqa: E402
    call_with_transient_retry,
    is_transient_cognite_error,
)


def test_is_transient_cognite_error_503() -> None:
    try:
        from cognite.client.exceptions import CogniteAPIError
    except ImportError:
        pytest.skip("cognite SDK not installed")

    ex = CogniteAPIError("Service unavailable", code=503)
    assert is_transient_cognite_error(ex) is True


def test_call_with_transient_retry_succeeds_after_503() -> None:
    try:
        from cognite.client.exceptions import CogniteAPIError
    except ImportError:
        pytest.skip("cognite SDK not installed")

    calls = {"n": 0}

    def _fn() -> str:
        calls["n"] += 1
        if calls["n"] < 2:
            raise CogniteAPIError("Service unavailable", code=503)
        return "ok"

    assert call_with_transient_retry(_fn, max_attempts=3, base_delay_sec=0.01) == "ok"
    assert calls["n"] == 2
