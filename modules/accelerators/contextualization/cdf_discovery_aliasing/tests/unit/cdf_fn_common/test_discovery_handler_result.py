"""Tests for discovery_handler_result."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
for _p in (str(_FUNCS), str(_MODULE_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from cdf_fn_common.discovery_handler_result import (  # noqa: E402
    DiscoveryPipelineError,
    apply_handler_output,
    discovery_handler_failure,
    discovery_handler_success,
    run_discovery_handler,
)


def test_discovery_handler_success_sets_data() -> None:
    data: dict = {}
    out = discovery_handler_success("fn_test", data, {"ok": True})
    assert out["status"] == "succeeded"
    assert data["status"] == "succeeded"
    assert "ok" in data["message"]


def test_discovery_handler_failure_raises() -> None:
    data: dict = {}
    with pytest.raises(DiscoveryPipelineError, match="fn_test failed"):
        discovery_handler_failure("fn_test", data, "boom")
    assert data["status"] == "failure"


def test_apply_handler_output_raises_on_failure() -> None:
    data: dict = {}
    with pytest.raises(DiscoveryPipelineError):
        apply_handler_output({"status": "failure", "message": "x"}, data)


def test_run_discovery_handler_raises_from_impl() -> None:
    data: dict = {}

    def _boom(_d, _c, _log):
        raise ValueError("bad config")

    with pytest.raises(DiscoveryPipelineError, match="fn_x failed"):
        run_discovery_handler("fn_x", data, object(), _boom)
