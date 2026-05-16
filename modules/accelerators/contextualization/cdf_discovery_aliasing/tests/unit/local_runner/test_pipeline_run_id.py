"""Tests for stable ``ctx.run_id`` before local DAG execution."""

from __future__ import annotations

import logging
import sys
from argparse import Namespace
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
for _p in (str(_FUNCS), str(_MODULE_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from local_runner.kahn_run_context import KahnRunContext  # noqa: E402
from local_runner.run import _ensure_pipeline_run_id  # noqa: E402


def _minimal_ctx(*, run_id: str = "") -> KahnRunContext:
    return KahnRunContext(
        args=Namespace(run_all=False),
        logger=logging.getLogger("test_pipeline_run_id"),
        client=None,
        pipe_logger=None,
        scope_yaml_path=Path("/tmp/scope.yaml"),
        scope_document={},
        wf_instance_space="",
        source_views=[],
        cdf_config=None,
        compiled_workflow={},
        run_id=run_id,
    )


def test_ensure_pipeline_run_id_generates_when_empty() -> None:
    ctx = _minimal_ctx(run_id="")
    _ensure_pipeline_run_id(ctx)
    assert str(ctx.run_id).strip()
    assert ctx.run_id[8] == "T"  # UTC timestamp fragment from strftime


def test_ensure_pipeline_run_id_idempotent_when_set() -> None:
    ctx = _minimal_ctx(run_id="fixed_run_42")
    _ensure_pipeline_run_id(ctx)
    assert ctx.run_id == "fixed_run_42"


def test_ensure_pipeline_run_id_stable_after_first_assign() -> None:
    ctx = _minimal_ctx(run_id="")
    _ensure_pipeline_run_id(ctx)
    first = ctx.run_id
    _ensure_pipeline_run_id(ctx)
    assert ctx.run_id == first
