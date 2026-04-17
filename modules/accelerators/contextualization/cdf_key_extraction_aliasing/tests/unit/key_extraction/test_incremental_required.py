"""CDF key extraction requires incremental_change_processing (cohort handoff)."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.pipeline import (  # noqa: E402
    require_incremental_change_processing_for_cdf,
)


def test_require_incremental_raises_when_explicitly_false() -> None:
    cfg = SimpleNamespace(
        parameters=SimpleNamespace(incremental_change_processing=False)
    )
    with pytest.raises(ValueError, match="incremental_change_processing"):
        require_incremental_change_processing_for_cdf(cfg)


def test_require_incremental_ok_when_true() -> None:
    cfg = SimpleNamespace(
        parameters=SimpleNamespace(incremental_change_processing=True)
    )
    require_incremental_change_processing_for_cdf(cfg)


def test_require_incremental_ok_when_omitted_matches_pydantic_default() -> None:
    """Missing flag matches Parameters default (true)."""
    cfg = SimpleNamespace(parameters=SimpleNamespace())
    require_incremental_change_processing_for_cdf(cfg)
