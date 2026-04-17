"""Tests for run_pipeline incremental requirements and logging adapters."""

from __future__ import annotations

import argparse
import logging
from unittest.mock import MagicMock

import pytest

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.cdf_adapter import (
    _DEFAULT_ALIASING_VALIDATION,
)

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.local_runner.run import (
    run_pipeline,
)


def _args(**overrides: object) -> argparse.Namespace:
    base = dict(
        limit=100,
        dry_run=True,
        write_foreign_keys=False,
        foreign_key_writeback_property=None,
        run_all=False,
        skip_reference_index=False,
        progress_every=0,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_run_pipeline_requires_scope_yaml_path():
    """Direct view listing was removed; local CLI always uses workflow parity with a scope file."""
    client = MagicMock()
    extraction_config: dict = {
        "parameters": {},
        "extraction_rules": [],
        "validation": {"min_confidence": 0.1},
    }
    aliasing_config: dict = {
        "rules": [],
        "validation": dict(_DEFAULT_ALIASING_VALIDATION),
    }
    source_views = [
        {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
            "entity_type": "asset",
            "batch_size": 10,
        }
    ]
    log = logging.getLogger("test_run_pipeline_incremental")
    log.setLevel(logging.INFO)
    with pytest.raises(ValueError, match="scope YAML path"):
        run_pipeline(
            _args(progress_every=0),
            log,
            client,
            extraction_config,
            aliasing_config,
            source_views,
            None,
            False,
            None,
            scope_yaml_path=None,
        )


def test_progress_every_parsed_on_namespace():
    """module.py uses argparse dest progress_every for --progress-every."""
    p = argparse.ArgumentParser()
    p.add_argument("--progress-every", type=int, default=0)
    ns = p.parse_args(["--progress-every", "50"])
    assert ns.progress_every == 50
