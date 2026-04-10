"""Tests for non-incremental run_pipeline logging (adapter injection, progress flag)."""

from __future__ import annotations

import argparse
import logging
from unittest.mock import MagicMock, patch

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.cdf_adapter import (
    _DEFAULT_ALIASING_VALIDATION,
)

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.function_logging import (
    StdlibLoggerAdapter,
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
        full_rescan=False,
        skip_reference_index=False,
        progress_every=0,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_non_incremental_passes_stdlib_adapter_to_both_engines():
    """Engines should receive the same StdlibLoggerAdapter as the CLI logger bridge."""
    captured: list[tuple[str, object]] = []

    def ke_ctor(*_a: object, **kw: object):
        captured.append(("ke", kw.get("logger")))
        m = MagicMock()
        m.extract_keys.return_value = MagicMock(
            entity_id="e1",
            candidate_keys=(),
            foreign_key_references=(),
            document_references=(),
            entity_type="asset",
            metadata={},
        )
        return m

    def ae_ctor(*_a: object, **kw: object):
        captured.append(("ae", kw.get("logger")))
        m = MagicMock()
        m.generate_aliases.return_value = MagicMock(aliases=[], metadata={})
        return m

    client = MagicMock()
    client.data_modeling.instances.list.return_value = []

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

    log = logging.getLogger("test_non_incremental_logging")
    log.setLevel(logging.INFO)

    with (
        patch(
            "modules.accelerators.contextualization.cdf_key_extraction_aliasing.local_runner.run.KeyExtractionEngine",
            side_effect=ke_ctor,
        ),
        patch(
            "modules.accelerators.contextualization.cdf_key_extraction_aliasing.local_runner.run.AliasingEngine",
            side_effect=ae_ctor,
        ),
    ):
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

    assert len(captured) == 2
    assert captured[0][0] == "ke"
    assert captured[1][0] == "ae"
    ke_log, ae_log = captured[0][1], captured[1][1]
    assert isinstance(ke_log, StdlibLoggerAdapter)
    assert isinstance(ae_log, StdlibLoggerAdapter)
    assert ke_log is ae_log


def test_progress_every_parsed_on_namespace():
    """module.py uses argparse dest progress_every for --progress-every."""
    p = argparse.ArgumentParser()
    p.add_argument("--progress-every", type=int, default=0)
    ns = p.parse_args(["--progress-every", "50"])
    assert ns.progress_every == 50
