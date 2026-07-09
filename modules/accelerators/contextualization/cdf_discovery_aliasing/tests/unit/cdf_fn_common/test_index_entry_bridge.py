"""Unit tests for index_entry_bridge."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
_DISCOVERY_ROOT = _MODULE_ROOT.parent / "cdf_discovery"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))
os.environ.setdefault(
    "CDF_INVERTED_INDEX_ROOT",
    str(_DISCOVERY_ROOT / "inverted_index"),
)

from cdf_fn_common.index_entry_bridge import (  # noqa: E402
    format_match_scope_key,
    postings_to_index_entries,
    resolve_match_scope_key_from_workflow,
    resolve_source_type,
    scope_dict_from_workflow_scope,
)


def test_resolve_source_type_file_metadata() -> None:
    assert resolve_source_type("metadata", "CogniteFile") == "file_metadata"
    assert resolve_source_type("metadata", "CogniteAsset") == "asset_metadata"


def test_scope_dict_from_workflow_scope_unit() -> None:
    assert scope_dict_from_workflow_scope("SITE_02__UNIT_A") == {
        "site": "SITE_02",
        "unit": "UNIT_A",
    }


def test_format_match_scope_key_template() -> None:
    scope_dict = {"site": "SITE_01"}
    cfg = {"scope_key_template": "site:{site}|unit:{unit}", "fallback_scope_key": "global"}
    assert format_match_scope_key(scope_dict, cfg) == "site:SITE_01"


def test_resolve_match_scope_key_from_configuration_path() -> None:
    data = {
        "configuration": {
            "scope": {
                "path": [
                    {"level": "site", "id": "SITE_01"},
                ]
            }
        }
    }
    scope_cfg = {"scope_key_template": "site:{site}", "fallback_scope_key": "global"}
    key, scope = resolve_match_scope_key_from_workflow(data, scope_cfg)
    assert key == "site:SITE_01"
    assert scope == {"site": "SITE_01"}


def test_postings_to_index_entries_shape() -> None:
    entries = postings_to_index_entries(
        [
            {
                "term": "P-101A",
                "external_id": "ext1",
                "instance_space": "sp",
                "view_external_id": "CogniteAsset",
                "source_property": "indexKey",
                "confidence": 0.9,
                "run_id": "run1",
            }
        ],
        lookup_key="p-101a",
        index_kind="metadata",
        match_scope_key="site:SITE_01",
        match_scope={"site": "SITE_01"},
        build_job_id="run1",
    )
    assert len(entries) == 1
    entry = entries[0]
    assert entry["source_type"] == "asset_metadata"
    assert entry["reference_external_id"] == "ext1"
    assert entry["match_scope_key"] == "site:SITE_01"
    assert entry["normalized_term"] == "p101a"
    assert entry["additional_metadata"]["confidence"] == 0.9
