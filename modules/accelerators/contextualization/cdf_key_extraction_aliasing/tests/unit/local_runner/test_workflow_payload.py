"""Tests for workflow-aligned local incremental payloads."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.local_runner.workflow_payload import (
    merged_scope_document_for_local_run,
    remap_associations_for_filtered_source_views,
    workflow_instance_space_for_local,
)


def test_remap_associations_after_source_view_filter(tmp_path: Path) -> None:
    """Filtered ``source_views`` reorders indices; associations must follow view identity."""
    scope = tmp_path / "scope.yaml"
    scope.write_text(
        yaml.dump(
            {
                "schemaVersion": 1,
                "source_views": [
                    {
                        "view_space": "s",
                        "view_external_id": "File",
                        "view_version": "v1",
                    },
                    {
                        "view_space": "s",
                        "view_external_id": "Asset",
                        "view_version": "v1",
                    },
                ],
                "associations": [
                    {
                        "kind": "source_view_to_extraction",
                        "source_view_index": 1,
                        "extraction_rule_name": "only_asset_rule",
                    }
                ],
                "key_extraction": {"config": {"data": {}}},
            }
        ),
        encoding="utf-8",
    )
    filtered = [
        {
            "view_space": "s",
            "view_external_id": "Asset",
            "view_version": "v1",
        },
    ]
    doc = merged_scope_document_for_local_run(scope, filtered)
    assert len(doc["source_views"]) == 1
    assert doc["associations"][0]["source_view_index"] == 0


def test_remap_associations_accepts_string_numeric_source_view_index() -> None:
    """JSON-style string indices must remap like integers (same view identity)."""
    original = [
        {"view_space": "s", "view_external_id": "A", "view_version": "v1"},
        {"view_space": "s", "view_external_id": "B", "view_version": "v1"},
    ]
    filtered = [
        {"view_space": "s", "view_external_id": "B", "view_version": "v1"},
    ]
    associations = [
        {
            "kind": "source_view_to_extraction",
            "source_view_index": "1",
            "extraction_rule_name": "r1",
        }
    ]
    out = remap_associations_for_filtered_source_views(
        associations, original, filtered
    )
    assert len(out) == 1
    assert out[0]["source_view_index"] == 0


def test_merged_scope_document_replaces_source_views(tmp_path: Path) -> None:
    scope = tmp_path / "scope.yaml"
    scope.write_text(
        yaml.dump(
            {
                "schemaVersion": 1,
                "source_views": [
                    {"view_external_id": "OLD", "instance_space": "sp-old"}
                ],
                "key_extraction": {"config": {"data": {}}},
                "aliasing": {"config": {"data": {}}},
            }
        ),
        encoding="utf-8",
    )
    filtered = [{"view_external_id": "NEW", "instance_space": "sp-new"}]
    doc = merged_scope_document_for_local_run(scope, filtered)
    assert doc["source_views"] == filtered
    assert "aliasing" in doc


def test_merged_scope_document_sets_top_level_source_views(tmp_path: Path) -> None:
    scope = tmp_path / "scope.yaml"
    scope.write_text(
        yaml.dump(
            {"schemaVersion": 1, "key_extraction": {"config": {"data": {}}}}
        ),
        encoding="utf-8",
    )
    views = [{"view_external_id": "A"}]
    doc = merged_scope_document_for_local_run(scope, views)
    assert doc["source_views"] == views


def test_merged_scope_document_rejects_non_mapping(tmp_path: Path) -> None:
    scope = tmp_path / "bad.yaml"
    scope.write_text("- not a mapping\n", encoding="utf-8")
    with pytest.raises(ValueError, match="mapping"):
        merged_scope_document_for_local_run(scope, [])


@pytest.mark.parametrize(
    "cli,views,expected",
    [
        ("  my-space  ", [], "my-space"),
        (
            None,
            [{"instance_space": "from-view"}],
            "from-view",
        ),
        (
            None,
            [
                {
                    "filters": [
                        {
                            "property_scope": "node",
                            "target_property": "space",
                            "operator": "EQUALS",
                            "values": ["dm-space"],
                        }
                    ]
                }
            ],
            "dm-space",
        ),
        (None, [], "all_spaces"),
    ],
)
def test_workflow_instance_space_for_local(
    cli: str | None, views: list, expected: str
) -> None:
    assert workflow_instance_space_for_local(views, cli) == expected
