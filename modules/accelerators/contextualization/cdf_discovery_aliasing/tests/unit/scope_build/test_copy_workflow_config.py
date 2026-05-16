"""Tests for reconcile_scope_document_for_destination and workflow trigger path resolution."""

from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest
import yaml

_PKG = Path(__file__).resolve().parents[3]
_SCRIPTS = _PKG / "scripts"
for _p in (_PKG, _SCRIPTS):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)

from scope_build.context import PathStep, ScopeBuildContext
from scope_build.hierarchy import build_contexts
from scope_build.scope_document_patch import (
    reconcile_scope_document_for_destination,
    strip_node_space_filters_from_top_level_source_views,
)
from scope_build.workflow_trigger_paths import resolve_workflow_trigger_path_and_context


def _ctx(
    scope_id: str,
    *,
    instance_space: str | None = None,
) -> ScopeBuildContext:
    node: dict = {}
    if instance_space is not None:
        node["instance_space"] = instance_space
    return ScopeBuildContext(
        module_root=Path("/tmp"),
        scope_id=scope_id,
        levels=["site"],
        path=[
            PathStep(
                level="site",
                name="Leaf",
                description=None,
                segment_id=scope_id.split("__")[-1],
                node=node,
            )
        ],
        dry_run=True,
    )


def test_strip_node_space_filters_removes_only_node_space() -> None:
    doc = {
        "source_views": [
            {
                "filters": [
                    {"operator": "IN", "target_property": "tags", "values": [1]},
                    {
                        "operator": "EQUALS",
                        "property_scope": "node",
                        "target_property": "space",
                        "values": ["old"],
                    },
                ]
            }
        ]
    }
    orig = copy.deepcopy(doc)
    strip_node_space_filters_from_top_level_source_views(doc)
    fl = doc["source_views"][0]["filters"]
    assert len(fl) == 1
    assert fl[0]["target_property"] == "tags"
    # unrelated keys unchanged
    strip_node_space_filters_from_top_level_source_views(orig)
    assert orig == doc


def test_reconcile_patches_destination_ids_scope_and_filters() -> None:
    src = {
        "schemaVersion": 1,
        "source_views": [
            {
                "view_external_id": "X",
                "filters": [
                    {
                        "operator": "EQUALS",
                        "property_scope": "node",
                        "target_property": "space",
                        "values": ["source_only"],
                    }
                ],
            }
        ],
        "key_extraction": {
            "externalId": "ctx_key_extraction_default",
            "config": {"parameters": {"workflow_scope": "site_01__foo"}},
        },
        "aliasing": {
            "externalId": "ctx_aliasing_default",
            "config": {"parameters": {}},
        },
    }
    dest = _ctx("site_02", instance_space="dm-space-02")
    out = reconcile_scope_document_for_destination(src, dest)
    assert out["key_extraction"]["externalId"] == "ctx_key_extraction_site_02"
    assert out["aliasing"]["externalId"] == "ctx_aliasing_site_02"
    assert out["key_extraction"]["config"]["parameters"]["workflow_scope"] == "site_02"
    assert out["scope"]["id"] == "site_02"
    space_filters = [
        f
        for f in out["source_views"][0]["filters"]
        if f.get("property_scope") == "node" and f.get("target_property") == "space"
    ]
    assert len(space_filters) == 1
    assert space_filters[0]["values"] == ["dm-space-02"]


def test_reconcile_uses_instance_space_placeholder_when_leaf_has_no_instance_space() -> None:
    src = {
        "schemaVersion": 1,
        "source_views": [{"view_external_id": "X", "filters": []}],
        "key_extraction": {
            "externalId": "ctx_key_extraction_default",
            "config": {"parameters": {}},
        },
        "aliasing": {"externalId": "ctx_aliasing_default", "config": {"parameters": {}}},
    }
    dest = _ctx("leaf_a")
    out = reconcile_scope_document_for_destination(src, dest)
    space_filters = [
        f
        for f in out["source_views"][0]["filters"]
        if f.get("property_scope") == "node" and f.get("target_property") == "space"
    ]
    assert space_filters[0]["values"] == ["{{instance_space}}"]


def test_resolve_workflow_trigger_path_scoped(tmp_path: Path) -> None:
    mod = tmp_path / "mod2"
    mod.mkdir()
    (mod / "workflows" / "alpha").mkdir(parents=True)
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels: [site]
          locations:
          - id: alpha
            name: A
        """
    )
    contexts = build_contexts(module_root=mod, doc=doc, dry_run=True)
    path, ctx = resolve_workflow_trigger_path_and_context(
        "alpha",
        module_root=mod,
        contexts=contexts,
        workflow_base="key_extraction_aliasing",
    )
    assert ctx.scope_id == "alpha"
    assert path == mod / "workflows" / "alpha" / "key_extraction_aliasing.alpha.WorkflowTrigger.yaml"


def test_resolve_workflow_trigger_path_by_file(tmp_path: Path) -> None:
    mod = tmp_path / "mod3"
    mod.mkdir()
    trig = mod / "workflows" / "beta" / "key_extraction_aliasing.beta.WorkflowTrigger.yaml"
    trig.parent.mkdir(parents=True)
    trig.write_text("externalId: x\ninput: {}\n", encoding="utf-8")
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels: [site]
          locations:
          - id: beta
            name: B
        """
    )
    contexts = build_contexts(module_root=mod, doc=doc, dry_run=True)
    path, ctx = resolve_workflow_trigger_path_and_context(
        str(trig),
        module_root=mod,
        contexts=contexts,
        workflow_base="key_extraction_aliasing",
    )
    assert path.resolve() == trig.resolve()
    assert ctx.scope_id == "beta"


def test_resolve_unknown_scope_raises() -> None:
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels: [site]
          locations:
          - id: only_one
            name: X
        """
    )
    contexts = build_contexts(module_root=Path("/tmp"), doc=doc, dry_run=True)
    with pytest.raises(ValueError, match="Unknown scope"):
        resolve_workflow_trigger_path_and_context(
            "nope",
            module_root=Path("/tmp"),
            contexts=contexts,
            workflow_base="key_extraction_aliasing",
        )
