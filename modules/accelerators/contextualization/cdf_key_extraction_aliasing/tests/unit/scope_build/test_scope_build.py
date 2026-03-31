"""Tests for scope_hierarchy parsing and pluggable scope artifact builders.

The built-in builder emits ``config/scopes/<id>/key_extraction_aliasing.yaml`` (v1 scope document).
"""

from __future__ import annotations

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

from scope_build.builders.scope_yaml import ScopeYamlBuilder
from scope_build.context import ScopeBuildContext
from scope_build.hierarchy import (
    build_contexts,
    collect_leaves,
    slugify_display_name,
)
from scope_build.orchestrate import main as build_scopes_main, run_build
from scope_build.registry import default_builders, filter_builders


def test_slugify_display_name_spaces() -> None:
    assert slugify_display_name("  South   Plant  ") == "South_Plant"


def test_collect_leaves_name_only_one_level() -> None:
    doc = yaml.safe_load(
        """
        scope_hierarchy:
          levels: [site]
        locations:
          - name: "Main Site"
        """
    )
    leaves = collect_leaves(doc)
    assert len(leaves) == 1
    assert leaves[0][0] == "Main_Site"


def test_collect_leaves_ids_four_levels() -> None:
    doc = yaml.safe_load(
        """
        scope_hierarchy:
          levels: [site, plant, area, system]
        locations:
          - id: S1
            name: Site One
            locations:
              - id: P1
                name: Plant
                locations:
                  - id: A1
                    name: Area
                    locations:
                      - id: SYS1
                        name: System
                        instance_space: sp_demo
        """
    )
    leaves = collect_leaves(doc)
    assert len(leaves) == 1
    assert leaves[0][0] == "S1__P1__A1__SYS1"
    leaf_node = leaves[0][1][-1]
    assert leaf_node.get("instance_space") == "sp_demo"


def test_duplicate_scope_id_errors() -> None:
    doc = yaml.safe_load(
        """
        scope_hierarchy:
          levels: [site, leaf]
        locations:
          - id: S
            locations:
              - id: X
          - id: S
            locations:
              - id: X
        """
    )
    with pytest.raises(ValueError, match="Duplicate scope_id"):
        collect_leaves(doc)


def test_depth_mismatch_errors() -> None:
    doc = yaml.safe_load(
        """
        scope_hierarchy:
          levels: [site, plant]
        locations:
          - id: S
            name: S
            locations: []
        """
    )
    with pytest.raises(ValueError, match="leaf at depth"):
        collect_leaves(doc)


MINIMAL_TEMPLATE = """
schemaVersion: 1
key_extraction:
  externalId: ctx_key_extraction_default
  config:
    parameters: {}
    data:
      validation: { min_confidence: 0.5 }
      source_views:
        - view_external_id: CogniteAsset
          view_space: cdf_cdm
          view_version: v1
          entity_type: asset
      extraction_rules: []
aliasing:
  externalId: ctx_aliasing_default
  config:
    parameters: {}
    data:
      aliasing_rules: []
      validation: {}
"""

MULTIVIEW_TEMPLATE = """
schemaVersion: 1
key_extraction:
  externalId: ctx_key_extraction_default
  config:
    parameters: {}
    data:
      validation: { min_confidence: 0.5 }
      source_views:
        - view_external_id: CogniteAsset
          view_space: cdf_cdm
          view_version: v1
          entity_type: asset
          filters:
            - operator: CONTAINSANY
              target_property: tags
              values: [asset_tag]
        - view_external_id: CogniteFile
          view_space: cdf_cdm
          view_version: v1
          entity_type: file
      extraction_rules: []
aliasing:
  externalId: ctx_aliasing_default
  config:
    parameters: {}
    data:
      aliasing_rules: []
      validation: {}
"""


def test_scope_yaml_builder_injects_node_space_all_views_prepends(tmp_path: Path) -> None:
    mod = tmp_path / "mod_mv"
    (mod / "config/scopes/default").mkdir(parents=True)
    tpl = mod / "config/scopes/default/key_extraction_aliasing.yaml"
    tpl.write_text(MULTIVIEW_TEMPLATE, encoding="utf-8")
    doc = yaml.safe_load(
        """
        scope_hierarchy:
          levels: [site]
        locations:
          - id: SITE_A
            name: Site A
        """
    )
    ctx = build_contexts(module_root=mod, doc=doc, dry_run=False)[0]
    ScopeYamlBuilder(tpl).run(ctx)
    out = mod / "config/scopes/SITE_A/key_extraction_aliasing.yaml"
    data = yaml.safe_load(out.read_text(encoding="utf-8"))
    views = data["key_extraction"]["config"]["data"]["source_views"]
    assert len(views) == 2
    for v in views:
        assert v["filters"][0]["property_scope"] == "node"
        assert v["filters"][0]["target_property"] == "space"
    assert views[0]["filters"][1]["operator"] == "CONTAINSANY"
    assert views[0]["filters"][1]["target_property"] == "tags"


def test_scope_yaml_builder_uses_leaf_instance_space_when_set(tmp_path: Path) -> None:
    mod = tmp_path / "mod_inst"
    (mod / "config/scopes/default").mkdir(parents=True)
    tpl = mod / "config/scopes/default/key_extraction_aliasing.yaml"
    tpl.write_text(MINIMAL_TEMPLATE, encoding="utf-8")
    doc = yaml.safe_load(
        """
        scope_hierarchy:
          levels: [site, plant]
        locations:
          - id: S
            locations:
              - id: P
                name: P
                instance_space: sp_acme_prod
        """
    )
    ctx = build_contexts(module_root=mod, doc=doc, dry_run=False)[0]
    ScopeYamlBuilder(tpl).run(ctx)
    out = mod / "config/scopes/S__P/key_extraction_aliasing.yaml"
    data = yaml.safe_load(out.read_text(encoding="utf-8"))
    v0 = data["key_extraction"]["config"]["data"]["source_views"][0]
    assert v0["filters"][0]["values"] == ["sp_acme_prod"]


def test_scope_yaml_builder_skips_inject_when_node_space_filter_exists(tmp_path: Path) -> None:
    mod = tmp_path / "mod_idem"
    (mod / "config/scopes/default").mkdir(parents=True)
    tpl = mod / "config/scopes/default/key_extraction_aliasing.yaml"
    tpl.write_text(
        """
schemaVersion: 1
key_extraction:
  externalId: ctx_key_extraction_default
  config:
    parameters: {}
    data:
      validation: { min_confidence: 0.5 }
      source_views:
        - view_external_id: CogniteAsset
          view_space: cdf_cdm
          view_version: v1
          entity_type: asset
          filters:
            - operator: IN
              property_scope: node
              target_property: space
              values: [sp_already_set]
      extraction_rules: []
aliasing:
  externalId: ctx_aliasing_default
  config:
    parameters: {}
    data:
      aliasing_rules: []
      validation: {}
""",
        encoding="utf-8",
    )
    doc = yaml.safe_load(
        """
        scope_hierarchy:
          levels: [site]
        locations:
          - id: Z1
            name: Z
        """
    )
    ctx = build_contexts(module_root=mod, doc=doc, dry_run=False)[0]
    ScopeYamlBuilder(tpl).run(ctx)
    out = mod / "config/scopes/Z1/key_extraction_aliasing.yaml"
    data = yaml.safe_load(out.read_text(encoding="utf-8"))
    fl = data["key_extraction"]["config"]["data"]["source_views"][0]["filters"]
    assert len(fl) == 1
    assert fl[0]["values"] == ["sp_already_set"]


def test_scope_yaml_builder_writes_and_skips(tmp_path: Path) -> None:
    mod = tmp_path / "mod"
    (mod / "config/scopes/default").mkdir(parents=True)
    tpl = mod / "config/scopes/default/key_extraction_aliasing.yaml"
    tpl.write_text(MINIMAL_TEMPLATE, encoding="utf-8")

    doc = yaml.safe_load(
        """
        scope_hierarchy:
          levels: [site]
        locations:
          - id: LEAF1
            name: Leaf One
        """
    )
    ctx = build_contexts(module_root=mod, doc=doc, dry_run=False)[0]
    ScopeYamlBuilder(tpl).run(ctx)
    out = mod / "config/scopes/LEAF1/key_extraction_aliasing.yaml"
    assert out.is_file()
    data = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert data["scope"]["id"] == "LEAF1"
    assert data["scope"]["name"] == "Leaf One"
    assert data["key_extraction"]["externalId"] == "ctx_key_extraction_leaf1"
    assert data["aliasing"]["externalId"] == "ctx_aliasing_leaf1"
    views = data["key_extraction"]["config"]["data"]["source_views"]
    assert len(views) == 1
    flt = views[0]["filters"][0]
    assert flt["property_scope"] == "node"
    assert flt["target_property"] == "space"
    assert flt["operator"] == "EQUALS"
    assert flt["values"] == ["PLACEHOLDER_INSTANCE_SPACE_FOR_SCOPE__leaf1"]

    ScopeYamlBuilder(tpl).run(ctx)
    assert out.read_text(encoding="utf-8").count("Leaf One") >= 1


def test_scope_yaml_builder_dry_run_no_file(tmp_path: Path) -> None:
    mod = tmp_path / "mod2"
    (mod / "config/scopes/default").mkdir(parents=True)
    tpl = mod / "config/scopes/default/key_extraction_aliasing.yaml"
    tpl.write_text(MINIMAL_TEMPLATE, encoding="utf-8")
    doc = yaml.safe_load(
        """
        scope_hierarchy:
          levels: [site]
        locations:
          - name: "Only Name"
        """
    )
    ctx = build_contexts(module_root=mod, doc=doc, dry_run=True)[0]
    ScopeYamlBuilder(tpl).run(ctx)
    assert not (mod / "config/scopes/Only_Name/key_extraction_aliasing.yaml").exists()


class _RecordingBuilder:
    name = "recording"

    def __init__(self) -> None:
        self.seen: list[str] = []

    def run(self, ctx: ScopeBuildContext) -> None:
        self.seen.append(ctx.scope_id)


def test_filter_builders_subset_and_dedupe(tmp_path: Path) -> None:
    tpl = tmp_path / "t.yaml"
    tpl.write_text("x: 1\n", encoding="utf-8")
    builders = default_builders(template_path=tpl)
    assert [b.name for b in filter_builders(builders, ["key_extraction_aliasing"])] == [
        "key_extraction_aliasing"
    ]
    assert [b.name for b in filter_builders(builders, ["key_extraction_aliasing"] * 2)] == [
        "key_extraction_aliasing"
    ]


def test_filter_builders_unknown_raises(tmp_path: Path) -> None:
    tpl = tmp_path / "t.yaml"
    tpl.write_text("x: 1\n", encoding="utf-8")
    builders = default_builders(template_path=tpl)
    with pytest.raises(ValueError, match="Unknown builder"):
        filter_builders(builders, ["no_such_builder"])


def test_run_build_missing_hierarchy_raises(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    tpl = tmp_path / "tpl.yaml"
    tpl.write_text(MINIMAL_TEMPLATE, encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        run_build(
            module_root=tmp_path,
            hierarchy_path=missing,
            template_path=tpl,
            builders=default_builders(template_path=tpl),
            dry_run=True,
        )


def test_build_scopes_main_unknown_only_exits_nonzero() -> None:
    assert build_scopes_main(["--only", "not_a_registered_builder"]) == 1


def test_run_build_invokes_all_builders(tmp_path: Path) -> None:
    hier = tmp_path / "h.yaml"
    hier.write_text(
        """
scope_hierarchy:
  levels: [a, b]
locations:
  - id: X
    locations:
      - id: Y
""",
        encoding="utf-8",
    )
    mod = tmp_path / "mod3"
    (mod / "config/scopes/default").mkdir(parents=True)
    tpl = mod / "config/scopes/default/key_extraction_aliasing.yaml"
    tpl.write_text(MINIMAL_TEMPLATE, encoding="utf-8")
    rec = _RecordingBuilder()
    run_build(
        module_root=mod,
        hierarchy_path=hier,
        template_path=tpl,
        builders=[rec, ScopeYamlBuilder(tpl)],
        dry_run=True,
    )
    assert rec.seen == ["X__Y"]
