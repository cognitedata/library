"""Tests for default.config scope tree parsing, scope document patching, and workflow trigger builder."""

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

from scope_build.builders.workflow_triggers import (
    DEFAULT_TRIGGER_TEMPLATE_REL,
    WorkflowTriggersBuilder,
    minified_json_utf8_length,
    verify_triggers_file,
    workflow_trigger_filename,
)
from scope_build.context import ScopeBuildContext
from scope_build.mode import ScopeBuildMode, scope_build_mode_from_doc
from scope_build.hierarchy import (
    build_contexts,
    collect_leaves,
    path_step_level_name,
    slugify_display_name,
)
from scope_build.orchestrate import (
    main as build_scopes_main,
    run_build,
    workflow_external_id_from_hierarchy,
)
from scope_build.registry import default_builders, filter_builders
from scope_build.scope_document_patch import prepare_scope_document_for_context


def test_slugify_display_name_spaces() -> None:
    assert slugify_display_name("  South   Plant  ") == "South_Plant"


def test_collect_leaves_name_only_one_level() -> None:
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          locations:
          - name: Main Site
        """
    )
    leaves = collect_leaves(doc)
    assert len(leaves) == 1
    assert leaves[0][0] == "Main_Site"


def test_collect_leaves_ids_four_levels() -> None:
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          - plant
          - area
          - system
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
        aliasing_scope_hierarchy:
          levels:
          - site
          - leaf
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


def test_collect_leaves_allows_shallow_leaf_when_more_levels_declared() -> None:
    """A leaf under the root is valid even when ``levels`` lists more tier names (optional depth)."""
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          - plant
          - area
          - system
          locations:
          - id: DEFAULT_SITE
            name: Main Site
            locations: []
        """
    )
    leaves = collect_leaves(doc)
    assert len(leaves) == 1
    assert leaves[0][0] == "DEFAULT_SITE"


def test_collect_leaves_deeper_tree_uses_synthetic_level_labels(tmp_path: Path) -> None:
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          - plant
          locations:
          - id: S
            locations:
            - id: P
              locations:
              - id: AREA
                name: Deep area
        """
    )
    leaves = collect_leaves(doc)
    assert len(leaves) == 1
    assert leaves[0][0] == "S__P__AREA"
    ctx = build_contexts(module_root=tmp_path, doc=doc, dry_run=True)[0]
    assert [s.level for s in ctx.path] == ["site", "plant", "level_3"]


def test_path_step_level_name() -> None:
    lv = ["site", "plant"]
    assert path_step_level_name(lv, 0) == "site"
    assert path_step_level_name(lv, 1) == "plant"
    assert path_step_level_name(lv, 2) == "level_3"


MINIMAL_TEMPLATE = """
schemaVersion: 1
source_views:
  - view_external_id: CogniteAsset
    view_space: cdf_cdm
    view_version: v1
    entity_type: asset
key_extraction:
  externalId: ctx_key_extraction_default
  config:
    parameters: {}
    data:
      validation: { min_confidence: 0.5 }
      extraction_rules: []
aliasing:
  externalId: ctx_aliasing_default
  config:
    parameters: {}
    data:
      aliasing_rules: []
      validation: {}
"""

MINIMAL_WORKFLOW_TRIGGER_TEMPLATE = """
externalId: key_extraction_aliasing.__KEA_CDF_SUFFIX__
triggerRule:
  triggerType: schedule
  cronExpression: '{{ key_extraction_aliasing_schedule }}'
workflowExternalId: {{ workflow }}
workflowVersion: v4
authentication:
  clientId: '{{functionClientId}}'
  clientSecret: '{{functionClientSecret}}'
input:
  full_rescan: false
  run_id: ''
"""


def _install_minimal_workflow_trigger_template(module_root: Path) -> Path:
    path = module_root / DEFAULT_TRIGGER_TEMPLATE_REL
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(MINIMAL_WORKFLOW_TRIGGER_TEMPLATE, encoding="utf-8")
    return path


def _install_minimal_scope_document(module_root: Path) -> Path:
    path = module_root / "workflow_template" / "workflow.template.config.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(MINIMAL_TEMPLATE, encoding="utf-8")
    return path


MINIMAL_WF_DEF_TEMPLATE = """externalId: __KEA_WORKFLOW_EXTERNAL_ID__
description: Test workflow
dataSetExternalId: 'ds_key_extraction'
"""

MINIMAL_WV_DEF_TEMPLATE = """workflowExternalId: __KEA_WORKFLOW_EXTERNAL_ID__
version: 'v4'
workflowDefinition:
  description: 'test'
  input:
    full_rescan: false
    run_id: ''
    configuration: {}
  tasks: []
"""


def _install_minimal_workflow_definition_templates(module_root: Path) -> None:
    d = module_root / "workflow_template"
    d.mkdir(parents=True, exist_ok=True)
    (d / "workflow.template.Workflow.yaml").write_text(
        MINIMAL_WF_DEF_TEMPLATE, encoding="utf-8"
    )
    (d / "workflow.template.WorkflowVersion.yaml").write_text(
        MINIMAL_WV_DEF_TEMPLATE, encoding="utf-8"
    )


def _tb(
    scope_document_path: Path,
    *,
    scope_build_mode: ScopeBuildMode = "trigger_only",
    workflow_base: str = "key_extraction_aliasing",
    overwrite: bool = False,
):
    """Default builders for tests (trigger_only unless overridden)."""
    return default_builders(
        scope_build_mode=scope_build_mode,
        workflow_base=workflow_base,
        scope_document_path=scope_document_path,
        overwrite=overwrite,
    )


MULTIVIEW_TEMPLATE = """
schemaVersion: 1
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
key_extraction:
  externalId: ctx_key_extraction_default
  config:
    parameters: {}
    data:
      validation: { min_confidence: 0.5 }
      extraction_rules: []
aliasing:
  externalId: ctx_aliasing_default
  config:
    parameters: {}
    data:
      aliasing_rules: []
      validation: {}
"""


def test_prepare_scope_document_injects_node_space_all_views_prepends(tmp_path: Path) -> None:
    mod = tmp_path / "mod_mv"
    doc = yaml.safe_load(MULTIVIEW_TEMPLATE)
    hdoc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          locations:
          - id: SITE_A
            name: Site A
        """
    )
    ctx = build_contexts(module_root=mod, doc=hdoc, dry_run=False)[0]
    data = prepare_scope_document_for_context(doc, ctx)
    views = data["source_views"]
    assert len(views) == 2
    for v in views:
        assert v["filters"][0]["property_scope"] == "node"
        assert v["filters"][0]["target_property"] == "space"
    assert views[0]["filters"][1]["operator"] == "CONTAINSANY"
    assert views[0]["filters"][1]["target_property"] == "tags"


def test_prepare_scope_document_uses_leaf_instance_space_when_set(tmp_path: Path) -> None:
    mod = tmp_path / "mod_inst"
    doc = yaml.safe_load(MINIMAL_TEMPLATE)
    hdoc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          - plant
          locations:
          - id: S
            locations:
            - id: P
              name: P
              instance_space: sp_acme_prod
        """
    )
    ctx = build_contexts(module_root=mod, doc=hdoc, dry_run=False)[0]
    data = prepare_scope_document_for_context(doc, ctx)
    v0 = data["source_views"][0]
    assert v0["filters"][0]["values"] == ["sp_acme_prod"]


def test_prepare_scope_document_skips_inject_when_node_space_filter_exists(tmp_path: Path) -> None:
    mod = tmp_path / "mod_idem"
    doc = yaml.safe_load(
        """
schemaVersion: 1
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
key_extraction:
  externalId: ctx_key_extraction_default
  config:
    parameters: {}
    data:
      validation: { min_confidence: 0.5 }
      extraction_rules: []
aliasing:
  externalId: ctx_aliasing_default
  config:
    parameters: {}
    data:
      aliasing_rules: []
      validation: {}
"""
    )
    hdoc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels: [site]
          locations:
            - id: Z1
              name: Z
        """
    )
    ctx = build_contexts(module_root=mod, doc=hdoc, dry_run=False)[0]
    data = prepare_scope_document_for_context(doc, ctx)
    fl = data["source_views"][0]["filters"]
    assert len(fl) == 1
    assert fl[0]["values"] == ["sp_already_set"]


def test_prepare_scope_document_external_ids_and_scope_block(tmp_path: Path) -> None:
    mod = tmp_path / "mod"
    doc = yaml.safe_load(MINIMAL_TEMPLATE)
    hdoc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          locations:
          - id: LEAF1
            name: Leaf One
        """
    )
    ctx = build_contexts(module_root=mod, doc=hdoc, dry_run=False)[0]
    data = prepare_scope_document_for_context(doc, ctx)
    assert data["scope"]["id"] == "LEAF1"
    assert data["scope"]["name"] == "Leaf One"
    assert data["key_extraction"]["externalId"] == "ctx_key_extraction_leaf1"
    assert data["aliasing"]["externalId"] == "ctx_aliasing_leaf1"
    assert data["key_extraction"]["config"]["parameters"]["workflow_scope"] == "LEAF1"
    views = data["source_views"]
    assert len(views) == 1
    flt = views[0]["filters"][0]
    assert flt["property_scope"] == "node"
    assert flt["target_property"] == "space"
    assert flt["operator"] == "EQUALS"
    assert flt["values"] == ["{{instance_space}}"]


class _RecordingBuilder:
    name = "recording"

    def __init__(self) -> None:
        self.seen: list[str] = []

    def run(self, ctx: ScopeBuildContext) -> None:
        self.seen.append(ctx.scope_id)


def test_filter_builders_subset_and_dedupe(tmp_path: Path) -> None:
    sd = tmp_path / "scope.yaml"
    sd.write_text(MINIMAL_TEMPLATE, encoding="utf-8")
    builders = _tb(sd)
    assert [b.name for b in builders] == ["workflow_definitions_root", "workflow_triggers"]
    assert [b.name for b in filter_builders(builders, ["workflow_triggers"])] == [
        "workflow_triggers"
    ]
    assert [b.name for b in filter_builders(builders, ["workflow_triggers"] * 2)] == [
        "workflow_triggers"
    ]


def test_filter_builders_unknown_raises(tmp_path: Path) -> None:
    sd = tmp_path / "s.yaml"
    sd.write_text(MINIMAL_TEMPLATE, encoding="utf-8")
    builders = _tb(sd)
    with pytest.raises(ValueError, match="Unknown builder"):
        filter_builders(builders, ["no_such_builder"])


def test_run_build_missing_hierarchy_raises(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    sd = tmp_path / "s.yaml"
    sd.write_text(MINIMAL_TEMPLATE, encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        run_build(
            module_root=tmp_path,
            hierarchy_path=missing,
            builders=_tb(sd),
            dry_run=True,
        )


def test_build_scopes_main_unknown_only_exits_nonzero() -> None:
    assert build_scopes_main(["--only", "not_a_registered_builder"]) == 1


def test_workflow_triggers_builder_two_leaves(tmp_path: Path) -> None:
    mod = tmp_path / "mod_wt"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    _install_minimal_scope_document(mod)
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          - plant
          locations:
          - id: S1
            locations:
            - id: P1
              name: Plant 1
          - id: S2
            locations:
            - id: P2
              name: Plant 2
        """
    )
    contexts = build_contexts(module_root=mod, doc=doc, dry_run=False)
    WorkflowTriggersBuilder().write_all(contexts, dry_run=False, module_root=mod)
    p1 = mod / "workflows" / workflow_trigger_filename("key_extraction_aliasing", "s1_p1")
    p2 = mod / "workflows" / workflow_trigger_filename("key_extraction_aliasing", "s2_p2")
    assert p1.is_file()
    assert p2.is_file()
    data0 = yaml.safe_load(p1.read_text(encoding="utf-8"))
    data1 = yaml.safe_load(p2.read_text(encoding="utf-8"))
    assert isinstance(data0, dict) and isinstance(data1, dict)
    assert data0["externalId"] == "key_extraction_aliasing.s1_p1"
    assert data0["workflowExternalId"] == "key_extraction_aliasing"
    assert data0["workflowVersion"] == "v4"
    assert "configuration" in data0["input"]
    assert data0["input"]["configuration"]["key_extraction"]["externalId"] == "ctx_key_extraction_s1_p1"
    assert "scope_config_file_external_id" not in data0["input"]
    assert data1["input"]["configuration"]["key_extraction"]["externalId"] == "ctx_key_extraction_s2_p2"


def test_workflow_triggers_skips_existing_files(tmp_path: Path) -> None:
    """Second --build must not overwrite an existing trigger file."""
    mod = tmp_path / "mod_wt_skip_existing"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    _install_minimal_scope_document(mod)
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          locations:
          - id: A
            name: A
        """
    )
    contexts = build_contexts(module_root=mod, doc=doc, dry_run=False)
    WorkflowTriggersBuilder().write_all(contexts, dry_run=False, module_root=mod)
    path = mod / "workflows" / workflow_trigger_filename("key_extraction_aliasing", "a")
    assert path.is_file()
    marker = "skip-existing-build-marker"
    path.write_text(path.read_text(encoding="utf-8") + f"\n# {marker}\n", encoding="utf-8")
    WorkflowTriggersBuilder().write_all(contexts, dry_run=False, module_root=mod)
    assert marker in path.read_text(encoding="utf-8")


def test_workflow_triggers_force_overwrites_existing(tmp_path: Path) -> None:
    """``overwrite=True`` replaces an existing trigger from templates."""
    mod = tmp_path / "mod_wt_force"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    sd = _install_minimal_scope_document(mod)
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          locations:
          - id: A
            name: A
        """
    )
    contexts = build_contexts(module_root=mod, doc=doc, dry_run=False)
    WorkflowTriggersBuilder(scope_document_path=sd).write_all(
        contexts, dry_run=False, module_root=mod
    )
    path = mod / "workflows" / workflow_trigger_filename("key_extraction_aliasing", "a")
    marker = "stale-marker-for-force-test"
    path.write_text(path.read_text(encoding="utf-8") + f"\n# {marker}\n", encoding="utf-8")
    WorkflowTriggersBuilder(
        scope_document_path=sd,
        overwrite=True,
    ).write_all(contexts, dry_run=False, module_root=mod)
    text = path.read_text(encoding="utf-8")
    assert marker not in text
    data = yaml.safe_load(text)
    assert data["workflowExternalId"] == "key_extraction_aliasing"


def test_workflow_triggers_build_does_not_remove_orphan_leaf_files(tmp_path: Path) -> None:
    """Rebuilding with fewer leaves must not delete existing key_extraction_aliasing.* triggers."""
    mod = tmp_path / "mod_wt_orphan"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    _install_minimal_scope_document(mod)
    doc_two = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels: [site, plant]
          locations:
            - id: S1
              locations:
                - id: P1
                  name: Plant 1
            - id: S2
              locations:
                - id: P2
                  name: Plant 2
        """
    )
    contexts_two = build_contexts(module_root=mod, doc=doc_two, dry_run=False)
    WorkflowTriggersBuilder().write_all(contexts_two, dry_run=False, module_root=mod)
    p2 = mod / "workflows" / workflow_trigger_filename("key_extraction_aliasing", "s2_p2")
    assert p2.is_file()
    marker = "orphan-preservation-marker"
    p2.write_text(p2.read_text(encoding="utf-8") + f"\n# {marker}\n", encoding="utf-8")

    doc_one = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels: [site, plant]
          locations:
            - id: S1
              locations:
                - id: P1
                  name: Plant 1
        """
    )
    contexts_one = build_contexts(module_root=mod, doc=doc_one, dry_run=False)
    WorkflowTriggersBuilder().write_all(contexts_one, dry_run=False, module_root=mod)

    assert p2.is_file()
    assert marker in p2.read_text(encoding="utf-8")


def test_verify_triggers_file_ignores_extra_dot_form_triggers(tmp_path: Path) -> None:
    mod = tmp_path / "mod_verify_extra"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    sd = _install_minimal_scope_document(mod)
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          locations:
          - id: Only
            name: Only
        """
    )
    contexts = build_contexts(module_root=mod, doc=doc, dry_run=False)
    WorkflowTriggersBuilder(scope_document_path=sd).write_all(contexts, dry_run=False, module_root=mod)
    orphan = mod / "workflows" / "key_extraction_aliasing.not_in_hierarchy.WorkflowTrigger.yaml"
    orphan.write_text(
        "externalId: key_extraction_aliasing.not_in_hierarchy\n"
        "workflowExternalId: key_extraction_aliasing\n"
        "workflowVersion: v4\n"
        "input: {}\n",
        encoding="utf-8",
    )
    verify_triggers_file(mod, list(contexts), scope_document_path=sd)


def test_verify_triggers_file_fails_when_required_file_missing(tmp_path: Path) -> None:
    mod = tmp_path / "mod_verify_missing"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    sd = _install_minimal_scope_document(mod)
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          - plant
          locations:
          - id: S1
            locations:
            - id: P1
              name: Plant 1
          - id: S2
            locations:
            - id: P2
              name: Plant 2
        """
    )
    contexts = build_contexts(module_root=mod, doc=doc, dry_run=False)
    WorkflowTriggersBuilder(scope_document_path=sd).write_all(contexts, dry_run=False, module_root=mod)
    (mod / "workflows" / workflow_trigger_filename("key_extraction_aliasing", "s2_p2")).unlink()
    with pytest.raises(SystemExit, match="Missing workflow trigger"):
        verify_triggers_file(mod, list(contexts), scope_document_path=sd)


def test_verify_triggers_file_fails_when_content_out_of_date(tmp_path: Path) -> None:
    mod = tmp_path / "mod_verify_stale"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    sd = _install_minimal_scope_document(mod)
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          locations:
          - id: A
            name: A
        """
    )
    contexts = build_contexts(module_root=mod, doc=doc, dry_run=False)
    WorkflowTriggersBuilder(scope_document_path=sd).write_all(contexts, dry_run=False, module_root=mod)
    path = mod / "workflows" / workflow_trigger_filename("key_extraction_aliasing", "a")
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace("ctx_key_extraction_a", "ctx_key_extraction_TAMPERED"), encoding="utf-8")
    with pytest.raises(SystemExit, match="out of date"):
        verify_triggers_file(mod, list(contexts), scope_document_path=sd)


def test_workflow_trigger_template_without_placeholder_raises(tmp_path: Path) -> None:
    mod = tmp_path / "mod_wt_bad"
    mod.mkdir()
    bad = mod / "bad_trigger.template.yaml"
    bad.write_text("externalId: only\n", encoding="utf-8")
    sd = mod / "scope.yaml"
    sd.write_text(MINIMAL_TEMPLATE, encoding="utf-8")
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels:
          - site
          locations:
          - id: A
            name: A
        """
    )
    contexts = build_contexts(module_root=mod, doc=doc, dry_run=False)
    with pytest.raises(ValueError, match="__KEA_CDF_SUFFIX__"):
        WorkflowTriggersBuilder(
            template_path=bad,
            scope_document_path=sd,
        ).write_all(contexts, dry_run=True)


def test_minified_json_utf8_length_no_whitespace() -> None:
    n = minified_json_utf8_length({"a": 1, "b": "x"})
    assert n == len('{"a":1,"b":"x"}'.encode("utf-8"))


def test_run_build_invokes_recording_and_triggers(tmp_path: Path) -> None:
    hier = tmp_path / "h.yaml"
    hier.write_text(
        """
aliasing_scope_hierarchy:
  levels: [a, b]
  locations:
    - id: X
      locations:
        - id: Y
""",
        encoding="utf-8",
    )
    mod = tmp_path / "mod3"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    sd = _install_minimal_scope_document(mod)
    rec = _RecordingBuilder()
    run_build(
        module_root=mod,
        hierarchy_path=hier,
        builders=[rec, WorkflowTriggersBuilder(scope_document_path=sd)],
        dry_run=True,
    )
    assert rec.seen == ["X__Y"]


def test_scope_build_mode_from_doc_defaults() -> None:
    assert scope_build_mode_from_doc({}) == "trigger_only"
    assert scope_build_mode_from_doc({"scope_build_mode": None}) == "trigger_only"


def test_scope_build_mode_full_variants() -> None:
    assert scope_build_mode_from_doc({"scope_build_mode": "full"}) == "full"
    assert scope_build_mode_from_doc({"scope_build_mode": "FULL"}) == "full"
    assert scope_build_mode_from_doc({"scope_build_mode": "trigger-only"}) == "trigger_only"


def test_scope_build_mode_invalid_raises() -> None:
    with pytest.raises(ValueError, match="scope_build_mode"):
        scope_build_mode_from_doc({"scope_build_mode": "bad"})


def test_default_builders_full_order(tmp_path: Path) -> None:
    sd = tmp_path / "s.yaml"
    sd.write_text(MINIMAL_TEMPLATE, encoding="utf-8")
    bs = default_builders(
        scope_build_mode="full",
        workflow_base="key_extraction_aliasing",
        scope_document_path=sd,
    )
    assert [b.name for b in bs] == ["workflow_definitions_scoped", "workflow_triggers"]


def test_trigger_only_run_build_force_overwrites_root_workflow(tmp_path: Path) -> None:
    mod = tmp_path / "mod_force_root"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    sd = _install_minimal_scope_document(mod)
    _install_minimal_workflow_definition_templates(mod)
    hier = tmp_path / "hier_force_root.yaml"
    hier.write_text(
        """
workflow: key_extraction_aliasing
scope_build_mode: trigger_only
aliasing_scope_hierarchy:
  levels: [site]
  locations:
    - id: B
      name: B
""",
        encoding="utf-8",
    )
    doc = yaml.safe_load(hier.read_text(encoding="utf-8"))
    common = dict(
        scope_build_mode=scope_build_mode_from_doc(doc),
        workflow_base=workflow_external_id_from_hierarchy(doc),
        scope_document_path=sd,
    )
    run_build(
        module_root=mod,
        hierarchy_path=hier,
        builders=default_builders(**common, overwrite=False),
        dry_run=False,
    )
    wf = mod / "workflows" / "key_extraction_aliasing.Workflow.yaml"
    wf.write_text(wf.read_text(encoding="utf-8") + "\n# TAMPER_ROOT\n", encoding="utf-8")
    run_build(
        module_root=mod,
        hierarchy_path=hier,
        builders=default_builders(**common, overwrite=True),
        dry_run=False,
    )
    assert "TAMPER_ROOT" not in wf.read_text(encoding="utf-8")


def test_trigger_only_run_build_writes_root_and_flat_triggers(tmp_path: Path) -> None:
    mod = tmp_path / "mod_root_wf"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    sd = _install_minimal_scope_document(mod)
    _install_minimal_workflow_definition_templates(mod)
    hier = tmp_path / "hier_root.yaml"
    hier.write_text(
        """
workflow: key_extraction_aliasing
scope_build_mode: trigger_only
aliasing_scope_hierarchy:
  levels: [site]
  locations:
    - id: B
      name: B
""",
        encoding="utf-8",
    )
    doc = yaml.safe_load(hier.read_text(encoding="utf-8"))
    run_build(
        module_root=mod,
        hierarchy_path=hier,
        builders=default_builders(
            scope_build_mode=scope_build_mode_from_doc(doc),
            workflow_base=workflow_external_id_from_hierarchy(doc),
            scope_document_path=sd,
        ),
        dry_run=False,
    )
    assert (mod / "workflows" / "key_extraction_aliasing.Workflow.yaml").is_file()
    assert (mod / "workflows" / "key_extraction_aliasing.WorkflowVersion.yaml").is_file()
    trig = mod / "workflows" / workflow_trigger_filename("key_extraction_aliasing", "b")
    assert trig.is_file()


def test_full_mode_run_build_writes_scoped_trio(tmp_path: Path) -> None:
    mod = tmp_path / "mod_full_scoped"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    sd = _install_minimal_scope_document(mod)
    _install_minimal_workflow_definition_templates(mod)
    hier = tmp_path / "hier_full.yaml"
    hier.write_text(
        """
workflow: key_extraction_aliasing
scope_build_mode: full
aliasing_scope_hierarchy:
  levels: [site]
  locations:
    - id: B
      name: B
""",
        encoding="utf-8",
    )
    doc = yaml.safe_load(hier.read_text(encoding="utf-8"))
    run_build(
        module_root=mod,
        hierarchy_path=hier,
        builders=default_builders(
            scope_build_mode=scope_build_mode_from_doc(doc),
            workflow_base=workflow_external_id_from_hierarchy(doc),
            scope_document_path=sd,
        ),
        dry_run=False,
    )
    scope_dir = mod / "workflows" / "b"
    assert (scope_dir / "key_extraction_aliasing.b.Workflow.yaml").is_file()
    assert (scope_dir / "key_extraction_aliasing.b.WorkflowVersion.yaml").is_file()
    assert (scope_dir / "key_extraction_aliasing.b.WorkflowTrigger.yaml").is_file()
    t = yaml.safe_load(
        (scope_dir / "key_extraction_aliasing.b.WorkflowTrigger.yaml").read_text(encoding="utf-8")
    )
    assert t["workflowExternalId"] == "key_extraction_aliasing.b"
    wf = yaml.safe_load((scope_dir / "key_extraction_aliasing.b.Workflow.yaml").read_text(encoding="utf-8"))
    assert wf["externalId"] == "key_extraction_aliasing.b"


def test_workflow_triggers_builder_full_mode_paths(tmp_path: Path) -> None:
    mod = tmp_path / "mod_wt_full_paths"
    mod.mkdir()
    _install_minimal_workflow_trigger_template(mod)
    sd = _install_minimal_scope_document(mod)
    doc = yaml.safe_load(
        """
        aliasing_scope_hierarchy:
          levels: [site]
          locations:
          - id: A
            name: A
        """
    )
    contexts = build_contexts(module_root=mod, doc=doc, dry_run=False)
    WorkflowTriggersBuilder(
        mode="full",
        workflow_base="key_extraction_aliasing",
        scope_document_path=sd,
    ).write_all(contexts, dry_run=False, module_root=mod)
    p = mod / "workflows" / "a" / workflow_trigger_filename("key_extraction_aliasing", "a")
    assert p.is_file()
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    assert data["workflowExternalId"] == "key_extraction_aliasing.a"
