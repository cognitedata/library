"""Governance build writes Space/Group YAML under module spaces/ and auth/."""

from __future__ import annotations

from pathlib import Path

import yaml

from governance_build.orchestrate import run_build_spaces
from governance_build.paths import governance_artifacts_root


def test_build_spaces_writes_under_module_root(tmp_path: Path) -> None:
    gov = tmp_path / "governance"
    gov.mkdir()
    (gov / "templates" / "spaces").mkdir(parents=True)
    (gov / "templates" / "spaces" / "t.Space.template.yaml").write_text(
        "space: '{{ instance_space }}'\nname: '{{ name }}'\n",
        encoding="utf-8",
    )
    doc = {
        "scope_hierarchy": {
            "type": "hierarchy",
            "levels": ["site"],
            "locations": [{"id": "SITE_A", "name": "Site A"}],
        },
        "spaces": {
            "template": "templates/spaces/t.Space.template.yaml",
            "output_dir": "spaces",
            "nodes": "leaves",
            "instance_space_id_template": "inst_site_a",
            "name_template": "{{ instance_space }}",
            "combine_with": [],
        },
    }
    written = run_build_spaces(
        module_root=gov,
        doc=doc,
        dry_run=False,
        force=True,
        prev_manifest_rels=None,
    )
    assert written
    out = governance_artifacts_root(gov) / written[0]
    assert out.is_file()
    assert out.resolve().is_relative_to(tmp_path.resolve())
    assert not str(out).startswith(str(gov.resolve()))


def test_build_spaces_dry_run_paths(tmp_path: Path) -> None:
    gov = tmp_path / "governance"
    tpl = gov / "templates" / "spaces" / "t.Space.template.yaml"
    tpl.parent.mkdir(parents=True)
    tpl.write_text("space: x\nname: y\n", encoding="utf-8")
    doc = yaml.safe_load(
        """
scope_hierarchy:
  type: hierarchy
  levels: [site]
  locations: [{id: SITE_A, name: Site A}]
spaces:
  template: templates/spaces/t.Space.template.yaml
  output_dir: spaces
  nodes: leaves
  instance_space_id_template: inst_x
  name_template: "{{ instance_space }}"
  combine_with: []
"""
    )
    run_build_spaces(
        module_root=gov,
        doc=doc,
        dry_run=True,
        force=False,
        prev_manifest_rels=None,
    )
    assert not (tmp_path / "spaces").exists() or not any((tmp_path / "spaces").rglob("*.yaml"))
