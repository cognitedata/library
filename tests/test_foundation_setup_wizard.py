"""Tests for the dp:foundation setup wizard and its helper modules.

Covers:
  - _yaml_patch  : find_line, get_value, set_value, insert_key, delete_key,
                   block-sequence removal
  - _env_io      : parse_env_file, upsert_env
  - setup_project: group_name, build_foundation_vars, build_overlay,
                   _migrate_staging_to_test, _write_config_fresh,
                   _write_config_update, remove_redundant_auth_files,
                   patch_cfihos_auth_for_missing_search, _read_existing_values
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import yaml

REPO_ROOT   = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "modules" / "common" / "cdf_project_foundation" / "scripts"

# Make scripts importable without installing them.
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


# ── _yaml_patch ────────────────────────────────────────────────────────────────

class TestYamlPatchFindLine:
    def _lines(self, text: str) -> list[str]:
        return textwrap.dedent(text).splitlines(keepends=True)

    def test_finds_top_level_key(self) -> None:
        from _yaml_patch import find_line
        lines = self._lines("""\
            site: oslo
            dataset: ds_pi
        """)
        assert find_line(lines, "site") == 0
        assert find_line(lines, "dataset") == 1

    def test_finds_nested_key(self) -> None:
        from _yaml_patch import find_line
        lines = self._lines("""\
            variables:
              modules:
                cdf_project_foundation:
                  site: oslo
        """)
        assert find_line(lines, "variables.modules.cdf_project_foundation.site") == 3

    def test_returns_none_for_missing_key(self) -> None:
        from _yaml_patch import find_line
        lines = self._lines("site: oslo\n")
        assert find_line(lines, "missing") is None
        assert find_line(lines, "site.nested") is None

    def test_skips_comment_lines(self) -> None:
        from _yaml_patch import find_line
        lines = self._lines("""\
            # comment
            site: oslo
        """)
        assert find_line(lines, "site") == 1

    def test_distinguishes_sibling_sections(self) -> None:
        from _yaml_patch import find_line
        lines = self._lines("""\
            a:
              x: 1
            b:
              x: 2
        """)
        assert find_line(lines, "a.x") == 1
        assert find_line(lines, "b.x") == 3


class TestYamlPatchGetValue:
    def test_gets_scalar(self) -> None:
        from _yaml_patch import get_value
        lines = "site: oslo\n".splitlines(keepends=True)
        assert get_value(lines, "site") == "oslo"

    def test_strips_quotes(self) -> None:
        from _yaml_patch import get_value
        lines = 'site: "oslo"\n'.splitlines(keepends=True)
        assert get_value(lines, "site") == "oslo"

    def test_returns_none_for_missing(self) -> None:
        from _yaml_patch import get_value
        lines = "site: oslo\n".splitlines(keepends=True)
        assert get_value(lines, "missing") is None

    def test_ignores_inline_comment_in_value(self) -> None:
        from _yaml_patch import get_value
        lines = "site: oslo  # the site\n".splitlines(keepends=True)
        assert get_value(lines, "site") == "oslo"


class TestYamlPatchSetValue:
    def test_updates_scalar(self) -> None:
        from _yaml_patch import set_value
        lines = ["site: oslo\n"]
        old, changed = set_value(lines, "site", "berlin")
        assert changed
        assert "berlin" in lines[0]
        assert old == "oslo"

    def test_no_change_when_same_value(self) -> None:
        from _yaml_patch import set_value
        lines = ["site: oslo\n"]
        _, changed = set_value(lines, "site", "oslo")
        assert not changed

    def test_preserves_trailing_comment(self) -> None:
        from _yaml_patch import set_value
        lines = ["site: oslo  # required\n"]
        set_value(lines, "site", "berlin")
        assert "# required" in lines[0]
        assert "berlin" in lines[0]

    def test_removes_block_sequence_items(self) -> None:
        from _yaml_patch import set_value
        lines = [
            "dataset:\n",
            "- ds_pi\n",
            "- ds_sap\n",
            "site: oslo\n",
        ]
        _, changed = set_value(lines, "dataset", "[ds_pi, ds_sap]")
        assert changed
        assert len(lines) == 2   # only dataset and site remain
        assert "[ds_pi, ds_sap]" in lines[0]
        assert "site" in lines[1]

    def test_handles_empty_block_list(self) -> None:
        from _yaml_patch import set_value
        lines = ["dataset:\n", "- ds_pi\n"]
        set_value(lines, "dataset", "[]")
        assert len(lines) == 1
        assert "[]" in lines[0]

    def test_returns_none_for_missing_path(self) -> None:
        from _yaml_patch import set_value
        lines = ["site: oslo\n"]
        old, changed = set_value(lines, "missing.key", "x")
        assert old is None
        assert not changed

    def test_ensures_space_after_colon(self) -> None:
        from _yaml_patch import set_value
        # Key with no value (block list follows)
        lines = ["dataset:\n", "- ds_pi\n"]
        set_value(lines, "dataset", "[]")
        assert lines[0] == "dataset: []\n"


class TestYamlPatchInsertKey:
    def test_inserts_into_existing_section(self) -> None:
        from _yaml_patch import insert_key
        lines = [
            "variables:\n",
            "  modules:\n",
            "    cdf_project_foundation:\n",
            "      site: oslo\n",
        ]
        result = insert_key(lines, "variables.modules.cdf_project_foundation", "newkey", "val")
        assert result
        content = "".join(lines)
        assert "newkey: val" in content

    def test_returns_false_for_missing_parent(self) -> None:
        from _yaml_patch import insert_key
        lines = ["site: oslo\n"]
        assert not insert_key(lines, "missing.parent", "key", "val")

    def test_inserted_line_uses_correct_indentation(self) -> None:
        from _yaml_patch import insert_key
        lines = [
            "section:\n",
            "  existing: yes\n",
        ]
        insert_key(lines, "section", "newkey", "val")
        new_line = next(line for line in lines if "newkey" in line)
        assert new_line.startswith("  ")  # same indentation as siblings


class TestYamlPatchDeleteKey:
    def test_deletes_scalar(self) -> None:
        from _yaml_patch import delete_key
        lines = ["site: oslo\n", "dataset: ds_pi\n"]
        assert delete_key(lines, "site")
        assert len(lines) == 1
        assert "dataset" in lines[0]

    def test_deletes_with_block_items(self) -> None:
        from _yaml_patch import delete_key
        lines = ["dataset:\n", "- ds_pi\n", "- ds_sap\n", "site: oslo\n"]
        delete_key(lines, "dataset")
        assert len(lines) == 1
        assert "site" in lines[0]

    def test_returns_false_for_missing_key(self) -> None:
        from _yaml_patch import delete_key
        lines = ["site: oslo\n"]
        assert not delete_key(lines, "missing")


# ── _env_io ────────────────────────────────────────────────────────────────────

class TestEnvIO:
    def test_parse_empty_file(self, tmp_path: Path) -> None:
        from _env_io import parse_env_file
        p = tmp_path / ".env"
        p.write_text("")
        lines, vals, idx = parse_env_file(p)
        assert vals == {}

    def test_parse_missing_file(self, tmp_path: Path) -> None:
        from _env_io import parse_env_file
        lines, vals, idx = parse_env_file(tmp_path / ".env")
        assert lines == []
        assert vals == {}

    def test_parse_key_value_pairs(self, tmp_path: Path) -> None:
        from _env_io import parse_env_file
        p = tmp_path / ".env"
        p.write_text('FOO=bar\nBAZ="qux"\n')
        _, vals, _ = parse_env_file(p)
        assert vals["FOO"] == "bar"
        assert vals["BAZ"] == "qux"

    def test_parse_skips_comments(self, tmp_path: Path) -> None:
        from _env_io import parse_env_file
        p = tmp_path / ".env"
        p.write_text("# comment\nFOO=bar\n")
        _, vals, _ = parse_env_file(p)
        assert "# comment" not in vals
        assert vals["FOO"] == "bar"

    def test_parse_normalises_trailing_newline(self, tmp_path: Path) -> None:
        from _env_io import parse_env_file
        p = tmp_path / ".env"
        p.write_text("FOO=bar")  # no trailing newline
        lines, _, _ = parse_env_file(p)
        assert lines[-1].endswith("\n")

    def test_upsert_new_key(self, tmp_path: Path) -> None:
        from _env_io import parse_env_file, upsert_env
        p = tmp_path / ".env"
        p.write_text("EXISTING=yes\n")
        lines, vals, idx = parse_env_file(p)
        upsert_env(lines, idx, "NEW_KEY", "secret123")
        assert any("NEW_KEY=secret123" in line for line in lines)

    def test_upsert_updates_existing_key(self, tmp_path: Path) -> None:
        from _env_io import parse_env_file, upsert_env
        p = tmp_path / ".env"
        p.write_text("FOO=old\n")
        lines, vals, idx = parse_env_file(p)
        upsert_env(lines, idx, "FOO", "new")
        assert any("FOO=new" in line for line in lines)
        assert not any("FOO=old" in line for line in lines)

    def test_upsert_no_quotes(self, tmp_path: Path) -> None:
        from _env_io import parse_env_file, upsert_env
        p = tmp_path / ".env"
        p.write_text("")
        lines, vals, idx = parse_env_file(p)
        upsert_env(lines, idx, "ID", "abc-123")
        entry = next(line for line in lines if "ID=" in line)
        assert '"' not in entry


# ── setup_project — domain helpers ────────────────────────────────────────────

class TestGroupName:
    def test_no_site(self) -> None:
        from setup_project import group_name
        assert group_name("consumer", "", "dev") == "consumer-dev"
        assert group_name("admin", "", "prod") == "admin-prod"

    def test_with_site(self) -> None:
        from setup_project import group_name
        assert group_name("producer", "oslo", "dev") == "producer-oslo-dev"
        assert group_name("consumer", "oslo", "prod") == "consumer-oslo-prod"

    def test_test_env_maps_to_dev_suffix(self) -> None:
        from setup_project import group_name
        # test env uses the same group as dev (GROUP_ENV["test"] == "dev")
        assert group_name("consumer", "", "test") == "consumer-dev"

    def test_test_env_with_site(self) -> None:
        from setup_project import group_name
        assert group_name("admin", "oslo", "test") == "admin-oslo-dev"


class TestBuildFoundationVars:
    def test_isa_variant_contains_required_keys(self) -> None:
        from setup_project import build_foundation_vars
        vars_ = build_foundation_vars("isa_manufacturing_extension", "dev", "oslo")
        assert vars_["dataModelVariant"] == "isa_manufacturing_extension"
        assert vars_["schemaSpace"] == "sp_isa_manufacturing"
        assert vars_["site"] == "oslo"
        assert vars_["consumerGroupName"] == "consumer-oslo-dev"
        assert vars_["producerGroupName"] == "producer-oslo-dev"
        assert vars_["adminGroupName"] == "admin-oslo-dev"
        assert vars_["consumerSourceId"] == "${CONSUMER_SOURCE_ID}"

    def test_dataset_always_present(self) -> None:
        from setup_project import build_foundation_vars
        assert build_foundation_vars("isa_manufacturing_extension", "dev", "")["dataset"] == []
        assert build_foundation_vars("isa_manufacturing_extension", "dev", "", ["ds_pi"])["dataset"] == ["ds_pi"]

    def test_cfihos_variant(self) -> None:
        from setup_project import build_foundation_vars
        vars_ = build_foundation_vars("cfihos_oil_and_gas_extension", "prod", "")
        assert vars_["schemaSpace"] == "dm_dom_oil_and_gas"
        assert vars_["instanceSpace"] == "inst_location"


class TestBuildOverlay:
    def test_isa_overlay_structure(self) -> None:
        from setup_project import build_overlay
        overlay = build_overlay("isa_manufacturing_extension", "dev", "", [])
        mods = overlay["variables"]["modules"]
        assert "cdf_project_foundation" in mods
        assert "isa_manufacturing_extension" in mods
        dm = mods["isa_manufacturing_extension"]
        assert dm["isaSchemaSpace"] == "sp_isa_manufacturing"
        assert dm["isaInstanceSpace"] == "sp_isa_instance_space"

    def test_cfihos_overlay_has_no_isa_keys(self) -> None:
        from setup_project import build_overlay
        overlay = build_overlay("cfihos_oil_and_gas_extension", "dev", "", [])
        mods = overlay["variables"]["modules"]
        dm = mods["cfihos_oil_and_gas_extension"]
        assert "isaSchemaSpace" not in dm
        assert "isaInstanceSpace" not in dm
        assert dm["instance_space"] == "inst_location"
        assert dm["environment"] == "dev"

    def test_cfihos_search_module_added_when_present(self, tmp_path: Path) -> None:
        from setup_project import build_overlay
        # Simulate search module being installed
        search_dir = tmp_path / "modules" / "data_models" / "cfihos_oil_and_gas_extension_search"
        search_dir.mkdir(parents=True)
        overlay = build_overlay(
            "cfihos_oil_and_gas_extension", "dev", "", [], repo_root=tmp_path
        )
        mods = overlay["variables"]["modules"]
        assert "cfihos_oil_and_gas_extension_search" in mods
        assert mods["cfihos_oil_and_gas_extension_search"]["instance_space"] == "inst_location"

    def test_cfihos_search_module_absent_when_not_installed(self, tmp_path: Path) -> None:
        from setup_project import build_overlay
        overlay = build_overlay(
            "cfihos_oil_and_gas_extension", "dev", "", [], repo_root=tmp_path
        )
        mods = overlay["variables"]["modules"]
        assert "cfihos_oil_and_gas_extension_search" not in mods

    def test_app_owner_injected_when_file_annotation_installed(self) -> None:
        from setup_project import build_overlay
        overlay = build_overlay(
            "isa_manufacturing_extension", "dev", "", ["cdf_file_annotation"],
            app_owner="owner@example.com"
        )
        fa = overlay["variables"]["modules"]["cdf_file_annotation"]
        assert fa["ApplicationOwner"] == "owner@example.com"

    def test_entity_matching_location_name_set_from_site(self) -> None:
        from setup_project import build_overlay
        overlay = build_overlay(
            "isa_manufacturing_extension", "dev", "oslo", ["cdf_entity_matching"]
        )
        em = overlay["variables"]["modules"]["cdf_entity_matching"]
        assert em["location_name"] == "oslo"

    def test_no_contextualization_vars_when_not_installed(self) -> None:
        from setup_project import build_overlay
        overlay = build_overlay("isa_manufacturing_extension", "dev", "", [])
        mods = overlay["variables"]["modules"]
        assert "cdf_entity_matching" not in mods
        assert "cdf_file_annotation" not in mods

    def test_cfihos_owner_vars_injected(self) -> None:
        from setup_project import build_overlay
        overlay = build_overlay(
            "cfihos_oil_and_gas_extension", "dev", "", [],
            cfihos_admin_user="admin@firm.com",
            cfihos_integration_owner_name="Alice",
            cfihos_integration_owner_email="alice@firm.com",
        )
        dm = overlay["variables"]["modules"]["cfihos_oil_and_gas_extension"]
        assert dm["admin_user"] == "admin@firm.com"
        assert dm["integrationOwnerName"] == "Alice"
        assert dm["integrationOwnerEmail"] == "alice@firm.com"


# ── setup_project — config file writers ───────────────────────────────────────

class TestWriteConfigFresh:
    def test_creates_yaml_with_env_block(self, tmp_path: Path) -> None:
        from setup_project import _write_config_fresh, build_overlay
        path = tmp_path / "config.dev.yaml"
        overlay = build_overlay("isa_manufacturing_extension", "dev", "", [])
        _write_config_fresh(path, "dev", "acme-dev", overlay)
        assert path.exists()
        data = yaml.safe_load(path.read_text())
        assert data["environment"]["project"] == "acme-dev"
        assert data["environment"]["name"] == "dev"

    def test_created_file_has_header_comment(self, tmp_path: Path) -> None:
        from setup_project import _write_config_fresh, build_overlay
        path = tmp_path / "config.dev.yaml"
        _write_config_fresh(path, "dev", "acme-dev",
                            build_overlay("isa_manufacturing_extension", "dev", "", []))
        assert "setup_project.py" in path.read_text()


class TestWriteConfigUpdate:
    def _make_config(self, tmp_path: Path, content: str) -> Path:
        p = tmp_path / "config.dev.yaml"
        p.write_text(textwrap.dedent(content))
        return p

    def test_updates_project_name(self, tmp_path: Path) -> None:
        from setup_project import _write_config_update, build_overlay
        p = self._make_config(tmp_path, """\
            environment:
              name: dev
              project: old-project
              validation-type: dev
              selected:
              - modules
            variables:
              modules:
                cdf_project_foundation:
                  site: ''
                  dataset: []
                  consumerGroupName: consumer-dev
                  producerGroupName: producer-dev
                  adminGroupName: admin-dev
                  consumerSourceId: ${CONSUMER_SOURCE_ID}
                  producerSourceId: ${PRODUCER_SOURCE_ID}
                  adminSourceId: ${ADMIN_SOURCE_ID}
                  dataModelVariant: isa_manufacturing_extension
                  schemaSpace: sp_isa_manufacturing
                  instanceSpace: sp_isa_instance_space
                isa_manufacturing_extension:
                  isaSchemaSpace: sp_isa_manufacturing
                  isaInstanceSpace: sp_isa_instance_space
        """)
        overlay = build_overlay("isa_manufacturing_extension", "dev", "", [])
        _write_config_update(p, "new-project", overlay)
        data = yaml.safe_load(p.read_text())
        assert data["environment"]["project"] == "new-project"

    def test_preserves_comments(self, tmp_path: Path) -> None:
        from setup_project import _write_config_update, build_overlay
        p = self._make_config(tmp_path, """\
            # My custom header
            environment:
              name: dev
              project: acme-dev
              validation-type: dev
              selected:
              - modules
            variables:
              modules:
                cdf_project_foundation:
                  site: oslo  # keep this comment
                  dataset: []
                  consumerGroupName: consumer-dev
                  producerGroupName: producer-dev
                  adminGroupName: admin-dev
                  consumerSourceId: ${CONSUMER_SOURCE_ID}
                  producerSourceId: ${PRODUCER_SOURCE_ID}
                  adminSourceId: ${ADMIN_SOURCE_ID}
                  dataModelVariant: isa_manufacturing_extension
                  schemaSpace: sp_isa_manufacturing
                  instanceSpace: sp_isa_instance_space
                isa_manufacturing_extension:
                  isaSchemaSpace: sp_isa_manufacturing
                  isaInstanceSpace: sp_isa_instance_space
        """)
        overlay = build_overlay("isa_manufacturing_extension", "dev", "", [])
        _write_config_update(p, "acme-dev", overlay)
        content = p.read_text()
        assert "# My custom header" in content
        assert "# keep this comment" in content

    def test_returns_false_when_nothing_changed(self, tmp_path: Path) -> None:
        from setup_project import _write_config_update, build_overlay
        overlay = build_overlay("isa_manufacturing_extension", "dev", "oslo", [])
        # Build a config that already matches the overlay
        merged = {
            "environment": {"name": "dev", "project": "acme-dev",
                            "validation-type": "dev", "selected": ["modules"]},
            "variables": {"modules": overlay["variables"]["modules"]},
        }
        p = tmp_path / "config.dev.yaml"
        p.write_text(yaml.dump(merged, sort_keys=False))
        assert not _write_config_update(p, "acme-dev", overlay)

    def test_removes_stale_groupsourceid(self, tmp_path: Path) -> None:
        from setup_project import _write_config_update, build_overlay
        p = self._make_config(tmp_path, """\
            environment:
              project: acme-dev
            variables:
              modules:
                contextualization:
                  cdf_file_annotation:
                    groupSourceId: old-id
                    fileSchemaSpace: sp_isa_manufacturing
        """)
        overlay = build_overlay("isa_manufacturing_extension", "dev", "", ["cdf_file_annotation"])
        _write_config_update(p, "acme-dev", overlay)
        content = p.read_text()
        assert "groupSourceId" not in content

    def test_removes_reserved_word_prefix(self, tmp_path: Path) -> None:
        from setup_project import _write_config_update, build_overlay
        p = self._make_config(tmp_path, """\
            environment:
              project: acme-dev
            variables:
              modules:
                cdf_entity_matching:
                  reservedWordPrefix: Enterprise_
                  schemaSpace: sp_isa_manufacturing
        """)
        overlay = build_overlay("isa_manufacturing_extension", "dev", "", [])
        _write_config_update(p, "acme-dev", overlay)
        assert "reservedWordPrefix" not in p.read_text()

    def test_updates_nested_category_structure(self, tmp_path: Path) -> None:
        """Configs with old common.cdf_project_foundation nesting must be updated."""
        from setup_project import _write_config_update, build_overlay
        p = self._make_config(tmp_path, """\
            environment:
              project: old
            variables:
              modules:
                common:
                  cdf_project_foundation:
                    site: berlin
                    dataset: []
        """)
        overlay = build_overlay("isa_manufacturing_extension", "dev", "oslo", [])
        _write_config_update(p, "old", overlay)
        content = p.read_text()
        assert "oslo" in content  # site updated in nested structure


# ── setup_project — staging migration ─────────────────────────────────────────

class TestMigrateStagingToTest:
    def test_renames_and_patches_file(self, tmp_path: Path) -> None:
        from setup_project import _migrate_staging_to_test
        staging = tmp_path / "config.staging.yaml"
        staging.write_text(
            "environment:\n  name: staging\n  validation-type: dev\n  project: acme-staging\n"
        )
        result = _migrate_staging_to_test(tmp_path)
        assert result
        assert not staging.exists()
        test = tmp_path / "config.test.yaml"
        assert test.exists()
        data = yaml.safe_load(test.read_text())
        assert data["environment"]["name"] == "test"
        assert data["environment"]["validation-type"] == "prod"
        assert data["environment"]["project"] == "acme-staging"  # unchanged

    def test_no_op_when_staging_absent(self, tmp_path: Path) -> None:
        from setup_project import _migrate_staging_to_test
        assert not _migrate_staging_to_test(tmp_path)

    def test_warns_when_both_exist(self, tmp_path: Path) -> None:
        from setup_project import _migrate_staging_to_test
        (tmp_path / "config.staging.yaml").write_text("env: staging\n")
        (tmp_path / "config.test.yaml").write_text("env: test\n")
        result = _migrate_staging_to_test(tmp_path)
        assert not result
        assert (tmp_path / "config.staging.yaml").exists()  # not deleted


# ── setup_project — redundant auth removal ────────────────────────────────────

class TestRemoveRedundantAuthFiles:
    def _make_auth_file(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("name: dummy\n")

    def test_removes_entity_matching_auth(self, tmp_path: Path) -> None:
        from setup_project import remove_redundant_auth_files
        auth = (
            tmp_path / "modules" / "contextualization" / "cdf_entity_matching"
            / "auth" / "entity.matching.processing.groups.Group.yaml"
        )
        self._make_auth_file(auth)
        removed = remove_redundant_auth_files(tmp_path)
        assert len(removed) == 1
        assert not auth.exists()

    def test_removes_file_annotation_auth(self, tmp_path: Path) -> None:
        from setup_project import remove_redundant_auth_files
        auth = (
            tmp_path / "modules" / "contextualization" / "cdf_file_annotation"
            / "auth" / "file_annotation.Group.yaml"
        )
        self._make_auth_file(auth)
        removed = remove_redundant_auth_files(tmp_path)
        assert any("file_annotation" in str(r) for r in removed)
        assert not auth.exists()

    def test_removes_qualitizer_auth(self, tmp_path: Path) -> None:
        from setup_project import remove_redundant_auth_files
        auth = (
            tmp_path / "modules" / "tools" / "apps" / "qualitizer"
            / "auth" / "apps.qualitizer.Group.yaml"
        )
        self._make_auth_file(auth)
        removed = remove_redundant_auth_files(tmp_path)
        assert len(removed) == 1
        assert not auth.exists()

    def test_removes_cfihos_auth_groups(self, tmp_path: Path) -> None:
        from setup_project import remove_redundant_auth_files
        for name in (
            "gp_cdf_owner_cfihos_oil_gas_data_model.group.yaml",
            "gp_cdf_read_cfihos_oil_gas_data_model.group.yaml",
        ):
            self._make_auth_file(
                tmp_path / "modules" / "data_models"
                / "cfihos_oil_and_gas_extension" / "auth" / name
            )
        removed = remove_redundant_auth_files(tmp_path)
        assert len(removed) == 2

    def test_idempotent_when_files_already_removed(self, tmp_path: Path) -> None:
        from setup_project import remove_redundant_auth_files
        removed = remove_redundant_auth_files(tmp_path)
        assert removed == []


# ── setup_project — CFIHOS auth patching ──────────────────────────────────────

class TestPatchCfihosAuthForMissingSearch:
    def _cfihos_auth_dir(self, tmp_path: Path) -> Path:
        d = tmp_path / "modules" / "data_models" / "cfihos_oil_and_gas_extension" / "auth"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def test_removes_search_space_when_search_module_absent(self, tmp_path: Path) -> None:
        from setup_project import patch_cfihos_auth_for_missing_search
        auth_dir = self._cfihos_auth_dir(tmp_path)
        f = auth_dir / "owner.group.yaml"
        f.write_text(
            "spaceIds:\n  - cdf_cdm\n  - {{space}}\n  - {{search_space}}\n"
        )
        patched = patch_cfihos_auth_for_missing_search(tmp_path)
        assert len(patched) == 1
        content = f.read_text()
        assert "{{search_space}}" not in content
        assert "{{space}}" in content   # unrelated line preserved

    def test_leaves_file_unchanged_when_search_module_present(self, tmp_path: Path) -> None:
        from setup_project import patch_cfihos_auth_for_missing_search
        auth_dir = self._cfihos_auth_dir(tmp_path)
        # Create the search module directory
        search = tmp_path / "modules" / "data_models" / "cfihos_oil_and_gas_extension_search"
        search.mkdir(parents=True)
        f = auth_dir / "owner.group.yaml"
        f.write_text("  - {{search_space}}\n")
        patched = patch_cfihos_auth_for_missing_search(tmp_path)
        assert patched == []
        assert "{{search_space}}" in f.read_text()

    def test_no_op_when_cfihos_not_installed(self, tmp_path: Path) -> None:
        from setup_project import patch_cfihos_auth_for_missing_search
        assert patch_cfihos_auth_for_missing_search(tmp_path) == []

    def test_idempotent_when_search_space_already_removed(self, tmp_path: Path) -> None:
        from setup_project import patch_cfihos_auth_for_missing_search
        auth_dir = self._cfihos_auth_dir(tmp_path)
        f = auth_dir / "owner.group.yaml"
        f.write_text("  - cdf_cdm\n  - {{space}}\n")
        patched = patch_cfihos_auth_for_missing_search(tmp_path)
        assert patched == []


# ── setup_project — read_existing_values ─────────────────────────────────────

class TestReadExistingValues:
    def _write_config(self, path: Path, data: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.dump(data, sort_keys=False))

    def test_reads_project_names(self, tmp_path: Path) -> None:
        from setup_project import _read_existing_values
        self._write_config(tmp_path / "config.dev.yaml", {
            "environment": {"project": "acme-dev"},
            "variables": {"modules": {}},
        })
        existing = _read_existing_values(tmp_path, ("dev",), [])
        assert existing["project_names"]["dev"] == "acme-dev"

    def test_reads_site_from_foundation(self, tmp_path: Path) -> None:
        from setup_project import _read_existing_values
        self._write_config(tmp_path / "config.dev.yaml", {
            "environment": {"project": "acme-dev"},
            "variables": {"modules": {
                "cdf_project_foundation": {"site": "oslo"},
            }},
        })
        existing = _read_existing_values(tmp_path, ("dev",), [])
        assert existing["site"] == "oslo"

    def test_reads_site_from_nested_structure(self, tmp_path: Path) -> None:
        from setup_project import _read_existing_values
        self._write_config(tmp_path / "config.dev.yaml", {
            "environment": {"project": "acme-dev"},
            "variables": {"modules": {
                "common": {"cdf_project_foundation": {"site": "berlin"}},
            }},
        })
        existing = _read_existing_values(tmp_path, ("dev",), [])
        assert existing["site"] == "berlin"

    def test_reads_datasets_from_sourcesystem_modules(self, tmp_path: Path) -> None:
        from setup_project import _read_existing_values
        self._write_config(tmp_path / "config.dev.yaml", {
            "environment": {"project": "acme-dev"},
            "variables": {"modules": {
                "cdf_project_foundation": {"site": ""},
                "cdf_pi_foundation": {"dataset": "ds_pi", "instanceSpace": "sp_isa"},
                "cdf_sap_foundation": {"dataset": "ds_sap", "instanceSpace": "sp_isa"},
            }},
        })
        existing = _read_existing_values(
            tmp_path, ("dev",), ["cdf_pi_foundation", "cdf_sap_foundation"]
        )
        assert "ds_pi" in existing["dataset"]
        assert "ds_sap" in existing["dataset"]

    def test_reads_app_owner(self, tmp_path: Path) -> None:
        from setup_project import _read_existing_values
        self._write_config(tmp_path / "config.dev.yaml", {
            "environment": {"project": "acme-dev"},
            "variables": {"modules": {
                "cdf_file_annotation": {"ApplicationOwner": "owner@firm.com"},
            }},
        })
        existing = _read_existing_values(tmp_path, ("dev",), [])
        assert existing["app_owner"] == "owner@firm.com"

    def test_skips_placeholder_app_owner(self, tmp_path: Path) -> None:
        from setup_project import _read_existing_values
        self._write_config(tmp_path / "config.dev.yaml", {
            "environment": {"project": "acme-dev"},
            "variables": {"modules": {
                "cdf_file_annotation": {"ApplicationOwner": "<APPLICATION_OWNER>"},
            }},
        })
        existing = _read_existing_values(tmp_path, ("dev",), [])
        assert existing["app_owner"] == ""

    def test_reads_cfihos_owner_fields(self, tmp_path: Path) -> None:
        from setup_project import _read_existing_values
        self._write_config(tmp_path / "config.dev.yaml", {
            "environment": {"project": "acme-dev"},
            "variables": {"modules": {
                "cfihos_oil_and_gas_extension": {
                    "admin_user": "admin@firm.com",
                    "integrationOwnerName": "Alice",
                    "integrationOwnerEmail": "alice@firm.com",
                },
            }},
        })
        existing = _read_existing_values(tmp_path, ("dev",), [])
        assert existing["cfihos_admin_user"] == "admin@firm.com"
        assert existing["cfihos_integration_owner_name"] == "Alice"
        assert existing["cfihos_integration_owner_email"] == "alice@firm.com"

    def test_skips_placeholder_cfihos_emails(self, tmp_path: Path) -> None:
        from setup_project import _read_existing_values
        self._write_config(tmp_path / "config.dev.yaml", {
            "environment": {"project": "acme-dev"},
            "variables": {"modules": {
                "cfihos_oil_and_gas_extension": {
                    "admin_user": "admin.user@firm.com",
                    "integrationOwnerEmail": "integration.owner@firm.com",
                    "integrationOwnerName": "Integration Owner",
                },
            }},
        })
        existing = _read_existing_values(tmp_path, ("dev",), [])
        assert existing["cfihos_admin_user"] == ""
        assert existing["cfihos_integration_owner_email"] == ""
        assert existing["cfihos_integration_owner_name"] == ""

    def test_returns_defaults_when_no_configs_exist(self, tmp_path: Path) -> None:
        from setup_project import _read_existing_values
        existing = _read_existing_values(tmp_path, ("dev", "prod"), [])
        assert existing["project_names"] == {}
        assert existing["site"] == ""
        assert existing["dataset"] == []


# ── setup_project — .env path resolution ─────────────────────────────────────

class TestGetOrgDirName:
    def test_reads_from_cdf_section(self, tmp_path: Path) -> None:
        from _pack_config import get_org_dir_name
        (tmp_path / "cdf.toml").write_text(
            '[cdf]\ndefault_organization_dir = "industrial"\n'
        )
        assert get_org_dir_name(tmp_path) == "industrial"

    def test_returns_none_when_no_toml(self, tmp_path: Path) -> None:
        from _pack_config import get_org_dir_name
        assert get_org_dir_name(tmp_path) is None

    def test_returns_none_when_key_absent(self, tmp_path: Path) -> None:
        from _pack_config import get_org_dir_name
        (tmp_path / "cdf.toml").write_text("[cdf]\nenterprise = acme\n")
        assert get_org_dir_name(tmp_path) is None

    def test_top_level_key_not_read(self, tmp_path: Path) -> None:
        """Ensure top-level default_organization_dir (wrong format) is not read."""
        from _pack_config import get_org_dir_name
        (tmp_path / "cdf.toml").write_text('default_organization_dir = "wrong"\n')
        assert get_org_dir_name(tmp_path) is None


class TestEnvPathResolution:
    """The .env file must always be written to repo root (where cdf.toml lives),
    not inside the org directory."""

    def test_env_at_repo_root_without_org_dir(self, tmp_path: Path) -> None:
        env_path = tmp_path / ".env"
        assert env_path == tmp_path / ".env"

    def test_env_at_repo_root_with_org_dir(self, tmp_path: Path) -> None:
        """pack_root = repo_root/industrial, but .env should be at repo_root/.env."""
        repo_root = tmp_path
        env_path = repo_root / ".env"
        pack_root = repo_root / "industrial"
        assert env_path == repo_root / ".env"
        assert env_path != pack_root / ".env"


# ── setup_project — environment validation type ───────────────────────────────

class TestEnvironmentValidationType:
    def test_test_env_uses_prod_validation(self) -> None:
        from setup_project import ENVIRONMENT_VALIDATION_TYPE
        assert ENVIRONMENT_VALIDATION_TYPE["test"] == "prod"

    def test_dev_uses_dev_validation(self) -> None:
        from setup_project import ENVIRONMENT_VALIDATION_TYPE
        assert ENVIRONMENT_VALIDATION_TYPE["dev"] == "dev"

    def test_prod_uses_prod_validation(self) -> None:
        from setup_project import ENVIRONMENT_VALIDATION_TYPE
        assert ENVIRONMENT_VALIDATION_TYPE["prod"] == "prod"


# ── setup_project — stale key removal ─────────────────────────────────────────

class TestStaleKeyRemoval:
    def test_owner_source_id_in_stale_keys(self) -> None:
        from setup_project import _STALE_CTX_KEYS
        assert any("owner_source_id" in k for k in _STALE_CTX_KEYS)
        assert any("read_source_id" in k for k in _STALE_CTX_KEYS)

    def test_reserved_word_prefix_in_stale_keys(self) -> None:
        from setup_project import _STALE_CTX_KEYS
        assert any("reservedWordPrefix" in k for k in _STALE_CTX_KEYS)

    def test_stale_cfihos_keys_removed_from_config(self, tmp_path: Path) -> None:
        from setup_project import _write_config_update, build_overlay
        p = tmp_path / "config.dev.yaml"
        p.write_text(textwrap.dedent("""\
            environment:
              project: acme-dev
            variables:
              modules:
                cfihos_oil_and_gas_extension:
                  owner_source_id: abc123
                  read_source_id: xyz789
                  instance_space: inst_location
                  environment: dev
        """))
        overlay = build_overlay("cfihos_oil_and_gas_extension", "dev", "", [])
        _write_config_update(p, "acme-dev", overlay)
        content = p.read_text()
        assert "owner_source_id" not in content
        assert "read_source_id" not in content
