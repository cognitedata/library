"""Tests for the file annotation scoped-matching test helper script."""

import importlib.util
from pathlib import Path
from types import ModuleType

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "modules"
    / "contextualization"
    / "cdf_file_annotation"
    / "local_setup"
    / "apply_scoped_matching.py"
)


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("apply_scoped_matching", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_default_config(
    path: Path,
    *,
    file_schema_space: str,
    file_external_id: str,
    primary: str,
) -> None:
    path.write_text(
        "\n".join(
            [
                f"fileSchemaSpace: {file_schema_space}",
                "fileVersion: v1",
                f"fileExternalId: {file_external_id}",
                "targetEntitySchemaSpace: cdf_cdm",
                "targetEntityExternalId: CogniteAsset",
                "targetEntityVersion: v1",
                "scopeSchemaSpace: sp_file_annotation_scope",
                "scopeDmVersion: v1",
                "scopeDataModelExternalId: FileAnnotationScope_SOL",
                f"primaryScopeProperty: {primary}",
                'secondaryScopeProperty: ""',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _seed_payload(local_setup_dir: Path, copies: tuple[tuple[str, str], ...]) -> None:
    for source_rel, _dest_rel in copies:
        source = local_setup_dir / source_rel
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(f"# fixture {source_rel}\n", encoding="utf-8")


def test_apply_copies_payload_and_points_config_at_scoped_views(tmp_path: Path) -> None:
    script = _load_script()
    module_dir = tmp_path / "module"
    local_setup_dir = module_dir / "local_setup"
    local_setup_dir.mkdir(parents=True)
    _write_default_config(
        module_dir / "default.config.yaml",
        file_schema_space="cdf_cdm",
        file_external_id="CogniteFile",
        primary='""',
    )
    _seed_payload(local_setup_dir, script.RESOURCE_COPIES)

    script.apply_scoped_matching(module_dir=module_dir, local_setup_dir=local_setup_dir)

    config = (module_dir / "default.config.yaml").read_text(encoding="utf-8")
    assert "fileSchemaSpace: sp_file_annotation_scope" in config
    assert "fileExternalId: ScopedFile" in config
    assert "targetEntitySchemaSpace: sp_file_annotation_scope" in config
    assert "targetEntityExternalId: Asset" in config
    assert "primaryScopeProperty: site" in config
    assert "secondaryScopeProperty: unit" in config
    for _source_rel, dest_rel in script.RESOURCE_COPIES:
        dest = module_dir / dest_rel
        assert dest.is_file(), dest
        assert dest.read_text(encoding="utf-8").startswith("# fixture")


def test_revert_removes_copied_files_and_restores_cdm_defaults(tmp_path: Path) -> None:
    script = _load_script()
    module_dir = tmp_path / "module"
    local_setup_dir = module_dir / "local_setup"
    local_setup_dir.mkdir(parents=True)
    _write_default_config(
        module_dir / "default.config.yaml",
        file_schema_space="sp_file_annotation_scope",
        file_external_id="ScopedFile",
        primary="site",
    )
    _seed_payload(local_setup_dir, script.RESOURCE_COPIES)
    script.apply_scoped_matching(module_dir=module_dir, local_setup_dir=local_setup_dir)

    script.revert_scoped_matching(module_dir=module_dir)

    config = (module_dir / "default.config.yaml").read_text(encoding="utf-8")
    assert "fileSchemaSpace: cdf_cdm" in config
    assert "fileExternalId: CogniteFile" in config
    assert "targetEntitySchemaSpace: cdf_cdm" in config
    assert "targetEntityExternalId: CogniteAsset" in config
    assert 'primaryScopeProperty: ""' in config
    assert 'secondaryScopeProperty: ""' in config
    for _source_rel, dest_rel in script.RESOURCE_COPIES:
        assert not (module_dir / dest_rel).exists()
