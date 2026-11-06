"""Tests for the entity matching scoped-matching test helper script."""

import importlib.util
from pathlib import Path
from types import ModuleType

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "modules"
    / "contextualization"
    / "cdf_entity_matching"
    / "testing"
    / "apply_scoped_matching.py"
)


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("apply_scoped_matching", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_default_config(path: Path, *, schema_space: str, asset_view: str, primary: str) -> None:
    path.write_text(
        "\n".join(
            [
                f"schemaSpace: {schema_space}",
                "viewVersion: v1",
                "scopeSchemaSpace: sp_entity_matching_scope",
                "scopeDmVersion: v1",
                "scopeDataModelExternalId: EntityMatchingScope_SOL",
                f"assetViewExternalId: {asset_view}",
                "timeseriesViewExternalId: CogniteTimeSeries",
                "targetViewExternalId: CogniteAsset",
                "entityViewExternalId: CogniteTimeSeries",
                f"primaryScopeProperty: {primary}",
                "secondaryScopeProperty: ''",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _seed_payload(testing_dir: Path, copies: tuple[tuple[str, str], ...]) -> None:
    for source_rel, _dest_rel in copies:
        source = testing_dir / source_rel
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(f"# fixture {source_rel}\n", encoding="utf-8")


def test_apply_copies_payload_and_points_config_at_scoped_views(tmp_path: Path) -> None:
    script = _load_script()
    module_dir = tmp_path / "module"
    testing_dir = module_dir / "testing"
    testing_dir.mkdir(parents=True)
    _write_default_config(
        module_dir / "default.config.yaml",
        schema_space="cdf_cdm",
        asset_view="CogniteAsset",
        primary="''",
    )
    _seed_payload(testing_dir, script.RESOURCE_COPIES)

    script.apply_scoped_matching(module_dir=module_dir, testing_dir=testing_dir)

    config = (module_dir / "default.config.yaml").read_text(encoding="utf-8")
    assert "schemaSpace: sp_entity_matching_scope" in config
    assert "assetViewExternalId: Asset" in config
    assert "timeseriesViewExternalId: ScopedTimeSeries" in config
    assert "primaryScopeProperty: site" in config
    assert "secondaryScopeProperty: unit" in config
    for _source_rel, dest_rel in script.RESOURCE_COPIES:
        dest = module_dir / dest_rel
        assert dest.is_file(), dest
        assert dest.read_text(encoding="utf-8").startswith("# fixture")


def test_revert_removes_copied_files_and_restores_cdm_defaults(tmp_path: Path) -> None:
    script = _load_script()
    module_dir = tmp_path / "module"
    testing_dir = module_dir / "testing"
    testing_dir.mkdir(parents=True)
    _write_default_config(
        module_dir / "default.config.yaml",
        schema_space="sp_entity_matching_scope",
        asset_view="Asset",
        primary="site",
    )
    _seed_payload(testing_dir, script.RESOURCE_COPIES)
    script.apply_scoped_matching(module_dir=module_dir, testing_dir=testing_dir)

    script.revert_scoped_matching(module_dir=module_dir)

    config = (module_dir / "default.config.yaml").read_text(encoding="utf-8")
    assert "schemaSpace: cdf_cdm" in config
    assert "assetViewExternalId: CogniteAsset" in config
    assert "timeseriesViewExternalId: CogniteTimeSeries" in config
    assert "targetViewExternalId: CogniteAsset" in config
    assert "entityViewExternalId: CogniteTimeSeries" in config
    assert "primaryScopeProperty: ''" in config
    assert "secondaryScopeProperty: ''" in config
    for _source_rel, dest_rel in script.RESOURCE_COPIES:
        assert not (module_dir / dest_rel).exists()
