"""Tests for validate_packages registry parsing."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validate_packages import (
    PackageSpec,
    PackagesRegistry,
    RawDatabaseGap,
    find_raw_database_gaps,
    parse_packages_registry,
)


def test_parse_packages_registry_accepts_minimal_valid_shape() -> None:
    data = {
        "library": {"description": "Test library"},
        "packages": {
            "demo": {
                "id": "dp:demo",
                "title": "Demo",
                "description": "Demo pack",
                "modules": ["common/cdf_common"],
            }
        },
    }

    registry = parse_packages_registry(data)

    assert registry == PackagesRegistry(
        description="Test library",
        packages={
            "demo": PackageSpec(
                id="dp:demo",
                title="Demo",
                description="Demo pack",
                modules=["common/cdf_common"],
            )
        },
    )


def test_parse_packages_registry_rejects_missing_library() -> None:
    assert parse_packages_registry({"packages": {}}) is None


def test_parse_packages_registry_rejects_empty_module_path() -> None:
    data = {
        "library": {"description": "Test library"},
        "packages": {
            "demo": {
                "id": "dp:demo",
                "title": "Demo",
                "description": "Demo pack",
                "modules": [""],
            }
        },
    }

    assert parse_packages_registry(data) is None


def test_find_raw_database_gaps_flags_table_with_no_local_database_yaml(tmp_path: Path) -> None:
    raw_dir = tmp_path / "sourcesystem" / "cdf_pi_data_dump" / "raw"
    raw_dir.mkdir(parents=True)
    (raw_dir / "timeseries.Table.yaml").write_text("dbName: cfihos_oil_and_gas\ntableName: timeseries\n")

    # A different, unrelated module declares the same database name -- this must
    # NOT count, because each module has to be independently deployable.
    other_raw_dir = tmp_path / "sourcesystem" / "cdf_sap_data_dump" / "raw"
    other_raw_dir.mkdir(parents=True)
    (other_raw_dir / "cfihos_oil_and_gas.Database.yaml").write_text("dbName: cfihos_oil_and_gas\n")

    gaps = find_raw_database_gaps(str(tmp_path))

    assert gaps == [
        RawDatabaseGap(
            module_path="sourcesystem/cdf_pi_data_dump",
            db_name="cfihos_oil_and_gas",
            table_files=("timeseries.Table.yaml",),
        )
    ]


def test_find_raw_database_gaps_accepts_matching_database_yaml_in_same_module(tmp_path: Path) -> None:
    raw_dir = tmp_path / "sourcesystem" / "cdf_sap_extractor" / "raw"
    raw_dir.mkdir(parents=True)
    (raw_dir / "equipment.Table.yaml").write_text('dbName: "db_{{location}}_sap"\ntableName: equipment\n')
    (raw_dir / "db_sap.Database.yaml").write_text('dbName: "db_{{location}}_sap"\n')

    assert find_raw_database_gaps(str(tmp_path)) == []


def test_find_raw_database_gaps_handles_list_shaped_table_files(tmp_path: Path) -> None:
    raw_dir = tmp_path / "datamodels" / "isa_manufacturing_extension" / "raw"
    raw_dir.mkdir(parents=True)
    (raw_dir / "isa_asset.Table.yaml").write_text("- dbName: {{ rawDatabase }}\n  tableName: isa_asset\n")
    (raw_dir / "isa_all_manufacturing.Database.yaml").write_text("dbName: {{ rawDatabase }}\n")

    assert find_raw_database_gaps(str(tmp_path)) == []


def test_find_raw_database_gaps_ignores_trailing_comments(tmp_path: Path) -> None:
    raw_dir = tmp_path / "sourcesystem" / "cdf_pi_data_dump" / "raw"
    raw_dir.mkdir(parents=True)
    (raw_dir / "timeseries.Table.yaml").write_text(
        "dbName: cfihos_oil_and_gas  # some comment\ntableName: timeseries\n"
    )
    (raw_dir / "cfihos_oil_and_gas.Database.yaml").write_text('dbName: "cfihos_oil_and_gas"  # another comment\n')

    assert find_raw_database_gaps(str(tmp_path)) == []


def test_find_raw_database_gaps_matches_table_yaml_case_insensitively(tmp_path: Path) -> None:
    raw_dir = tmp_path / "common" / "cdf_common" / "raw"
    raw_dir.mkdir(parents=True)
    (raw_dir / "contextualization_state.table.yaml").write_text(
        "dbName: contextualizationState\ntableName: diagramParsing\n"
    )
    (raw_dir / "contextualization_state.DataBase.yaml").write_text("dbName: contextualizationState\n")

    assert find_raw_database_gaps(str(tmp_path)) == []
