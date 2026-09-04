"""Tests for the Data Quality Toolkit module scripts."""

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPTS = (
    Path(__file__).resolve().parents[1] / "modules" / "common" / "cdf_dq_runtime" / "scripts"
)
_MODULE = _SCRIPTS.parent


def _load_cli():
    spec = importlib.util.spec_from_file_location("cdf_dq_runtime_cli", _SCRIPTS / "_cli.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def cli():
    return _load_cli()


def test_flatten_keeps_global_and_module_variables(cli) -> None:
    config = {
        "variables": {
            "global_var": "global_val",
            "modules": {
                "data_quality": {
                    "cdf_dq_runtime": {
                        "dq_pypi_version": "0.4.9",
                    }
                }
            },
        }
    }
    flat = cli.flatten_toolkit_variables(config)
    assert flat["global_var"] == "global_val"
    assert flat["dq_pypi_version"] == "0.4.9"


def test_materialize_cog_ai_yourorg_sample(cli, tmp_path: Path) -> None:
    variables = cli.load_default_pack_variables(_MODULE)
    assert variables == {"dq_pypi_version": "0.4.9"}
    dest = cli.materialize_pack_yaml(_MODULE, tmp_path / "out", variables)
    settings = (dest / "settings.yaml").read_text(encoding="utf-8")
    assert "enterprise-process-industry" in settings
    assert "timeseries:" in settings
    assert "data_product_sync_cron" in settings
    assert "dq-ts-shacl" in settings
    assert "{{" not in settings
    version_yaml = (
        dest / "data_products" / "enterprise_process_industry.DataProductVersion.yaml"
    ).read_text(encoding="utf-8")
    for view in (
        "YourOrgAsset",
        "YourOrgEquipment",
        "YourOrgMaintenanceOrder",
        "YourOrgNotification",
        "YourOrgOperation",
        "YourOrgTimeSeries",
    ):
        assert view in version_yaml
    assert len(list((dest / "views").glob("yourorg_*.yaml"))) == 6
    assert (dest / "timeseries" / "yourorg_timeseries_quality.yaml").is_file()
    assert not (dest / "timeseries" / "demo_timeseries_quality.yaml").exists()
    ts_yaml = (dest / "timeseries" / "yourorg_timeseries_quality.yaml").read_text(encoding="utf-8")
    assert "ruleset_references" in ts_yaml


def test_data_quality_space_from_settings(cli, tmp_path: Path) -> None:
    settings = tmp_path / "settings.yaml"
    settings.write_text("config_space: dataQuality\n", encoding="utf-8")
    assert cli.data_quality_space(cli.load_settings_raw(settings)) == "dataQuality"
