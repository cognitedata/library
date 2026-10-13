"""Tests for the Data Quality Toolkit module scripts."""

import importlib.util
import os
import re
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

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
    sys.modules["_cli"] = module
    spec.loader.exec_module(module)
    return module


def _load_pipeline(cli_module):
    sys.modules["_cli"] = cli_module
    name = "cdf_dq_runtime_deploy_pipeline"
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / "deploy_pipeline.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def cli():
    return _load_cli()


_CORE_VIEWS = {
    ("cdf_cdm", "CogniteAsset"),
    ("cdf_cdm", "CogniteEquipment"),
    ("cdf_cdm", "CogniteTimeSeries"),
    ("cdf_idm", "CogniteMaintenanceOrder"),
    ("cdf_idm", "CogniteOperation"),
    ("cdf_idm", "CogniteNotification"),
}
_VIEW_NAMESPACE = re.compile(r"http://purl\.org/cognite/([A-Za-z0-9_]+)/([A-Za-z0-9_]+)/")


def _materialize(cli, tmp_path: Path, overrides: dict[str, str] | None = None) -> Path:
    variables = cli.load_default_pack_variables(_MODULE)
    variables.update(overrides or {})
    return cli.materialize_pack_yaml(_MODULE, tmp_path / "out", variables)


def _load_yaml(path: Path) -> dict:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


def _data_product_version(dest: Path) -> dict:
    (path,) = (dest / "data_products").glob("*.DataProductVersion.yaml")
    return _load_yaml(path)


def _view_namespaces(dest: Path) -> set[tuple[str, str]]:
    namespaces: set[tuple[str, str]] = set()
    for path in (dest / "rulesets").glob("*.RuleSetVersion.yaml"):
        for rule in _load_yaml(path)["rules"]:
            namespaces.update(_VIEW_NAMESPACE.findall(rule))
    return namespaces


def test_module_variables_scope_to_dq_runtime(cli) -> None:
    config = {
        "variables": {
            "global_var": "global_val",
            "modules": {
                "contextualization": {"cdf_entity_matching": {"schemaSpace": "sp_entity_matching_scope"}},
                "common": {
                    "shared_var": "shared",
                    "cdf_dq_runtime": {"schemaSpace": "cdf_cdm", "instanceSpace": "inst_location"},
                },
                "sourcesystem": {"cdf_pi_extractor": {"instanceSpace": "sp_test_pi"}},
            },
        }
    }
    resolved = cli.module_toolkit_variables(config)
    assert resolved == {
        "global_var": "global_val",
        "shared_var": "shared",
        "schemaSpace": "cdf_cdm",
        "instanceSpace": "inst_location",
    }


def test_materialize_defaults_target_cognite_core_views(cli, tmp_path: Path) -> None:
    dest = _materialize(cli, tmp_path)

    for path in dest.rglob("*.y*ml"):
        assert "{{" not in path.read_text(encoding="utf-8"), path

    variables = cli.load_default_pack_variables(_MODULE)
    version = _data_product_version(dest)
    assert {(view["space"], view["externalId"]) for view in version["views"]} == _CORE_VIEWS
    assert {space for view in version["views"] for space in view["instanceSpaces"]["read"]} == {"inst_location"}
    assert {rule["version"] for rule in version["quality"]["rules"]} == {variables["ruleSetVersion"]}

    (product_path,) = (dest / "data_products").glob("*.DataProduct.yaml")
    product = _load_yaml(product_path)
    assert product["schemaSpace"] == variables["dataProductSchemaSpace"]
    assert not product["schemaSpace"].startswith("cdf_")

    settings = _load_yaml(dest / "settings.yaml")
    assert settings["external_dataproducts"] == [
        {"external_id": version["dataProductExternalId"], "version": version["version"]}
    ]
    assert settings["config_space"] == settings["records"]["space"] == "dataQuality"

    for path in (dest / "views").glob("*.yaml"):
        override = _load_yaml(path)
        assert (override["view"]["space"], override["view"]["external_id"]) in _CORE_VIEWS
        assert override["instance_spaces"] == ["inst_location"]

    (ts_path,) = (dest / "timeseries").glob("*.yaml")
    ts_config = _load_yaml(ts_path)
    assert (ts_config["datamodel"]["space"], ts_config["datamodel"]["external_id"]) == ("cdf_cdm", "CogniteTimeSeries")
    assert ts_config["filter"]["space"] == "inst_location"


def test_sample_shacl_rules_target_configured_views(cli, tmp_path: Path) -> None:
    dest = _materialize(cli, tmp_path)
    helper_types = {("cdf_cdm", "CogniteAssetType"), ("cdf_cdm", "CogniteEquipmentType")}
    assert _view_namespaces(dest) <= _CORE_VIEWS | helper_types
    for path in (dest / "rulesets").glob("*.yaml"):
        text = path.read_text(encoding="utf-8")
        for legacy in ("YourOrg", "sp_enterprise_process_industry", "springfield_instances", "test_data_pumps"):
            assert legacy not in text, f"{path.name} still references {legacy}"


def test_customer_overrides_flow_into_rules_and_views(cli, tmp_path: Path) -> None:
    dest = _materialize(
        cli,
        tmp_path,
        {
            "schemaSpace": "sp_acme",
            "secondarySchemaSpace": "sp_acme",
            "viewAssetExternalId": "AcmeAsset",
            "instanceSpace": "inst_acme",
        },
    )
    namespaces = _view_namespaces(dest)
    assert ("sp_acme", "AcmeAsset") in namespaces
    assert ("sp_acme", "CogniteMaintenanceOrder") in namespaces
    assert not any(view == "CogniteAsset" for _, view in namespaces)

    version = _data_product_version(dest)
    assert {view["space"] for view in version["views"]} == {"sp_acme"}
    assert {space for view in version["views"] for space in view["instanceSpaces"]["read"]} == {"inst_acme"}

    ts_rules = _load_yaml(dest / "rulesets" / "timeseries_quality_rules.RuleSetVersion.yaml")["rules"][0]
    assert "http://purl.org/cognite/inst_acme/pi:160554" in ts_rules


def test_data_quality_space_from_settings(cli, tmp_path: Path) -> None:
    settings = tmp_path / "settings.yaml"
    settings.write_text("config_space: dataQuality\n", encoding="utf-8")
    assert cli.data_quality_space(cli.load_settings_raw(settings)) == "dataQuality"


def test_load_project_env_uses_cdf_toml_root(cli, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "toolkit-project"
    project.mkdir()
    (project / "cdf.toml").write_text("[cdf]\n", encoding="utf-8")
    (project / ".env").write_text(
        "CDF_PROJECT=from-env\nIDP_CLIENT_ID=client-id\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(project)
    monkeypatch.delenv("CDF_PROJECT", raising=False)
    monkeypatch.setenv("IDP_CLIENT_ID", "already-set")

    loaded = cli.load_project_env()
    assert loaded == project / ".env"
    assert os.environ["CDF_PROJECT"] == "from-env"
    assert os.environ["IDP_CLIENT_ID"] == "already-set"


def test_load_project_env_honors_explicit_path(cli, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / "custom.env"
    env_file.write_text("COGNITE_PROJECT=custom-project\n", encoding="utf-8")
    monkeypatch.delenv("COGNITE_PROJECT", raising=False)

    loaded = cli.load_project_env(env_file)
    assert loaded == env_file.resolve()
    assert os.environ["COGNITE_PROJECT"] == "custom-project"


def test_print_infrastructure_summary_formats_external_dataproducts(cli, capsys) -> None:
    results = {
        "functions": {"function": {"function": "data-quality-validation", "status": "deployed"}},
        "external_dataproduct_workflows": [
            {
                "dataProduct": "enterprise-process-industry",
                "version": "1.1.10",
                "status": "deployed",
                "views": 5,
                "workflows": [
                    {
                        "workflow_external_id": "dq-shacl-enterprise-process-industry",
                        "uniqueness_workflow_external_id": "dq-shacl-enterprise-process-industry-uniqueness",
                        "edge_existence_workflow_external_id": "dq-shacl-enterprise-process-industry-edge-existence",
                    }
                ],
            }
        ],
        "data_product_sync": [
            {
                "workflow_external_id": "dq-data-product-sync",
                "trigger": "dq-data-product-sync-trigger",
                "status": "deployed",
            }
        ],
        "historic_queue_manager": [
            {
                "workflow_external_id": "dq-historic-queue-manager",
                "trigger": "dq-historic-queue-manager-trigger",
                "status": "deployed",
            }
        ],
    }
    cli.print_infrastructure_summary(results)
    output = capsys.readouterr().out
    assert "enterprise-process-industry @ 1.1.10" in output
    assert "sync-cursor: dq-shacl-enterprise-process-industry" in output
    assert "dq-data-product-sync (deployed)" in output
    assert "dq-historic-queue-manager (deployed)" in output


def test_function_secrets_from_toml(cli, tmp_path: Path) -> None:
    toml_path = tmp_path / "config.toml"
    toml_path.write_text(
        '[cognite]\nclient_id = "abc"\nclient_secret = "s3cret"\n',
        encoding="utf-8",
    )
    assert cli.function_secrets_from_toml(toml_path) == {
        "client-id": "abc",
        "client-secret": "s3cret",
    }
    assert cli.function_secrets_from_toml(tmp_path / "missing.toml") is None


def _install_fake_data_quality(monkeypatch: pytest.MonkeyPatch, *, pipeline_impl, settings) -> None:
    fake_cdq = types.ModuleType("cognite_data_quality")
    fake_cdq.deploy_validation_pipeline = pipeline_impl
    fake_deploy = types.ModuleType("cognite_data_quality.deploy")
    fake_deploy.load_settings = lambda _path: settings
    monkeypatch.setitem(sys.modules, "cognite_data_quality", fake_cdq)
    monkeypatch.setitem(sys.modules, "cognite_data_quality.deploy", fake_deploy)


def test_deploy_pipeline_dry_run_does_not_call_pipeline(
    cli, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    pipeline = _load_pipeline(cli)
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text("config_space: dataQuality\n", encoding="utf-8")

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("deploy_validation_pipeline must not run during --dry-run")

    _install_fake_data_quality(
        monkeypatch,
        pipeline_impl=fail_if_called,
        settings=SimpleNamespace(
            external_dataproducts=[SimpleNamespace(external_id="dp-1")],
            effective_config_space="dataQuality",
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "resolve_cognite_client",
        lambda _path: (_ for _ in ()).throw(AssertionError("client must not be created during --dry-run")),
    )

    assert pipeline.main(["--dry-run", "--settings-path", str(settings_path)]) == 0
    output = capsys.readouterr().out
    assert "[DRY RUN] No changes were made" in output
    assert "historic_mode: enqueue" in output
    assert "data_product_external_id: dp-1" in output


def test_deploy_pipeline_live_calls_package(cli, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipeline = _load_pipeline(cli)
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text("config_space: dataQuality\n", encoding="utf-8")
    captured: dict[str, object] = {}

    def fake_pipeline(*_args, **kwargs):
        captured.update(kwargs)
        return {"status": "ok", "total_enqueued": 2}

    _install_fake_data_quality(
        monkeypatch,
        pipeline_impl=fake_pipeline,
        settings=SimpleNamespace(
            external_dataproducts=[SimpleNamespace(external_id="dp-1")],
            effective_config_space="dataQuality",
        ),
    )

    class _Client:
        config = SimpleNamespace(project="demo")

    monkeypatch.setattr(pipeline, "resolve_cognite_client", lambda _path: _Client())

    assert pipeline.main(["--settings-path", str(settings_path), "--historic-mode", "enqueue"]) == 0
    assert captured["historic_mode"] == "enqueue"
    assert captured["data_product_external_id"] == "dp-1"
    assert "dry_run" not in captured
