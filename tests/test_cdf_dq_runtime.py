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

_SCRIPTS = Path(__file__).resolve().parents[1] / "modules" / "common" / "cdf_dq_runtime" / "scripts"
_MODULE = _SCRIPTS.parent


def _load_cli() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("cdf_dq_runtime_cli", _SCRIPTS / "_cli.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    sys.modules["_cli"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def cli() -> types.ModuleType:
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


def _materialize(cli: types.ModuleType, tmp_path: Path, overrides: dict[str, str] | None = None) -> Path:
    variables = cli.load_default_pack_variables(_MODULE)
    variables.update(overrides or {})
    return cli.materialize_pack_yaml(_MODULE, tmp_path / "out", variables)


def _load_yaml(path: Path) -> dict[str, object]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


def _data_product_version(dest: Path) -> dict[str, object]:
    (path,) = (dest / "data_products").glob("*.DataProductVersion.yaml")
    return _load_yaml(path)


def _view_namespaces(dest: Path) -> set[tuple[str, str]]:
    namespaces: set[tuple[str, str]] = set()
    for path in (dest / "rulesets").glob("*.RuleSetVersion.yaml"):
        for rule in _load_yaml(path)["rules"]:
            namespaces.update(_VIEW_NAMESPACE.findall(rule))
    return namespaces


def test_module_variables_scope_to_dq_runtime(cli: types.ModuleType) -> None:
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


def test_materialize_defaults_target_cognite_core_views(cli: types.ModuleType, tmp_path: Path) -> None:
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
    assert settings["config_space"] == settings["records"]["space"] == "inst_data_quality"

    for path in (dest / "views").glob("*.yaml"):
        override = _load_yaml(path)
        assert (override["view"]["space"], override["view"]["external_id"]) in _CORE_VIEWS
        assert override["instance_spaces"] == ["inst_location"]

    (ts_path,) = (dest / "timeseries_config").glob("*.yaml")
    ts_config = _load_yaml(ts_path)
    assert (ts_config["datamodel"]["space"], ts_config["datamodel"]["external_id"]) == ("cdf_cdm", "CogniteTimeSeries")
    assert ts_config["filter"]["space"] == "inst_location"


def test_sample_shacl_rules_target_configured_views(cli: types.ModuleType, tmp_path: Path) -> None:
    dest = _materialize(cli, tmp_path)
    helper_types = {("cdf_cdm", "CogniteAssetType"), ("cdf_cdm", "CogniteEquipmentType")}
    assert _view_namespaces(dest) <= _CORE_VIEWS | helper_types
    for path in (dest / "rulesets").glob("*.yaml"):
        text = path.read_text(encoding="utf-8")
        for legacy in ("YourOrg", "sp_enterprise_process_industry", "springfield_instances", "test_data_pumps"):
            assert legacy not in text, f"{path.name} still references {legacy}"


def test_materialize_skips_files_under_dest_inside_source(cli: types.ModuleType, tmp_path: Path) -> None:
    source = tmp_path / "pack"
    source.mkdir()
    (source / "settings.yaml").write_text("space: {{schemaSpace}}\n", encoding="utf-8")
    dest = source / "build"
    stale = dest / "settings.yaml"
    stale.parent.mkdir()
    stale.write_text("space: STALE\n", encoding="utf-8")

    cli.materialize_pack_yaml(source, dest, {"schemaSpace": "cdf_cdm"})

    assert dest.joinpath("settings.yaml").read_text(encoding="utf-8") == "space: cdf_cdm\n"


def test_customer_overrides_flow_into_rules_and_views(cli: types.ModuleType, tmp_path: Path) -> None:
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
    assert "http://purl.org/cognite/sp_acme/CogniteTimeSeries/" in ts_rules
    assert "pi:160554" not in ts_rules
    assert "pi:160245" not in ts_rules
    assert "sh:targetNode" not in ts_rules
    assert not any(
        path.name.startswith("maintenance_order_rule_logic_test") for path in (dest / "rulesets").glob("*.yaml")
    )


def test_load_settings_raw_returns_typed_runtime_settings(cli: types.ModuleType, tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        "\n".join(
            [
                "config_space: inst_data_quality",
                "config_source: dataproduct",
                "data_product_sync_cron: '15 * * * *'",
                "records:",
                "  space: inst_data_quality",
                "  stream_id: dq_all_validation_results",
                "",
            ]
        ),
        encoding="utf-8",
    )
    settings = cli.load_settings_raw(settings_path)
    assert isinstance(settings, cli.RuntimeSettings)
    assert settings.config_space == "inst_data_quality"
    assert settings.config_source == "dataproduct"
    assert settings.data_product_sync_cron == "15 * * * *"
    assert settings.records is not None
    assert settings.records.space == "inst_data_quality"
    assert settings.records.stream_id == "dq_all_validation_results"
    assert settings.effective_config_space == "inst_data_quality"


def test_data_quality_space_from_settings(cli: types.ModuleType, tmp_path: Path) -> None:
    settings = tmp_path / "settings.yaml"
    settings.write_text("config_space: inst_data_quality\n", encoding="utf-8")
    assert cli.data_quality_space(cli.load_settings_raw(settings)) == "inst_data_quality"


def test_data_quality_space_falls_back_to_records_space(cli: types.ModuleType, tmp_path: Path) -> None:
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "records:\n  space: fromRecords\n  stream_id: dq_all_validation_results\n",
        encoding="utf-8",
    )
    assert cli.data_quality_space(cli.load_settings_raw(settings)) == "fromRecords"


def test_records_stream_id_from_settings(cli: types.ModuleType, tmp_path: Path) -> None:
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "records:\n  space: inst_data_quality\n  stream_id: dq_all_validation_results\n",
        encoding="utf-8",
    )
    assert cli.records_stream_id(cli.load_settings_raw(settings)) == "dq_all_validation_results"


def test_ensure_records_stream_skips_when_exists(cli: types.ModuleType) -> None:
    class _Client:
        config = SimpleNamespace(project="demo")

        def get(self, url: str):
            assert url.endswith("/streams/dq_all_validation_results")
            return SimpleNamespace(status_code=200)

        def post(self, *_args, **_kwargs):
            raise AssertionError("must not create when stream exists")

    assert cli.ensure_records_stream(_Client(), "dq_all_validation_results") == "exists"


def test_ensure_records_stream_url_encodes_stream_id(cli: types.ModuleType) -> None:
    from cognite.client.exceptions import CogniteAPIError

    seen: dict[str, str] = {}

    class _Client:
        config = SimpleNamespace(project="demo")

        def get(self, url: str):
            seen["url"] = url
            raise CogniteAPIError("Stream not found", code=404)

        def post(self, *_args, **_kwargs):
            raise AssertionError("dry-run must not create")

    assert cli.ensure_records_stream(_Client(), "stream with/slash", dry_run=True) == "would_create"
    assert seen["url"] == "/api/v1/projects/demo/streams/stream%20with%2Fslash"


def test_ensure_records_stream_creates_when_missing(cli: types.ModuleType) -> None:
    from cognite.client.exceptions import CogniteAPIError

    posted: dict[str, object] = {}

    class _Client:
        config = SimpleNamespace(project="demo")

        def get(self, url: str):
            raise CogniteAPIError("Stream not found", code=404)

        def post(self, url: str, json: dict[str, object]):
            posted["url"] = url
            posted["json"] = json
            return SimpleNamespace(status_code=200)

    assert cli.ensure_records_stream(_Client(), "dq_all_validation_results") == "created"
    assert posted["url"] == "/api/v1/projects/demo/streams"
    assert posted["json"] == {
        "items": [
            {
                "externalId": "dq_all_validation_results",
                "settings": {"template": {"name": "BasicArchive"}},
            }
        ]
    }


def test_ensure_records_stream_dry_run_does_not_create(cli: types.ModuleType) -> None:
    from cognite.client.exceptions import CogniteAPIError

    class _Client:
        config = SimpleNamespace(project="demo")

        def get(self, _url: str):
            raise CogniteAPIError("Stream not found", code=404)

        def post(self, *_args, **_kwargs):
            raise AssertionError("must not create during dry-run")

    assert cli.ensure_records_stream(_Client(), "dq_all_validation_results", dry_run=True) == "would_create"


def test_ensure_records_stream_forbidden_raises_permission_error(cli: types.ModuleType) -> None:
    from cognite.client.exceptions import CogniteAPIError

    class _Client:
        config = SimpleNamespace(project="demo")

        def get(self, _url: str):
            raise CogniteAPIError("The user does not have the required Streams capabilities.", code=403)

    with pytest.raises(PermissionError, match="streamsAcl:READ"):
        cli.ensure_records_stream(_Client(), "dq_all_validation_results")


def test_collect_granted_actions_from_token_dump(cli: types.ModuleType) -> None:
    granted = cli.collect_granted_actions(
        [
            {
                "projectScope": {"projects": ["demo"]},
                "streamsAcl": {"actions": ["READ", "CREATE"], "scope": {"all": {}}},
            },
            {
                "projectScope": {"all": {}},
                "streamRecordsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}},
            },
            {
                "dataProductsAcl": {"actions": ["read"], "scope": {"all": {}}},
                "projectScope": {"projects": ["demo"]},
            },
        ]
    )
    assert granted["streamsAcl"] == {"READ", "CREATE"}
    assert granted["streamRecordsAcl"] == {"READ", "WRITE"}
    assert granted["dataProductsAcl"] == {"READ"}


def test_missing_capabilities_lists_absent_actions(cli: types.ModuleType) -> None:
    granted = {"streamsAcl": {"READ"}, "dataProductsAcl": {"READ", "CREATE", "UPDATE", "DELETE"}}
    missing = cli.missing_capabilities(granted, cli.INSTALL_CAPABILITIES)
    assert "streamsAcl:CREATE" in missing
    assert "streamsAcl:DELETE" in missing
    assert "ruleSetsAcl:READ" in missing
    assert "dataProductsAcl:READ" not in missing


def test_create_client_from_credentials_prefers_explicit_project_and_base_url(
    cli: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    class _FakeCredentials:
        def __init__(self, **kwargs: object) -> None:
            captured["credentials"] = kwargs

    class _FakeClientConfig:
        def __init__(self, **kwargs: object) -> None:
            captured["config"] = kwargs

    class _FakeCogniteClient:
        def __init__(self, config: object) -> None:
            self.config = config

    fake_cognite = types.ModuleType("cognite")
    fake_client = types.ModuleType("cognite.client")
    fake_client.ClientConfig = _FakeClientConfig
    fake_client.CogniteClient = _FakeCogniteClient
    fake_credentials = types.ModuleType("cognite.client.credentials")
    fake_credentials.OAuthClientCredentials = _FakeCredentials
    monkeypatch.setitem(sys.modules, "cognite", fake_cognite)
    monkeypatch.setitem(sys.modules, "cognite.client", fake_client)
    monkeypatch.setitem(sys.modules, "cognite.client.credentials", fake_credentials)

    monkeypatch.delenv("CDF_PROJECT", raising=False)
    monkeypatch.delenv("COGNITE_PROJECT", raising=False)
    monkeypatch.delenv("CDF_URL", raising=False)
    monkeypatch.delenv("COGNITE_BASE_URL", raising=False)
    monkeypatch.setenv("IDP_TENANT_ID", "tenant-id")
    monkeypatch.setenv("CDF_CLUSTER", "westeurope-1")

    cli.create_client_from_credentials(
        "runtime-id",
        "runtime-secret",
        project="from-toml",
        base_url="https://westeurope-1.cognitedata.com",
    )
    config = captured["config"]
    assert isinstance(config, dict)
    assert config["project"] == "from-toml"
    assert config["base_url"] == "https://westeurope-1.cognitedata.com"


def test_verify_dq_access_passes_install_client_config_to_runtime_client(
    cli: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    class _InstallClient:
        config = SimpleNamespace(project="from-toml", base_url="https://westeurope-1.cognitedata.com")

        def __init__(self) -> None:
            self.iam = SimpleNamespace(token=SimpleNamespace(inspect=self._inspect))

        def _inspect(self):
            return SimpleNamespace(
                capabilities=SimpleNamespace(
                    dump=lambda: [
                        {
                            "dataProductsAcl": {"actions": ["CREATE", "READ", "UPDATE", "DELETE"]},
                            "projectScope": {"all": {}},
                        },
                        {
                            "ruleSetsAcl": {"actions": ["CREATE", "READ", "UPDATE", "DELETE"]},
                            "projectScope": {"all": {}},
                        },
                        {
                            "streamsAcl": {"actions": ["READ", "CREATE", "DELETE"]},
                            "projectScope": {"all": {}},
                        },
                    ]
                )
            )

    class _RuntimeClient:
        def __init__(self) -> None:
            self.iam = SimpleNamespace(token=SimpleNamespace(inspect=self._inspect))

        def _inspect(self):
            return SimpleNamespace(
                capabilities=SimpleNamespace(
                    dump=lambda: [
                        {
                            "streamsAcl": {"actions": ["READ"]},
                            "projectScope": {"all": {}},
                        },
                        {
                            "streamRecordsAcl": {"actions": ["READ", "WRITE"]},
                            "projectScope": {"all": {}},
                        },
                    ]
                )
            )

    def _fake_create(
        client_id: str,
        client_secret: str,
        project: str | None = None,
        base_url: str | None = None,
    ) -> _RuntimeClient:
        captured["call"] = {
            "client_id": client_id,
            "client_secret": client_secret,
            "project": project,
            "base_url": base_url,
        }
        return _RuntimeClient()

    monkeypatch.setattr(cli, "create_client_from_credentials", _fake_create)

    report = cli.verify_dq_access(
        _InstallClient(),
        function_secrets={"client-id": "runtime-id", "client-secret": "runtime-secret"},
        deploy_client_id="deploy-id",
    )
    assert captured["call"] == {
        "client_id": "runtime-id",
        "client_secret": "runtime-secret",
        "project": "from-toml",
        "base_url": "https://westeurope-1.cognitedata.com",
    }
    assert report.runtime_checked is True


def test_verify_dq_access_reports_missing_for_install_and_runtime(cli: types.ModuleType) -> None:
    install_caps = {
        "dataProductsAcl": {"CREATE", "READ", "UPDATE", "DELETE"},
        "ruleSetsAcl": {"CREATE", "READ", "UPDATE", "DELETE"},
        "streamsAcl": {"READ"},
    }
    runtime_caps = {
        "streamsAcl": {"READ"},
        # missing streamRecordsAcl WRITE
    }

    class _InstallClient:
        config = SimpleNamespace(project="demo")

        def __init__(self) -> None:
            self.iam = SimpleNamespace(token=SimpleNamespace(inspect=self._inspect))

        def _inspect(self):
            return SimpleNamespace(
                capabilities=SimpleNamespace(
                    dump=lambda: [
                        {
                            "dataProductsAcl": {"actions": sorted(install_caps["dataProductsAcl"])},
                            "projectScope": {"all": {}},
                        },
                        {
                            "ruleSetsAcl": {"actions": sorted(install_caps["ruleSetsAcl"])},
                            "projectScope": {"all": {}},
                        },
                        {
                            "streamsAcl": {"actions": sorted(install_caps["streamsAcl"])},
                            "projectScope": {"all": {}},
                        },
                    ]
                )
            )

    class _RuntimeClient:
        config = SimpleNamespace(project="demo")

        def __init__(self) -> None:
            self.iam = SimpleNamespace(token=SimpleNamespace(inspect=self._inspect))

        def _inspect(self):
            return SimpleNamespace(
                capabilities=SimpleNamespace(
                    dump=lambda: [
                        {"streamsAcl": {"actions": sorted(runtime_caps["streamsAcl"])}, "projectScope": {"all": {}}},
                    ]
                )
            )

    report = cli.verify_dq_access(
        _InstallClient(),
        function_secrets={"client-id": "runtime-id", "client-secret": "runtime-secret"},
        runtime_client=_RuntimeClient(),
    )
    assert report.ok is False
    assert "streamsAcl:CREATE" in report.install_missing
    assert "streamRecordsAcl:WRITE" in report.runtime_missing
    assert report.runtime_checked is True
    assert report.same_identity is False


def test_verify_dq_access_same_identity_checks_both_requirement_sets(cli: types.ModuleType) -> None:
    caps = {
        "dataProductsAcl": {"CREATE", "READ", "UPDATE", "DELETE"},
        "ruleSetsAcl": {"CREATE", "READ", "UPDATE", "DELETE"},
        "streamsAcl": {"READ", "CREATE", "DELETE"},
        "streamRecordsAcl": {"READ", "WRITE"},
    }

    class _Client:
        config = SimpleNamespace(project="demo")

        def __init__(self) -> None:
            self.iam = SimpleNamespace(token=SimpleNamespace(inspect=self._inspect))

        def _inspect(self):
            return SimpleNamespace(
                capabilities=SimpleNamespace(
                    dump=lambda: [
                        {acl: {"actions": sorted(actions)}, "projectScope": {"all": {}}}
                        for acl, actions in caps.items()
                    ]
                )
            )

    report = cli.verify_dq_access(
        _Client(),
        function_secrets={"client-id": "same", "client-secret": "secret"},
        deploy_client_id="same",
    )
    assert report.ok is True
    assert report.same_identity is True
    assert report.runtime_checked is True


def test_load_project_env_uses_cdf_toml_root(
    cli: types.ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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


def test_load_project_env_honors_explicit_path(
    cli: types.ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / "custom.env"
    env_file.write_text("COGNITE_PROJECT=custom-project\n", encoding="utf-8")
    monkeypatch.delenv("COGNITE_PROJECT", raising=False)

    loaded = cli.load_project_env(env_file)
    assert loaded == env_file.resolve()
    assert os.environ["COGNITE_PROJECT"] == "custom-project"


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("KEY=value # comment", ("KEY", "value")),
        ('KEY="quoted value" # comment', ("KEY", "quoted value")),
        ("KEY='quoted value' # comment", ("KEY", "quoted value")),
        ("export KEY=value # comment", ("KEY", "value")),
        ('KEY="hash#inside"', ("KEY", "hash#inside")),
        ("# full line comment", None),
        ("NOEQUALS", None),
    ],
)
def test_parse_env_line_handles_inline_comments(
    cli: types.ModuleType, line: str, expected: tuple[str, str] | None
) -> None:
    assert cli._parse_env_line(line) == expected


def test_print_infrastructure_summary_formats_external_dataproducts(
    cli: types.ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
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
                        "workflow_external_id": "wf_all_validate_instances-enterprise-process-industry",
                        "uniqueness_workflow_external_id": (
                            "wf_all_validate_instances-enterprise-process-industry-uniqueness"
                        ),
                        "edge_existence_workflow_external_id": (
                            "wf_all_validate_instances-enterprise-process-industry-edge-existence"
                        ),
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
    assert "sync-cursor: wf_all_validate_instances-enterprise-process-industry" in output
    assert "dq-data-product-sync (deployed)" in output
    assert "dq-historic-queue-manager (deployed)" in output


def test_function_secrets_from_toml(cli: types.ModuleType, tmp_path: Path) -> None:
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


def test_bootstrap_resources_are_present_and_do_not_duplicate_record_views() -> None:
    bootstrap = _MODULE / "functions" / "fn_dataproduct_all_bootstrap_infrastructure"
    assert (bootstrap / "handler.py").is_file()
    assert (bootstrap / "models.py").is_file()
    assert (bootstrap / "logger.py").is_file()
    assert (bootstrap / "requirements.txt").is_file()
    assert (bootstrap / "config" / "settings.yaml").is_file()
    assert (bootstrap / "config" / "views").is_dir()
    assert (bootstrap / "config" / "timeseries_config").is_dir()
    assert {p.name for p in (bootstrap / "config" / "views").glob("*.yaml")} == {
        "asset.yaml",
        "equipment.yaml",
        "maintenance_order.yaml",
        "notification.yaml",
        "operation.yaml",
    }
    assert {p.name for p in (bootstrap / "config" / "timeseries_config").glob("*.yaml")} == {"timeseries_quality.yaml"}
    function_yaml = _MODULE / "functions" / "fn_dataproduct_all_bootstrap_infrastructure.Function.yaml"
    assert function_yaml.is_file()
    function_text = function_yaml.read_text(encoding="utf-8")
    assert "space: {{ dataQualitySpace }}" in function_text
    assert 'owner: "{{ functionOwner }}"' in function_text
    assert 'client-id: "{{ functionClientId }}"' in function_text
    assert 'client-secret: "{{ functionClientSecret }}"' in function_text
    defaults = _load_yaml(_MODULE / "default.config.yaml")
    assert defaults["functionOwner"] == "cdf_dq_runtime"
    space_dq = (_MODULE / "data_modeling" / "dataQuality.Space.yaml").read_text(encoding="utf-8")
    space_dp = (_MODULE / "data_modeling" / "data_product_schema.Space.yaml").read_text(encoding="utf-8")
    assert "name: Data quality runtime" in space_dq
    assert "name: Data quality data product schema" in space_dp
    schedule = _MODULE / "functions" / "fn_dataproduct_all_bootstrap_infrastructure.Schedule.yaml"
    assert schedule.is_file()
    schedule_text = schedule.read_text(encoding="utf-8")
    assert "functionExternalId: fn_dataproduct_all_bootstrap_infrastructure" in schedule_text
    assert 'cronExpression: "*/5 * * * *"' in schedule_text
    assert 'clientId: "{{ functionClientId }}"' in schedule_text
    assert 'clientSecret: "{{ functionClientSecret }}"' in schedule_text
    assert 'dataQualitySpace: "{{ dataQualitySpace }}"' in schedule_text
    assert 'recordsStreamId: "{{ recordsStreamId }}"' in schedule_text
    assert 'functionCodeDatasetExternalId: "{{ functionCodeDatasetExternalId }}"' in schedule_text
    assert 'dataProductExternalId: "{{ dataProductExternalId }}"' in schedule_text
    assert 'dataProductVersion: "{{ dataProductVersion }}"' in schedule_text
    assert 'schemaSpace: "{{ schemaSpace }}"' in schedule_text
    assert 'instanceSpace: "{{ instanceSpace }}"' in schedule_text
    assert 'ruleSetVersion: "{{ ruleSetVersion }}"' in schedule_text
    assert 'timeSeriesExternalIdPrefix: "{{ timeSeriesExternalIdPrefix }}"' in schedule_text
    assert not (_MODULE / "workflows" / "fn_dataproduct_all_bootstrap_infrastructure.Workflow.yaml").exists()
    assert not (_MODULE / "workflows" / "fn_dataproduct_all_bootstrap_infrastructure.WorkflowVersion.yaml").exists()
    assert not (_MODULE / "workflows" / "fn_dataproduct_all_bootstrap_infrastructure.WorkflowTrigger.yaml").exists()
    assert not (_MODULE / "data_modeling" / "views" / "DataQualityValidationRecord.View.yaml").exists()
    requirements = (bootstrap / "requirements.txt").read_text(encoding="utf-8")
    assert "cognite-data-quality==" in requirements
    assert "pydantic>=" in requirements

    settings = _load_yaml(bootstrap / "config" / "settings.yaml")
    assert settings["config_space"] == defaults["dataQualitySpace"]
    assert settings["records"]["space"] == defaults["dataQualitySpace"]
    assert settings["records"]["stream_id"] == defaults["recordsStreamId"]
    assert settings["function_code_dataset_external_id"] == defaults["functionCodeDatasetExternalId"]
    assert settings["external_dataproducts"] == [
        {"external_id": defaults["dataProductExternalId"], "version": defaults["dataProductVersion"]}
    ]
    handler_text = (bootstrap / "handler.py").read_text(encoding="utf-8")
    assert "_delete_bootstrap_schedule(client, logger)" in handler_text
    assert "_ensure_records_prerequisites" in handler_text
    assert "_write_settings_with_overrides" in handler_text
    assert "FunctionInput.model_validate" in handler_text
    assert "Leave the five-minute schedule in place" in handler_text


def _load_bootstrap_handler(
    monkeypatch: pytest.MonkeyPatch,
    deploy_impl: object,
    *,
    ensure_impl: object | None = None,
) -> types.ModuleType:
    fake_cdq = types.ModuleType("cognite_data_quality")
    fake_cdq.deploy_validation_infrastructure = deploy_impl
    monkeypatch.setitem(sys.modules, "cognite_data_quality", fake_cdq)
    name = "cdf_dq_runtime_bootstrap_handler"
    sys.modules.pop(name, None)
    handler_path = _MODULE / "functions" / "fn_dataproduct_all_bootstrap_infrastructure" / "handler.py"
    spec = importlib.util.spec_from_file_location(name, handler_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    # Package internals are exercised in dedicated tests; default stub keeps handle() unit tests offline.
    monkeypatch.setattr(
        module,
        "_ensure_records_prerequisites",
        ensure_impl if ensure_impl is not None else (lambda *_args, **_kwargs: None),
    )
    return module


def _bootstrap_client(
    *,
    schedules: list[SimpleNamespace] | None = None,
    delete: object | None = None,
    retrieve: object | None = None,
    credentials: object | None = None,
) -> SimpleNamespace:
    """Minimal CogniteClient stub for bootstrap handler schedule self-delete."""
    schedule_api = SimpleNamespace(
        list=lambda **_kwargs: list(schedules or []),
        delete=delete or (lambda *, id: None),
    )
    functions_api = SimpleNamespace(
        retrieve=retrieve or (lambda *, external_id: SimpleNamespace(id=42, external_id=external_id)),
        schedules=schedule_api,
    )
    if credentials is None:
        return SimpleNamespace(functions=functions_api)
    return SimpleNamespace(functions=functions_api, config=SimpleNamespace(credentials=credentials))


def test_function_input_rejects_invalid_log_level() -> None:
    from pydantic import ValidationError

    sys.path.insert(0, str(_MODULE / "functions" / "fn_dataproduct_all_bootstrap_infrastructure"))
    try:
        from models import FunctionInput
    finally:
        sys.path.pop(0)

    with pytest.raises(ValidationError):
        FunctionInput.model_validate({"logLevel": "VERBOSE"})


def test_bootstrap_handler_passes_materialized_views_and_timeseries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materialized: list[Path] = []

    def deploy(**kwargs: object) -> dict[str, object]:
        views = kwargs["views_dir"]
        ts = kwargs["timeseries_dir"]
        assert isinstance(views, Path) and views.is_dir()
        assert isinstance(ts, Path) and ts.is_dir()
        materialized.extend((views, ts))
        asset = yaml.safe_load((views / "asset.yaml").read_text(encoding="utf-8"))
        equipment = yaml.safe_load((views / "equipment.yaml").read_text(encoding="utf-8"))
        ts_cfg = yaml.safe_load((ts / "timeseries_quality.yaml").read_text(encoding="utf-8"))
        assert asset["view"]["space"] == "custom_cdm"
        assert asset["view"]["external_id"] == "CustomAsset"
        assert asset["instance_spaces"] == ["inst_custom"]
        assert equipment["uniqueness_cron"] == "0 */4 * * *"
        assert ts_cfg["filter"]["space"] == "inst_custom"
        assert ts_cfg["filter"]["external_id_prefix"] == "ts:"
        assert ts_cfg["shacl_rules"]["ruleset_references"][0]["version"] == "3.0.0"
        return {"status": "completed"}

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)
    client = _bootstrap_client()

    assert handler_module.handle(
        {
            "schemaSpace": "custom_cdm",
            "instanceSpace": "inst_custom",
            "viewAssetExternalId": "CustomAsset",
            "timeSeriesExternalIdPrefix": "ts:",
            "ruleSetVersion": "3.0.0",
        },
        client,
    ) == {"status": "completed"}
    assert len(materialized) == 2
    for path in materialized:
        assert not path.exists()


def test_bootstrap_handler_deletes_all_schedules_for_function_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    list_calls: list[dict[str, object]] = []
    deleted: list[int] = []

    def deploy(**_kwargs: object) -> dict[str, object]:
        return {"status": "completed"}

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)
    schedules = [
        SimpleNamespace(id=1, name="fn_dataproduct_all_bootstrap_infrastructure", function_id=42),
        SimpleNamespace(id=2, name=handler_module.BOOTSTRAP_SCHEDULE_NAME, function_id=42),
        SimpleNamespace(id=3, name="other-function-schedule", function_id=99),
    ]
    # Toolkit creates authenticated schedules with functionId (nonce/session). The SDK cannot
    # filter by function_id, so the handler lists all schedules and filters in Python.
    client = _bootstrap_client(
        schedules=schedules,
        delete=lambda *, id: deleted.append(id),
    )
    original_list = client.functions.schedules.list

    def tracking_list(**kwargs: object) -> list[SimpleNamespace]:
        list_calls.append(kwargs)
        return original_list(**kwargs)

    client.functions.schedules.list = tracking_list

    assert handler_module.handle({}, client) == {"status": "completed"}
    assert list_calls == [{"limit": None}]
    assert deleted == [1, 2]


def test_bootstrap_handler_logs_when_bootstrap_function_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def deploy(**_kwargs: object) -> dict[str, object]:
        return {"status": "completed"}

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)
    client = _bootstrap_client(retrieve=lambda *, external_id: None)

    assert handler_module.handle({}, client) == {"status": "completed"}
    assert f"Bootstrap function not found: {handler_module.BOOTSTRAP_FUNCTION_EXTERNAL_ID}" in capsys.readouterr().out


def test_bootstrap_handler_keeps_schedule_when_deployment_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    deleted: list[int] = []

    def deploy(**_kwargs: object) -> dict[str, object]:
        raise RuntimeError("temporary deployment failure")

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)
    client = _bootstrap_client(
        schedules=[SimpleNamespace(id=2, name=handler_module.BOOTSTRAP_SCHEDULE_NAME, function_id=42)],
        delete=lambda *, id: deleted.append(id),
    )

    with pytest.raises(RuntimeError, match="temporary deployment failure"):
        handler_module.handle({}, client)
    assert deleted == []


def test_bootstrap_handler_keeps_schedule_when_external_dataproduct_workflows_skipped(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Package soft-skips missing DP versions/views; bootstrap must fail so the schedule retries."""
    deleted: list[int] = []

    def deploy(**_kwargs: object) -> dict[str, object]:
        return {
            "external_dataproduct_workflows": [
                {
                    "dataProduct": "enterprise-process-industry",
                    "version": "1.1.10",
                    "status": "skipped",
                    "reason": "no_published_version",
                }
            ]
        }

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)
    client = _bootstrap_client(
        schedules=[SimpleNamespace(id=2, name=handler_module.BOOTSTRAP_SCHEDULE_NAME, function_id=42)],
        delete=lambda *, id: deleted.append(id),
    )

    with pytest.raises(RuntimeError, match=r"skipped.*no_published_version"):
        handler_module.handle({}, client)
    assert deleted == []
    assert "Data Quality Validation infrastructure bootstrap failed" in capsys.readouterr().out


def test_bootstrap_handler_deletes_schedule_when_external_dataproduct_workflows_deployed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deleted: list[int] = []

    def deploy(**_kwargs: object) -> dict[str, object]:
        return {
            "external_dataproduct_workflows": [
                {
                    "dataProduct": "enterprise-process-industry",
                    "version": "1.1.10",
                    "status": "deployed",
                    "views": 5,
                }
            ]
        }

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)
    client = _bootstrap_client(
        schedules=[SimpleNamespace(id=2, name=handler_module.BOOTSTRAP_SCHEDULE_NAME, function_id=42)],
        delete=lambda *, id: deleted.append(id),
    )

    result = handler_module.handle({}, client)
    assert result["status"] == "succeeded"
    assert deleted == [2]


def test_bootstrap_handler_logs_schedule_delete_failure_without_failing_deploy(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cognite.client.exceptions import CogniteAPIError

    def deploy(**_kwargs: object) -> dict[str, object]:
        return {"status": "completed"}

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)

    def delete_forbidden(*, id: int) -> None:
        raise CogniteAPIError(message="missing capability", code=403, x_request_id="req")

    client = _bootstrap_client(
        schedules=[SimpleNamespace(id=2, name=handler_module.BOOTSTRAP_SCHEDULE_NAME, function_id=42)],
        delete=delete_forbidden,
    )

    assert handler_module.handle({}, client) == {"status": "completed"}
    out = capsys.readouterr().out
    assert "Failed to delete bootstrap schedule id=2" in out
    assert "code=403" in out


def test_bootstrap_handler_applies_schedule_data_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_settings: dict[str, object] = {}
    bundled = _MODULE / "functions" / "fn_dataproduct_all_bootstrap_infrastructure" / "config" / "settings.yaml"

    def deploy(**kwargs: object) -> dict[str, object]:
        settings_path = kwargs["settings_path"]
        assert isinstance(settings_path, Path)
        assert settings_path != bundled
        captured_settings.update(yaml.safe_load(settings_path.read_text(encoding="utf-8")))
        return {"status": "completed"}

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)
    client = _bootstrap_client()

    result = handler_module.handle(
        {
            "dataQualitySpace": "customDq",
            "recordsStreamId": "custom_stream",
            "functionCodeDatasetExternalId": "custom_dataset",
            "dataProductExternalId": "custom-dp",
            "dataProductVersion": "9.9.9",
        },
        client,
    )
    assert result == {"status": "completed"}
    assert captured_settings["config_space"] == "customDq"
    assert captured_settings["records"] == {"space": "customDq", "stream_id": "custom_stream"}
    assert captured_settings["function_code_dataset_external_id"] == "custom_dataset"
    assert captured_settings["external_dataproducts"] == [{"external_id": "custom-dp", "version": "9.9.9"}]


def test_write_settings_overrides_preserve_other_external_dataproducts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        yaml.safe_dump(
            {
                "external_dataproducts": [
                    {"external_id": "dp-primary", "version": "1.0.0"},
                    {"external_id": "dp-secondary", "version": "2.0.0"},
                ]
            }
        ),
        encoding="utf-8",
    )
    handler_module = _load_bootstrap_handler(monkeypatch, lambda **_kwargs: {"status": "completed"})

    override_path = handler_module._write_settings_with_overrides(
        settings_path,
        {"dataProductExternalId": "dp-override", "dataProductVersion": "3.0.0"},
    )
    assert override_path is not None
    try:
        loaded = yaml.safe_load(override_path.read_text(encoding="utf-8"))
    finally:
        override_path.unlink(missing_ok=True)

    assert loaded["external_dataproducts"] == [
        {"external_id": "dp-primary", "version": "1.0.0"},
        {"external_id": "dp-secondary", "version": "2.0.0"},
        {"external_id": "dp-override", "version": "3.0.0"},
    ]


def test_write_settings_overrides_rename_when_single_product_id_mismatches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Schedule Toolkit vars rename the sole bundled placeholder product, not append."""
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        yaml.safe_dump(
            {
                "external_dataproducts": [
                    {"external_id": "existing-dp", "version": "1.0.0"},
                ]
            }
        ),
        encoding="utf-8",
    )
    handler_module = _load_bootstrap_handler(monkeypatch, lambda **_kwargs: {"status": "completed"})

    override_path = handler_module._write_settings_with_overrides(
        settings_path,
        {"dataProductExternalId": "new-dp", "dataProductVersion": "2.0.0"},
    )
    assert override_path is not None
    try:
        loaded = yaml.safe_load(override_path.read_text(encoding="utf-8"))
    finally:
        override_path.unlink(missing_ok=True)

    assert loaded["external_dataproducts"] == [
        {"external_id": "new-dp", "version": "2.0.0"},
    ]


def test_write_settings_overrides_version_only_raises_when_ambiguous(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Version-only overrides must fail when the target product is ambiguous."""
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        yaml.safe_dump(
            {
                "external_dataproducts": [
                    {"external_id": "dp-primary", "version": "1.0.0"},
                    {"external_id": "dp-secondary", "version": "2.0.0"},
                ]
            }
        ),
        encoding="utf-8",
    )
    handler_module = _load_bootstrap_handler(monkeypatch, lambda **_kwargs: {"status": "completed"})

    with pytest.raises(ValueError, match="Cannot apply dataProductVersion override"):
        handler_module._write_settings_with_overrides(
            settings_path,
            {"dataProductVersion": "3.0.0"},
        )


def test_write_settings_overrides_version_only_raises_when_no_products(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Version-only overrides must fail when no external data products exist to update."""
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        yaml.safe_dump({"external_dataproducts": []}),
        encoding="utf-8",
    )
    handler_module = _load_bootstrap_handler(monkeypatch, lambda **_kwargs: {"status": "completed"})

    with pytest.raises(ValueError, match="Cannot apply dataProductVersion override"):
        handler_module._write_settings_with_overrides(
            settings_path,
            {"dataProductVersion": "3.0.0"},
        )


def test_write_settings_overrides_replace_null_records_and_products(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """YAML null for records/external_dataproducts must not silently skip Schedule overrides."""
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        "records: null\nexternal_dataproducts: null\n",
        encoding="utf-8",
    )
    handler_module = _load_bootstrap_handler(monkeypatch, lambda **_kwargs: {"status": "completed"})

    override_path = handler_module._write_settings_with_overrides(
        settings_path,
        {
            "dataQualitySpace": "customDq",
            "recordsStreamId": "custom_stream",
            "dataProductExternalId": "custom-dp",
            "dataProductVersion": "9.9.9",
        },
    )
    assert override_path is not None
    try:
        loaded = yaml.safe_load(override_path.read_text(encoding="utf-8"))
    finally:
        override_path.unlink(missing_ok=True)

    assert loaded["config_space"] == "customDq"
    assert loaded["records"] == {"space": "customDq", "stream_id": "custom_stream"}
    assert loaded["external_dataproducts"] == [{"external_id": "custom-dp", "version": "9.9.9"}]


def test_function_input_settings_overrides_exclude_explicit_none() -> None:
    """Explicit null Schedule keys must not appear in settings.yaml merge overrides."""
    sys.path.insert(0, str(_MODULE / "functions" / "fn_dataproduct_all_bootstrap_infrastructure"))
    try:
        from models import FunctionInput
    finally:
        sys.path.pop(0)

    overrides = FunctionInput.model_validate(
        {
            "dataQualitySpace": None,
            "recordsStreamId": "keep_stream",
            "dataProductVersion": None,
        }
    ).settings_overrides()

    assert overrides == {"recordsStreamId": "keep_stream"}
    assert "dataQualitySpace" not in overrides
    assert "dataProductVersion" not in overrides


def test_write_settings_overrides_ignore_explicit_null_payload_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Null Schedule values must not overwrite existing settings.yaml entries."""
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        yaml.safe_dump(
            {
                "config_space": "existingDq",
                "records": {"space": "existingDq", "stream_id": "existing_stream"},
                "function_code_dataset_external_id": "existing_dataset",
                "external_dataproducts": [{"external_id": "existing-dp", "version": "1.0.0"}],
            }
        ),
        encoding="utf-8",
    )
    handler_module = _load_bootstrap_handler(monkeypatch, lambda **_kwargs: {"status": "completed"})

    override_path = handler_module._write_settings_with_overrides(
        settings_path,
        {
            "dataQualitySpace": None,
            "recordsStreamId": "new_stream",
            "functionCodeDatasetExternalId": None,
            "dataProductExternalId": None,
            "dataProductVersion": "2.0.0",
        },
    )
    assert override_path is not None
    try:
        loaded = yaml.safe_load(override_path.read_text(encoding="utf-8"))
    finally:
        override_path.unlink(missing_ok=True)

    assert loaded["config_space"] == "existingDq"
    assert loaded["records"] == {"space": "existingDq", "stream_id": "new_stream"}
    assert loaded["function_code_dataset_external_id"] == "existing_dataset"
    assert loaded["external_dataproducts"] == [{"external_id": "existing-dp", "version": "2.0.0"}]


def test_bootstrap_handler_injects_runtime_credentials_from_function_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    bundled = _MODULE / "functions" / "fn_dataproduct_all_bootstrap_infrastructure" / "config" / "settings.yaml"

    def deploy(**kwargs: object) -> dict[str, object]:
        settings_path = kwargs["settings_path"]
        assert isinstance(settings_path, Path)
        assert settings_path != bundled
        captured["settings"] = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
        captured["function_secrets"] = kwargs["function_secrets"]
        return {"status": "completed"}

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)
    client = _bootstrap_client()
    secrets = {"client-id": "runtime-id", "client-secret": "runtime-secret"}

    result = handler_module.handle({}, client, secrets=secrets)
    assert result == {"status": "completed"}
    assert captured["function_secrets"] == secrets
    settings = captured["settings"]
    assert isinstance(settings, dict)
    assert settings["runtime_credentials"] == {
        "functions": {"client_id": "runtime-id", "client_secret": "runtime-secret"},
        "workflows": {"client_id": "runtime-id", "client_secret": "runtime-secret"},
        "schedules": {"client_id": "runtime-id", "client_secret": "runtime-secret"},
    }


def test_bootstrap_handler_prefers_specialized_workflow_secret_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_settings: dict[str, object] = {}

    def deploy(**kwargs: object) -> dict[str, object]:
        settings_path = kwargs["settings_path"]
        assert isinstance(settings_path, Path)
        captured_settings.update(yaml.safe_load(settings_path.read_text(encoding="utf-8")))
        return {"status": "completed"}

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)
    client = _bootstrap_client()

    handler_module.handle(
        {},
        client,
        secrets={
            "client-id": "fn-id",
            "client-secret": "fn-secret",
            "workflow-client-id": "wf-id",
            "workflow-client-secret": "wf-secret",
            "schedule-client-id": "sched-id",
            "schedule-client-secret": "sched-secret",
        },
    )
    assert captured_settings["runtime_credentials"] == {
        "functions": {"client_id": "fn-id", "client_secret": "fn-secret"},
        "workflows": {"client_id": "wf-id", "client_secret": "wf-secret"},
        "schedules": {"client_id": "sched-id", "client_secret": "sched-secret"},
    }


def test_bootstrap_handler_overlays_client_credentials_for_workflow_triggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Package 0.5.x drops function_secrets for external DataProduct workflows.

    ``_build_trigger_credentials`` then only reads client_id/client_secret from
    ``client.config.credentials``. Schedule auth is token-only, so the handler must
    overlay Function secrets onto the client before deploy.
    """
    captured_client: dict[str, object] = {}

    def deploy(**kwargs: object) -> dict[str, object]:
        captured_client["client"] = kwargs["client"]
        return {"status": "completed"}

    handler_module = _load_bootstrap_handler(monkeypatch, deploy)
    token_creds = SimpleNamespace(authorization_header=lambda: ("Authorization", "Bearer tok"))
    client = _bootstrap_client(credentials=token_creds)

    handler_module.handle(
        {},
        client,
        secrets={"client-id": "runtime-id", "client-secret": "runtime-secret"},
    )
    deploy_client = captured_client["client"]
    assert deploy_client is client
    creds = client.config.credentials
    assert creds.client_id == "runtime-id"
    assert creds.client_secret == "runtime-secret"
    assert creds.authorization_header() == ("Authorization", "Bearer tok")


def test_bootstrap_handler_ensures_records_before_deploy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stream/views must be ensured before package deploy (package applies views too early)."""
    calls: list[str] = []

    def ensure(client: object, settings_path: Path, logger: object) -> None:
        calls.append(f"ensure:{settings_path.name}")

    def deploy(**_kwargs: object) -> dict[str, object]:
        calls.append("deploy")
        return {"status": "completed"}

    handler_module = _load_bootstrap_handler(monkeypatch, deploy, ensure_impl=ensure)
    assert handler_module.handle({}, _bootstrap_client()) == {"status": "completed"}
    assert calls == ["ensure:settings.yaml", "deploy"]


def test_bootstrap_handler_fails_when_records_prerequisites_fail(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deleted: list[int] = []

    def ensure(_client: object, _settings_path: Path, _logger: object) -> None:
        raise RuntimeError("Record views were not applied: streamId would change")

    def deploy(**_kwargs: object) -> dict[str, object]:
        raise AssertionError("deploy must not run after records prerequisite failure")

    handler_module = _load_bootstrap_handler(monkeypatch, deploy, ensure_impl=ensure)
    client = _bootstrap_client(
        schedules=[SimpleNamespace(id=2, name=handler_module.BOOTSTRAP_SCHEDULE_NAME, function_id=42)],
        delete=lambda *, id: deleted.append(id),
    )

    with pytest.raises(RuntimeError, match="Record views were not applied"):
        handler_module.handle({}, client)
    assert deleted == []
    assert "Data Quality Validation infrastructure bootstrap failed" in capsys.readouterr().out


def test_ensure_records_prerequisites_creates_stream_before_views(monkeypatch: pytest.MonkeyPatch) -> None:
    """Package soft-fails view apply; bootstrap must stream → containers → views and raise on error."""
    order: list[str] = []

    records = SimpleNamespace(
        stream_id="dq_all_validation_results",
        stream_type="BasicArchive",
        space="inst_data_quality",
        container="DataQualityValidationRecord",
        rule_engine_container="RuleEngineResult",
        rule_engine_stream_id=None,
    )
    fake_deploy = types.ModuleType("cognite_data_quality.deploy")
    fake_deploy.load_settings = lambda _path: SimpleNamespace(records=records)
    fake_records = types.ModuleType("cognite_data_quality._records")
    fake_records._ensure_records_stream = lambda *_a, **_k: order.append("stream")
    fake_records._ensure_records_container = lambda *_a, **_k: order.append("validation_container")
    fake_records._ensure_rule_engine_container = lambda *_a, **_k: order.append("rule_engine_container")
    fake_records.build_record_view_items = lambda **_k: [{"externalId": "DataQualityValidationRecord"}]
    fake_records._apply_record_views = lambda *_a, **_k: order.append("views")

    fake_cdq = types.ModuleType("cognite_data_quality")
    fake_cdq.deploy_validation_infrastructure = lambda **_k: {"status": "completed"}
    monkeypatch.setitem(sys.modules, "cognite_data_quality", fake_cdq)
    monkeypatch.setitem(sys.modules, "cognite_data_quality.deploy", fake_deploy)
    monkeypatch.setitem(sys.modules, "cognite_data_quality._records", fake_records)

    name = "cdf_dq_runtime_bootstrap_handler_records"
    sys.modules.pop(name, None)
    handler_path = _MODULE / "functions" / "fn_dataproduct_all_bootstrap_infrastructure" / "handler.py"
    spec = importlib.util.spec_from_file_location(name, handler_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)

    logger = module.CogniteFunctionLogger("INFO")
    module._ensure_records_prerequisites(SimpleNamespace(), Path("settings.yaml"), logger)
    assert order == ["stream", "validation_container", "rule_engine_container", "views"]


def test_ensure_records_prerequisites_raises_when_views_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    records = SimpleNamespace(
        stream_id="dq_all_validation_results",
        stream_type="BasicArchive",
        space="inst_data_quality",
        container="DataQualityValidationRecord",
        rule_engine_container="RuleEngineResult",
        rule_engine_stream_id=None,
    )
    fake_deploy = types.ModuleType("cognite_data_quality.deploy")
    fake_deploy.load_settings = lambda _path: SimpleNamespace(records=records)
    fake_records = types.ModuleType("cognite_data_quality._records")
    fake_records._ensure_records_stream = lambda *_a, **_k: None
    fake_records._ensure_records_container = lambda *_a, **_k: None
    fake_records._ensure_rule_engine_container = lambda *_a, **_k: None
    fake_records.build_record_view_items = lambda **_k: [{"externalId": "DataQualityValidationRecord"}]

    def apply_fail(_client: object, _items: list[dict[str, object]]) -> None:
        raise RuntimeError(
            "Cannot update view 'inst_data_quality:DataQualityValidationRecord/v1', "
            "streamId would change from 'cdf_all_validation_results' to 'dq_all_validation_results'"
        )

    fake_records._apply_record_views = apply_fail
    fake_cdq = types.ModuleType("cognite_data_quality")
    fake_cdq.deploy_validation_infrastructure = lambda **_k: {"status": "completed"}
    monkeypatch.setitem(sys.modules, "cognite_data_quality", fake_cdq)
    monkeypatch.setitem(sys.modules, "cognite_data_quality.deploy", fake_deploy)
    monkeypatch.setitem(sys.modules, "cognite_data_quality._records", fake_records)

    name = "cdf_dq_runtime_bootstrap_handler_records_fail"
    sys.modules.pop(name, None)
    handler_path = _MODULE / "functions" / "fn_dataproduct_all_bootstrap_infrastructure" / "handler.py"
    spec = importlib.util.spec_from_file_location(name, handler_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)

    logger = module.CogniteFunctionLogger("INFO")
    with pytest.raises(RuntimeError, match="Record views were not applied"):
        module._ensure_records_prerequisites(SimpleNamespace(), Path("settings.yaml"), logger)
