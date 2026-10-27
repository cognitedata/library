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


def _load_pipeline(cli_module: types.ModuleType) -> types.ModuleType:
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
    assert settings["config_space"] == settings["records"]["space"] == "dataQuality"

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
    assert "http://purl.org/cognite/inst_acme/pi:160554" in ts_rules


def test_load_settings_raw_returns_typed_runtime_settings(cli: types.ModuleType, tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        "\n".join(
            [
                "config_space: dataQuality",
                "config_source: dataproduct",
                "data_product_sync_cron: '15 * * * *'",
                "records:",
                "  space: dataQuality",
                "  stream_id: test_stream_1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    settings = cli.load_settings_raw(settings_path)
    assert isinstance(settings, cli.RuntimeSettings)
    assert settings.config_space == "dataQuality"
    assert settings.config_source == "dataproduct"
    assert settings.data_product_sync_cron == "15 * * * *"
    assert settings.records is not None
    assert settings.records.space == "dataQuality"
    assert settings.records.stream_id == "test_stream_1"
    assert settings.effective_config_space == "dataQuality"


def test_data_quality_space_from_settings(cli: types.ModuleType, tmp_path: Path) -> None:
    settings = tmp_path / "settings.yaml"
    settings.write_text("config_space: dataQuality\n", encoding="utf-8")
    assert cli.data_quality_space(cli.load_settings_raw(settings)) == "dataQuality"


def test_data_quality_space_falls_back_to_records_space(cli: types.ModuleType, tmp_path: Path) -> None:
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "records:\n  space: fromRecords\n  stream_id: test_stream_1\n",
        encoding="utf-8",
    )
    assert cli.data_quality_space(cli.load_settings_raw(settings)) == "fromRecords"


def test_records_stream_id_from_settings(cli: types.ModuleType, tmp_path: Path) -> None:
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "records:\n  space: dataQuality\n  stream_id: test_stream_1\n",
        encoding="utf-8",
    )
    assert cli.records_stream_id(cli.load_settings_raw(settings)) == "test_stream_1"


def test_ensure_records_stream_skips_when_exists(cli: types.ModuleType) -> None:
    class _Client:
        config = SimpleNamespace(project="demo")

        def get(self, url: str):
            assert url.endswith("/streams/test_stream_1")
            return SimpleNamespace(status_code=200)

        def post(self, *_args, **_kwargs):
            raise AssertionError("must not create when stream exists")

    assert cli.ensure_records_stream(_Client(), "test_stream_1") == "exists"


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

    assert cli.ensure_records_stream(_Client(), "test_stream_1") == "created"
    assert posted["url"] == "/api/v1/projects/demo/streams"
    assert posted["json"] == {
        "items": [
            {
                "externalId": "test_stream_1",
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

    assert cli.ensure_records_stream(_Client(), "test_stream_1", dry_run=True) == "would_create"


def test_ensure_records_stream_forbidden_raises_permission_error(cli: types.ModuleType) -> None:
    from cognite.client.exceptions import CogniteAPIError

    class _Client:
        config = SimpleNamespace(project="demo")

        def get(self, _url: str):
            raise CogniteAPIError("The user does not have the required Streams capabilities.", code=403)

    with pytest.raises(PermissionError, match="streamsAcl:READ"):
        cli.ensure_records_stream(_Client(), "test_stream_1")


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
    bootstrap = _MODULE / "functions" / "dq_infrastructure_bootstrap"
    assert (bootstrap / "handler.py").is_file()
    assert (bootstrap / "requirements.txt").is_file()
    assert (bootstrap / "config" / "settings.yaml").is_file()
    assert (_MODULE / "functions" / "dq_infrastructure_bootstrap.Function.yaml").is_file()
    assert (_MODULE / "workflows" / "dq_infrastructure_bootstrap.Workflow.yaml").is_file()
    assert (_MODULE / "workflows" / "dq_infrastructure_bootstrap.WorkflowVersion.yaml").is_file()
    assert (_MODULE / "workflows" / "dq_infrastructure_bootstrap.WorkflowTrigger.yaml").is_file()
    assert not (_MODULE / "data_modeling" / "views" / "DataQualityValidationRecord.View.yaml").exists()
    assert "cognite-data-quality==" in (bootstrap / "requirements.txt").read_text(encoding="utf-8")


def _install_fake_data_quality(monkeypatch: pytest.MonkeyPatch, *, pipeline_impl: object, settings: object) -> None:
    fake_cdq = types.ModuleType("cognite_data_quality")
    fake_cdq.deploy_validation_pipeline = pipeline_impl
    fake_deploy = types.ModuleType("cognite_data_quality.deploy")
    fake_deploy.load_settings = lambda _path: settings
    monkeypatch.setitem(sys.modules, "cognite_data_quality", fake_cdq)
    monkeypatch.setitem(sys.modules, "cognite_data_quality.deploy", fake_deploy)


def test_deploy_pipeline_dry_run_does_not_call_pipeline(
    cli: types.ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline = _load_pipeline(cli)
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text("config_space: dataQuality\n", encoding="utf-8")

    def fail_if_called(*_args: object, **_kwargs: object) -> None:
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


def test_deploy_pipeline_live_calls_package(
    cli: types.ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = _load_pipeline(cli)
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text("config_space: dataQuality\n", encoding="utf-8")
    captured: dict[str, object] = {}

    def fake_pipeline(*_args: object, **kwargs: object) -> dict[str, object]:
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
    assert isinstance(captured["settings_path"], str)
    assert "dry_run" not in captured


def test_deploy_pipeline_requires_data_product_external_id(
    cli: types.ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline = _load_pipeline(cli)
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text("config_space: dataQuality\n", encoding="utf-8")

    def fail_if_called(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("deploy_validation_pipeline must not run without data product")

    _install_fake_data_quality(
        monkeypatch,
        pipeline_impl=fail_if_called,
        settings=SimpleNamespace(external_dataproducts=[], effective_config_space="dataQuality"),
    )
    monkeypatch.setattr(
        pipeline,
        "resolve_cognite_client",
        lambda _path: (_ for _ in ()).throw(AssertionError("client must not be created")),
    )

    assert pipeline.main(["--settings-path", str(settings_path)]) == 1
    err = capsys.readouterr().err
    assert "data_product_external_id is missing" in err
    assert "--data-product-external-id" in err
