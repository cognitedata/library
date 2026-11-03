"""Tests for reading the Operations Summarizer config node."""

from types import SimpleNamespace

import pytest

from app_config import AppConfigError, load_app_config
from config import DATA_TYPE_CONFIGS, get_config, runtime


def test_load_lists_models_and_reads_the_node():
    client = FakeClient(default_node())
    loaded = load_app_config(client)
    assert client.list_kwargs["space"] == "dm_sol_ops_summarizer_app"
    assert client.retrieve_nodes.space == "inst_ops_summarizer_app_config"
    assert client.retrieve_nodes.external_id == "ops_summarizer_config"
    assert client.list_kwargs["all_versions"] is False
    assert loaded["spaces"]["summaries"] == "ops_summaries"


def test_load_raises_when_the_model_is_missing():
    client = FakeClient(default_node(), models=[])
    with pytest.raises(AppConfigError, match="OpsSummarizerConfig_SOL"):
        load_app_config(client)


def test_load_raises_when_the_node_is_missing():
    client = FakeClient(default_node(), nodes=[])
    with pytest.raises(AppConfigError, match="ops_summarizer_config"):
        load_app_config(client)


def test_load_raises_on_http_error():
    client = FakeClient(default_node(), error="list")
    with pytest.raises(AppConfigError, match="status 403"):
        load_app_config(client)


def test_load_raises_on_unsupported_schema_version():
    node = default_node()
    node["schemaVersion"] = 2
    with pytest.raises(AppConfigError, match="schemaVersion 2"):
        load_app_config(FakeClient(node))


def test_get_config_uses_node_defaults():
    cfg = get_config({}, FakeClient(default_node()))
    assert cfg["summary_space"] == "ops_summaries"
    assert cfg["chunk_size"] == 50
    assert cfg["max_instances_per_datatype"] == 15_000
    assert cfg["helper_agent_name"] == "Operation Summarizer Agent"
    assert runtime.instance_query_limit == 1000
    assert DATA_TYPE_CONFIGS["assets"].view_id.version == "v1"
    assert DATA_TYPE_CONFIGS["assets"].table_columns[0].header == "id"
    assert DATA_TYPE_CONFIGS["files"].table_columns[2].formatter(["A", "B"]) == "A, B"
    assert DATA_TYPE_CONFIGS["work_orders"].is_special_case is True


def test_engineer_persona_summarizes_work_orders_only():
    cfg = get_config({"persona_key": "engineer"}, FakeClient(default_node()))
    assert cfg["summary_contents"]["work_orders"] is True
    assert cfg["summary_contents"]["assets"] is False
    assert cfg["summary_contents"]["files"] is False


def test_missing_prompts_names_the_property():
    node = default_node()
    del node["prompts"]
    with pytest.raises(AppConfigError, match="prompts"):
        get_config({}, FakeClient(node))


def test_prompts_come_from_the_node():
    node = default_node()
    node["prompts"]["dataTypes"]["assets"] = "from-node"
    cfg = get_config({}, FakeClient(node))
    assert cfg["prompt_templates"]["assets"] == "from-node"
    assert cfg["agent_instructions"] == "agent"
    assert runtime.summary_period_days == 7
    assert "PROMPT_HEADER" not in open(__file__.replace("test_app_config.py", "config.py"), encoding="utf-8").read()


def test_agent_yaml_matches_node_instructions():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    node = (root / "data_modeling/ops_summarizer_config.Node.yaml").read_text(encoding="utf-8")
    agent = (root / "agents/operation_summarizer.Agent.yaml").read_text(encoding="utf-8")
    assert "agentInstructions:" in node
    assert "You are an Operations Summarizer AI agent" in node
    assert "You are an Operations Summarizer AI agent" in agent


def test_unknown_persona_key_fails():
    with pytest.raises(AppConfigError, match="unknown persona_key"):
        get_config({"persona_key": "operator"}, FakeClient(default_node()))


def test_get_config_call_data_overrides_the_node():
    node = default_node()
    node["spaces"]["summaries"] = "from-node"
    node["limits"]["chunkSize"] = 7
    cfg = get_config(
        {"summary_space": "from-call", "chunk_size": 3},
        FakeClient(node),
    )
    assert cfg["summary_space"] == "from-call"
    assert cfg["chunk_size"] == 3
    assert runtime.summary_space == "from-call"
    assert runtime.chunk_size == 3


class FakeClient:
    def __init__(self, properties, models=None, nodes=None, error=None):
        self.properties = properties
        self._models = models
        self._nodes = nodes
        self._error = error
        self.list_kwargs = None
        self.retrieve_nodes = None
        self.data_modeling = self
        self.data_models = self
        self.instances = self

    def list(self, **kwargs):
        self.list_kwargs = kwargs
        if self._error == "list":
            raise _http_error(403)
        if self._models is not None:
            return self._models
        view = SimpleNamespace(external_id="OpsSummarizerConfig", version="v1")
        return [SimpleNamespace(external_id="OpsSummarizerConfig_SOL", views=[view])]

    def retrieve(self, nodes=None, **_kwargs):
        self.retrieve_nodes = nodes
        if self._nodes is not None:
            return SimpleNamespace(nodes=self._nodes)
        key = _ViewKey("v1")
        return SimpleNamespace(nodes=[SimpleNamespace(properties={key: self.properties})])


class _ViewKey:
    def __init__(self, version: str) -> None:
        self.external_id = "OpsSummarizerConfig"
        self.version = version

    def __hash__(self) -> int:
        return hash((self.external_id, self.version))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _ViewKey) and other.version == self.version


def _http_error(status: int) -> RuntimeError:
    error = RuntimeError("denied")
    error.code = status
    return error


def _view(external_id: str) -> dict:
    return {"space": "cdf_cdm", "externalId": external_id, "version": "v1"}


def _type(display: str, view: str, columns: list, special: bool = False) -> dict:
    item = {
        "displayName": display,
        "view": view,
        "enabledByDefault": True,
        "isSpecialCase": special,
        "tableColumns": columns,
    }
    if special:
        item["subcategories"] = [
            "completed_work_orders",
            "ongoing_work_orders",
            "scheduled_work_orders",
        ]
    return item


def default_node() -> dict:
    return {
        "schemaVersion": 1,
        "companyName": "",
        "spaces": {
            "summaries": "ops_summaries",
            "dataInstances": "inst_data",
        },
        "externalIdPrefixes": {
            "operationSummary": "ops_summary_",
            "testReport": "test_report_",
            "podcast": "ops_podcast_",
        },
        "agent": {
            "name": "Operation Summarizer Agent",
            "externalIdSuffix": "_operation_summarizer_helper_agent",
            "defaultLanguageModel": "cognite/balanced",
            "maxConcurrentTasks": 3,
        },
        "dataModel": {
            "views": {
                "summaryOutputFile": _view("CogniteFile"),
                "file": _view("CogniteFile"),
                "asset": _view("CogniteAsset"),
                "timeSeries": _view("CogniteTimeSeries"),
                "activity": _view("CogniteActivity"),
                "equipment": _view("CogniteEquipment"),
            }
        },
        "limits": {
            "chunkSize": 50,
            "instanceQueryLimit": 1000,
            "maxInstancesPerDatatype": 15_000,
            "summaryListPageSize": 15,
            "operationSummaryPickerMax": 500,
            "defaultPageSize": 5,
            "retry": {"baseMs": 1000, "maxDelayMs": 30_000},
            "concurrency": {"default": 8, "delete": 2, "agent": 3},
        },
        "prompts": {
            "header": "header",
            "holistic": "holistic",
            "agentInstructions": "agent",
            "dataTypes": {
                "assets": "assets prompt",
                "files": "files prompt",
                "work_orders": "work orders prompt",
                "time_series": "time series prompt",
                "equipment": "equipment prompt",
            },
        },
        "defaults": {
            "timezoneOffsetHours": -7,
            "summaryPeriodDays": 7,
            "personaPresets": {"engineer": ["work_orders"]},
        },
        "dataTypes": {
            "assets": _type(
                "Assets",
                "asset",
                [
                    {"header": "id", "property": "externalId"},
                    {"header": "name", "property": "name"},
                    {"header": "description", "property": "description"},
                    {"header": "parent", "property": "parent", "default": "None"},
                ],
            ),
            "files": _type(
                "Files",
                "file",
                [
                    {"header": "name", "property": "name"},
                    {"header": "description", "property": "description"},
                    {
                        "header": "asset",
                        "property": "assets",
                        "formatter": "assetsList",
                        "default": "None",
                    },
                ],
            ),
            "work_orders": _type(
                "Work Orders",
                "activity",
                [
                    {"header": "id", "property": "externalId"},
                    {"header": "name", "property": "name"},
                ],
                special=True,
            ),
            "time_series": _type(
                "Time Series",
                "timeSeries",
                [{"header": "name", "property": "name"}],
            ),
            "equipment": _type(
                "Equipment",
                "equipment",
                [{"header": "id", "property": "externalId"}],
            ),
        },
    }
