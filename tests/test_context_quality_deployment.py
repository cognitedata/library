"""The production deployment of the Context Quality backend: workflow, trigger, groups and variables.

These read the module's Toolkit YAML the way a build does (substituting {{ variables }}) and check that the pieces agree
with each other and with the function: the workflow gives the function every setting it requires, runs nothing else,
the groups grant what the function needs and no more, and nothing production-side names a demo module or the legacy
deployment.
"""

import json
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.context_quality_module import MODULE, VARIABLE, read_variables, render

REPO_ROOT = Path(__file__).resolve().parents[1]
FUNCTION_DIR = MODULE / "functions" / "fn_context_quality_metrics"
sys.path.insert(0, str(FUNCTION_DIR))

from cq.config import DEFAULT_CONFIG, build_config, require_resources

DEFAULTS = read_variables(MODULE / "default.config.yaml")
LEGACY_RESOURCES = {
    "metricsSchemaSpace": "sp_cq_demo_metrics",
    "metricsInstanceSpace": "sp_cq_demo_metrics_data",
    "metricsStreamExternalId": "cq_demo_metrics",
    "metricsStreamTemplate": "ImmutableTestStream",
}
WORKFLOWS = MODULE / "workflows"
AUTH = MODULE / "auth"
PRODUCTION_FILES = [
    path for path in MODULE.rglob("*") if path.is_file() and "fn_context_quality_metrics" not in path.parts
]
SEED_WORDS = ("seed_datapoints", "seed_history", "context_quality_v2_demo", "cq_demo", "fn_cq_service", "rawDatabase")


def rendered(name: str, folder: Path = WORKFLOWS, variables: dict[str, str] | None = None) -> dict[str, Any]:
    return render(folder / name, variables or DEFAULTS)


def workflow_version(variables: dict[str, str] | None = None) -> dict[str, Any]:
    return rendered("context_quality.WorkflowVersion.yaml", variables=variables)


def tasks_by_id(variables: dict[str, str] | None = None) -> dict[str, dict[str, Any]]:
    return {task["externalId"]: task for task in workflow_version(variables)["workflowDefinition"]["tasks"]}


def function_task(variables: dict[str, str] | None = None) -> dict[str, Any]:
    """The plan task: it carries the configuration every shard receives."""
    return tasks_by_id(variables)["plan"]


def capabilities(group: str, variables: dict[str, str] | None = None) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for capability in rendered(group, AUTH, variables)["capabilities"]:
        ((name, body),) = capability.items()
        out.setdefault(name, []).append(body)
    return out


# ---------------------------------------------------------------- the workflow


def test_the_workflow_has_the_production_id_and_runs_one_execution_at_a_time() -> None:
    workflow = rendered("context_quality.Workflow.yaml")

    assert workflow["externalId"] == "wf_context_quality_metrics"
    assert workflow["maxConcurrentExecutions"] == 1
    assert workflow["dataSetExternalId"] == DEFAULTS["metricsDatasetExternalId"]


def test_the_workflow_version_belongs_to_the_workflow() -> None:
    version = workflow_version()

    assert version["workflowExternalId"] == rendered("context_quality.Workflow.yaml")["externalId"]
    assert version["version"] == DEFAULTS["metricsWorkflowVersion"]


def test_the_workflow_plans_runs_the_shards_in_parallel_and_finalizes_calling_only_the_production_function() -> None:
    tasks = tasks_by_id()

    assert list(tasks) == ["plan", "shards", "finalize", "export"]
    for name, action in (("plan", "plan"), ("finalize", "finalize"), ("export", "export")):
        assert tasks[name]["type"] == "function"
        assert tasks[name]["parameters"]["function"]["externalId"] == "fn_context_quality_metrics"
        assert tasks[name]["parameters"]["function"]["data"]["action"] == action
    # the dynamic task runs what the plan returned, and the shards call this same function
    assert tasks["shards"]["type"] == "dynamic"
    assert tasks["shards"]["parameters"]["dynamic"]["tasks"] == "${plan.output.response.tasks}"
    assert tasks["plan"]["parameters"]["function"]["data"]["functionExternalId"] == "fn_context_quality_metrics"
    assert [dep["externalId"] for dep in tasks["shards"]["dependsOn"]] == ["plan"]
    assert [dep["externalId"] for dep in tasks["finalize"]["dependsOn"]] == ["shards"]
    assert [dep["externalId"] for dep in tasks["export"]["dependsOn"]] == ["finalize"]
    assert "dependsOn" not in tasks["plan"]


def test_finalize_is_given_the_run_the_plan_started_and_the_resources_it_writes_to() -> None:
    data = tasks_by_id()["finalize"]["parameters"]["function"]["data"]

    assert data["runId"] == "${plan.output.response.runId}"
    assert data["runTimestamp"] == "${plan.output.response.runTimestamp}"
    assert data["shardCount"] == "${plan.output.response.shardCount}"
    assert data["config"] == {
        "stream": DEFAULTS["metricsStreamExternalId"],
        "schemaSpace": DEFAULTS["metricsSchemaSpace"],
        "instanceSpace": DEFAULTS["metricsInstanceSpace"],
    }
    require_resources(build_config(data["config"]), writes=True)


def test_a_failed_step_fails_the_execution_so_the_run_is_never_completed() -> None:
    for name in ("plan", "shards", "finalize"):
        assert tasks_by_id()[name]["onFailure"] == "abortWorkflow"
    # the workbook is a side product of a run that is already complete: its failure leaves the run as it is
    assert tasks_by_id()["export"]["onFailure"] == "skipTask"
    assert tasks_by_id()["shards"]["dependsOn"] and tasks_by_id()["finalize"]["dependsOn"]


def test_the_workflow_stays_within_the_limits_of_a_workflow_execution() -> None:
    """200 tasks per execution, generated ones included: the plan, the dynamic task and finalize are three of them."""
    assert DEFAULT_CONFIG["maxShards"] + len(tasks_by_id()) <= 200


def test_the_workflow_runs_no_seed_demo_or_transformation_task() -> None:
    text = (WORKFLOWS / "context_quality.WorkflowVersion.yaml").read_text()

    assert not [word for word in (*SEED_WORDS, "transformation") if word in text]
    assert {task["type"] for task in workflow_version()["workflowDefinition"]["tasks"]} == {"function", "dynamic"}


def test_the_workflow_descriptions_are_short_enough_for_the_api() -> None:
    """The workflows API rejected a 211-character workflowDefinition description at deploy time (documented limit: 500)."""
    definition = workflow_version()["workflowDefinition"]

    assert len(definition["description"]) <= 100
    assert all(len(task.get("description", "")) <= 100 for task in definition["tasks"])


def test_the_function_steps_stay_within_the_function_runtime_limit() -> None:
    for name in ("plan", "finalize"):
        assert tasks_by_id()[name]["timeout"] <= 600
    assert "dry_run" not in function_task()["parameters"]["function"]["data"]  # a run is real


def test_the_task_gives_the_function_the_resources_it_requires() -> None:
    config = function_task()["parameters"]["function"]["data"]["config"]

    assert config["stream"] == DEFAULTS["metricsStreamExternalId"]
    assert config["schemaSpace"] == DEFAULTS["metricsSchemaSpace"]
    assert config["instanceSpace"] == DEFAULTS["metricsInstanceSpace"]
    require_resources(build_config(config), writes=True)


def test_the_resources_follow_the_variables_and_legacy_names_are_only_ever_supplied() -> None:
    config = function_task({**DEFAULTS, **LEGACY_RESOURCES})["parameters"]["function"]["data"]["config"]

    assert (config["stream"], config["schemaSpace"], config["instanceSpace"]) == (
        "cq_demo_metrics",
        "sp_cq_demo_metrics",
        "sp_cq_demo_metrics_data",
    )
    assert "cq_demo" not in json.dumps(function_task())  # the defaults name nothing legacy


def test_the_task_config_passes_only_settings_the_function_knows() -> None:
    config = function_task()["parameters"]["function"]["data"]["config"]

    assert set(config) <= set(DEFAULT_CONFIG)
    assert not {"rawDatabase", "rawDatapointsTable"} & set(config)


def test_the_task_config_is_empty_by_default_so_no_demo_rule_is_passed() -> None:
    config = function_task()["parameters"]["function"]["data"]["config"]
    rules = {
        key: config[key]
        for key in (
            "facilityNames",
            "tierByTypeCode",
            "defaultOwners",
            "ownerPrefixRules",
            "divisionTagPrefix",
            "retiredTag",
        )
    }

    assert all(not value for value in rules.values())
    assert all(not value for value in config["exclusions"].values())
    assert set(config["exclusions"]) == {
        "syntheticTsPrefixes",
        "locationCentricFileCategories",
        "excludedFileExtensions",
        "decommissionedTags",
    }


def test_the_task_config_is_accepted_by_the_function_as_it_is() -> None:
    config = function_task()["parameters"]["function"]["data"]["config"]

    cfg = build_config(config)

    assert cfg["tierByTypeCode"] == {}
    assert cfg["freshnessDays"] == DEFAULT_CONFIG["freshnessDays"]
    assert cfg["maxDepth"] == DEFAULT_CONFIG["maxDepth"]


def test_a_project_can_supply_its_rules_as_json_variables() -> None:
    variables = {
        **DEFAULTS,
        "metricsTierByTypeCode": '{"pump": 1, "valve": 2}',
        "metricsDivisionTagPrefix": "team-",
        "metricsOwnerPrefixRules": '[{"prefix": "PI-", "ownerId": "o"}]',
        "metricsExclusions": '{"decommissionedTags": ["gone"]}',
    }

    cfg = build_config(function_task(variables)["parameters"]["function"]["data"]["config"])

    assert cfg["tierByTypeCode"] == {"pump": 1, "valve": 2}
    assert cfg["divisionTagPrefix"] == "team-"
    assert cfg["ownerPrefixRules"] == [{"prefix": "PI-", "ownerId": "o"}]
    assert cfg["exclusions"]["decommissionedTags"] == ["gone"]


def test_an_invalid_tier_in_the_variables_is_rejected_by_the_function_before_it_runs() -> None:
    variables = {**DEFAULTS, "metricsTierByTypeCode": '{"pump": "1"}'}

    with pytest.raises(ValueError, match="tierByTypeCode"):
        build_config(function_task(variables)["parameters"]["function"]["data"]["config"])


# ---------------------------------------------------------------- the trigger


def test_the_trigger_runs_the_production_workflow_on_the_schedule_variable() -> None:
    trigger = rendered("context_quality.WorkflowTrigger.yaml")

    assert trigger["externalId"] == "wf_context_quality_metrics_schedule"
    assert trigger["workflowExternalId"] == "wf_context_quality_metrics"
    assert trigger["workflowVersion"] == DEFAULTS["metricsWorkflowVersion"]
    assert trigger["triggerRule"] == {
        "triggerType": "schedule",
        "cronExpression": DEFAULTS["metricsWorkflowSchedule"],
        "timezone": DEFAULTS["metricsWorkflowScheduleTimezone"],
    }


def test_the_trigger_credentials_and_group_sources_come_from_the_environment_and_are_never_literal() -> None:
    trigger = rendered("context_quality.WorkflowTrigger.yaml")

    assert trigger["authentication"] == {
        "clientId": "${CQ_FUNCTION_CLIENT_ID}",
        "clientSecret": "${CQ_FUNCTION_CLIENT_SECRET}",
    }
    for key in ("metricsFunctionGroupSourceId", "metricsDashboardGroupSourceId"):
        assert DEFAULTS[key].startswith("${") and DEFAULTS[key].endswith("}")


def test_no_secret_or_identity_value_is_written_in_the_module() -> None:
    for path in PRODUCTION_FILES:
        if path.suffix not in {".yaml", ".md", ".toml"}:
            continue
        text = path.read_text()
        for line in text.splitlines():
            if "clientSecret" in line or "sourceId" in line:
                assert "{{" in line or "`" in line or line.lstrip().startswith("#"), (path.name, line)


# ---------------------------------------------------------------- variables


def test_every_variable_the_module_uses_is_defined_and_every_defined_variable_is_used() -> None:
    used = {
        name for path in PRODUCTION_FILES if path.suffix in {".yaml"} for name in VARIABLE.findall(path.read_text())
    }

    assert used - set(DEFAULTS) == set()
    assert set(DEFAULTS) - used <= {"creator_email"}  # only the template's own placeholder may be unused


def test_the_required_resource_variables_have_neutral_defaults_and_no_legacy_name() -> None:
    for key in ("metricsStreamExternalId", "metricsSchemaSpace", "metricsInstanceSpace"):
        assert DEFAULTS[key]
        assert "cq_demo" not in DEFAULTS[key]


def test_the_mapping_variables_are_json_text_because_toolkit_reads_a_mapping_as_a_namespace() -> None:
    raw = yaml.safe_load((MODULE / "default.config.yaml").read_text())

    for key in (
        "metricsFacilityNames",
        "metricsTierByTypeCode",
        "metricsDefaultOwners",
        "metricsOwnerPrefixRules",
        "metricsExclusions",
    ):
        assert isinstance(raw[key], str), key
        json.loads(raw[key])
    assert not [key for key, value in raw.items() if isinstance(value, dict)]


def test_the_function_task_variables_match_the_functions_own_scalar_defaults() -> None:
    cfg = build_config(function_task()["parameters"]["function"]["data"]["config"])

    names = ("freshnessDays", "maxDepth", "maxShards", "shardTargetEntities", "maxConcurrentShards")
    assert [cfg[name] for name in names] == [DEFAULT_CONFIG[name] for name in names]  # type: ignore[literal-required]


# ---------------------------------------------------------------- unique identifiers


def library_ids(skip: Path) -> dict[str, list[str]]:
    """externalId / name -> files, for every module in the library except `skip`, read without rendering."""
    found: dict[str, list[str]] = {}
    for path in (REPO_ROOT / "modules").rglob("*.yaml"):
        if skip in path.parents or "tests" in path.parts:
            continue
        for line in path.read_text().splitlines():
            stripped = line.strip().removeprefix("- ")
            for key in ("externalId:", "name:"):
                if stripped.startswith(key):
                    found.setdefault(stripped[len(key) :].strip().strip("\"'"), []).append(
                        str(path.relative_to(REPO_ROOT))
                    )
    return found


PRODUCTION_IDS = (
    "wf_context_quality_metrics",
    "wf_context_quality_metrics_schedule",
    "fn_context_quality_metrics",
    "ds_context_quality",
    "gp_context_quality_function",
    "gp_context_quality_dashboard_users",
    "context_quality_metrics",
)


@pytest.mark.parametrize("identifier", PRODUCTION_IDS)
def test_a_production_identifier_is_not_used_by_any_other_module(identifier: str) -> None:
    files = library_ids(MODULE).get(identifier, [])

    assert files == []


def test_the_defaults_do_not_collide_with_each_other_or_with_the_legacy_demo() -> None:
    ids = [
        DEFAULTS[key]
        for key in (
            "metricsWorkflowExternalId",
            "metricsTriggerExternalId",
            "metricsFunctionExternalId",
            "metricsDatasetExternalId",
            "metricsStreamExternalId",
            "metricsFunctionGroupName",
            "metricsDashboardGroupName",
        )
    ]

    assert len(set(ids)) == len(ids)
    assert not {"cq_demo", "fn_cq_service", "ds_cq_demo", "cq_demo_metrics"} & set(ids)


def test_the_group_names_are_the_proposed_ones() -> None:
    assert rendered("context_quality_function.Group.yaml", AUTH)["name"] == "gp_context_quality_function"
    assert rendered("context_quality_dashboard_users.Group.yaml", AUTH)["name"] == "gp_context_quality_dashboard_users"


# ---------------------------------------------------------------- access


def write_actions(group: dict[str, list[dict[str, Any]]]) -> dict[str, set[str]]:
    return {
        name: {a for body in bodies for a in body["actions"] if a in {"WRITE", "WRITE_PROPERTIES", "CREATE", "DELETE"}}
        for name, bodies in group.items()
    }


def scope_spaces(body: dict[str, Any]) -> list[str]:
    return body["scope"]["spaceIdScope"]["spaceIds"]


def test_the_function_group_reads_entities_and_writes_only_records() -> None:
    group = capabilities("context_quality_function.Group.yaml")

    (instances,) = group["dataModelInstancesAcl"]
    assert instances["actions"] == ["READ"]
    assert scope_spaces(instances) == ["cdf_cdm", "cdf_idm"]  # the default; a deployment adds its facility spaces
    (records,) = group["streamRecordsAcl"]
    assert records["actions"] == ["READ", "WRITE"]  # READ: finalize finds the records the shards wrote
    assert scope_spaces(records) == [DEFAULTS["metricsInstanceSpace"]]
    assert "streamsAcl" not in group  # it never reads or creates a stream
    assert "workflowOrchestrationAcl" not in group


def test_the_function_group_has_no_instance_write_and_no_infrastructure_deployment_rights() -> None:
    writes = write_actions(capabilities("context_quality_function.Group.yaml"))

    assert {name: actions for name, actions in writes.items() if actions} == {
        "sessionsAcl": {"CREATE"},
        "functionsAcl": {"WRITE"},
        "streamRecordsAcl": {"WRITE"},
        "filesAcl": {"WRITE"},
    }
    deploy = {"dataModelsAcl", "dataModelInstancesAcl", "rawAcl", "transformationsAcl", "streamsAcl", "groupsAcl"}
    assert not {name for name, actions in writes.items() if actions} & deploy


def test_the_function_group_reads_what_the_function_loads() -> None:
    group = capabilities("context_quality_function.Group.yaml")

    (models,) = group["dataModelsAcl"]
    assert models["actions"] == ["READ"]
    assert scope_spaces(models) == ["cdf_cdm", "cdf_idm", DEFAULTS["metricsSchemaSpace"]]
    (time_series,) = group["timeSeriesAcl"]
    assert time_series["actions"] == ["READ"]
    assert {"projectsAcl", "groupsAcl", "sessionsAcl", "functionsAcl"} <= set(group)


def test_a_deployment_names_its_own_source_spaces() -> None:
    variables = {**DEFAULTS, "metricsSourceSpaces": str(["cdf_cdm", "cdf_idm", "plant_a", "plant_b"])}

    (instances,) = capabilities("context_quality_function.Group.yaml", variables)["dataModelInstancesAcl"]

    assert scope_spaces(instances) == ["cdf_cdm", "cdf_idm", "plant_a", "plant_b"]


def test_the_dashboard_group_is_read_only_and_scoped_to_the_run_log() -> None:
    group = capabilities("context_quality_dashboard_users.Group.yaml")

    assert not {name for name, actions in write_actions(group).items() if actions}
    assert all(set(body["actions"]) <= {"READ", "LIST"} for bodies in group.values() for body in bodies)
    (records,) = group["streamRecordsAcl"]
    assert records["actions"] == ["READ"]
    assert scope_spaces(records) == [DEFAULTS["metricsInstanceSpace"]]
    (models,) = group["dataModelsAcl"]
    assert scope_spaces(models) == [DEFAULTS["metricsSchemaSpace"]]
    assert set(group) == {"projectsAcl", "groupsAcl", "streamRecordsAcl", "dataModelsAcl", "streamsAcl", "filesAcl"}
    (files,) = group["filesAcl"]
    assert files["actions"] == ["READ"]
    assert files["scope"]["datasetScope"]["ids"] == [DEFAULTS["metricsDatasetExternalId"]]


def test_both_groups_follow_the_stream_spaces_when_they_are_renamed() -> None:
    variables = {**DEFAULTS, **LEGACY_RESOURCES}

    function = capabilities("context_quality_function.Group.yaml", variables)
    dashboard = capabilities("context_quality_dashboard_users.Group.yaml", variables)

    assert scope_spaces(function["streamRecordsAcl"][0]) == ["sp_cq_demo_metrics_data"]
    assert scope_spaces(dashboard["streamRecordsAcl"][0]) == ["sp_cq_demo_metrics_data"]


def test_each_group_has_its_own_identity_provider_source() -> None:
    function = rendered("context_quality_function.Group.yaml", AUTH)
    dashboard = rendered("context_quality_dashboard_users.Group.yaml", AUTH)

    assert function["sourceId"] == DEFAULTS["metricsFunctionGroupSourceId"]
    assert dashboard["sourceId"] == DEFAULTS["metricsDashboardGroupSourceId"]
    assert function["sourceId"] != dashboard["sourceId"]


# ---------------------------------------------------------------- isolation from the demo and the legacy deployment


def test_no_production_file_refers_to_the_demo_module_a_seed_action_or_the_legacy_deployment() -> None:
    offenders = []
    for path in PRODUCTION_FILES:
        if path.suffix not in {".yaml", ".toml"} and path.name != "default.config.yaml":
            continue
        text = path.read_text()
        offenders += [(path.name, word) for word in SEED_WORDS if word in text]

    assert offenders == []


def test_only_the_production_module_is_registered() -> None:
    import tomllib

    registry = tomllib.loads((REPO_ROOT / "modules" / "packages.toml").read_text())["packages"]["dashboards"]["modules"]

    assert "dashboards/context_quality_v2" in registry
    assert not [name for name in registry if name.startswith("dashboards/context_quality_v2_")]
    assert not (MODULE.parent / "context_quality_v2_demo").exists()
