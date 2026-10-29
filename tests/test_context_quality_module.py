"""Structure tests for modules/dashboards/context_quality_v2: its containers, views, stream, variables and registration."""

import tomllib

import pytest

from tests.context_quality_module import (
    CONTAINERS,
    DEFAULTS,
    MODULE,
    REPO_ROOT,
    container_definition,
    module_files,
    render,
    variables_used,
    view_definition,
)


@pytest.mark.parametrize("container_key", list(CONTAINERS))
def test_containers_are_record_containers_with_the_expected_names(container_key: str) -> None:
    container = container_definition(container_key)

    assert container["externalId"] == CONTAINERS[container_key]
    assert container["usedFor"] == "record"
    assert container["space"] == DEFAULTS["metricsSchemaSpace"]


@pytest.mark.parametrize("container_key", list(CONTAINERS))
def test_only_the_shared_properties_are_required_by_the_containers(container_key: str) -> None:
    properties = container_definition(container_key)["properties"]

    required = {name for name, spec in properties.items() if spec["nullable"] is False}

    assert required == {"rowKind", "runId", "runTimestamp"}


@pytest.mark.parametrize("container_key", list(CONTAINERS))
def test_views_expose_every_container_property_and_point_at_their_own_container(container_key: str) -> None:
    container = container_definition(container_key)
    view = view_definition(container_key)

    assert view["externalId"] == container["externalId"]
    assert view["space"] == container["space"]
    assert view["version"] == DEFAULTS["metricsVersion"]
    assert view["streamId"] == [DEFAULTS["metricsStreamExternalId"]]
    assert set(view["properties"]) == set(container["properties"])
    for name, spec in view["properties"].items():
        reference = spec["container"]
        assert (reference["space"], reference["externalId"], reference["type"]) == (
            container["space"],
            container["externalId"],
            "container",
        )
        assert spec["containerPropertyIdentifier"] == name


def test_stream_comes_from_variables() -> None:
    stream = render(MODULE / "streams" / "context_quality_metrics.Streams.yaml", DEFAULTS)

    assert stream == {
        "externalId": DEFAULTS["metricsStreamExternalId"],
        "settings": {"template": {"name": DEFAULTS["metricsStreamTemplate"]}},
    }


def test_default_config_defines_exactly_the_variables_the_module_uses() -> None:
    used: set[str] = set()
    for folder, suffix in (
        ("data_modeling", ".Space.yaml"),
        ("streams", ".Streams.yaml"),
        ("data_sets", ".DataSet.yaml"),
        ("functions", ".Function.yaml"),
        ("data_modeling/containers", ".Container.yaml"),
        ("data_modeling/views", ".View.yaml"),
        ("workflows", ".Workflow.yaml"),
        ("workflows", ".WorkflowVersion.yaml"),
        ("workflows", ".WorkflowTrigger.yaml"),
        ("auth", ".Group.yaml"),
    ):
        for path in module_files(folder, suffix):
            used |= variables_used(path)

    assert used == set(DEFAULTS) - {"creator_email"}


def test_the_function_resource_is_named_after_its_folder_and_data_set() -> None:
    functions = render(MODULE / "functions" / "context_quality.Function.yaml", DEFAULTS)
    data_set = render(MODULE / "data_sets" / "context_quality.DataSet.yaml", DEFAULTS)

    assert [function["externalId"] for function in functions] == [DEFAULTS["metricsFunctionExternalId"]]
    assert (MODULE / "functions" / DEFAULTS["metricsFunctionExternalId"] / "handler.py").is_file()
    assert functions[0]["dataSetExternalId"] == data_set["externalId"]
    assert functions[0]["functionPath"] == "handler.py"


def test_module_is_registered_with_the_dashboards_package() -> None:
    module = tomllib.loads((MODULE / "module.toml").read_text())["module"]
    registry = tomllib.loads((REPO_ROOT / "modules" / "packages.toml").read_text())

    assert module["id"] == "dp:dashboards:context_quality_v2"
    assert module["package_id"] == "dp:dashboards"
    assert "dashboards/context_quality_v2" in registry["packages"]["dashboards"]["modules"]
