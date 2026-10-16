"""Contract tests for modules/dashboards/context_quality_v2.

The canonical contract lives in the module's contract/ folder. These tests keep it, the container/view/stream
definitions and the legacy deployment's identifiers in step:

- the golden samples fit the containers,
- contract.json accounts for every container property,
- the tables in CONTRACT.md match contract.json,
- the module's variables reproduce, with legacy values, the definitions that are already deployed.
"""

import re
import tomllib
from pathlib import Path
from typing import Literal

import pytest
import yaml
from pydantic import BaseModel, JsonValue

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE = REPO_ROOT / "modules" / "dashboards" / "context_quality_v2"
CONTRACT_DIR = MODULE / "contract"
LEGACY = REPO_ROOT / "tests" / "fixtures" / "context_quality" / "legacy_deployment"

CONTAINERS = {"relationship": "ContextQualityRelationship", "fileAnnotation": "ContextQualityFileAnnotation"}
SHARED_PROPERTIES = ["rowKind", "runId", "runTimestamp"]
EMPTY = "\u2013"  # the marker CONTRACT.md uses for "none" in its tables
ISO_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
VARIABLE = re.compile(r"\{\{\s*(\w+)\s*\}\}")

type Properties = dict[str, JsonValue]


# ---------------------------------------------------------------- models of the contract files


class RowKindSpec(BaseModel):
    container: Literal["relationship", "fileAnnotation"]
    required: list[str]
    optional: list[str]


class TierSpec(BaseModel):
    value: int
    meaning: str


class DimensionSpec(BaseModel):
    id: str
    optional: bool
    headlineMetric: str
    headlineSubKey: str | None


class MetricSpec(BaseModel):
    id: str
    dimension: str
    kind: Literal["aggregate", "drilldown"]
    direction: Literal["critical", "informational"]
    issueKind: str | None
    lifecycle: bool
    emitted: bool


class ExclusionSpec(BaseModel):
    id: str
    source: Literal["system", "user"]
    emitted: bool


class WarningCodeSpec(BaseModel):
    id: str
    subject: str | None


class Contract(BaseModel):
    contractVersion: str
    description: str
    containers: dict[str, str]
    viewVersionVariable: str
    rowKinds: dict[str, RowKindSpec]
    runStatuses: dict[str, str]
    recordKeyPrefixes: list[str]
    tiers: list[TierSpec]
    dimensions: list[DimensionSpec]
    metrics: list[MetricSpec]
    exclusionCategories: list[ExclusionSpec]
    classificationKinds: list[str]
    warningCodes: list[WarningCodeSpec]


class SampleExample(BaseModel):
    name: str
    properties: Properties
    parsed: Properties | None = None


class SampleInvalid(BaseModel):
    reason: str
    properties: Properties


class KindSamples(BaseModel):
    container: str
    appRequired: list[str]
    examples: list[SampleExample]
    invalid: list[SampleInvalid]


class Samples(BaseModel):
    contractVersion: int
    note: str
    kinds: dict[str, KindSamples]


# ---------------------------------------------------------------- loading and rendering


def load_contract() -> Contract:
    return Contract.model_validate_json((CONTRACT_DIR / "contract.json").read_text())


def load_samples() -> Samples:
    return Samples.model_validate_json((CONTRACT_DIR / "samples.json").read_text())


def read_variables(path: Path) -> dict[str, str]:
    loaded = yaml.safe_load(path.read_text())
    return {key: str(value) for key, value in loaded.items()}


def render(path: Path, variables: dict[str, str]) -> dict:
    """Parse a Toolkit YAML file after substituting its {{ variables }}."""
    text = VARIABLE.sub(lambda match: variables[match.group(1)], path.read_text())
    return yaml.safe_load(text)


def variables_used(path: Path) -> set[str]:
    return set(VARIABLE.findall(path.read_text()))


def module_files(folder: str, suffix: str) -> list[Path]:
    return sorted((MODULE / folder).glob(f"*{suffix}"))


DEFAULTS = read_variables(MODULE / "default.config.yaml")
LEGACY_VARIABLES = read_variables(LEGACY / "config.yaml")


def container_definition(key: str, variables: dict[str, str] | None = None) -> dict:
    name = CONTAINERS[key]
    return render(MODULE / "data_modeling" / "containers" / f"{name}.Container.yaml", variables or DEFAULTS)


def view_definition(key: str, variables: dict[str, str] | None = None) -> dict:
    name = CONTAINERS[key]
    return render(MODULE / "data_modeling" / "views" / f"{name}.View.yaml", variables or DEFAULTS)


def fits(container_type: str, value: JsonValue) -> bool:
    if container_type == "text":
        return isinstance(value, str)
    if container_type == "timestamp":
        return isinstance(value, str) and bool(ISO_UTC.match(value))
    if container_type in ("int32", "int64"):
        return isinstance(value, int) and not isinstance(value, bool)
    if container_type == "float64":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if container_type == "boolean":
        return isinstance(value, bool)
    raise AssertionError(f"unhandled container type {container_type}")


def properties_fit_container(properties: Properties, container_key: str) -> list[str]:
    """Why the record does not fit the container, or an empty list when it does."""
    defined = container_definition(container_key)["properties"]
    problems = [
        f"'{name}' is not a property of {CONTAINERS[container_key]}" for name in properties if name not in defined
    ]
    problems += [
        f"'{name}' = {value!r} does not fit {defined[name]['type']['type']}"
        for name, value in properties.items()
        if name in defined and not fits(defined[name]["type"]["type"], value)
    ]
    problems += [
        f"required property '{name}' is missing"
        for name, spec in defined.items()
        if spec.get("nullable") is False and name not in properties
    ]
    return problems


CONTRACT = load_contract()
SAMPLES = load_samples()
SAMPLE_CASES = [(kind, example) for kind, samples in SAMPLES.kinds.items() for example in samples.examples]


def markdown_section(start: str, end: str) -> str:
    text = (CONTRACT_DIR / "CONTRACT.md").read_text()
    begin = text.index(start)
    return text[begin : text.index(end, begin + len(start))]


# ---------------------------------------------------------------- the contract file


def test_contract_is_version_one_and_names_the_two_containers() -> None:
    assert CONTRACT.contractVersion == "1"
    assert CONTRACT.containers == CONTAINERS


def test_the_row_kinds_are_the_six_of_the_baseline_and_extra_links() -> None:
    assert list(CONTRACT.rowKinds) == [
        "run",
        "aggregate",
        "exclusion",
        "drilldown",
        "classification",
        "annotationBucket",
        "extraLink",
    ]


def test_run_statuses_and_record_keys_cover_the_run_marker() -> None:
    assert list(CONTRACT.runStatuses) == ["started", "complete", "failed"]
    assert {"run:started", "run:complete"} <= set(CONTRACT.recordKeyPrefixes)


def test_every_row_kind_requires_the_shared_properties_and_declares_no_property_twice() -> None:
    for kind, spec in CONTRACT.rowKinds.items():
        assert spec.required[: len(SHARED_PROPERTIES)] == SHARED_PROPERTIES, kind
        assert not set(spec.required) & set(spec.optional), kind
        assert len(spec.required + spec.optional) == len(set(spec.required + spec.optional)), kind


@pytest.mark.parametrize("container_key", list(CONTAINERS))
def test_row_kinds_account_for_every_property_of_their_container(container_key: str) -> None:
    declared = {
        name
        for spec in CONTRACT.rowKinds.values()
        if spec.container == container_key
        for name in spec.required + spec.optional
    }

    assert declared == set(container_definition(container_key)["properties"])


@pytest.mark.parametrize("container_key", list(CONTAINERS))
def test_only_the_shared_properties_are_required_by_the_containers(container_key: str) -> None:
    properties = container_definition(container_key)["properties"]

    required = [name for name, spec in properties.items() if spec["nullable"] is False]

    assert sorted(required) == sorted(SHARED_PROPERTIES)


def test_identifiers_are_unique_and_consistent() -> None:
    dimensions = {dimension.id: dimension for dimension in CONTRACT.dimensions}
    metrics = {metric.id: metric for metric in CONTRACT.metrics}

    assert len(dimensions) == 7
    assert len(metrics) == len(CONTRACT.metrics) == 57
    assert len(CONTRACT.exclusionCategories) == 8
    assert len(CONTRACT.classificationKinds) == 4
    for metric in metrics.values():
        assert metric.dimension in dimensions, metric.id
        assert (metric.issueKind is None) or metric.kind == "drilldown", metric.id
    for dimension in dimensions.values():
        headline = metrics[dimension.headlineMetric]
        assert headline.dimension == dimension.id
        assert headline.kind == "aggregate"
    assert [dimension.id for dimension in CONTRACT.dimensions if dimension.optional] == [
        "maintenance",
        "file-annotation",
        "model-3d",
    ]


def test_tiers_are_one_two_and_three() -> None:
    assert [tier.value for tier in CONTRACT.tiers] == [1, 2, 3]


# ---------------------------------------------------------------- containers, views and stream


@pytest.mark.parametrize("container_key", list(CONTAINERS))
def test_containers_are_record_containers_with_the_contract_names(container_key: str) -> None:
    container = container_definition(container_key)

    assert container["externalId"] == CONTAINERS[container_key]
    assert container["usedFor"] == "record"
    assert container["space"] == DEFAULTS["metricsSchemaSpace"]


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


def test_the_module_has_no_resources_of_the_superseded_four_container_design() -> None:
    names = {path.name for path in (MODULE / "data_modeling").rglob("*.yaml")}

    assert names == {
        "context_quality_metrics.Space.yaml",
        "context_quality_instances.Space.yaml",
        "ContextQualityRelationship.Container.yaml",
        "ContextQualityFileAnnotation.Container.yaml",
        "ContextQualityRelationship.View.yaml",
        "ContextQualityFileAnnotation.View.yaml",
    }


# ---------------------------------------------------------------- golden samples


def test_samples_are_version_one_and_cover_exactly_the_row_kinds() -> None:
    assert SAMPLES.contractVersion == 1
    assert list(SAMPLES.kinds) == list(CONTRACT.rowKinds)


@pytest.mark.parametrize(
    ("kind", "example"), SAMPLE_CASES, ids=[f"{kind}/{example.name}" for kind, example in SAMPLE_CASES]
)
def test_golden_sample_fits_its_container(kind: str, example: SampleExample) -> None:
    container_key = CONTRACT.rowKinds[kind].container

    assert SAMPLES.kinds[kind].container == CONTAINERS[container_key]
    assert properties_fit_container(example.properties, container_key) == []


@pytest.mark.parametrize(
    ("kind", "example"), SAMPLE_CASES, ids=[f"{kind}/{example.name}" for kind, example in SAMPLE_CASES]
)
def test_golden_sample_has_the_kind_it_claims_and_only_properties_of_that_kind(
    kind: str, example: SampleExample
) -> None:
    spec = CONTRACT.rowKinds[kind]

    assert example.properties["rowKind"] == kind
    assert set(spec.required) <= set(example.properties)
    assert set(example.properties) <= set(spec.required + spec.optional)


@pytest.mark.parametrize("kind", list(CONTRACT.rowKinds))
def test_properties_the_samples_say_a_consumer_requires_are_required_by_the_contract(kind: str) -> None:
    assert set(SAMPLES.kinds[kind].appRequired) <= set(CONTRACT.rowKinds[kind].required)


def invalid_cases() -> list[tuple[str, SampleInvalid]]:
    return [(kind, invalid) for kind, samples in SAMPLES.kinds.items() for invalid in samples.invalid]


@pytest.mark.parametrize(
    ("kind", "invalid"), invalid_cases(), ids=[f"{kind}/{invalid.reason}" for kind, invalid in invalid_cases()]
)
def test_invalid_samples_break_a_rule_of_the_contract(kind: str, invalid: SampleInvalid) -> None:
    spec = CONTRACT.rowKinds[kind]
    container_key = spec.container
    wrong_type = properties_fit_container(invalid.properties, container_key)
    nulled_required = [name for name in spec.required if invalid.properties.get(name) is None]

    assert wrong_type or nulled_required


def test_samples_use_only_identifiers_of_the_contract() -> None:
    metrics = {metric.id for metric in CONTRACT.metrics}
    dimensions = {dimension.id for dimension in CONTRACT.dimensions}
    categories = {category.id for category in CONTRACT.exclusionCategories}
    for kind, example in SAMPLE_CASES:
        properties = example.properties
        label = f"{kind}/{example.name}"
        if kind == "extraLink":  # named by the user's config file: a property, and a model and view
            continue
        assert properties.get("metricId", next(iter(metrics))) in metrics, label
        assert properties.get("dimensionId", next(iter(dimensions))) in dimensions, label
        assert properties.get("exclusionCategory", next(iter(categories))) in categories, label
        assert properties.get("classificationKind", CONTRACT.classificationKinds[0]) in CONTRACT.classificationKinds, (
            label
        )


# ---------------------------------------------------------------- CONTRACT.md matches contract.json


def test_contract_md_lists_the_dimensions_of_contract_json() -> None:
    rows = re.findall(
        r"^\| `([a-z0-9-]+)` \| (yes|no) \| `(\w+)` \| (?:`(\w+)`)?\s*\|",
        markdown_section("### 5.1 Dimension IDs", "### 5.2"),
        re.MULTILINE,
    )

    assert rows == [
        (dimension.id, "yes" if dimension.optional else "no", dimension.headlineMetric, dimension.headlineSubKey or "")
        for dimension in CONTRACT.dimensions
    ]


def test_contract_md_lists_the_metrics_of_contract_json() -> None:
    rows = re.findall(
        rf"^\| `(\w+)` \| ([a-z0-9-]+) \| (aggregate|drilldown) \| (critical|informational) \| ([a-z0-9-]+|{EMPTY}) \| (yes|{EMPTY}) \| (yes|{EMPTY}) \|$",
        markdown_section("### 5.2 Metric IDs", "### 5.3"),
        re.MULTILINE,
    )

    assert rows == [
        (
            metric.id,
            metric.dimension,
            metric.kind,
            metric.direction,
            metric.issueKind or EMPTY,
            "yes" if metric.lifecycle else EMPTY,
            "yes" if metric.emitted else EMPTY,
        )
        for metric in CONTRACT.metrics
    ]


def test_contract_md_lists_the_exclusion_categories_of_contract_json() -> None:
    rows = re.findall(
        r"^\| `([a-z:-]+)` \| (system|user) \| (yes|no) \|$",
        markdown_section("### 5.3 Exclusion categories", "### 5.4"),
        re.MULTILINE,
    )

    assert rows == [
        (category.id, category.source, "yes" if category.emitted else "no") for category in CONTRACT.exclusionCategories
    ]


def test_contract_md_lists_the_run_warning_codes_of_contract_json() -> None:
    rows = re.findall(r"^\| `([a-z-]+)` \|", markdown_section("### 3.3 Run warnings", "## 4."), re.MULTILINE)

    assert rows == [code.id for code in CONTRACT.warningCodes]


def test_contract_md_lists_the_classification_kinds_and_row_kinds() -> None:
    text = (CONTRACT_DIR / "CONTRACT.md").read_text()

    for kind in CONTRACT.classificationKinds:
        assert f"`{kind}`" in markdown_section("### 5.4", "## 6.")
    for kind in CONTRACT.rowKinds:
        assert f"| `{kind}` |" in markdown_section("## 2. Row kinds", "### 2.1"), kind
    assert "version 1" in text


@pytest.mark.parametrize("container_key", list(CONTAINERS))
def test_contract_md_describes_every_container_property(container_key: str) -> None:
    text = (CONTRACT_DIR / "CONTRACT.md").read_text()

    for name in container_definition(container_key)["properties"]:
        assert f"`{name}`" in text, f"CONTRACT.md does not mention {CONTAINERS[container_key]}.{name}"


def test_contract_md_names_every_deployment_variable() -> None:
    text = (CONTRACT_DIR / "CONTRACT.md").read_text()

    for variable in (
        "metricsStreamExternalId",
        "metricsStreamTemplate",
        "metricsSchemaSpace",
        "metricsInstanceSpace",
        "metricsVersion",
    ):
        assert f"`{variable}`" in text, variable
        assert variable in DEFAULTS


# ---------------------------------------------------------------- the legacy deployment is preserved


SCHEMA_VARIABLES = {
    "metricsSchemaSpace",
    "metricsInstanceSpace",
    "metricsVersion",
    "metricsStreamExternalId",
    "metricsStreamTemplate",
}


def test_legacy_variable_set_is_complete_and_differs_from_the_defaults() -> None:
    assert set(LEGACY_VARIABLES) == SCHEMA_VARIABLES
    assert all(
        LEGACY_VARIABLES[name] != DEFAULTS[name]
        for name in ("metricsSchemaSpace", "metricsInstanceSpace", "metricsStreamExternalId")
    )


def test_the_defaults_name_nothing_from_the_legacy_deployment() -> None:
    assert "cq_demo" not in " ".join(DEFAULTS.values())


@pytest.mark.parametrize("container_key", list(CONTAINERS))
def test_legacy_variables_reproduce_the_deployed_container(container_key: str) -> None:
    deployed = yaml.safe_load((LEGACY / f"{CONTAINERS[container_key]}.Container.yaml").read_text())

    assert container_definition(container_key, LEGACY_VARIABLES) == deployed


@pytest.mark.parametrize("container_key", list(CONTAINERS))
def test_legacy_variables_reproduce_the_deployed_view(container_key: str) -> None:
    deployed = yaml.safe_load((LEGACY / f"{CONTAINERS[container_key]}.View.yaml").read_text())

    assert view_definition(container_key, LEGACY_VARIABLES) == deployed


def test_legacy_variables_reproduce_the_deployed_stream() -> None:
    rendered = render(MODULE / "streams" / "context_quality_metrics.Streams.yaml", LEGACY_VARIABLES)

    assert rendered == yaml.safe_load((LEGACY / "cq_demo_metrics.Streams.yaml").read_text())


def test_legacy_variables_reproduce_the_deployed_space_identifiers() -> None:
    schema = render(MODULE / "data_modeling" / "context_quality_metrics.Space.yaml", LEGACY_VARIABLES)
    instances = render(MODULE / "data_modeling" / "context_quality_instances.Space.yaml", LEGACY_VARIABLES)

    assert schema["space"] == yaml.safe_load((LEGACY / "sp_cq_demo_metrics.Space.yaml").read_text())["space"]
    assert instances["space"] == yaml.safe_load((LEGACY / "sp_cq_demo_metrics_data.Space.yaml").read_text())["space"]


@pytest.mark.parametrize("container_key", list(CONTAINERS))
def test_changes_to_the_containers_are_additive_only(container_key: str) -> None:
    """A property that is already deployed keeps its definition. New properties must be optional."""
    deployed = yaml.safe_load((LEGACY / f"{CONTAINERS[container_key]}.Container.yaml").read_text())["properties"]
    current = container_definition(container_key, LEGACY_VARIABLES)["properties"]

    for name, spec in deployed.items():
        assert current[name] == spec, name
    for name in set(current) - set(deployed):
        assert current[name]["nullable"] is True, f"new property {name} must be nullable"


# ---------------------------------------------------------------- module registration


def test_module_is_registered_with_the_dashboards_package() -> None:
    module = tomllib.loads((MODULE / "module.toml").read_text())["module"]
    registry = tomllib.loads((REPO_ROOT / "modules" / "packages.toml").read_text())

    assert module["id"] == "dp:dashboards:context_quality_v2"
    assert module["package_id"] == "dp:dashboards"
    assert "dashboards/context_quality_v2" in registry["packages"]["dashboards"]["modules"]
