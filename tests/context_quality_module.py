"""Helpers for the tests of modules/dashboards/context_quality_v2: its files, variables and containers."""

import re
from pathlib import Path

import yaml
from pydantic import JsonValue

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE = REPO_ROOT / "modules" / "dashboards" / "context_quality_v2"

CONTAINERS = {"relationship": "ContextQualityRelationship", "fileAnnotation": "ContextQualityFileAnnotation"}
ISO_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
VARIABLE = re.compile(r"\{\{\s*(\w+)\s*\}\}")

type Properties = dict[str, JsonValue]


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
