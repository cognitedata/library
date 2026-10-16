"""The resources a run reads and writes are configured explicitly; nothing falls back to a legacy deployment."""

import json
from typing import Any

import handler
import pytest
from cq import runner
from cq.config import DEFAULT_CONFIG, RESOURCE_KEYS, build_config, require_resources
from sample_rules import SAMPLE_RESOURCES, SAMPLE_RULES
from tiny_data import NOW, asset, data

NEW_PROJECT = {"stream": "cq_metrics", "schemaSpace": "sp_cq_schema", "instanceSpace": "sp_cq_data"}


class NoCalls:
    """A client that fails on any use, to show nothing was read or written."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"the client was used: {name}")


# ---------------------------------------------------------------- no silent legacy defaults


@pytest.mark.parametrize("key", RESOURCE_KEYS)
def test_there_is_no_default_for_the_resources_a_run_needs(key: str) -> None:
    assert DEFAULT_CONFIG[key] == ""  # type: ignore[literal-required]


def test_the_defaults_name_no_legacy_or_demo_resource() -> None:
    text = json.dumps(DEFAULT_CONFIG)

    assert not [word for word in ("cq_demo", "db_cq", "cq_ts_datapoints", "sp_cq_demo") if word in text]


def test_the_contract_container_names_remain_defaults_because_they_are_not_environment_settings() -> None:
    cfg = build_config(NEW_PROJECT)

    assert (cfg["relationshipContainer"], cfg["fileAnnotationContainer"]) == (
        "ContextQualityRelationship",
        "ContextQualityFileAnnotation",
    )


# ---------------------------------------------------------------- required resources fail before the client is touched


@pytest.mark.parametrize("key", RESOURCE_KEYS)
@pytest.mark.parametrize("bad", [None, "", "  ", 5, ["a"]])
def test_a_missing_or_invalid_resource_is_a_configuration_error(key: str, bad: Any) -> None:
    cfg = build_config({**NEW_PROJECT, key: bad})

    with pytest.raises(ValueError, match=key):
        require_resources(cfg, writes=True)


def test_every_missing_resource_is_named_in_one_error() -> None:
    with pytest.raises(ValueError) as error:
        require_resources(build_config(None), writes=True)

    assert all(key in str(error.value) for key in RESOURCE_KEYS)


@pytest.mark.parametrize("action", ["compute_metrics", "validate"])
@pytest.mark.parametrize("config", [None, {}, {"stream": "s"}, {"schemaSpace": "a"}, {"instanceSpace": "b"}])
def test_an_unconfigured_run_fails_before_the_client_is_touched(action: str, config: dict[str, Any] | None) -> None:
    payload: dict[str, Any] = {"action": action, **({"config": config} if config is not None else {})}

    with pytest.raises(ValueError, match=r"stream|schemaSpace|instanceSpace"):
        handler.handle(payload, NoCalls())  # type: ignore[arg-type]


def test_the_stream_is_required_only_when_the_run_writes() -> None:
    spaces_only = build_config({"schemaSpace": "a", "instanceSpace": "b"})

    require_resources(spaces_only, writes=False)
    with pytest.raises(ValueError, match="stream"):
        require_resources(spaces_only, writes=True)


def test_a_dry_run_and_validate_need_the_spaces_but_not_the_stream() -> None:
    cfg = build_config({"schemaSpace": "a", "instanceSpace": "b"})

    with pytest.raises(AssertionError, match="the client was used"):  # past the config check, into the reader
        runner.compute_metrics(NoCalls(), cfg, {"dry_run": True})  # type: ignore[arg-type]
    with pytest.raises(AssertionError, match="the client was used"):
        runner.validate(NoCalls(), cfg, {})  # type: ignore[arg-type]


def test_a_real_run_without_a_stream_fails_before_the_client_is_touched() -> None:
    cfg = build_config({"schemaSpace": "a", "instanceSpace": "b"})

    with pytest.raises(ValueError, match="stream"):
        runner.compute_metrics(NoCalls(), cfg, {})  # type: ignore[arg-type]


# ---------------------------------------------------------------- explicit identifiers


def test_a_new_project_names_its_own_resources() -> None:
    cfg = build_config(NEW_PROJECT)

    require_resources(cfg, writes=True)
    assert (cfg["stream"], cfg["schemaSpace"], cfg["instanceSpace"]) == ("cq_metrics", "sp_cq_schema", "sp_cq_data")


def test_explicitly_supplied_resource_identifiers_are_accepted() -> None:
    cfg = build_config(SAMPLE_RESOURCES)

    require_resources(cfg, writes=True)


def test_the_sample_rules_works_with_explicit_resources() -> None:
    cfg = build_config({**SAMPLE_RULES, **SAMPLE_RESOURCES})

    require_resources(cfg, writes=True)
    assert cfg["divisionTagPrefix"] == "division:"


def test_the_sample_rules_alone_does_not_name_a_resource() -> None:
    assert not set(SAMPLE_RULES) & set(RESOURCE_KEYS)
    with pytest.raises(ValueError):
        require_resources(build_config(SAMPLE_RULES), writes=True)


def test_the_discovery_exclusion_uses_the_configured_spaces() -> None:
    from cq.discovery import discover_facility_spaces

    cfg = build_config({**NEW_PROJECT, "minFacilityEntities": 1})

    assert discover_facility_spaces({"sp_cq_data": 50, "sp_cq_schema": 50, "plant": 50}, cfg) == {"plant": "plant"}


# ---------------------------------------------------------------- RAW settings are not part of the function


def test_the_function_has_no_raw_settings() -> None:
    assert "rawDatabase" not in DEFAULT_CONFIG
    assert "rawDatapointsTable" not in DEFAULT_CONFIG
    assert not {"rawDatabase", "rawDatapointsTable"} & set(SAMPLE_RULES)


def test_no_production_code_reads_a_raw_setting() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent
    sources = [root / "handler.py", *sorted((root / "cq").glob("*.py"))]

    assert [p.name for p in sources if "rawDatabase" in p.read_text() or "rawDatapointsTable" in p.read_text()] == []
    assert [p.name for p in sources if ".raw" in p.read_text() or "RawRow" in p.read_text()] == []


# ---------------------------------------------------------------- division convention


def division_run(overrides: dict[str, Any]) -> list[Any]:
    from cq.metrics import compute

    loaded = data(
        assets=[
            asset("sp_a", "root", tags=("division:north", "team-x")),
            asset("sp_a", "x", parent=("sp_a", "root"), tags=("division:north",)),
        ]
    )
    compute(loaded, build_config(overrides), {}, now=NOW)  # type: ignore[arg-type]
    return [a["div"] for a in loaded["assets"]]


def test_no_division_prefix_is_active_in_the_production_defaults() -> None:
    assert DEFAULT_CONFIG["divisionTagPrefix"] == ""
    assert division_run({}) == [None, None]


def test_an_explicitly_empty_prefix_assigns_no_division_not_the_first_tag() -> None:
    assert division_run({"divisionTagPrefix": ""}) == [None, None]
    assert division_run({**SAMPLE_RULES, "divisionTagPrefix": ""}) == [None, None]


def test_the_sample_rules_restores_the_sample_division() -> None:
    assert division_run(SAMPLE_RULES) == ["north", "north"]


def test_a_project_can_configure_its_own_division_prefix() -> None:
    assert division_run({"divisionTagPrefix": "team-"}) == ["x", None]


def test_the_division_prefix_must_be_text() -> None:
    with pytest.raises(ValueError, match="divisionTagPrefix"):
        build_config({"divisionTagPrefix": None})
    with pytest.raises(ValueError, match="divisionTagPrefix"):
        build_config({"divisionTagPrefix": 5})
