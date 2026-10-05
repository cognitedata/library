"""Tests for matching entities only against targets in the same primary and secondary scope."""

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent))

import em_submit  # isort: skip
from em_config import Config, ConfigData, Parameters, ViewPropertyConfig  # isort: skip
from em_logger import CogniteFunctionLogger  # isort: skip
from em_scope import scope_batches, scope_of, scope_properties  # isort: skip


def build_config(primary: str | None = None, secondary: str | None = None) -> Config:
    view = ViewPropertyConfig(schemaSpace="cdf_cdm", instanceSpace="sp", externalId="CogniteAsset", version="v1")
    return Config(
        parameters=Parameters(
            dmUpdate=False,
            runAll=False,
            removeOldLinks=False,
            rawDb="db",
            rawTableState="state",
            rawTableCtxGood="good",
            rawTableCtxBad="bad",
            autoApprovalThreshold=0.85,
            primaryScopeProperty=primary,
            secondaryScopeProperty=secondary,
        ),
        data=ConfigData(entityView=view, targetView=view),
    )


def target(ext_id: str, primary: str = "", secondary: str = "", scope_wide: bool = False) -> dict[str, Any]:
    return {
        "asset_ext_id": ext_id,
        "asset_space": "sp",
        "org_name": ext_id,
        "name": ext_id,
        "rule_keys": None,
        "scope_primary": primary,
        "scope_secondary": secondary,
        "scope_wide": scope_wide,
    }


def entity(ext_id: str, primary: str = "", secondary: str = "") -> dict[str, Any]:
    return {
        "entity_ext_id": ext_id,
        "entity_space": "sp",
        "org_name": ext_id,
        "name": ext_id,
        "assets": "[]",
        "rule_keys": None,
        "scope_primary": primary,
        "scope_secondary": secondary,
    }


@pytest.fixture
def logger() -> CogniteFunctionLogger:
    return CogniteFunctionLogger("ERROR")


def ids(records: list[dict[str, Any]], key: str) -> list[str]:
    return [record[key] for record in records]


def test_blank_scope_properties_turn_scoping_off() -> None:
    assert scope_properties(build_config(primary="", secondary=" ").parameters) == []


def test_scoping_reads_the_scope_properties_and_tags() -> None:
    assert scope_properties(build_config(primary="site", secondary="unit").parameters) == ["site", "unit", "tags"]


def test_scope_of_reads_missing_values_as_empty() -> None:
    parameters = build_config(primary="site", secondary="unit").parameters

    assert scope_of({"site": "A"}, parameters) == ("A", "")


def test_unscoped_run_is_one_batch_of_everything(logger: CogniteFunctionLogger) -> None:
    targets = [target("A-1", "x"), target("A-2", "y")]
    entities = [entity("TS-1", "z")]

    batches = scope_batches(build_config().parameters, logger, targets, entities)  # type: ignore[arg-type]

    assert batches == [(targets, entities)]


def test_entities_only_meet_targets_in_their_primary_scope(logger: CogniteFunctionLogger) -> None:
    targets = [target("A-1", "site_a"), target("B-1", "site_b")]
    entities = [entity("TS-A", "site_a"), entity("TS-B", "site_b")]

    batches = scope_batches(build_config(primary="site").parameters, logger, targets, entities)  # type: ignore[arg-type]

    assert [(ids(t, "asset_ext_id"), ids(e, "entity_ext_id")) for t, e in batches] == [
        (["A-1"], ["TS-A"]),
        (["B-1"], ["TS-B"]),
    ]


def test_scope_wide_targets_join_every_secondary_scope_of_their_primary(logger: CogniteFunctionLogger) -> None:
    targets = [
        target("U1-PUMP", "site_a", "unit_1"),
        target("U2-PUMP", "site_a", "unit_2"),
        target("A-WIDE", "site_a", "unit_9", scope_wide=True),
        target("B-WIDE", "site_b", "unit_1", scope_wide=True),
    ]
    entities = [entity("TS-1", "site_a", "unit_1")]
    parameters = build_config(primary="site", secondary="unit").parameters

    batches = scope_batches(parameters, logger, targets, entities)  # type: ignore[arg-type]

    assert [ids(t, "asset_ext_id") for t, _ in batches] == [["U1-PUMP", "A-WIDE"]]


def test_entities_without_scope_are_matched_against_all_targets(logger: CogniteFunctionLogger) -> None:
    targets = [target("A-1", "site_a"), target("B-1", "site_b"), target("NO-SITE")]
    entities = [entity("TS-NO-SITE")]

    batches = scope_batches(build_config(primary="site").parameters, logger, targets, entities)  # type: ignore[arg-type]

    assert [ids(t, "asset_ext_id") for t, _ in batches] == [["A-1", "B-1", "NO-SITE"]]
    assert ids(batches[0][1], "entity_ext_id") == ["TS-NO-SITE"]


def test_entities_without_scope_still_get_a_job_when_no_empty_scope_targets_exist(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Previously these were skipped; they must fall back to every target."""
    targets = [target("A-1", "site_a"), target("B-1", "site_b")]
    entities = [entity("TS-1"), entity("TS-2")]
    logger = CogniteFunctionLogger("WARNING")

    with caplog.at_level("WARNING"):
        batches = scope_batches(build_config(primary="site").parameters, logger, targets, entities)  # type: ignore[arg-type]

    assert [ids(t, "asset_ext_id") for t, _ in batches] == [["A-1", "B-1"]]
    assert ids(batches[0][1], "entity_ext_id") == ["TS-1", "TS-2"]
    assert any(
        "Entities without scope (primary='', secondary='') - 2 is tried matched against all Targets" in message
        for message in caplog.messages
    )


def test_a_scope_without_targets_is_skipped(logger: CogniteFunctionLogger) -> None:
    targets = [target("A-1", "site_a")]
    entities = [entity("TS-B", "site_b")]

    assert scope_batches(build_config(primary="site").parameters, logger, targets, entities) == []  # type: ignore[arg-type]


def test_predict_job_counts_unique_entities_minus_those_already_matched() -> None:
    """One entity can be sent once per search value; rule matches must not count as still to match."""
    entities = [entity("TS-A"), entity("TS-A"), entity("TS-B")]
    staged = [{"entity_ext_id": "TS-A", "entity_space": "sp"}]

    source_records, unique, already, to_match = em_submit.predict_job_entity_counts(
        entities,  # type: ignore[arg-type]
        staged,  # type: ignore[arg-type]
    )

    assert source_records == 3
    assert unique == 2
    assert already == 1
    assert to_match == 1


def test_predict_job_log_includes_entities_to_match(monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config(primary="site")
    logger = MagicMock()
    predicted: list[list[str]] = []

    def fake_predict(client: object, cfg: Config, log: object, model_id: str, targets: list, entities: list) -> Any:
        predicted.append(ids(entities, "entity_ext_id"))
        job_id = str(len(predicted))
        return SimpleNamespace(job_id=job_id, job_token=f"token-{job_id}", model_id=42)

    monkeypatch.setattr(em_submit, "_raw_upload_queue", MagicMock())
    monkeypatch.setattr(em_submit, "read_state_store", lambda *a: "")
    monkeypatch.setattr(em_submit, "read_manual_mappings", lambda *a: ([], {}))
    monkeypatch.setattr(em_submit, "read_rule_mappings", lambda *a: [])
    monkeypatch.setattr(em_submit, "get_all_targets", lambda *a: [target("A-1", "site_a"), target("B-1", "site_b")])
    monkeypatch.setattr(em_submit, "apply_manual_mappings", lambda *a: ([], 0))
    monkeypatch.setattr(em_submit, "get_new_entities", lambda *a: [entity("TS-A", "site_a"), entity("TS-B", "site_b")])

    def apply_rules(
        client: object,
        cfg: Config,
        log: object,
        good: list[dict[str, str]],
        targets: list,
        entities: list,
    ) -> tuple[list[dict[str, str]], int]:
        if entities and entities[0]["entity_ext_id"] == "TS-A":
            return [*good, {"entity_ext_id": "TS-A", "entity_space": "sp"}], 1
        return good, 0

    monkeypatch.setattr(em_submit, "apply_rule_mappings", apply_rules)
    monkeypatch.setattr(em_submit, "submit_predict_job", fake_predict)
    monkeypatch.setattr(em_submit, "write_staged_matches", lambda *a: None)
    monkeypatch.setattr(em_submit, "append_predict_job", lambda *a, **kw: None)
    monkeypatch.setattr(em_submit, "update_pipeline_run", MagicMock())

    em_submit.submit_entity_matching(MagicMock(), logger, {"ExtractionPipelineExtId": "ep"}, config)

    info = [call.args[0] for call in logger.info.call_args_list]
    assert any("Predict job submitted - jobId: 1, 0 entities to match" in msg for msg in info)
    assert any("Predict job submitted - jobId: 2, 1 entities to match" in msg for msg in info)
    assert any("Submitted 2 predict job(s) totalling 1 entities to match" in msg for msg in info)


def test_submit_starts_one_predict_job_per_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config(primary="site")
    manual = {"match_type": "Manual Mapping", "entity_ext_id": "TS-M", "entity_space": "sp"}
    predicted: list[tuple[str, list[str]]] = []
    staged: dict[str, list[dict[str, Any]]] = {}
    queued: list[str] = []

    def fake_predict(client: object, cfg: Config, log: object, model_id: str, targets: list, entities: list) -> Any:
        job_id = str(len(predicted) + 1)
        predicted.append((model_id, ids(targets, "asset_ext_id")))
        return SimpleNamespace(job_id=job_id, job_token=f"token-{job_id}", model_id=42)

    monkeypatch.setattr(em_submit, "_raw_upload_queue", MagicMock())
    monkeypatch.setattr(em_submit, "read_state_store", lambda *a: "")
    monkeypatch.setattr(em_submit, "read_manual_mappings", lambda *a: ([], {}))
    monkeypatch.setattr(em_submit, "read_rule_mappings", lambda *a: [])
    monkeypatch.setattr(em_submit, "get_all_targets", lambda *a: [target("A-1", "site_a"), target("B-1", "site_b")])
    monkeypatch.setattr(em_submit, "apply_manual_mappings", lambda *a: ([manual], 1))
    monkeypatch.setattr(em_submit, "get_new_entities", lambda *a: [entity("TS-A", "site_a"), entity("TS-B", "site_b")])
    monkeypatch.setattr(em_submit, "apply_rule_mappings", lambda c, cfg, log, good, t, e: (good, 0))
    monkeypatch.setattr(em_submit, "submit_predict_job", fake_predict)
    monkeypatch.setattr(em_submit, "write_staged_matches", lambda c, log, job_id, m, ds: staged.update({job_id: m}))
    monkeypatch.setattr(em_submit, "append_predict_job", lambda *a, job_id, **kw: queued.append(job_id))
    monkeypatch.setattr(em_submit, "update_pipeline_run", MagicMock())

    em_submit.submit_entity_matching(
        MagicMock(),
        CogniteFunctionLogger("ERROR"),
        {"ExtractionPipelineExtId": "ep"},
        config,
    )

    assert predicted == [("", ["A-1"]), ("42", ["B-1"])], "the model fitted for the first scope is reused"
    assert queued == ["1", "2"]
    assert staged == {"1": [manual], "2": []}, "manual matches are staged once, with the first job"
