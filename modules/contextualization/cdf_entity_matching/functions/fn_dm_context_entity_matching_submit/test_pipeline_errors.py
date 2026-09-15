"""Unexpected mapping errors must fail the run instead of returning partial success."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.exceptions import CogniteAPIError
from tenacity import wait_none

sys.path.append(str(Path(__file__).parent))

import em_pipeline  # isort: skip
from em_pipeline_optimizations import RobustAPIClient  # isort: skip
from em_constants import (  # isort: skip
    COL_KEY_MAN_MAPPING_ENTITY,
    COL_KEY_MAN_MAPPING_TARGET,
    KEY_ENTITY_EXT_ID,
    KEY_ENTITY_SPACE,
    KEY_NAME,
    KEY_ORG_NAME,
    KEY_RULE,
    KEY_RULE_KEYS,
    KEY_TARGET_EXT_ID,
    KEY_TARGET_LINKS,
    KEY_TARGET_SPACE,
    STATUS_FAILURE,
)
from test_submit import build_config  # isort: skip


def test_manual_mapping_propagates_unexpected_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_to_read_instances(*args: object, **kwargs: object) -> list[object]:
        raise RuntimeError("programming error")

    monkeypatch.setattr(em_pipeline, "list_instances_by_external_id_direct", fail_to_read_instances)
    mappings = [
        {
            KEY_RULE: "row-1",
            COL_KEY_MAN_MAPPING_ENTITY: "entity-1",
            COL_KEY_MAN_MAPPING_TARGET: "target-1",
        }
    ]

    with pytest.raises(RuntimeError, match="programming error"):
        em_pipeline.apply_manual_mappings(
            MagicMock(),
            MagicMock(),
            build_config(),
            MagicMock(),
            mappings,
            {"row-1": {}},
        )


def test_rule_mapping_propagates_unexpected_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_to_build_key(*args: object, **kwargs: object) -> tuple[str, str]:
        raise RuntimeError("programming error")

    monkeypatch.setattr(em_pipeline, "instance_key", fail_to_build_key)
    targets = [
        {
            KEY_TARGET_EXT_ID: "target-1",
            KEY_TARGET_SPACE: "target-space",
            KEY_ORG_NAME: "Target",
            KEY_NAME: "Target",
            KEY_RULE_KEYS: ["rule-1"],
        }
    ]
    entities = [
        {
            KEY_ENTITY_EXT_ID: "entity-1",
            KEY_ENTITY_SPACE: "entity-space",
            KEY_ORG_NAME: "Entity",
            KEY_NAME: "Entity",
            KEY_TARGET_LINKS: "[]",
            KEY_RULE_KEYS: ["rule-1"],
        }
    ]

    with pytest.raises(RuntimeError, match="programming error"):
        em_pipeline.apply_rule_mappings(MagicMock(), build_config(), MagicMock(), [], targets, entities)


def test_create_table_ignores_conflict_and_propagates_other_api_errors() -> None:
    client = MagicMock()
    client.raw.databases.create.side_effect = CogniteAPIError("Already exists", code=409)
    client.raw.tables.create.side_effect = CogniteAPIError("Forbidden", code=403)

    with pytest.raises(CogniteAPIError) as failed:
        em_pipeline.create_table(client, "db", "tbl")

    assert failed.value.code == 403
    client.raw.tables.create.assert_called_once_with("db", "tbl")


def test_is_retryable_lives_with_the_retry_helpers() -> None:
    from em_pipeline_optimizations import is_retryable

    assert is_retryable(CogniteAPIError("timed out", code=408))
    assert is_retryable(CogniteAPIError("too many requests", code=429))
    assert is_retryable(CogniteAPIError("unavailable", code=503))
    assert not is_retryable(CogniteAPIError("forbidden", code=403))
    assert not is_retryable(TypeError("bug"))


def _api_client_without_backoff(monkeypatch: pytest.MonkeyPatch) -> RobustAPIClient:
    monkeypatch.setattr(RobustAPIClient.robust_api_call.retry, "wait", wait_none())  # pyright: ignore[reportFunctionMemberAccess]
    return RobustAPIClient(MagicMock())


def test_robust_api_call_does_not_retry_client_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _api_client_without_backoff(monkeypatch)
    forbidden = CogniteAPIError("Forbidden", code=403)
    operation = MagicMock(side_effect=forbidden)

    with pytest.raises(CogniteAPIError) as failed:
        client.robust_api_call(operation)

    assert failed.value is forbidden
    assert operation.call_count == 1


def test_raw_upload_queue_trigger_log_level_is_a_name() -> None:
    """extractorutils calls .upper() on this, so an int like logging.INFO fails at runtime."""
    from em_constants import LOG_LEVEL_DEBUG, LOG_LEVEL_INFO

    assert LOG_LEVEL_INFO == "INFO"
    assert LOG_LEVEL_DEBUG == "DEBUG"

    core = Path(__file__).parents[1] / "_entity_matching_core"
    for module in ("em_submit.py", "em_collect.py"):
        source = (core / module).read_text(encoding="utf-8")
        assert "trigger_log_level=LOG_LEVEL_INFO" in source
        assert "trigger_log_level=logging." not in source


def test_robust_api_call_retries_transient_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _api_client_without_backoff(monkeypatch)
    operation = MagicMock(
        side_effect=[CogniteAPIError("Unavailable", code=503), "ok"],
    )

    assert client.robust_api_call(operation) == "ok"
    assert operation.call_count == 2


def _failure_run_message(client: MagicMock) -> str:
    run = client.extraction_pipelines.runs.create.call_args.args[0]
    return run.message


def test_failed_pipeline_run_without_exception_does_not_log_a_traceback() -> None:
    client = MagicMock()
    logger = MagicMock()

    em_pipeline.update_pipeline_run(
        client,
        logger,
        "ep-entity-matching",
        STATUS_FAILURE,
        2,
        1,
        "Predict job(s) failed: 42",
    )

    message = _failure_run_message(client)
    assert "Predict job(s) failed: 42" in message
    assert "traceback" not in message
    assert "NoneType: None" not in message


def test_failed_pipeline_run_inside_except_includes_the_traceback() -> None:
    client = MagicMock()
    logger = MagicMock()

    try:
        raise RuntimeError("predict exploded")
    except RuntimeError as e:
        em_pipeline.update_pipeline_run(
            client,
            logger,
            "ep-entity-matching",
            STATUS_FAILURE,
            0,
            0,
            f"failed, Message: {e!s}",
        )

    message = _failure_run_message(client)
    assert "failed, Message: predict exploded" in message
    assert "traceback" in message
    assert "RuntimeError: predict exploded" in message
