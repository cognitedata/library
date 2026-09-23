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
    COL_KEY_MAN_CONTEXTUALIZED,
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
    PROP_COL_NAME,
    STATUS_FAILURE,
    STATUS_SUCCESS,
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


def test_manual_mapping_uploads_raw_even_when_dm_update_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contextualized flags must be persisted even when dmUpdate skips DMS writes."""
    config = build_config().model_copy(
        update={
            "parameters": build_config().parameters.model_copy(update={"raw_table_ctx_manual": "manual"}),
        }
    )
    assert config.parameters.dm_update is False

    view_id = config.data.entity_view.as_view_id()
    entity = MagicMock()
    entity.external_id = "entity-1"
    entity.space = "inst_location"
    entity.properties = {view_id: {PROP_COL_NAME: "Entity 1"}}

    monkeypatch.setattr(
        em_pipeline,
        "list_instances_by_external_id_direct",
        lambda *args, **kwargs: [entity],
    )

    client = MagicMock()
    raw_uploader = MagicMock()
    mappings = [
        {
            KEY_RULE: "row-1",
            COL_KEY_MAN_MAPPING_ENTITY: "entity-1",
            COL_KEY_MAN_MAPPING_TARGET: "target-1",
        }
    ]

    em_pipeline.apply_manual_mappings(
        client,
        MagicMock(),
        config,
        raw_uploader,
        mappings,
        {"row-1": {}},
    )

    client.data_modeling.instances.apply.assert_not_called()
    raw_uploader.upload.assert_called_once()
    queued_row = raw_uploader.add_to_upload_queue.call_args.args[2]
    assert queued_row.columns[COL_KEY_MAN_CONTEXTUALIZED] is True


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


def test_create_table_ignores_an_existing_database_and_table() -> None:
    """RAW answers 400, not 409, and words it differently for databases and tables."""
    client = MagicMock()
    client.raw.databases.create.side_effect = CogniteAPIError(
        "Databases with the following names already exists: db_asset_entity_matching", code=400
    )
    client.raw.tables.create.side_effect = CogniteAPIError("Tables already created: contextualization_bad", code=400)

    em_pipeline.create_table(client, "db", "tbl")

    client.raw.tables.create.assert_called_once_with("db", "tbl")


@pytest.mark.parametrize("code", [401, 403, 500])
def test_create_table_propagates_errors_that_stop_the_run(code: int) -> None:
    client = MagicMock()
    client.raw.databases.create.side_effect = CogniteAPIError("Nope", code=code)

    with pytest.raises(CogniteAPIError) as failed:
        em_pipeline.create_table(client, "db", "tbl")

    assert failed.value.code == code
    client.raw.tables.create.assert_not_called()


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

    function_dir = Path(__file__).parent
    for module in ("em_submit.py", "em_collect.py"):
        source = (function_dir / module).read_text(encoding="utf-8")
        assert "trigger_log_level=LOG_LEVEL_INFO" in source
        assert "trigger_log_level=logging." not in source


def test_robust_api_call_retries_transient_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _api_client_without_backoff(monkeypatch)
    operation = MagicMock(
        side_effect=[CogniteAPIError("Unavailable", code=503), "ok"],
    )

    assert client.robust_api_call(operation) == "ok"
    assert operation.call_count == 2


def test_handler_raises_so_cdf_marks_the_call_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A handler that returns normally is a succeeded function call in the CDF UI."""
    import handler
    from stages import stage_runtime

    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("submit exploded")

    monkeypatch.setattr(stage_runtime, "load_config_parameters", fail)

    with pytest.raises(RuntimeError, match="submit exploded"):
        handler.handle({"stage": "submit", "ExtractionPipelineExtId": "ep", "logLevel": "INFO"}, MagicMock())


def test_handler_rejects_missing_or_invalid_stage() -> None:
    import handler

    with pytest.raises(ValueError, match="Invalid or missing 'stage'"):
        handler.handle({"ExtractionPipelineExtId": "ep", "logLevel": "INFO"}, MagicMock())

    with pytest.raises(ValueError, match="Invalid or missing 'stage'"):
        handler.handle({"stage": "promote", "ExtractionPipelineExtId": "ep"}, MagicMock())


def _run_message(client: MagicMock) -> str:
    run = client.extraction_pipelines.runs.create.call_args.args[0]
    return run.message


def test_pipeline_run_reports_the_input_count_it_is_given() -> None:
    """Collect knows how many entities it looked at; matched plus low score is not that number."""
    client = MagicMock()

    em_pipeline.update_pipeline_run(
        client,
        MagicMock(),
        "ep-entity-matching",
        STATUS_SUCCESS,
        350,
        0,
        "Collected 1 predict job(s)",
        input_count=1200,
    )

    assert "Entity matching of: 1200 input entities, Matched: 350" in _run_message(client)


def test_a_run_that_has_not_scored_anything_reports_no_score_counts() -> None:
    """Submit only matches manual and rule mappings; the model scores in collect."""
    client = MagicMock()

    em_pipeline.update_pipeline_run(
        client,
        MagicMock(),
        "ep-entity-matching",
        STATUS_SUCCESS,
        0,
        None,
        "Predict submitted (jobId=8114603307314696), collect pending",
        input_count=738,
    )

    message = _run_message(client)
    assert "738 input entities, 0 matched by manual or rule mapping" in message
    assert "low score" not in message


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

    message = _run_message(client)
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

    message = _run_message(client)
    assert "failed, Message: predict exploded" in message
    assert "traceback" in message
    assert "RuntimeError: predict exploded" in message
