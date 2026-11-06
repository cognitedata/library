"""A caught stage error fails the CDF call after the extraction pipeline run is recorded."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.exceptions import CogniteAPIError
from pydantic import TypeAdapter, ValidationError

sys.path.append(str(Path(__file__).parent))


def _patch_common(monkeypatch: pytest.MonkeyPatch, module: object, format_name: str) -> MagicMock:
    pipeline = MagicMock()
    monkeypatch.setattr(module, "create_config_service", lambda **kwargs: (MagicMock(), MagicMock()))
    monkeypatch.setattr(module, "create_logger_service", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(module, format_name, lambda *args, **kwargs: "")
    if hasattr(module, "create_general_pipeline_service"):
        monkeypatch.setattr(module, "create_general_pipeline_service", lambda *args, **kwargs: pipeline)
    return pipeline


def _run(handle: object, error: BaseException) -> None:
    data = {"ExtractionPipelineExtId": "ep_file_annotation", "logLevel": "INFO"}
    assert callable(handle)
    message = "cdf down" if isinstance(error, CogniteAPIError) else "bad config"
    with pytest.raises(type(error), match=message):
        handle(data, {"function_id": 1, "call_id": 2}, MagicMock())


@pytest.mark.parametrize("error", [CogniteAPIError("cdf down", code=503), ValueError("bad config")])
def test_prepare_records_pipeline_failure_and_reraises(monkeypatch: pytest.MonkeyPatch, error: BaseException) -> None:
    import stages.prepare as prepare_stage

    pipeline = _patch_common(monkeypatch, prepare_stage, "format_prepare_config")
    service = MagicMock()
    service.run.side_effect = error
    monkeypatch.setattr(prepare_stage, "_create_prepare_service", lambda *args, **kwargs: service)

    _run(prepare_stage.handle, error)

    pipeline.upload_extraction_pipeline.assert_called_once_with(status="failure")


def test_launch_records_pipeline_failure_and_reraises(monkeypatch: pytest.MonkeyPatch) -> None:
    import stages.launch as launch_stage

    error = CogniteAPIError("cdf down", code=503)
    pipeline = _patch_common(monkeypatch, launch_stage, "format_launch_config")
    service = MagicMock()
    service.run.side_effect = error
    monkeypatch.setattr(launch_stage, "_create_launch_service", lambda *args, **kwargs: service)

    _run(launch_stage.handle, error)

    pipeline.upload_extraction_pipeline.assert_called_once_with(status="failure")


def test_finalize_records_pipeline_failure_and_reraises(monkeypatch: pytest.MonkeyPatch) -> None:
    import stages.finalize as finalize_stage

    error = ValueError("bad config")
    pipeline = _patch_common(monkeypatch, finalize_stage, "format_finalize_config")
    monkeypatch.setattr(finalize_stage.time, "sleep", lambda seconds: None)
    service = MagicMock()
    service.run.side_effect = error
    monkeypatch.setattr(finalize_stage, "_create_finalize_service", lambda *args, **kwargs: service)

    _run(finalize_stage.handle, error)

    pipeline.upload_extraction_pipeline.assert_called_once_with(status="failure")


def test_promote_reraises_a_stage_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import stages.promote as promote_stage

    error = ValueError("bad config")
    pipeline = _patch_common(monkeypatch, promote_stage, "format_promote_config")
    monkeypatch.setattr(promote_stage, "create_entity_search_service", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(promote_stage, "create_promote_cache_service", lambda *args, **kwargs: MagicMock())
    service = MagicMock()
    service.run.side_effect = error
    monkeypatch.setattr(promote_stage, "GeneralPromoteService", lambda *args, **kwargs: service)

    _run(promote_stage.handle, error)

    pipeline.upload_extraction_pipeline.assert_called_once_with(status="failure")


def test_promote_defaults_to_info_and_records_a_successful_pipeline_run(monkeypatch: pytest.MonkeyPatch) -> None:
    import stages.promote as promote_stage

    pipeline = MagicMock()
    seen: dict[str, str] = {}

    def create_logger(level: str, path: str | None = None) -> MagicMock:
        seen["level"] = level
        return MagicMock()

    monkeypatch.setattr(promote_stage, "create_config_service", lambda **kwargs: (MagicMock(), MagicMock()))
    monkeypatch.setattr(promote_stage, "create_logger_service", create_logger)
    monkeypatch.setattr(promote_stage, "format_promote_config", lambda *args, **kwargs: "")
    monkeypatch.setattr(promote_stage, "create_general_pipeline_service", lambda *args, **kwargs: pipeline)
    monkeypatch.setattr(promote_stage, "create_entity_search_service", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(promote_stage, "create_promote_cache_service", lambda *args, **kwargs: MagicMock())
    service = MagicMock()
    service.run.return_value = "Done"
    monkeypatch.setattr(promote_stage, "GeneralPromoteService", lambda *args, **kwargs: service)

    result = promote_stage.handle({"ExtractionPipelineExtId": "ep_file_annotation"}, {}, MagicMock())

    assert seen["level"] == "INFO"
    assert result["status"] == "success"
    pipeline.upload_extraction_pipeline.assert_called_once_with(status="success")


def _validation_error() -> ValidationError:
    try:
        TypeAdapter(int).validate_python("not-an-int")
    except ValidationError as error:
        return error
    raise AssertionError("expected a ValidationError")


@pytest.mark.parametrize("stage_name", ["prepare", "launch", "finalize", "promote"])
def test_config_validation_error_records_pipeline_failure_and_reraises(
    monkeypatch: pytest.MonkeyPatch, stage_name: str
) -> None:
    import importlib

    stage = importlib.import_module(f"stages.{stage_name}")
    error = _validation_error()
    pipeline = MagicMock()
    logger = MagicMock()

    def fail_config(**kwargs: object) -> None:
        raise error

    monkeypatch.setattr(stage, "create_config_service", fail_config)
    monkeypatch.setattr(stage, "create_logger_service", lambda *args, **kwargs: logger)
    monkeypatch.setattr(stage, "create_general_pipeline_service", lambda *args, **kwargs: pipeline)
    if stage_name == "finalize":
        monkeypatch.setattr(stage.time, "sleep", lambda seconds: None)

    data = {"ExtractionPipelineExtId": "ep_file_annotation", "logLevel": "INFO"}
    with pytest.raises(ValidationError):
        stage.handle(data, {"function_id": 1, "call_id": 2}, MagicMock())

    logger.error.assert_called_once()
    pipeline.upload_extraction_pipeline.assert_called_once_with(status="failure")


@pytest.mark.parametrize("stage_name", ["prepare", "launch", "finalize", "promote"])
def test_unexpected_error_records_pipeline_failure(monkeypatch: pytest.MonkeyPatch, stage_name: str) -> None:
    import importlib

    stage = importlib.import_module(f"stages.{stage_name}")
    pipeline = MagicMock()

    def fail_config(**kwargs: object) -> None:
        raise KeyError("missing")

    monkeypatch.setattr(stage, "create_config_service", fail_config)
    monkeypatch.setattr(stage, "create_logger_service", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(stage, "create_general_pipeline_service", lambda *args, **kwargs: pipeline)
    if stage_name == "finalize":
        monkeypatch.setattr(stage.time, "sleep", lambda seconds: None)

    with pytest.raises(KeyError):
        stage.handle({"ExtractionPipelineExtId": "ep_file_annotation"}, {}, MagicMock())

    pipeline.upload_extraction_pipeline.assert_called_once_with(status="failure")


@pytest.mark.parametrize("stage_name", ["prepare", "launch", "finalize", "promote"])
def test_missing_pipeline_id_is_rejected(stage_name: str) -> None:
    import importlib

    stage = importlib.import_module(f"stages.{stage_name}")

    with pytest.raises(ValidationError):
        stage.handle({"logLevel": "INFO"}, {}, MagicMock())


@pytest.mark.parametrize(
    "data",
    [
        {"ExtractionPipelineExtId": "ep_file_annotation"},
        {"stage": "unknown", "ExtractionPipelineExtId": "ep_file_annotation"},
        {"stage": "prepare", "ExtractionPipelineExtId": "ep_file_annotation", "logLevel": "VERBOSE"},
    ],
)
def test_handler_rejects_invalid_input(data: dict[str, str]) -> None:
    from handler import handle

    with pytest.raises(ValidationError):
        handle(data, {}, MagicMock())


def test_handler_imports_without_python_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    """CDF Functions does not install python-dotenv. The handler must still import."""
    import importlib
    import importlib.util
    import sys

    monkeypatch.setitem(sys.modules, "dotenv", None)
    monkeypatch.delitem(sys.modules, "dependencies", raising=False)
    importlib.import_module("dependencies")

    name = "fn_file_annotation_handler_without_dotenv"
    handler_path = Path(__file__).resolve().parents[2] / "fn_file_annotation" / "handler.py"
    spec = importlib.util.spec_from_file_location(name, handler_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
