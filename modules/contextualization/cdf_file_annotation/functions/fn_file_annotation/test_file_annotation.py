"""Behavior tests for the unified file annotation function."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent))


def test_dispatch_rejects_unknown_stage() -> None:
    import handler

    with pytest.raises(ValueError, match="stage"):
        handler.handle({"stage": "unknown"}, {}, MagicMock())


@pytest.mark.parametrize("stage", ["prepare", "launch", "finalize", "promote"])
def test_dispatch_routes_each_stage(monkeypatch: pytest.MonkeyPatch, stage: str) -> None:
    import handler

    expected = {"status": "success"}
    stage_handler = MagicMock(return_value=expected)
    monkeypatch.setattr(handler, "report_usage", MagicMock())
    monkeypatch.setitem(handler.STAGE_HANDLERS, stage, stage_handler)
    data = {"stage": stage}
    client = MagicMock()

    assert handler.handle(data, {"call_id": 1}, client) == expected
    stage_handler.assert_called_once_with(data, {"call_id": 1}, client)


def test_config_uses_parameters_and_data_shape() -> None:
    from services.ConfigService import Config

    config = Config.model_validate(
        {
            "parameters": {
                "patternMode": True,
                "cleanOldAnnotations": True,
                "autoApprovalThreshold": 1.0,
                "autoSuggestThreshold": 1.0,
                "rawDb": "db_file_annotation",
                "patternPromote": {
                    "textNormalization": {
                        "convertToLowercase": False,
                        "substitutions": [{"pattern": "^[A-Z]{2}-", "replacement": ""}],
                    }
                },
            },
            "data": {
                "fileView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "files",
                    "externalId": "CogniteFile",
                    "version": "v1",
                    "searchProperty": "aliases",
                },
                "targetEntitiesView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "assets",
                    "externalId": "CogniteAsset",
                    "version": "v1",
                    "searchProperty": "aliases",
                },
                "annotationStateView": {
                    "schemaSpace": "sp_hdm",
                    "instanceSpace": "files",
                    "externalId": "FileAnnotationState",
                    "version": "v1",
                },
                "sinkNode": {"space": "patterns", "externalId": "pattern_sink"},
            },
        }
    )

    assert config.parameters.raw_db == "db_file_annotation"
    assert config.data.file_view.search_property == "aliases"
    assert config.raw_tables.raw_table_doc_tag == "annotation_documents_tags"


def test_config_uses_raw_table_names_from_parameters() -> None:
    from services.ConfigService import Config

    config = Config.model_validate(
        {
            "parameters": {
                "rawDb": "db_custom",
                "rawTableCache": "custom_cache",
                "rawTableDocTag": "custom_tags",
                "rawTableDocDoc": "custom_docs",
                "rawTableDocPattern": "custom_patterns",
                "rawTablePromoteCache": "custom_promote",
                "rawManualPatternsCatalog": "custom_manual",
            },
            "data": {
                "fileView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "files",
                    "externalId": "CogniteFile",
                    "version": "v1",
                },
                "targetEntitiesView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "assets",
                    "externalId": "CogniteAsset",
                    "version": "v1",
                },
                "annotationStateView": {
                    "schemaSpace": "sp_hdm",
                    "instanceSpace": "files",
                    "externalId": "FileAnnotationState",
                    "version": "v1",
                },
                "sinkNode": {"space": "patterns", "externalId": "pattern_sink"},
            },
        }
    )

    assert config.raw_tables.raw_db == "db_custom"
    assert config.raw_tables.raw_table_cache == "custom_cache"
    assert config.raw_tables.raw_table_doc_tag == "custom_tags"
    assert config.raw_tables.raw_table_doc_doc == "custom_docs"
    assert config.raw_tables.raw_table_doc_pattern == "custom_patterns"
    assert config.raw_tables.raw_table_promote_cache == "custom_promote"
    assert config.raw_tables.raw_manual_patterns_catalog == "custom_manual"


def test_normalization_applies_customer_then_builtin_substitutions() -> None:
    from normalization import normalize_text, text_variations

    assert normalize_text("AT-V-009_1", [(r"^[A-Z]{2}-", "")], convert_to_lowercase=False) == "V91"
    assert set(text_variations("V-0912", [], convert_to_lowercase=False)) == {
        "V-0912",
        "V0912",
        "V-912",
        "V912",
    }


def test_promote_cleanup_policy_is_fixed() -> None:
    from fa_constants import DELETE_REJECTED_EDGES, DELETE_SUGGESTED_EDGES

    assert DELETE_REJECTED_EDGES is True
    assert DELETE_SUGGESTED_EDGES is False


def test_deployed_rate_limit_policy_stops_the_stage() -> None:
    from services.LaunchService import DeployedRateLimitPolicy

    assert DeployedRateLimitPolicy().handle(MagicMock()) == "Done"


def test_local_rate_limit_policy_waits_and_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.LaunchService as launch_service

    sleep = MagicMock()
    monkeypatch.setattr(launch_service.time, "sleep", sleep)

    assert launch_service.LocalRateLimitPolicy().handle(MagicMock()) is None
    sleep.assert_called_once_with(900)


def test_logger_skips_blank_lines(capsys: pytest.CaptureFixture[str]) -> None:
    from services.LoggerService import CogniteFunctionLogger

    logger = CogniteFunctionLogger("INFO")

    logger.info("first\n\nsecond")
    logger.info("")

    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 2
    assert lines[0].endswith("first")
    assert lines[1].endswith("second")


def test_logger_prefixes_lines_with_run_number(capsys: pytest.CaptureFixture[str]) -> None:
    from services.LoggerService import CogniteFunctionLogger

    logger = CogniteFunctionLogger("INFO")

    logger.info("config")
    logger.start_run()
    logger.info("files launched")
    logger.start_run()
    logger.info("No files found to launch")

    lines = capsys.readouterr().out.splitlines()
    assert "[run" not in lines[0]
    assert "[run 1]" in lines[1]
    assert "[run 2]" in lines[2]


def test_config_log_names_extraction_pipeline_source() -> None:
    from services.ConfigService import (
        Config,
        format_finalize_config,
        format_launch_config,
        format_prepare_config,
        format_promote_config,
    )

    config = Config.model_validate(
        {
            "parameters": {"rawDb": "db_file_annotation"},
            "data": {
                "fileView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "files",
                    "externalId": "CogniteFile",
                    "version": "v1",
                },
                "targetEntitiesView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "assets",
                    "externalId": "CogniteAsset",
                    "version": "v1",
                },
                "annotationStateView": {
                    "schemaSpace": "sp_hdm",
                    "instanceSpace": "files",
                    "externalId": "FileAnnotationState",
                    "version": "v1",
                },
                "sinkNode": {"space": "patterns", "externalId": "pattern_sink"},
            },
        }
    )

    for formatter in (format_prepare_config, format_launch_config, format_finalize_config, format_promote_config):
        report = formatter(config, "ep_file_annotation")
        assert "CONFIG SOURCE: extraction pipeline 'ep_file_annotation'" in report
        assert "" not in report.split("\n")
