"""Behavior tests for the unified file annotation function."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest

_FUNCTION_DIR = Path(__file__).parent
sys.path.append(str(_FUNCTION_DIR))


def _load_file_annotation_handler() -> ModuleType:
    """Load this function's handler.py under a unique module name.

    Several CDF functions ship a file named handler.py. Importing that as
    ``handler`` makes pytest and CodeQL bind the wrong ``handle``.
    """
    name = "fn_file_annotation_handler"
    loaded = sys.modules.get(name)
    if loaded is not None:
        return loaded
    spec = importlib.util.spec_from_file_location(name, _FUNCTION_DIR / "handler.py")
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {_FUNCTION_DIR / 'handler.py'}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


file_annotation_handler = _load_file_annotation_handler()


def test_dispatch_rejects_unknown_stage() -> None:
    with pytest.raises(ValueError, match="stage"):
        file_annotation_handler.handle({"stage": "unknown"}, {}, MagicMock())


@pytest.mark.parametrize("stage", ["prepare", "launch", "finalize", "promote"])
def test_dispatch_routes_each_stage(monkeypatch: pytest.MonkeyPatch, stage: str) -> None:
    expected = {"status": "success"}
    stage_handler = MagicMock(return_value=expected)
    monkeypatch.setattr(file_annotation_handler, "report_usage", MagicMock())
    monkeypatch.setitem(file_annotation_handler.STAGE_HANDLERS, stage, stage_handler)
    data = {"stage": stage}
    client = MagicMock()

    assert file_annotation_handler.handle(data, {"call_id": 1}, client) == expected
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
                        "normalizePattern": r"^([A-Z]{2})-(.+)$",
                        "normalizeSelection": "all",
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
    assert config.parameters.pattern_promote.text_normalization.normalize_patterns == [r"^([A-Z]{2})-(.+)$"]
    assert config.parameters.pattern_promote.text_normalization.normalize_selection == "all"


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


def test_normalization_extracts_capture_groups_then_applies_hygiene() -> None:
    from normalization import extract_forms, normalize_text, text_variations

    patterns = [r"^([A-Z]{2})-(.+)$", r"^AT-(.+)$"]
    assert extract_forms("AT-V-009_1", patterns, "all") == ["AT_V-009_1", "V-009_1"]
    assert extract_forms("AT-V-009_1", patterns, "longest") == ["AT_V-009_1"]
    assert normalize_text("AT-V-009_1", [r"^([A-Z]{2})-(.+)$"], "all", convert_to_lowercase=False) == "ATV91"
    assert set(text_variations("V-0912", [], "all", convert_to_lowercase=False)) == {
        "V-0912",
        "V0912",
        "V-912",
        "V912",
    }
    assert "23_KA_9101" in text_variations(
        "VAL_23-KA-9101",
        [r"([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})"],
        "all",
        convert_to_lowercase=False,
    )


def test_promote_cleanup_policy_is_fixed() -> None:
    from fa_constants import DELETE_REJECTED_EDGES, DELETE_SUGGESTED_EDGES

    assert DELETE_REJECTED_EDGES is True
    assert DELETE_SUGGESTED_EDGES is False


def test_deployed_rate_limit_policy_stops_the_stage() -> None:
    import services.LaunchService as launch_service

    assert launch_service.DeployedRateLimitPolicy().handle(MagicMock()) == "Done"


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


def test_config_validator_lets_pydantic_report_malformed_nested_dicts() -> None:
    from pydantic import ValidationError
    from services.ConfigService import Config

    with pytest.raises(ValidationError):
        Config.model_validate(
            {
                "parameters": {"rawDb": "db_file_annotation"},
                "data": {
                    "targetEntitiesView": {
                        "schemaSpace": "cdf_cdm",
                        "externalId": "CogniteAsset",
                        "version": "v1",
                    },
                    "annotationStateView": {
                        "schemaSpace": "sp_hdm",
                        "externalId": "FileAnnotationState",
                        "version": "v1",
                    },
                    "sinkNode": {"space": "patterns", "externalId": "pattern_sink"},
                },
            }
        )


def test_config_validator_lets_pydantic_report_missing_raw_db() -> None:
    from pydantic import ValidationError
    from services.ConfigService import Config

    with pytest.raises(ValidationError):
        Config.model_validate(
            {
                "parameters": {},
                "data": {
                    "fileView": {
                        "schemaSpace": "cdf_cdm",
                        "externalId": "CogniteFile",
                        "version": "v1",
                    },
                    "targetEntitiesView": {
                        "schemaSpace": "cdf_cdm",
                        "externalId": "CogniteAsset",
                        "version": "v1",
                    },
                    "annotationStateView": {
                        "schemaSpace": "sp_hdm",
                        "externalId": "FileAnnotationState",
                        "version": "v1",
                    },
                    "sinkNode": {"space": "patterns", "externalId": "pattern_sink"},
                },
            }
        )


def test_config_validator_lets_pydantic_report_missing_sink_node() -> None:
    from pydantic import ValidationError
    from services.ConfigService import Config

    with pytest.raises(ValidationError):
        Config.model_validate(
            {
                "parameters": {"rawDb": "db_file_annotation"},
                "data": {
                    "fileView": {
                        "schemaSpace": "cdf_cdm",
                        "externalId": "CogniteFile",
                        "version": "v1",
                    },
                    "targetEntitiesView": {
                        "schemaSpace": "cdf_cdm",
                        "externalId": "CogniteAsset",
                        "version": "v1",
                    },
                    "annotationStateView": {
                        "schemaSpace": "sp_hdm",
                        "externalId": "FileAnnotationState",
                        "version": "v1",
                    },
                },
            }
        )


def test_file_entity_resource_type_falls_back_when_property_is_missing() -> None:
    from services.ConfigService import Config
    from services.EntityCacheService import GeneralCacheService

    config = Config.model_validate(
        {
            "parameters": {"rawDb": "db_file_annotation"},
            "data": {
                "fileView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": "files",
                    "externalId": "CogniteFile",
                    "version": "v1",
                    "resourceProperty": "type",
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
    cache = GeneralCacheService(config, MagicMock(), MagicMock())
    file_node = MagicMock()
    file_node.external_id = "doc-1"
    file_node.space = "files"
    file_node.properties.get.return_value = {"name": "P&ID-1"}

    _, file_entities = cache._convert_instances_to_entities([], [file_node])

    assert file_entities[0]["resource_type"] == "CogniteFile"


def test_file_entity_conversion_uses_empty_properties_when_view_is_missing() -> None:
    from services.ConfigService import Config
    from services.EntityCacheService import GeneralCacheService

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
    cache = GeneralCacheService(config, MagicMock(), MagicMock())
    file_node = MagicMock()
    file_node.external_id = "doc-1"
    file_node.space = "files"
    file_node.properties.get.return_value = None

    _, file_entities = cache._convert_instances_to_entities([], [file_node])

    assert file_entities[0]["resource_type"] == "CogniteFile"
    assert file_entities[0]["name"] is None


def test_asset_entity_conversion_uses_empty_properties_when_view_is_missing() -> None:
    from services.ConfigService import Config
    from services.EntityCacheService import GeneralCacheService

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
                    "resourceProperty": "type",
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
    cache = GeneralCacheService(config, MagicMock(), MagicMock())
    asset_node = MagicMock()
    asset_node.external_id = "asset-1"
    asset_node.space = "assets"
    asset_node.properties.get.return_value = None

    target_entities, _ = cache._convert_instances_to_entities([asset_node], [])

    assert target_entities[0]["resource_type"] == "CogniteAsset"
    assert target_entities[0]["name"] is None


def test_launch_service_handles_file_node_with_none_properties() -> None:
    import services.LaunchService as launch_service
    from services.ConfigService import Config

    config = Config.model_validate(
        {
            "parameters": {"rawDb": "db_file_annotation", "primaryScopeProperty": "site"},
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
    launch_svc = launch_service.GeneralLaunchService(
        client=MagicMock(),
        config=config,
        logger=MagicMock(),
        tracker=MagicMock(),
        data_model_service=MagicMock(),
        cache_service=MagicMock(),
        annotation_service=MagicMock(),
        function_call_info={},
        rate_limit_policy=MagicMock(),
    )
    file_node = MagicMock()
    file_node.properties = None

    batches = launch_svc._organize_files_for_processing([file_node])

    assert len(batches) == 1
    assert batches[0].primary_scope_value is None
    assert batches[0].files == [file_node]


def test_batch_of_paired_nodes_create_file_reference_handles_none_properties() -> None:
    from cognite.client.data_classes.data_modeling import NodeId, ViewId
    from utils.DataStructures import BatchOfPairedNodes

    state_view_id = ViewId("sp_hdm", "FileAnnotationState", "v1")
    file_node_id = NodeId("files", "file-1")
    state_node = MagicMock()
    state_node.properties = None

    paired = BatchOfPairedNodes(file_to_state_map={file_node_id: state_node})
    ref = paired.create_file_reference(file_node_id, page_range=50, annotation_state_view_id=state_view_id)

    assert ref.first_page == 1
    assert ref.last_page == 50
