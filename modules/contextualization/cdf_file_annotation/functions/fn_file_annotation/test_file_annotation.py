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


@pytest.mark.parametrize("stage", ["prepare", "launch", "finalize", "promote"])
def test_warning_run_reports_peak_memory_of_the_stage(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], stage: str
) -> None:
    def allocate_5_mib(data: dict, function_call_info: dict, client: object) -> dict:
        _ = bytearray(5 * 1024 * 1024)
        return {"status": "success"}

    monkeypatch.setattr(file_annotation_handler, "report_usage", MagicMock())
    monkeypatch.setitem(file_annotation_handler.STAGE_HANDLERS, stage, allocate_5_mib)

    file_annotation_handler.handle({"stage": stage, "logLevel": "WARNING"}, {}, MagicMock())

    (report,) = [line for line in capsys.readouterr().out.splitlines() if "Peak memory" in line]
    assert "[WARNING]" in report
    assert f"'{stage}'" in report
    assert float(report.split("Peak memory")[1].split(":")[1].split("MiB")[0]) >= 5


@pytest.mark.parametrize("log_level", ["DEBUG", "INFO", "ERROR"])
def test_only_warning_runs_trace_memory(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], log_level: str
) -> None:
    import tracemalloc

    tracing_during_stage: list[bool] = []

    def stage_handler(data: dict, function_call_info: dict, client: object) -> dict:
        tracing_during_stage.append(tracemalloc.is_tracing())
        return {"status": "success"}

    monkeypatch.setattr(file_annotation_handler, "report_usage", MagicMock())
    monkeypatch.setitem(file_annotation_handler.STAGE_HANDLERS, "launch", stage_handler)

    file_annotation_handler.handle({"stage": "launch", "logLevel": log_level}, {}, MagicMock())

    assert tracing_during_stage == [False]
    assert "Peak memory" not in capsys.readouterr().out


def test_config_uses_parameters_and_data_shape() -> None:
    from services.ConfigService import Config

    config = Config.model_validate(
        {
            "parameters": {
                "patternMode": True,
                "cleanOldAnnotations": True,
                "assetAutoApprovalThreshold": 1.0,
                "assetAutoSuggestThreshold": 1.0,
                "rawDb": "db_file_annotation",
                "patternPromote": {
                    "textNormalization": {
                        "entityNormalizationPatterns": r"^([A-Z]{2})-(.+)$",
                        "fileNormalizationPatterns": r"^DOC-(.+)$",
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
    assert config.parameters.pattern_promote.text_normalization.entity_normalization_patterns == [r"^([A-Z]{2})-(.+)$"]
    assert config.parameters.pattern_promote.text_normalization.file_normalization_patterns == [r"^DOC-(.+)$"]


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


def test_config_uses_default_tag_filters() -> None:
    from fa_constants import EXCLUDED_PREPARE_TAGS, TAG_DETECT_IN_DIAGRAMS, TAG_TO_ANNOTATE
    from services.ConfigService import Config

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

    prepare_query = config.prepare_function.get_files_to_annotate_query
    assert not isinstance(prepare_query, list)
    prepare_filters = prepare_query.filters
    assert prepare_filters[0].values == [TAG_TO_ANNOTATE]
    assert prepare_filters[1].values == EXCLUDED_PREPARE_TAGS
    assert prepare_filters[1].negate is True
    file_entities_query = config.launch_function.data_model_service.get_file_entities_query
    target_entities_query = config.launch_function.data_model_service.get_target_entities_query
    assert not isinstance(file_entities_query, list)
    assert not isinstance(target_entities_query, list)
    assert file_entities_query.filters[0].values == [TAG_DETECT_IN_DIAGRAMS]
    assert target_entities_query.filters[0].values == [TAG_DETECT_IN_DIAGRAMS]


def test_config_uses_custom_tag_filters_and_include_overrides_exclude() -> None:
    from services.ConfigService import Config

    config = Config.model_validate(
        {
            "parameters": {
                "rawDb": "db_file_annotation",
                "filesToAnnotateTags": ["ToAnnotate", "Annotated"],
                "fileEntitiesTags": ["DetectInDiagrams", "ToAnnotate"],
                "targetEntitiesTags": ["DetectInDiagrams"],
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

    prepare_query = config.prepare_function.get_files_to_annotate_query
    assert not isinstance(prepare_query, list)
    prepare_filters = prepare_query.filters
    assert prepare_filters[0].values == ["ToAnnotate", "Annotated"]
    assert prepare_filters[1].values == ["AnnotationInProcess", "AnnotationFailed"]
    file_entities_query = config.launch_function.data_model_service.get_file_entities_query
    target_entities_query = config.launch_function.data_model_service.get_target_entities_query
    assert not isinstance(file_entities_query, list)
    assert not isinstance(target_entities_query, list)
    assert file_entities_query.filters[0].values == ["DetectInDiagrams", "ToAnnotate"]
    assert target_entities_query.filters[0].values == ["DetectInDiagrams"]


def test_normalization_extracts_capture_groups_then_applies_hygiene() -> None:
    from normalization import extract_forms, normalize_text, text_variations

    patterns = [r"^([A-Z]{2})-(.+)$", r"^AT-(.+)$"]
    assert extract_forms("AT-V-009_1", patterns) == ["AT_V-009_1"]
    assert extract_forms("REPEATED", patterns) == []
    assert normalize_text("AT-V-009_1", [r"^([A-Z]{2})-(.+)$"]) == "ATV91"
    # No pattern match → empty variations (do not search)
    assert text_variations("REPEATED", [r"^([A-Z]{2})-(.+)$"]) == []
    # Hygiene still applies after a successful extraction
    assert "23_KA_9101" in text_variations(
        "VAL_23-KA-9101",
        [r"([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})"],
    )
    # Matching text also keeps original + hygiene forms for alias lookup
    matched = set(text_variations("V-0912", [r"^([A-Z])-(.+)$"]))
    assert "V-0912" in matched
    assert "V_0912" in matched or "V0912" in matched


def test_normalization_strips_leading_zeros_from_every_numeric_segment() -> None:
    """Removing separators first merged "0151" and "001", so the zeros of "001" were kept."""
    from normalization import normalize_text

    assert normalize_text("PH_ME_P_0151_001", []) == "PHMEP1511"
    assert normalize_text("PH-ME-P-151-1", []) == normalize_text("PH_ME_P_0151_001", [])


def test_normalization_ignores_text_that_is_not_a_string() -> None:
    """Detect results can carry a null text; that must not crash promote or pattern sampling."""
    from normalization import extract_forms, text_variations

    assert extract_forms(None, [r"^([A-Z]{2})-(.+)$"]) == []  # type: ignore[arg-type]
    assert text_variations(None, [r"^([A-Z]{2})-(.+)$"]) == []  # type: ignore[arg-type]


@pytest.mark.parametrize("factory", ["create_logger_service", "create_write_logger_service"])
def test_log_level_is_case_insensitive(factory: str, tmp_path: Path) -> None:
    import dependencies

    logger = getattr(dependencies, factory)("debug", str(tmp_path / "run.log"))

    assert logger.log_level == "DEBUG"


def test_launch_releases_claimed_files_on_an_unexpected_error() -> None:
    """Files tagged AnnotationInProcess would otherwise stay locked out of Prepare forever."""
    import services.LaunchService as launch_service

    service = launch_service.GeneralLaunchService(
        MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), {}, MagicMock()
    )
    file_nodes = [MagicMock()]
    service.data_model_service.get_files_to_process.return_value = (file_nodes, {"file": "state"})
    service._organize_files_for_processing = MagicMock(return_value=[MagicMock()])
    service._ensure_cache_for_batch = MagicMock(side_effect=KeyError("primary"))
    service._release_unlaunched_files = MagicMock()

    with pytest.raises(KeyError):
        service.run()

    service._release_unlaunched_files.assert_called_once_with(file_nodes, set())


def test_hyphenated_drawing_text_searches_for_the_underscore_alias() -> None:
    """aliases_update stores a tag with "_" between tokens; drawings print it with "-"."""
    from normalization import text_variations

    file_patterns = [r"(?<![A-Z])([A-Z]{2,4}[-_][A-Z0-9]+[-_][A-Z][-_][0-9]+[-_][0-9]+)"]
    entity_patterns = [r"([0-9]{2}[-_.:][A-Z]{2,4}[-_.:][0-9]{4,5})"]

    assert "PH_ME_P_0151_001" in text_variations("PH-ME-P-0151-001", file_patterns)
    assert "23_DB_9101" in text_variations("23-DB-9101", entity_patterns)


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


def test_a_cleared_alias_property_falls_back_to_the_name() -> None:
    """Aliases can be cleared on either view; the name is what is left to match on."""
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
    file_node.properties.get.return_value = {"name": "P&ID-1", "aliases": None}

    asset_node = MagicMock()
    asset_node.external_id = "asset-1"
    asset_node.space = "assets"
    asset_node.properties.get.return_value = {"name": "23-DB-9101", "aliases": []}

    target_entities, file_entities = cache._convert_instances_to_entities([asset_node], [file_node])

    assert file_entities[0]["search_property"] == ["P&ID-1"]
    assert target_entities[0]["search_property"] == ["23-DB-9101"]


def test_an_instance_without_aliases_or_a_name_has_nothing_to_search_on() -> None:
    """With neither, the entity is left out rather than sent as a null search field."""
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
    file_node.properties.get.return_value = {"name": None, "aliases": None}

    asset_node = MagicMock()
    asset_node.external_id = "asset-1"
    asset_node.space = "assets"
    asset_node.properties.get.return_value = {"name": "   ", "aliases": None}

    target_entities, file_entities = cache._convert_instances_to_entities([asset_node], [file_node])

    assert file_entities[0]["search_property"] == []
    assert target_entities[0]["search_property"] == []


def test_an_asset_without_aliases_still_matches_on_its_name() -> None:
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
    asset_node = MagicMock()
    asset_node.external_id = "asset-1"
    asset_node.space = "assets"
    asset_node.properties.get.return_value = {"name": "23-DB-9101"}

    target_entities, _ = cache._convert_instances_to_entities([asset_node], [])

    assert target_entities[0]["search_property"] == ["23-DB-9101"]


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


def test_launch_omits_scope_logs_when_unscoped() -> None:
    import services.LaunchService as launch_service
    from cognite.client.data_classes.data_modeling import NodeId
    from services.ConfigService import Config

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
    logger = MagicMock()
    file_id = NodeId("files", "file-1")
    file_node = MagicMock()
    file_node.properties = None
    file_node.as_id.return_value = file_id
    state_node = MagicMock()
    state_node.properties = None
    data_model_service = MagicMock()
    data_model_service.get_files_to_process.return_value = ([file_node], {file_id: state_node})
    launch_svc = launch_service.GeneralLaunchService(
        client=MagicMock(),
        config=config,
        logger=logger,
        tracker=MagicMock(),
        data_model_service=data_model_service,
        cache_service=MagicMock(),
        annotation_service=MagicMock(),
        function_call_info={},
        rate_limit_policy=MagicMock(),
    )
    launch_svc._process_batch = MagicMock()
    launch_svc._ensure_cache_for_batch = MagicMock()

    launch_svc.run()

    messages = [call.kwargs["message"] for call in logger.info.call_args_list if "message" in call.kwargs]
    assert not any(
        message.startswith("Processing ") and " files in " in message and "remaining" not in message
        for message in messages
    )
    assert not any(message.startswith("Created batch of") for message in messages)
    assert not any(message.startswith("Finished processing for") for message in messages)


def _file_node_with_tags(file_id, tags: list[str]) -> MagicMock:
    """A file node carrying `tags` on the configured file view."""
    from cognite.client.data_classes.data_modeling import ViewId

    file_node = MagicMock()
    file_node.space = file_id.space
    file_node.external_id = file_id.external_id
    file_node.as_id.return_value = file_id
    file_node.properties = {ViewId("cdf_cdm", "CogniteFile", "v1"): {"tags": tags}}
    return file_node


def test_a_failed_launch_releases_the_files_it_claimed() -> None:
    """A stuck 'AnnotationInProcess' tag keeps Prepare from ever picking the file up again."""
    import services.LaunchService as launch_service
    from cognite.client.data_classes.data_modeling import NodeId
    from cognite.client.exceptions import CogniteAPIError
    from services.ConfigService import Config

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
    file_id = NodeId("files", "file-1")
    file_node = _file_node_with_tags(file_id, ["ToAnnotate", "AnnotationInProcess"])
    data_model_service = MagicMock()
    data_model_service.get_files_to_process.return_value = ([file_node], {file_id: MagicMock(properties=None)})

    launch_svc = launch_service.GeneralLaunchService(
        client=MagicMock(),
        config=config,
        logger=MagicMock(),
        tracker=MagicMock(),
        data_model_service=data_model_service,
        cache_service=MagicMock(),
        annotation_service=MagicMock(),
        function_call_info={},
        rate_limit_policy=MagicMock(),
    )
    launch_svc._ensure_cache_for_batch = MagicMock()
    launch_svc._process_batch = MagicMock(
        side_effect=CogniteAPIError("Entity[searchField=search_property] must be a string", code=400)
    )

    with pytest.raises(CogniteAPIError):
        launch_svc.run()

    released = data_model_service.update_annotation_state.call_args.args[0]
    assert [node.external_id for node in released] == ["file-1"]
    assert released[0].sources[0].properties["tags"] == ["ToAnnotate"]


def test_a_rate_limited_launch_keeps_the_files_claimed() -> None:
    """429 means try again shortly, so the claim has to survive for the next run."""
    import services.LaunchService as launch_service
    from cognite.client.data_classes.data_modeling import NodeId
    from cognite.client.exceptions import CogniteAPIError
    from services.ConfigService import Config

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
    file_id = NodeId("files", "file-1")
    file_node = _file_node_with_tags(file_id, ["ToAnnotate", "AnnotationInProcess"])
    data_model_service = MagicMock()
    data_model_service.get_files_to_process.return_value = ([file_node], {file_id: MagicMock(properties=None)})

    launch_svc = launch_service.GeneralLaunchService(
        client=MagicMock(),
        config=config,
        logger=MagicMock(),
        tracker=MagicMock(),
        data_model_service=data_model_service,
        cache_service=MagicMock(),
        annotation_service=MagicMock(),
        function_call_info={},
        rate_limit_policy=MagicMock(),
    )
    launch_svc._ensure_cache_for_batch = MagicMock()
    launch_svc._process_batch = MagicMock(side_effect=CogniteAPIError("too many jobs", code=429))

    launch_svc.run()

    data_model_service.update_annotation_state.assert_not_called()


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


def test_unique_tags_keeps_first_occurrence() -> None:
    from utils.DataStructures import unique_tags

    assert unique_tags(["ToAnnotate", "DetectInDiagrams", "Annotated", "Annotated"]) == [
        "ToAnnotate",
        "DetectInDiagrams",
        "Annotated",
    ]


def test_replace_tag_does_not_duplicate_existing() -> None:
    from utils.DataStructures import replace_tag

    tags = ["ToAnnotate", "DetectInDiagrams", "Annotated", "AnnotationInProcess"]

    assert replace_tag(tags, "AnnotationInProcess", "Annotated") == [
        "ToAnnotate",
        "DetectInDiagrams",
        "Annotated",
    ]


def test_add_unique_tags_skips_existing() -> None:
    from utils.DataStructures import add_unique_tags

    assert add_unique_tags(["PromoteAttempted"], "PromoteAttempted", "AmbiguousMatch") == [
        "PromoteAttempted",
        "AmbiguousMatch",
    ]


def test_tags_apply_deduplicates() -> None:
    from cognite.client.data_classes.data_modeling import ViewId
    from utils.DataStructures import tags_apply

    node_apply = tags_apply(
        MagicMock(), ViewId("cdf_cdm", "CogniteFile", "v1"), ["ToAnnotate", "Annotated", "Annotated"]
    )

    assert node_apply.sources[0].properties == {"tags": ["ToAnnotate", "Annotated"]}


def test_launch_overall_report_includes_stage_entities_and_patterns() -> None:
    from datetime import timedelta

    from utils.DataStructures import PerformanceTracker

    tracker = PerformanceTracker()
    tracker.add_files(success=20)
    tracker.total_runs = 1
    tracker.total_time_delta = timedelta(seconds=3)
    tracker.set_detect_input(entities=1094, patterns=16)

    report = tracker.generate_overall_report("Launch")

    assert report.startswith(" Launch run started")
    assert "- total files processed: 20" in report
    assert "- successful files: 20" in report
    assert "- entities found: 1094" in report
    assert "- patterns created: 16" in report


def test_launch_overall_report_omits_patterns_when_not_used() -> None:
    from utils.DataStructures import PerformanceTracker

    tracker = PerformanceTracker()
    tracker.set_detect_input(entities=10, patterns=None)

    report = tracker.generate_overall_report("Launch")

    assert "- entities found: 10" in report
    assert "patterns created" not in report


def _config_with_debug_file(debug_file_external_id: str | None, file_instance_space: str | None = "files"):
    from services.ConfigService import Config

    parameters: dict[str, object] = {"rawDb": "db_file_annotation"}
    if debug_file_external_id is not None:
        parameters["debugFileExternalId"] = debug_file_external_id
    return Config.model_validate(
        {
            "parameters": parameters,
            "data": {
                "fileView": {
                    "schemaSpace": "cdf_cdm",
                    "instanceSpace": file_instance_space,
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


@pytest.mark.parametrize("value", [None, "", "  "])
def test_debug_file_is_off_when_unset_or_blank(value: str | None) -> None:
    assert _config_with_debug_file(value).debug_file is None


def test_debug_file_uses_file_view_instance_space() -> None:
    from cognite.client.data_classes.data_modeling import NodeId

    assert _config_with_debug_file("PID-001").debug_file == NodeId("files", "PID-001")


def test_debug_file_requires_file_view_instance_space() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="instanceSpace"):
        _config_with_debug_file("PID-001", file_instance_space=None)


def test_prepare_in_debug_mode_only_retrieves_the_debug_file() -> None:
    from services.DataModelService import GeneralDataModelService

    client = MagicMock()
    client.data_modeling.instances.query.return_value = _query_page([], cursor=None)
    GeneralDataModelService(_config_with_debug_file("PID-001"), client, MagicMock()).get_files_to_annotate()

    query_filter = str(client.data_modeling.instances.query.call_args.args[0].with_["files"].filter.dump())
    assert "PID-001" in query_filter
    assert "ToAnnotate" not in query_filter
    assert "AnnotationInProcess" in query_filter


def test_launch_in_debug_mode_only_retrieves_the_debug_file_state() -> None:
    from services.DataModelService import GeneralDataModelService

    client = MagicMock()
    client.data_modeling.instances.query.return_value = _query_page([], cursor=None)
    GeneralDataModelService(_config_with_debug_file("PID-001"), client, MagicMock()).get_files_to_process()

    query = client.data_modeling.instances.query.call_args.args[0]
    query_filter = str({name: expression.dump() for name, expression in query.with_.items()})
    assert "linkedFile" in query_filter
    assert "PID-001" in query_filter
    assert "Finalizing" not in query_filter


def test_finalize_in_debug_mode_only_claims_jobs_for_the_debug_file() -> None:
    from services.RetrieveService import GeneralRetrieveService

    service = GeneralRetrieveService(MagicMock(), _config_with_debug_file("PID-001"), MagicMock())

    query_filter = str(service.filter_jobs.dump())
    assert "linkedFile" in query_filter
    assert "PID-001" in query_filter


def test_promote_in_debug_mode_only_retrieves_edges_from_the_debug_file() -> None:
    from services.PromoteService import GeneralPromoteService

    client = MagicMock()
    service = GeneralPromoteService(
        client, _config_with_debug_file("PID-001"), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )
    service._get_promote_candidates()

    query_filter = str(client.data_modeling.instances.list.call_args.kwargs["filter"].dump())
    assert "startNode" in query_filter
    assert "PID-001" in query_filter


def test_debug_mode_still_retrieves_all_match_entities() -> None:
    """The debug file limits what is annotated, not which assets and files it can be matched against."""
    from services.DataModelService import GeneralDataModelService

    client = MagicMock()
    client.raw.rows.retrieve.return_value = None
    client.data_modeling.instances.sync.return_value = _query_page([], cursor=None)
    GeneralDataModelService(_config_with_debug_file("PID-001"), client, MagicMock()).get_instances_entities(
        "", None, None
    )

    entity_filters = [
        str(call.args[0].with_["entities"].filter.dump()) for call in client.data_modeling.instances.sync.call_args_list
    ]
    assert len(entity_filters) == 2
    for entity_filter in entity_filters:
        assert "PID-001" not in entity_filter


def _query_page(nodes: list, cursor: str | None) -> MagicMock:
    page = MagicMock()
    page.__getitem__.return_value = nodes
    page.cursors = {"entities": cursor}
    return page


def test_match_entities_carry_only_the_properties_launch_uses() -> None:
    """Full nodes with every view property ran the function out of memory on large projects."""
    from services.DataModelService import GeneralDataModelService

    client = MagicMock()
    client.raw.rows.retrieve.return_value = None
    client.data_modeling.instances.sync.return_value = _query_page([], cursor=None)
    GeneralDataModelService(_config_with_debug_file(None), client, MagicMock()).get_instances_entities("", None, None)

    client.data_modeling.instances.list.assert_not_called()
    client.data_modeling.instances.query.assert_not_called()
    for call in client.data_modeling.instances.sync.call_args_list:
        assert set(call.args[0].select["entities"].sources[0].properties) == {"name", "tags", "aliases"}


def _file_node(tags: list[str]):
    from cognite.client.data_classes.data_modeling import Node

    return Node.load(
        {
            "instanceType": "node",
            "space": "files",
            "externalId": "doc-1",
            "version": 1,
            "lastUpdatedTime": 0,
            "createdTime": 0,
            "properties": {"cdf_cdm": {"CogniteFile/v1": {"name": "doc-1", "description": "big text", "tags": tags}}},
        }
    )


def test_prepare_writes_only_the_tags_of_a_file() -> None:
    from services.PrepareService import GeneralPrepareService

    data_model_service = MagicMock()
    data_model_service.get_files_to_annotate.return_value = [_file_node(["ToAnnotate"])]
    service = GeneralPrepareService(
        MagicMock(), _config_with_debug_file(None), MagicMock(), MagicMock(), data_model_service, {}
    )

    service.run()

    (file_apply,) = data_model_service.update_annotation_state.call_args.args[0]
    assert file_apply.sources[0].properties == {"tags": ["ToAnnotate", "AnnotationInProcess"]}


def test_finalize_writes_only_the_tags_of_an_annotated_file() -> None:
    from cognite.client.data_classes.data_modeling import Node, NodeId, NodeList
    from services.FinalizeService import GeneralFinalizeService

    state_node = Node.load(
        {
            "instanceType": "node",
            "space": "files",
            "externalId": "state-1",
            "version": 1,
            "lastUpdatedTime": 0,
            "createdTime": 0,
            "properties": {"sp_hdm": {"FileAnnotationState/v1": {"annotationStatus": "Finalizing"}}},
        }
    )
    retrieve_service = MagicMock()
    retrieve_service.get_job_id.return_value = ((1, "token"), None, {NodeId("files", "doc-1"): state_node})
    retrieve_service.get_diagram_detect_job_result.return_value = {
        "items": [{"fileInstanceId": {"space": "files", "externalId": "doc-1"}, "pageCount": 1, "annotations": []}]
    }
    client = MagicMock()
    client.data_modeling.instances.retrieve_nodes.return_value = NodeList(
        [_file_node(["ToAnnotate", "AnnotationInProcess"])]
    )
    apply_service = MagicMock()
    apply_service.process_and_apply_annotations_for_file.return_value = ("regular", "pattern")
    service = GeneralFinalizeService(
        client,
        _config_with_debug_file(None),
        MagicMock(log_level="INFO"),
        MagicMock(),
        retrieve_service,
        apply_service,
        {},
    )

    service.run()

    applies = apply_service.update_instances.call_args.kwargs["list_node_apply"]
    (file_apply,) = [apply for apply in applies if apply.external_id == "doc-1"]
    assert file_apply.sources[0].properties == {"tags": ["ToAnnotate", "Annotated"]}


def test_launch_does_not_serialize_entities_below_debug(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.LaunchService as launch_service

    dumps = MagicMock(return_value="")
    monkeypatch.setattr(launch_service.json, "dumps", dumps)
    launch_svc = launch_service.GeneralLaunchService(
        client=MagicMock(),
        config=_config_with_debug_file(None),
        logger=MagicMock(log_level="INFO"),
        tracker=MagicMock(),
        data_model_service=MagicMock(),
        cache_service=MagicMock(),
        annotation_service=MagicMock(**{"run_diagram_detect.return_value": (1, "token")}),
        function_call_info={},
        rate_limit_policy=MagicMock(),
    )
    launch_svc.in_memory_cache = [{"external_id": "asset-1", "search_property": ["P-101"]}]

    launch_svc._process_batch(MagicMock(**{"is_empty.return_value": False}))

    dumps.assert_not_called()


@pytest.mark.parametrize("status", ["Completed", "Running"])
def test_finalize_does_not_decode_job_response_text_below_debug(status: str) -> None:
    from unittest.mock import PropertyMock

    from services.RetrieveService import GeneralRetrieveService

    client = MagicMock()
    response = client.get.return_value
    response.status_code = 200
    response.json.return_value = {"status": status, "items": []}
    type(response).text = PropertyMock(side_effect=AssertionError("response.text decoded below DEBUG"))
    service = GeneralRetrieveService(client, _config_with_debug_file(None), MagicMock(log_level="INFO"))

    service.get_diagram_detect_job_result(1, "token")
