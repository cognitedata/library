"""Unit tests for structural vs legacy auto pattern sample generation."""

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.append(str(Path(__file__).parent))

from services.EntityCacheService import (
    GeneralCacheService,
    count_pattern_sample_strings,
    entities_missing_search_property,
    split_entities_by_kind,
)
from services.LoggerService import CogniteFunctionLogger


def _cache_service(*, structural_auto_patterns: bool) -> GeneralCacheService:
    service = GeneralCacheService.__new__(GeneralCacheService)
    service.logger = CogniteFunctionLogger("ERROR")
    service.config = SimpleNamespace(launch_function=SimpleNamespace(structural_auto_patterns=structural_auto_patterns))
    return service


def _entity(external_id: str, aliases: list[str], *, annotation_type: str = "diagrams.AssetLink") -> dict:
    return {
        "external_id": external_id,
        "name": external_id,
        "space": "inst_location",
        "annotation_type": annotation_type,
        "resource_type": "CogniteAsset",
        "search_property": aliases,
    }


def test_structural_patterns_use_letter_wildcards_not_code_enums() -> None:
    service = _cache_service(structural_auto_patterns=True)
    entities = [
        _entity("23-XX-9106", ["23_XX_9106"]),
        _entity("23-KA-9101-A", ["23_KA_9101_A"]),
        _entity("23-FE-92537", ["23_FE_92537"]),
    ]

    result = service._generate_tag_samples_from_entities(entities)

    assert len(result) == 1
    samples = result[0]["sample"]
    assert "00-AA-0000" in samples
    assert "00-AA-00000" in samples
    assert "00-AA-0000-A" in samples
    assert not any("|" in s for s in samples), samples
    assert not any("[_]" in s for s in samples), samples


def test_structural_patterns_collapse_underscore_and_hyphen_aliases() -> None:
    service = _cache_service(structural_auto_patterns=True)
    entities = [
        _entity("a", ["23_XX_9106"]),
        _entity("b", ["23-XX-9106"]),
        _entity("c", ["23.XX.9106"]),
    ]

    result = service._generate_tag_samples_from_entities(entities)
    samples = result[0]["sample"]

    assert samples == ["00-AA-0000"]


def test_separators_never_become_required_constants_even_in_legacy_mode() -> None:
    service = _cache_service(structural_auto_patterns=False)
    entities = [
        _entity("23-XX-9106", ["23_XX_9106"]),
        _entity("23-KA-9101-A", ["23_KA_9101_A"]),
    ]

    result = service._generate_tag_samples_from_entities(entities)
    samples = result[0]["sample"]

    assert not any("[_]" in s for s in samples), samples
    assert any("-" in s for s in samples), samples
    # Legacy still expands letter codes into required constants
    assert any("XX" in s or "KA" in s for s in samples), samples


def test_config_wires_structural_auto_patterns_from_parameters() -> None:
    from services.ConfigService import Config

    config = Config.model_validate(
        {
            "parameters": {
                "rawDb": "db_file_annotation",
                "patternMode": True,
                "structuralAutoPatterns": True,
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
                "sinkNode": {"space": "sp_pattern", "externalId": "sink"},
            },
        }
    )
    assert config.parameters.structural_auto_patterns is True
    assert config.launch_function.structural_auto_patterns is True


def test_count_pattern_sample_strings_sums_all_groups() -> None:
    groups = [
        {"sample": ["00-AA-0000", "00-AA-00000"]},
        {"sample": ["AA-AA-A-0000"]},
        {"sample": []},
        {},
    ]
    assert count_pattern_sample_strings(groups) == 3


def test_split_entities_by_kind_and_missing_aliases() -> None:
    entities = [
        _entity("a1", ["23_PT_1"]),
        _entity("f1", ["DOC-1"], annotation_type="diagrams.FileLink"),
        _entity("a2", []),
    ]
    assets, files = split_entities_by_kind(entities)
    assert len(assets) == 2
    assert len(files) == 1
    missing = entities_missing_search_property(entities)
    assert [m["external_id"] for m in missing] == ["a2"]


def test_launch_input_summary_logs_info_counts(capsys) -> None:
    service = _cache_service(structural_auto_patterns=True)
    service.logger = CogniteFunctionLogger("INFO")
    service.config = SimpleNamespace(launch_function=SimpleNamespace(structural_auto_patterns=True, pattern_mode=True))
    service._log_launch_input_summary(
        scope_key="",
        source="CDF",
        asset_entities=[_entity("a1", ["23_PT_1"]), _entity("a2", [])],
        file_entities=[_entity("f1", ["DOC"], annotation_type="diagrams.FileLink")],
        pattern_samples=[{"sample": ["00-AA-0000", "00-AA-00000"], "resource_type": "CogniteAsset"}],
        manual_pattern_groups=0,
    )
    out = capsys.readouterr().out
    assert "Target entities (assets): 2" in out
    assert "File entities: 1" in out
    assert "Pattern sample strings: 2" in out
    assert "[DEBUG]" not in out


def test_launch_input_summary_logs_debug_aliases(capsys) -> None:
    service = _cache_service(structural_auto_patterns=True)
    service.logger = CogniteFunctionLogger("DEBUG")
    service.config = SimpleNamespace(launch_function=SimpleNamespace(structural_auto_patterns=True, pattern_mode=True))
    service._log_launch_input_summary(
        scope_key="",
        source="CACHE",
        asset_entities=[_entity("23-PT-1", ["23_PT_1"])],
        file_entities=[],
        pattern_samples=[{"sample": ["00-AA-00000"], "resource_type": "CogniteAsset", "annotation_type": "x"}],
    )
    out = capsys.readouterr().out
    assert "[DEBUG]" in out
    assert "23_PT_1" in out
    assert "00-AA-00000" in out
