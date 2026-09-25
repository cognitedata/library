"""Tests for reading the fn_file_annotation extraction pipeline config into the dashboard's shape."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from data_structures import ExtractionPipelineConfig  # isort: skip

VIEWS = {
    "fileView": {"schemaSpace": "cdf_cdm", "instanceSpace": "files", "externalId": "CogniteFile", "version": "v1"},
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
}


def test_configured_raw_table_names_are_used() -> None:
    config = ExtractionPipelineConfig.from_dict(
        {
            "parameters": {
                "rawDb": "db_custom",
                "rawTableDocTag": "custom_tags",
                "rawTableDocDoc": "custom_docs",
                "rawTableDocPattern": "custom_patterns",
                "rawTableCache": "custom_cache",
                "rawManualPatternsCatalog": "custom_manual",
            },
            "data": VIEWS,
        }
    )

    assert config.raw_db == "db_custom"
    assert config.raw_table_asset_tags == "custom_tags"
    assert config.raw_table_file_tags == "custom_docs"
    assert config.raw_table_pattern_tags == "custom_patterns"
    assert config.raw_table_pattern_cache == "custom_cache"
    assert config.raw_manual_patterns_catalog == "custom_manual"


def test_raw_table_names_default_to_the_module_defaults() -> None:
    config = ExtractionPipelineConfig.from_dict({"parameters": {"rawDb": "db_file_annotation"}, "data": VIEWS})

    assert config.raw_table_asset_tags == "annotation_documents_tags"
    assert config.raw_table_file_tags == "annotation_documents_docs"
    assert config.raw_table_pattern_tags == "annotation_documents_patterns"
    assert config.raw_table_pattern_cache == "annotation_entities_cache"
    assert config.raw_manual_patterns_catalog == "manual_patterns_catalog"


def test_views_are_read_from_data() -> None:
    config = ExtractionPipelineConfig.from_dict({"parameters": {"rawDb": "db"}, "data": VIEWS})

    assert config.file_view_cfg.instance_space == "files"
    assert config.asset_view_cfg.external_id == "CogniteAsset"
