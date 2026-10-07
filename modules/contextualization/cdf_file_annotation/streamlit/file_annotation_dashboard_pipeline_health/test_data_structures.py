"""Tests for reading the fn_file_annotation extraction pipeline config into the dashboard's shape."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from data_structures import ExtractionPipelineConfig  # isort: skip

VIEWS = {
    "fileView": {"schemaSpace": "cdf_cdm", "instanceSpace": "files", "externalId": "CogniteFile", "version": "v1"},
    "annotationStateView": {
        "schemaSpace": "sp_hdm",
        "instanceSpace": "files",
        "externalId": "FileAnnotationState",
        "version": "v1",
    },
}


def test_views_are_read_from_data() -> None:
    """New fn_file_annotation config keeps views under data, not dataModelViews."""
    config = ExtractionPipelineConfig.from_dict({"parameters": {}, "data": VIEWS})

    assert config.file_view_cfg is not None
    assert config.annotation_state_view_cfg is not None
    assert config.file_view_cfg.instance_space == "files"
    assert config.annotation_state_view_cfg.external_id == "FileAnnotationState"


def test_views_are_read_from_legacy_data_model_views() -> None:
    """Older four-function pipelines still expose views under dataModelViews."""
    config = ExtractionPipelineConfig.from_dict({"dataModelViews": VIEWS})

    assert config.file_view_cfg is not None
    assert config.annotation_state_view_cfg is not None
    assert config.annotation_state_view_cfg.schema_space == "sp_hdm"


def test_missing_views_return_empty_config() -> None:
    config = ExtractionPipelineConfig.from_dict({"parameters": {}})

    assert config.file_view_cfg is None
    assert config.annotation_state_view_cfg is None
