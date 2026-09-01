"""Tests for extraction pipeline config parsing."""

import sys
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

sys.path.append(str(Path(__file__).parent))

from config import Config, ViewPropertyConfig


def test_config_accepts_raw_table_ctx_keys_from_pipeline_yaml() -> None:
    """Pipeline YAML uses rawTableCtx* keys, not the old rawTaleCtx* typo."""
    raw_config = yaml.safe_load(
        """
        parameters:
          debug: false
          runAll: true
          dmUpdate: true
          removeOldLinks: false
          rawDb: db_asset_entity_matching
          rawTableState: contextualization_state_store
          rawTableCtxGood: contextualization_good
          rawTableCtxBad: contextualization_bad
          rawTableCtxManual: contextualization_manual_input
          rawTableCtxRule: contextualization_rule_input
          autoApprovalThreshold: 0.85
        data:
          targetView:
            schemaSpace: cdf_cdm
            instanceSpace: inst_location
            externalId: CogniteAsset
            version: v1
            searchProperty: name
            filterProperty: tags
            filterValues: []
          entityView:
            schemaSpace: cdf_cdm
            instanceSpace: inst_location
            externalId: CogniteTimeSeries
            version: v1
            searchProperty: name
            filterProperty: tags
            filterValues: []
        """
    )

    config = Config.model_validate(raw_config)

    assert config.parameters.raw_table_ctx_good == "contextualization_good"
    assert config.parameters.raw_table_ctx_bad == "contextualization_bad"
    assert config.parameters.raw_table_ctx_manual == "contextualization_manual_input"
    assert config.parameters.raw_table_ctx_rule == "contextualization_rule_input"


def _view_config(instance_space: str | list[str]) -> ViewPropertyConfig:
    return ViewPropertyConfig.model_validate(
        {
            "schemaSpace": "cdf_cdm",
            "instanceSpace": instance_space,
            "externalId": "CogniteTimeSeries",
            "version": "v1",
        }
    )


def test_single_instance_space_is_exposed_as_one_element_list() -> None:
    """A plain string stays valid and reads back as a single-space list."""
    view = _view_config("inst_location")

    assert view.instance_spaces == ["inst_location"]
    assert view.default_instance_space == "inst_location"


def test_multiple_instance_spaces_keep_config_order() -> None:
    """Several spaces can be queried, and the first one is the write fallback."""
    view = _view_config(["inst_location", "inst_timeseries"])

    assert view.instance_spaces == ["inst_location", "inst_timeseries"]
    assert view.default_instance_space == "inst_location"


def test_empty_instance_space_list_is_rejected() -> None:
    """An empty list would leave no space to read from or write to."""
    with pytest.raises(ValidationError):
        _view_config([])
