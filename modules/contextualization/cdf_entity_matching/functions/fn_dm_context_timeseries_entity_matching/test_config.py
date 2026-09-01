"""Tests for extraction pipeline config parsing."""

import sys
from pathlib import Path

import yaml

sys.path.append(str(Path(__file__).parent))

from config import Config


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
