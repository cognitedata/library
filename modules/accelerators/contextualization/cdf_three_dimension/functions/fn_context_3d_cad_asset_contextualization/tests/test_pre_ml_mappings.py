"""Unit tests for pre-ML mappings (manual and rule-based). No CDF connection required."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pre_ml_mappings import (
    _asset_ext_id_to_info,
    _node_id_to_name,
    _qc_friendly_3d_name,
    apply_manual_mappings,
    apply_rule_mappings,
)


class TestHelpers:
    def test_node_id_to_name(self):
        tree_d_nodes = {
            "/path/node1": [{"id": 101}, {"id": 102}],
            "/path/node2": [{"id": 201}],
        }
        out = _node_id_to_name(tree_d_nodes)
        assert out == {101: "/path/node1", 102: "/path/node1", 201: "/path/node2"}

    def test_asset_ext_id_to_info(self):
        assets = [
            {"id": "ext-a", "name": "Asset A", "external_id": "ext-a"},
            {"id": "ext-b", "name": "Asset B", "external_id": "ext-b"},
        ]
        out = _asset_ext_id_to_info(assets)
        assert out["ext-a"] == {"name": "Asset A"}
        assert out["ext-b"] == {"name": "Asset B"}

    def test_qc_friendly_3d_name(self):
        assert _qc_friendly_3d_name("/prefix/short") == "short"
        assert _qc_friendly_3d_name("single") == "single"
        assert _qc_friendly_3d_name("/a/b/c") == "b"


class TestApplyManualMappings:
    """Test apply_manual_mappings with mock client; debug=True so no DM writes."""

    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def mock_config(self):
        config = MagicMock()
        config.debug = True
        config.asset_dm_space = "test-space"
        return config

    @pytest.fixture
    def tree_d_nodes(self):
        return {"/path/n1": [{"id": 100}], "/path/n2": [{"id": 200}]}

    @pytest.fixture
    def asset_entities(self):
        return [
            {"id": "A1", "name": "Asset One", "external_id": "A1"},
            {"id": "A2", "name": "Asset Two", "external_id": "A2"},
        ]

    def test_empty_manual_mappings(self, mock_client, mock_config, tree_d_nodes, asset_entities):
        with patch("pre_ml_mappings.build_cad_node_lookup", return_value={}):
            good, matched = apply_manual_mappings(
                mock_client,
                mock_config,
                [],
                model_id=1,
                revision_id=2,
                tree_d_nodes=tree_d_nodes,
                asset_entities=asset_entities,
            )
        assert good == []
        assert matched == set()

    def test_manual_mappings_applied(self, mock_client, mock_config, tree_d_nodes, asset_entities):
        manual = [
            {"sourceId": 100, "targetId": "A1"},
            {"sourceId": 200, "targetId": "A2"},
        ]
        with patch("pre_ml_mappings.build_cad_node_lookup", return_value={}):
            good, matched = apply_manual_mappings(
                mock_client,
                mock_config,
                manual,
                model_id=1,
                revision_id=2,
                tree_d_nodes=tree_d_nodes,
                asset_entities=asset_entities,
            )
        assert len(good) == 2
        assert matched == {100, 200}
        assert good[0]["matchType"] == "manual"
        assert good[0]["nodeId"] == 100
        assert good[0]["assetId"] == "A1"
        assert good[0]["assetExternalId"] == "A1"
        assert good[0]["assetName"] == "Asset One"
        assert good[1]["nodeId"] == 200 and good[1]["assetId"] == "A2"

    def test_manual_mappings_without_debug_calls_dm_apply(self, mock_client, tree_d_nodes, asset_entities):
        config = MagicMock()
        config.debug = False
        config.asset_dm_space = "test-space"
        manual = [{"sourceId": 100, "targetId": "A1"}]
        with patch("pre_ml_mappings.build_cad_node_lookup", return_value={}), \
             patch("pre_ml_mappings.create_cad_node_mappings") as mock_create:
            good, matched = apply_manual_mappings(
                mock_client,
                config,
                manual,
                model_id=1,
                revision_id=2,
                tree_d_nodes=tree_d_nodes,
                asset_entities=asset_entities,
            )
        assert len(good) == 1 and matched == {100}
        mock_create.assert_called_once()


class TestApplyRuleMappings:
    """Test apply_rule_mappings with mocked read_rule_mappings (no RAW client calls)."""

    def test_no_rules_returns_empty(self):
        client = MagicMock()
        config = MagicMock()
        config.rawdb = "db"
        config.raw_table_rule = "rule_table"
        config.asset_dm_space = "test-space"
        with patch("pre_ml_mappings.read_rule_mappings", return_value=[]), \
             patch("pre_ml_mappings.build_cad_node_lookup", return_value={}):
            good, matched = apply_rule_mappings(
                client,
                config,
                tree_d_nodes={"/n1": [{"id": 1}]},
                asset_entities=[{"id": "a", "name": "A", "external_id": "a"}],
                model_id=1,
                revision_id=2,
                already_matched_node_ids=set(),
            )
        assert good == []
        assert matched == set()

    def test_rule_matches_one_node_one_asset(self):
        client = MagicMock()
        config = MagicMock()
        config.debug = True
        config.asset_dm_space = "test-space"
        rules = [{"regexp_entity": r"^/n1$", "regexp_asset": r"^AssetA$"}]
        with patch("pre_ml_mappings.read_rule_mappings", return_value=rules), \
             patch("pre_ml_mappings.build_cad_node_lookup", return_value={}):
            good, matched = apply_rule_mappings(
                client,
                config,
                tree_d_nodes={"/n1": [{"id": 1}]},
                asset_entities=[{"id": "a", "name": "AssetA", "external_id": "a"}],
                model_id=1,
                revision_id=2,
                already_matched_node_ids=set(),
            )
        assert len(good) == 1
        assert good[0]["matchType"] == "rule"
        assert good[0]["nodeId"] == 1 and good[0]["assetId"] == "a"
        assert matched == {1}
