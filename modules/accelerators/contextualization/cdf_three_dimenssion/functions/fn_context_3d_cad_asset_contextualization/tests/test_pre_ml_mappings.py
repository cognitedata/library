"""Unit tests for pre-ML mappings (manual and rule-based). No CDF connection required."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pre_ml_mappings import (
    _asset_id_to_info,
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

    def test_asset_id_to_info(self):
        assets = [
            {"id": 1, "name": "Asset A", "external_id": "ext-a"},
            {"id": 2, "name": "Asset B"},
        ]
        out = _asset_id_to_info(assets)
        assert out[1] == {"name": "Asset A", "external_id": "ext-a"}
        assert out[2] == {"name": "Asset B", "external_id": None}

    def test_qc_friendly_3d_name(self):
        assert _qc_friendly_3d_name("/prefix/short") == "short"
        assert _qc_friendly_3d_name("single") == "single"
        assert _qc_friendly_3d_name("/a/b/c") == "b"


class TestApplyManualMappings:
    """Test apply_manual_mappings with mock client; debug=True so no CDF writes."""

    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def mock_config(self):
        config = MagicMock()
        config.debug = True  # skip client.three_d.asset_mappings.create
        return config

    @pytest.fixture
    def tree_d_nodes(self):
        return {"/path/n1": [{"id": 100}], "/path/n2": [{"id": 200}]}

    @pytest.fixture
    def asset_entities(self):
        return [
            {"id": 500, "name": "Asset One", "external_id": "A1"},
            {"id": 600, "name": "Asset Two", "external_id": "A2"},
        ]

    def test_empty_manual_mappings(self, mock_client, mock_config, tree_d_nodes, asset_entities):
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
        mock_client.three_d.asset_mappings.create.assert_not_called()

    def test_manual_mappings_applied(self, mock_client, mock_config, tree_d_nodes, asset_entities):
        manual = [
            {"sourceId": 100, "targetId": 500},
            {"sourceId": 200, "targetId": 600},
        ]
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
        assert good[0]["3DId"] == 100
        assert good[0]["assetId"] == 500
        assert good[0]["assetName"] == "Asset One"
        assert good[0]["assetExternalId"] == "A1"
        assert good[1]["3DId"] == 200 and good[1]["assetId"] == 600
        # debug=True so create not called
        mock_client.three_d.asset_mappings.create.assert_not_called()

    def test_manual_mappings_without_debug_calls_create(self, mock_client, tree_d_nodes, asset_entities):
        config = MagicMock()
        config.debug = False
        manual = [{"sourceId": 100, "targetId": 500}]
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
        mock_client.three_d.asset_mappings.create.assert_called_once()
        call_kw = mock_client.three_d.asset_mappings.create.call_args[1]
        assert call_kw["model_id"] == 1 and call_kw["revision_id"] == 2
        assert len(call_kw["asset_mapping"]) == 1
        assert call_kw["asset_mapping"][0].node_id == 100 and call_kw["asset_mapping"][0].asset_id == 500


class TestApplyRuleMappings:
    """Test apply_rule_mappings with mocked read_rule_mappings (no RAW client calls)."""

    def test_no_rules_returns_empty(self):
        client = MagicMock()
        config = MagicMock()
        config.rawdb = "db"
        config.raw_table_rule = "rule_table"
        # read_rule_mappings will be called and return [] if we don't patch;
        # we need to patch so it returns [] without touching client.raw
        with patch("pre_ml_mappings.read_rule_mappings", return_value=[]):
            good, matched = apply_rule_mappings(
                client,
                config,
                tree_d_nodes={"/n1": [{"id": 1}]},
                asset_entities=[{"id": 10, "name": "A", "external_id": "a"}],
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
        rules = [{"regexp_entity": r"^/n1$", "regexp_asset": r"^AssetA$"}]
        with patch("pre_ml_mappings.read_rule_mappings", return_value=rules):
            good, matched = apply_rule_mappings(
                client,
                config,
                tree_d_nodes={"/n1": [{"id": 1}]},
                asset_entities=[{"id": 10, "name": "AssetA", "external_id": "a"}],
                model_id=1,
                revision_id=2,
                already_matched_node_ids=set(),
            )
        assert len(good) == 1
        assert good[0]["matchType"] == "rule"
        assert good[0]["3DId"] == 1 and good[0]["assetId"] == 10
        assert matched == {1}
        client.three_d.asset_mappings.create.assert_not_called()
