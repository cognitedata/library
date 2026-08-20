"""Tests for metadata update pipeline helpers."""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).parent))

from cognite.client import data_modeling as dm
from config import Config, ConfigData, JobConfig, Parameters, ViewPropertyConfig
from logger import CogniteFunctionLogger
from pipeline import (
    _process_assets_optimized,
    _process_timeseries_optimized,
    describe_processing_mode,
    effective_run_all,
    get_asset_filter,
    get_ts_filter,
)


class TestPipelineHelpers(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")
        self.view_config = ViewPropertyConfig(
            schemaSpace="cdf_cdm",
            instanceSpace="inst_location",
            externalId="CogniteTimeSeries",
            version="v1",
        )

    def _config(self, run_all: bool, update_all: bool) -> Config:
        view = ViewPropertyConfig(
            schemaSpace="cdf_cdm",
            instanceSpace="inst_location",
            externalId="CogniteAsset",
            version="v1",
        )
        return Config(
            parameters=Parameters(
                debug=False,
                runAll=run_all,
                updateAll=update_all,
                rawDb="db",
                rawTableState="state",
            ),
            data=ConfigData(
                job=JobConfig(
                    timeseriesView=self.view_config,
                    assetView=view,
                )
            ),
        )

    def test_effective_run_all_when_update_all_enabled(self) -> None:
        config = self._config(run_all=False, update_all=True)
        self.assertTrue(effective_run_all(config))

    def test_describe_processing_mode_update_all(self) -> None:
        config = self._config(run_all=False, update_all=True)
        self.assertIn("updateAll", describe_processing_mode(config))

    def test_describe_processing_mode_incremental(self) -> None:
        config = self._config(run_all=False, update_all=False)
        self.assertIn("incremental", describe_processing_mode(config))

    def test_get_ts_filter_skips_alias_exists_when_incremental(self) -> None:
        filter_query = get_ts_filter(self.view_config, None, run_all=False, logger=self.logger)
        self.assertIsInstance(filter_query, dm.filters.And)

    def test_get_ts_filter_fetches_all_when_run_all(self) -> None:
        filter_query = get_ts_filter(self.view_config, None, run_all=True, logger=self.logger)
        self.assertIsInstance(filter_query, dm.filters.HasData)

    def test_get_asset_filter_skips_alias_exists_when_incremental(self) -> None:
        filter_query = get_asset_filter(self.view_config, self.logger, run_all=False)
        self.assertIsInstance(filter_query, dm.filters.And)

    def test_get_asset_filter_fetches_all_when_run_all(self) -> None:
        filter_query = get_asset_filter(self.view_config, self.logger, run_all=True)
        self.assertIsInstance(filter_query, dm.filters.HasData)

    def test_process_timeseries_fetches_once_in_incremental_mode(self) -> None:
        """Incremental mode must fetch once; get_new_items already returns every match."""
        config = self._config(run_all=False, update_all=False)
        processor = MagicMock()
        processor.process_timeseries_metadata.return_value = MagicMock()
        batch_processor = MagicMock()
        batch_processor.apply_updates_in_batches.return_value = 2

        with patch("pipeline.get_new_items", return_value=[MagicMock(), MagicMock()]) as fetch:
            total = _process_timeseries_optimized(
                MagicMock(), self.logger, config, processor, batch_processor
            )

        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(total, 2)

    def test_process_assets_fetches_once_in_incremental_mode(self) -> None:
        """Incremental mode must fetch once; get_new_items already returns every match."""
        config = self._config(run_all=False, update_all=False)
        processor = MagicMock()
        processor.process_asset_metadata.return_value = MagicMock()
        batch_processor = MagicMock()
        batch_processor.apply_updates_in_batches.return_value = 2

        with patch("pipeline.get_new_items", return_value=[MagicMock(), MagicMock()]) as fetch:
            total = _process_assets_optimized(
                MagicMock(), self.logger, config, processor, batch_processor
            )

        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(total, 2)

    def test_process_timeseries_handles_empty_fetch(self) -> None:
        config = self._config(run_all=False, update_all=False)

        with patch("pipeline.get_new_items", return_value=None):
            total = _process_timeseries_optimized(
                MagicMock(), self.logger, config, MagicMock(), MagicMock()
            )

        self.assertEqual(total, 0)


if __name__ == "__main__":
    unittest.main()
