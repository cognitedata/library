"""Tests for metadata update pipeline helpers."""

import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from cognite.client import data_modeling as dm
from config import Config, ConfigData, JobConfig, Parameters, ViewPropertyConfig
from logger import CogniteFunctionLogger
from pipeline import describe_processing_mode, effective_run_all, get_asset_filter, get_ts_filter


class TestPipelineHelpers(unittest.TestCase):
    def setUp(self):
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

    def test_effective_run_all_when_update_all_enabled(self):
        config = self._config(run_all=False, update_all=True)
        self.assertTrue(effective_run_all(config))

    def test_describe_processing_mode_update_all(self):
        config = self._config(run_all=False, update_all=True)
        self.assertIn("updateAll", describe_processing_mode(config))

    def test_describe_processing_mode_incremental(self):
        config = self._config(run_all=False, update_all=False)
        self.assertIn("incremental", describe_processing_mode(config))

    def test_get_ts_filter_skips_alias_exists_when_incremental(self):
        filter_query = get_ts_filter(self.view_config, None, run_all=False, logger=self.logger)
        self.assertIsInstance(filter_query, dm.filters.And)

    def test_get_ts_filter_fetches_all_when_run_all(self):
        filter_query = get_ts_filter(self.view_config, None, run_all=True, logger=self.logger)
        self.assertIsInstance(filter_query, dm.filters.HasData)

    def test_get_asset_filter_skips_alias_exists_when_incremental(self):
        filter_query = get_asset_filter(self.view_config, self.logger, run_all=False)
        self.assertIsInstance(filter_query, dm.filters.And)

    def test_get_asset_filter_fetches_all_when_run_all(self):
        filter_query = get_asset_filter(self.view_config, self.logger, run_all=True)
        self.assertIsInstance(filter_query, dm.filters.HasData)


if __name__ == "__main__":
    unittest.main()
