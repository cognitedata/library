"""Tests for metadata update pipeline helpers."""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).parent))

from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError, CogniteConnectionError
from pydantic import ValidationError

from config import Config, ConfigData, JobConfig, Parameters, ViewPropertyConfig  # isort: skip
from constants import DEFAULT_ALIAS_PATTERN, TS_NODE  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip
from pipeline import (  # isort: skip
    _process_assets_optimized,
    _process_files_optimized,
    _process_timeseries_optimized,
    describe_processing_mode,
    effective_run_all,
    get_alias_filter,
    get_new_items,
)


class TestPipelineHelpers(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")
        self.view_config = ViewPropertyConfig(
            schemaSpace="cdf_cdm",
            instanceSpace="inst_cfihos_oil_and_gas",
            externalId="CogniteTimeSeries",
            version="v1",
        )

    def _config(self, run_all: bool, update_all: bool, file_alias_pattern: str | None = None) -> Config:
        view = ViewPropertyConfig(
            schemaSpace="cdf_cdm",
            instanceSpace="inst_cfihos_oil_and_gas",
            externalId="CogniteAsset",
            version="v1",
        )
        file_view = (
            ViewPropertyConfig(
                schemaSpace="cdf_cdm",
                instanceSpace="inst_cfihos_oil_and_gas",
                externalId="CogniteFile",
                version="v1",
                aliasPattern=file_alias_pattern,
            )
            if file_alias_pattern
            else None
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
                    fileView=file_view,
                )
            ),
        )

    def test_file_view_is_optional(self) -> None:
        """A config written before file support existed must still load."""
        self.assertIsNone(self._config(run_all=False, update_all=False).data.job.file_view)

    def test_files_are_skipped_when_no_file_view_is_configured(self) -> None:
        """Without a fileView there is nothing to fetch, so no query is issued."""
        client = MagicMock()
        config = self._config(run_all=True, update_all=False)

        updates = _process_files_optimized(
            client, self.logger, config, MagicMock(), MagicMock()
        )

        self.assertEqual(updates, 0)
        client.data_modeling.instances.list.assert_not_called()

    def test_file_view_carries_its_own_alias_pattern(self) -> None:
        """Documents can follow a different naming convention than assets."""
        config = self._config(run_all=False, update_all=False, file_alias_pattern=r"([A-Z]{3})[-_]?([0-9]{4})")

        self.assertEqual(config.data.job.file_view.alias_patterns, [r"([A-Z]{3})[-_]?([0-9]{4})"])

    def test_alias_pattern_defaults_to_the_shared_tag_shape(self) -> None:
        """A config written before the pattern was configurable keeps working unchanged."""
        self.assertEqual(self.view_config.alias_patterns, [DEFAULT_ALIAS_PATTERN])

    def test_each_view_carries_its_own_alias_pattern(self) -> None:
        """Timeseries and asset names can follow different conventions."""
        view = ViewPropertyConfig(
            schemaSpace="cdf_cdm",
            instanceSpace="inst_cfihos_oil_and_gas",
            externalId="CogniteAsset",
            version="v1",
            aliasPattern=r"(\d{3})-([A-Z]{4})",
        )

        self.assertEqual(view.alias_patterns, [r"(\d{3})-([A-Z]{4})"])

    def test_several_alias_patterns_are_accepted(self) -> None:
        """A view whose names follow more than one convention configures a pattern each."""
        view = ViewPropertyConfig(
            schemaSpace="cdf_cdm",
            instanceSpace="inst_cfihos_oil_and_gas",
            externalId="CogniteAsset",
            version="v1",
            aliasPattern=[r"(\d{3})-([A-Z]{4})", r"([A-Z]{3})[-_]?(\d{4})"],
        )

        self.assertEqual(view.alias_patterns, [r"(\d{3})-([A-Z]{4})", r"([A-Z]{3})[-_]?(\d{4})"])

    def test_an_empty_alias_pattern_list_is_rejected(self) -> None:
        """Configuring no pattern at all is a mistake, not a way to disable aliases."""
        with self.assertRaises(ValidationError):
            ViewPropertyConfig(
                schemaSpace="cdf_cdm",
                instanceSpace="inst",
                externalId="CogniteAsset",
                version="v1",
                aliasPattern=[],
            )

    def test_an_invalid_pattern_anywhere_in_the_list_is_rejected(self) -> None:
        """A broken pattern must not hide behind a valid first entry."""
        with self.assertRaises(ValidationError):
            ViewPropertyConfig(
                schemaSpace="cdf_cdm",
                instanceSpace="inst",
                externalId="CogniteAsset",
                version="v1",
                aliasPattern=[DEFAULT_ALIAS_PATTERN, r"(\d{2}"],
            )

    def test_alias_selection_defaults_to_keeping_every_alias(self) -> None:
        """Existing behaviour: one pattern, one alias, nothing discarded."""
        self.assertEqual(self.view_config.alias_selection, "all")

    def test_an_unknown_alias_selection_is_rejected(self) -> None:
        """A typo here would silently change which aliases are written."""
        with self.assertRaises(ValidationError):
            ViewPropertyConfig(
                schemaSpace="cdf_cdm",
                instanceSpace="inst",
                externalId="CogniteAsset",
                version="v1",
                aliasSelection="shortest",
            )

    def test_an_invalid_alias_pattern_is_rejected(self) -> None:
        """A broken regex must fail at config load, not on the first node processed."""
        with self.assertRaises(ValidationError):
            ViewPropertyConfig(
                schemaSpace="cdf_cdm",
                instanceSpace="inst",
                externalId="CogniteAsset",
                version="v1",
                aliasPattern=r"(\d{2}",
            )

    def test_an_alias_pattern_without_capture_groups_is_rejected(self) -> None:
        """The alias is the capture groups joined, so no groups means no alias at all."""
        with self.assertRaises(ValidationError):
            ViewPropertyConfig(
                schemaSpace="cdf_cdm",
                instanceSpace="inst",
                externalId="CogniteAsset",
                version="v1",
                aliasPattern=r"\d{2}-[A-Z]{2,3}",
            )

    def test_a_rejected_request_is_not_retried(self) -> None:
        """A 400 means the request itself is wrong, so retrying only delays the failure."""
        client = MagicMock()
        client.data_modeling.instances.list.side_effect = CogniteAPIError("Bad request", code=400)

        get_new_items(client, self.logger, self.view_config.as_view_id(), self._config(True, False), TS_NODE)

        self.assertEqual(client.data_modeling.instances.list.call_count, 1)

    def test_a_server_error_is_retried(self) -> None:
        """A 5xx is transient, so the next attempt stands a chance of succeeding."""
        client = MagicMock()
        client.data_modeling.instances.list.side_effect = [CogniteAPIError("Unavailable", code=503), ["node"]]

        with patch("pipeline.time.sleep"):
            result = get_new_items(
                client, self.logger, self.view_config.as_view_id(), self._config(True, False), TS_NODE
            )

        self.assertEqual(result, ["node"])
        self.assertEqual(client.data_modeling.instances.list.call_count, 2)

    def test_a_connection_error_is_retried(self) -> None:
        """A dropped connection is transient, so the next attempt stands a chance."""
        client = MagicMock()
        client.data_modeling.instances.list.side_effect = [
            CogniteConnectionError("Connection reset"),
            ["node"],
        ]

        with patch("pipeline.time.sleep"):
            result = get_new_items(
                client, self.logger, self.view_config.as_view_id(), self._config(True, False), TS_NODE
            )

        self.assertEqual(result, ["node"])
        self.assertEqual(client.data_modeling.instances.list.call_count, 2)

    def test_a_retry_backs_off_before_trying_again(self) -> None:
        """Retrying a rate-limited request with no delay just hits the limiter again."""
        client = MagicMock()
        client.data_modeling.instances.list.side_effect = [
            CogniteAPIError("Too many requests", code=429),
            CogniteAPIError("Too many requests", code=429),
            ["node"],
        ]

        with patch("pipeline.time.sleep") as sleep:
            result = get_new_items(
                client, self.logger, self.view_config.as_view_id(), self._config(True, False), TS_NODE
            )

        self.assertEqual(result, ["node"])
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 4])

    def test_effective_run_all_when_update_all_enabled(self) -> None:
        config = self._config(run_all=False, update_all=True)
        self.assertTrue(effective_run_all(config))

    def test_describe_processing_mode_update_all(self) -> None:
        config = self._config(run_all=False, update_all=True)
        self.assertIn("updateAll", describe_processing_mode(config))

    def test_describe_processing_mode_incremental(self) -> None:
        config = self._config(run_all=False, update_all=False)
        self.assertIn("incremental", describe_processing_mode(config))

    def test_timeseries_are_fetched_on_aliases_alone(self) -> None:
        """Time series are selected like assets and files: nothing but the alias check."""
        client = MagicMock()
        client.data_modeling.instances.list.return_value = ["node"]

        get_new_items(
            client, self.logger, self.view_config.as_view_id(), self._config(True, False), TS_NODE
        )

        filter_query = client.data_modeling.instances.list.call_args.kwargs["filter"]
        self.assertIsInstance(filter_query, dm.filters.HasData)

    def test_get_alias_filter_skips_alias_exists_when_incremental(self) -> None:
        filter_query = get_alias_filter(self.view_config, self.logger, run_all=False)
        self.assertIsInstance(filter_query, dm.filters.And)

    def test_get_alias_filter_fetches_all_when_run_all(self) -> None:
        filter_query = get_alias_filter(self.view_config, self.logger, run_all=True)
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
