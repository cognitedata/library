"""Tests for metadata update pipeline helpers."""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).parent))

from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError, CogniteConnectionError
from pydantic import ValidationError

from config import Config, ConfigData, JobConfig, Parameters, ViewPropertyConfig, format_config_for_log  # isort: skip
from constants import ALIAS_PAGE_SIZE, DEFAULT_ALIAS_PATTERN, TS_NODE  # isort: skip
from handler import handle  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip
from pipeline import (  # isort: skip
    _process_assets_optimized,
    _process_files_optimized,
    _process_timeseries_optimized,
    describe_processing_mode,
    effective_run_all,
    get_alias_filter,
    iter_new_items,
)


class Page:
    """Stand-in for a query result: one page of nodes and the cursor to the next."""

    def __init__(self, nodes: list[object], cursor: str | None = None) -> None:
        self.nodes = nodes
        self.cursors = {"items": cursor}

    def __getitem__(self, key: str) -> list[object]:
        return self.nodes


class TestPipelineHelpers(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")
        self.view_config = ViewPropertyConfig(
            schemaSpace="cdf_cdm",
            instanceSpace="inst_cfihos_oil_and_gas",
            externalId="CogniteTimeSeries",
            version="v1",
        )

    def _config(
        self,
        run_all: bool,
        update_all: bool,
        file_alias_pattern: str | None = None,
        remove_old_aliases: bool = False,
    ) -> Config:
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
                runAll=run_all,
                updateAll=update_all,
                removeOldAliases=remove_old_aliases,
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

    def test_format_config_for_log_includes_parameters_and_views(self) -> None:
        summary = format_config_for_log(self._config(run_all=True, update_all=False))
        self.assertIn("runAll: True", summary)
        self.assertIn("updateAll: False", summary)
        self.assertIn("timeseriesView:", summary)
        self.assertIn("CogniteTimeSeries", summary)
        self.assertIn("assetView:", summary)

    def test_file_view_is_optional(self) -> None:
        """A config written before file support existed must still load."""
        self.assertIsNone(self._config(run_all=False, update_all=False).data.job.file_view)

    def test_files_are_skipped_when_no_file_view_is_configured(self) -> None:
        """Without a fileView there is nothing to fetch, so no query is issued."""
        client = MagicMock()
        config = self._config(run_all=True, update_all=False)

        updates = _process_files_optimized(client, self.logger, config, MagicMock(), MagicMock())

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

    def _pages(self, client: MagicMock) -> list[list[object]]:
        return list(iter_new_items(client, self.logger, self.view_config, run_all=True, label=TS_NODE))

    def test_a_rejected_request_fails_the_run_without_retrying(self) -> None:
        """A 400 means the request itself is wrong; an empty result would hide that."""
        client = MagicMock()
        client.data_modeling.instances.query.side_effect = CogniteAPIError("Bad request", code=400)

        with self.assertRaises(CogniteAPIError):
            self._pages(client)

        self.assertEqual(client.data_modeling.instances.query.call_count, 1)

    def test_a_server_error_is_retried(self) -> None:
        """A 5xx is transient, so the next attempt stands a chance of succeeding."""
        client = MagicMock()
        client.data_modeling.instances.query.side_effect = [CogniteAPIError("Unavailable", code=503), Page(["node"])]

        with patch("pipeline.time.sleep"):
            pages = self._pages(client)

        self.assertEqual(pages, [["node"]])
        self.assertEqual(client.data_modeling.instances.query.call_count, 2)

    def test_a_timed_out_read_is_retried(self) -> None:
        client = MagicMock()
        client.data_modeling.instances.query.side_effect = [CogniteAPIError("Timeout", code=408), Page(["node"])]

        with patch("pipeline.time.sleep"):
            self.assertEqual(self._pages(client), [["node"]])

    def test_a_connection_error_is_retried(self) -> None:
        """A dropped connection is transient, so the next attempt stands a chance."""
        client = MagicMock()
        client.data_modeling.instances.query.side_effect = [CogniteConnectionError("Connection reset"), Page(["node"])]

        with patch("pipeline.time.sleep"):
            pages = self._pages(client)

        self.assertEqual(pages, [["node"]])
        self.assertEqual(client.data_modeling.instances.query.call_count, 2)

    def test_a_retry_backs_off_before_trying_again(self) -> None:
        """Retrying a rate-limited request with no delay just hits the limiter again."""
        client = MagicMock()
        client.data_modeling.instances.query.side_effect = [
            CogniteAPIError("Too many requests", code=429),
            CogniteAPIError("Too many requests", code=429),
            Page(["node"]),
        ]

        with patch("pipeline.time.sleep") as sleep:
            pages = self._pages(client)

        self.assertEqual(pages, [["node"]])
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 4])

    def test_pages_are_read_until_the_cursor_runs_out(self) -> None:
        client = MagicMock()
        client.data_modeling.instances.query.side_effect = [Page(["a"] * ALIAS_PAGE_SIZE, "c1"), Page(["b"])]

        pages = self._pages(client)

        self.assertEqual([len(page) for page in pages], [ALIAS_PAGE_SIZE, 1])
        second_query = client.data_modeling.instances.query.call_args_list[1].args[0]
        self.assertEqual(second_query.cursors, {"items": "c1"})

    def test_only_the_properties_alias_generation_uses_are_read(self) -> None:
        client = MagicMock()
        client.data_modeling.instances.query.return_value = Page([])

        self._pages(client)

        query = client.data_modeling.instances.query.call_args.args[0]
        self.assertEqual(query.select["items"].sources[0].properties, ["name", "aliases"])

    def test_effective_run_all_when_update_all_enabled(self) -> None:
        config = self._config(run_all=False, update_all=True)
        self.assertTrue(effective_run_all(config))

    def test_effective_run_all_when_remove_old_aliases_enabled(self) -> None:
        config = self._config(run_all=False, update_all=False, remove_old_aliases=True)
        self.assertTrue(effective_run_all(config))

    def test_describe_processing_mode_update_all(self) -> None:
        config = self._config(run_all=False, update_all=True)
        self.assertIn("updateAll", describe_processing_mode(config))

    def test_describe_processing_mode_remove_old_aliases(self) -> None:
        config = self._config(run_all=False, update_all=False, remove_old_aliases=True)
        self.assertIn("removeOldAliases", describe_processing_mode(config))

    def test_describe_processing_mode_incremental(self) -> None:
        config = self._config(run_all=False, update_all=False)
        self.assertIn("incremental", describe_processing_mode(config))

    def test_timeseries_are_fetched_on_space_and_aliases_alone(self) -> None:
        """Time series are selected like assets and files: nothing but space and the alias check."""
        client = MagicMock()
        client.data_modeling.instances.query.return_value = Page([])

        self._pages(client)

        query = client.data_modeling.instances.query.call_args.args[0]
        expected = dm.filters.And(
            dm.filters.In(["node", "space"], self.view_config.instance_spaces),
            get_alias_filter(self.view_config, self.logger, run_all=True),
        )
        self.assertEqual(query.with_["items"].filter.dump(), expected.dump())

    def test_get_alias_filter_skips_alias_exists_when_incremental(self) -> None:
        filter_query = get_alias_filter(self.view_config, self.logger, run_all=False)
        self.assertIsInstance(filter_query, dm.filters.And)

    def test_get_alias_filter_fetches_all_when_run_all(self) -> None:
        filter_query = get_alias_filter(self.view_config, self.logger, run_all=True)
        self.assertIsInstance(filter_query, dm.filters.HasData)

    def test_each_page_is_written_before_the_next_is_read(self) -> None:
        """Holding every instance in memory is what times a large project out."""
        config = self._config(run_all=False, update_all=False)
        processor = MagicMock()
        processor.process_timeseries_metadata.return_value = MagicMock()
        batch_processor = MagicMock()
        batch_processor.apply_updates_in_batches.side_effect = [2, 1]

        with patch("pipeline.iter_new_items", return_value=iter([[MagicMock(), MagicMock()], [MagicMock()]])):
            total = _process_timeseries_optimized(MagicMock(), self.logger, config, processor, batch_processor)

        self.assertEqual(batch_processor.apply_updates_in_batches.call_count, 2)
        self.assertEqual(total, 3)

    def test_assets_use_the_asset_view_and_processor(self) -> None:
        config = self._config(run_all=False, update_all=False)
        processor = MagicMock()
        batch_processor = MagicMock()
        batch_processor.apply_updates_in_batches.return_value = 1

        with patch("pipeline.iter_new_items", return_value=iter([[MagicMock()]])) as fetch:
            total = _process_assets_optimized(MagicMock(), self.logger, config, processor, batch_processor)

        self.assertIs(fetch.call_args.args[2], config.data.job.asset_view)
        processor.process_asset_metadata.assert_called_once()
        self.assertEqual(total, 1)

    def test_process_timeseries_handles_empty_fetch(self) -> None:
        config = self._config(run_all=False, update_all=False)

        with patch("pipeline.iter_new_items", return_value=iter([])):
            total = _process_timeseries_optimized(MagicMock(), self.logger, config, MagicMock(), MagicMock())

        self.assertEqual(total, 0)

    def test_a_failed_run_fails_the_function_call(self) -> None:
        """A returned failure dict is a succeeded call to CDF, so the workflow would carry on."""
        with (
            patch("handler._report_usage"),
            patch("handler.load_config_parameters", side_effect=ValueError("bad config")),
            self.assertRaises(ValueError),
        ):
            handle({"ExtractionPipelineExtId": "ep"}, MagicMock())

    def test_invalid_input_fails_before_any_cdf_call(self) -> None:
        for data in ({}, {"ExtractionPipelineExtId": ""}, {"ExtractionPipelineExtId": "ep", "logLevel": "VERBOSE"}):
            client = MagicMock()
            with self.subTest(data=data), patch("handler.load_config_parameters") as load:
                with self.assertRaises(ValidationError):
                    handle(data, client)
                load.assert_not_called()
                self.assertEqual(client.mock_calls, [])


if __name__ == "__main__":
    unittest.main()
