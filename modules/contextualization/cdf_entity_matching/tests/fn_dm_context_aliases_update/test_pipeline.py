"""Tests for metadata update pipeline helpers."""

import sys
import unittest
from contextlib import AbstractContextManager
from pathlib import Path
from unittest.mock import DEFAULT, MagicMock, patch

sys.path.append(str(Path(__file__).resolve().parents[2] / "functions" / "fn_dm_context_aliases_update"))

from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError, CogniteConnectionError
from pydantic import ValidationError

from config import Config, ConfigData, JobConfig, Parameters, ViewPropertyConfig, format_config_for_log  # isort: skip
from constants import ALIAS_PAGE_SIZE, ASSET_NODE, DEFAULT_ALIAS_PATTERN, TS_NODE  # isort: skip
import alias_sync  # isort: skip
import handler  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip
import pipeline  # isort: skip


class Page:
    """Stand-in for a sync result: one page of nodes and the cursor to the next."""

    def __init__(self, nodes: list[object], cursor: str | None = "next") -> None:
        self.nodes = nodes
        self.cursors = {"items": cursor}

    def __getitem__(self, key: str) -> list[object]:
        return self.nodes


def node(external_id: str = "n", deleted: bool = False) -> MagicMock:
    item = MagicMock()
    item.space = "sp"
    item.external_id = external_id
    item.deleted_time = 1 if deleted else None
    return item


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
        self.assertIn("filterProperty: None", summary)

    def test_file_view_is_optional(self) -> None:
        """A config written before file support existed must still load."""
        self.assertIsNone(self._config(run_all=False, update_all=False).data.job.file_view)

    def test_files_are_skipped_when_no_file_view_is_configured(self) -> None:
        """Without a fileView there is nothing to fetch, so no read is issued."""
        client = MagicMock()
        config = self._config(run_all=True, update_all=False)

        updates = pipeline._process_files_optimized(client, self.logger, config, MagicMock(), MagicMock())

        self.assertEqual(updates, 0)
        client.data_modeling.instances.sync.assert_not_called()

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

    def _filtered_view(self, filter_property: object, filter_values: object) -> ViewPropertyConfig:
        return ViewPropertyConfig(
            schemaSpace="cdf_cdm",
            instanceSpace="inst",
            externalId="CogniteAsset",
            version="v1",
            filterProperty=filter_property,
            filterValues=filter_values,
        )

    def test_a_single_filter_value_is_wrapped_in_a_list(self) -> None:
        self.assertEqual(self._filtered_view("tags", "pump").filter_values, ["pump"])

    def test_an_empty_filter_is_unset(self) -> None:
        """The module defaults render as an empty list and must not filter everything out."""
        view = self._filtered_view("", [])
        self.assertIsNone(view.filter_property)
        self.assertIsNone(view.filter_values)

    def test_the_property_filter_is_left_out_without_values(self) -> None:
        expected = dm.filters.And(
            dm.filters.In(["node", "space"], ["inst"]),
            dm.filters.HasData(views=[dm.ViewId("cdf_cdm", "CogniteAsset", "v1")]),
        )
        self.assertEqual(alias_sync.sync_filter(self._filtered_view("tags", [])).dump(), expected.dump())

    def test_the_property_filter_narrows_the_read(self) -> None:
        view = self._filtered_view("tags", ["pump", "valve"])
        expected = dm.filters.And(
            dm.filters.In(["node", "space"], ["inst"]),
            dm.filters.HasData(views=[view.as_view_id()]),
            dm.filters.In(view.as_property_ref("tags"), ["pump", "valve"]),
        )
        self.assertEqual(alias_sync.sync_filter(view).dump(), expected.dump())

    def test_a_filter_change_gets_a_cursor_of_its_own(self) -> None:
        """A cursor taken over one population must never be applied to another."""
        unfiltered = alias_sync.sync_state_key(self._filtered_view("tags", []), ASSET_NODE)
        filtered = alias_sync.sync_state_key(self._filtered_view("tags", ["pump"]), ASSET_NODE)
        self.assertNotEqual(unfiltered, filtered)

    def _pages(self, client: MagicMock, cursor: str | None = None) -> list[tuple[list[object], alias_sync.SyncState]]:
        state = alias_sync.SyncState(cursor, ALIAS_PAGE_SIZE)
        return list(alias_sync.iter_synced_pages(client, self.logger, self.view_config, state, TS_NODE))

    def test_a_rejected_request_fails_the_run_without_retrying(self) -> None:
        """A 400 means the request itself is wrong; an empty result would hide that."""
        client = MagicMock()
        client.data_modeling.instances.sync.side_effect = CogniteAPIError("Bad request", code=400)

        with self.assertRaises(CogniteAPIError):
            self._pages(client)

        self.assertEqual(client.data_modeling.instances.sync.call_count, 1)

    def test_a_rejected_stored_cursor_reads_everything_again(self) -> None:
        """An expired cursor leaves no way to tell what changed."""
        client = MagicMock()
        client.data_modeling.instances.sync.side_effect = [CogniteAPIError("Bad cursor", code=400), Page([node()])]

        pages = self._pages(client, cursor="expired")

        self.assertEqual(len(pages), 1)
        retried = client.data_modeling.instances.sync.call_args.args[0]
        self.assertEqual(retried.cursors, {"items": None})

    def test_a_server_error_is_retried(self) -> None:
        """A 5xx is transient, so the next attempt stands a chance of succeeding."""
        client = MagicMock()
        client.data_modeling.instances.sync.side_effect = [CogniteAPIError("Unavailable", code=503), Page([node()])]

        with patch("alias_sync.time.sleep"):
            pages = self._pages(client)

        self.assertEqual(len(pages), 1)
        self.assertEqual(client.data_modeling.instances.sync.call_count, 2)

    def test_a_timed_out_read_is_read_again_with_a_smaller_page(self) -> None:
        client = MagicMock()
        client.data_modeling.instances.sync.side_effect = [CogniteAPIError("Timeout", code=408), Page([node()])]

        pages = self._pages(client)

        self.assertEqual(pages[0][1].batch_size, int(ALIAS_PAGE_SIZE * 0.8))
        expression = client.data_modeling.instances.sync.call_args.args[0].with_["items"]
        self.assertEqual(expression.limit, int(ALIAS_PAGE_SIZE * 0.8))

    def test_a_connection_error_is_retried(self) -> None:
        """A dropped connection is transient, so the next attempt stands a chance."""
        client = MagicMock()
        client.data_modeling.instances.sync.side_effect = [CogniteConnectionError("Connection reset"), Page([node()])]

        with patch("alias_sync.time.sleep"):
            pages = self._pages(client)

        self.assertEqual(len(pages), 1)
        self.assertEqual(client.data_modeling.instances.sync.call_count, 2)

    def test_a_retry_backs_off_before_trying_again(self) -> None:
        """Retrying a rate-limited request with no delay just hits the limiter again."""
        client = MagicMock()
        client.data_modeling.instances.sync.side_effect = [
            CogniteAPIError("Too many requests", code=429),
            CogniteAPIError("Too many requests", code=429),
            Page([node()]),
        ]

        with patch("alias_sync.time.sleep") as sleep:
            self._pages(client)

        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 4])

    def test_pages_are_read_until_a_short_page(self) -> None:
        client = MagicMock()
        client.data_modeling.instances.sync.side_effect = [
            Page([node()] * ALIAS_PAGE_SIZE, "c1"),
            Page([node()], "c2"),
        ]

        pages = self._pages(client)

        self.assertEqual([len(page) for page, _ in pages], [ALIAS_PAGE_SIZE, 1])
        self.assertEqual(pages[-1][1].cursor, "c2")
        second_query = client.data_modeling.instances.sync.call_args_list[1].args[0]
        self.assertEqual(second_query.cursors, {"items": "c1"})

    def test_the_stored_cursor_is_passed_to_the_first_read(self) -> None:
        client = MagicMock()
        client.data_modeling.instances.sync.return_value = Page([])

        pages = self._pages(client, cursor="stored")

        self.assertEqual(client.data_modeling.instances.sync.call_args.args[0].cursors, {"items": "stored"})
        self.assertEqual(pages, [([], alias_sync.SyncState("next", ALIAS_PAGE_SIZE))])

    def test_deleted_instances_are_not_returned(self) -> None:
        """Sync reports deletes too, and there is nothing to alias on those."""
        client = MagicMock()
        client.data_modeling.instances.sync.return_value = Page([node("kept"), node("gone", deleted=True)])

        pages = self._pages(client)

        self.assertEqual([item.external_id for item in pages[0][0]], ["kept"])

    def test_only_the_properties_alias_generation_uses_are_read(self) -> None:
        client = MagicMock()
        client.data_modeling.instances.sync.return_value = Page([])

        self._pages(client)

        query = client.data_modeling.instances.sync.call_args.args[0]
        self.assertEqual(query.select["items"].sources[0].properties, ["name", "aliases"])

    def test_effective_run_all_when_update_all_enabled(self) -> None:
        config = self._config(run_all=False, update_all=True)
        self.assertTrue(pipeline.effective_run_all(config))

    def test_effective_run_all_when_remove_old_aliases_enabled(self) -> None:
        config = self._config(run_all=False, update_all=False, remove_old_aliases=True)
        self.assertTrue(pipeline.effective_run_all(config))

    def test_describe_processing_mode_update_all(self) -> None:
        config = self._config(run_all=False, update_all=True)
        self.assertIn("updateAll", pipeline.describe_processing_mode(config))

    def test_describe_processing_mode_remove_old_aliases(self) -> None:
        config = self._config(run_all=False, update_all=False, remove_old_aliases=True)
        self.assertIn("removeOldAliases", pipeline.describe_processing_mode(config))

    def test_describe_processing_mode_incremental(self) -> None:
        config = self._config(run_all=False, update_all=False)
        self.assertIn("incremental", pipeline.describe_processing_mode(config))

    def _patched_state(
        self, stored_cursor: str | None = None, full_read_in_progress: bool = False
    ) -> AbstractContextManager[dict[str, MagicMock]]:
        stored = alias_sync.SyncState(stored_cursor, ALIAS_PAGE_SIZE, full_read_in_progress)
        return patch.multiple(pipeline, read_sync_state=MagicMock(return_value=stored), write_sync_state=DEFAULT)

    def test_each_page_is_written_before_the_next_is_read(self) -> None:
        """Holding every instance in memory is what runs a large project out of memory."""
        config = self._config(run_all=False, update_all=False)
        processor = MagicMock()
        processor.process_timeseries_metadata.return_value = MagicMock()
        batch_processor = MagicMock()
        batch_processor.apply_updates_in_batches.side_effect = [2, 1]
        pages = iter(
            [
                ([node(), node()], alias_sync.SyncState("c1", 1000)),
                ([node()], alias_sync.SyncState("c2", 1000)),
            ]
        )

        with self._patched_state(), patch.object(pipeline, "iter_synced_pages", return_value=pages):
            total = pipeline._process_timeseries_optimized(MagicMock(), self.logger, config, processor, batch_processor)

        self.assertEqual(batch_processor.apply_updates_in_batches.call_count, 2)
        self.assertEqual(total, 3)

    def test_assets_use_the_asset_view_and_processor(self) -> None:
        config = self._config(run_all=False, update_all=False)
        processor = MagicMock()
        batch_processor = MagicMock()
        batch_processor.apply_updates_in_batches.return_value = 1
        pages = iter([([node()], alias_sync.SyncState("c1", 1000))])

        with self._patched_state(), patch.object(pipeline, "iter_synced_pages", return_value=pages) as fetch:
            total = pipeline._process_assets_optimized(MagicMock(), self.logger, config, processor, batch_processor)

        self.assertIs(fetch.call_args.args[2], config.data.job.asset_view)
        processor.process_asset_metadata.assert_called_once()
        self.assertEqual(total, 1)

    def test_incremental_reads_from_the_stored_cursor(self) -> None:
        config = self._config(run_all=False, update_all=False)

        with (
            self._patched_state(stored_cursor="stored"),
            patch.object(pipeline, "iter_synced_pages", return_value=iter([])) as fetch,
        ):
            pipeline._process_timeseries_optimized(MagicMock(), self.logger, config, MagicMock(), MagicMock())

        self.assertEqual(fetch.call_args.args[3].cursor, "stored")

    def test_run_all_reads_everything_and_stores_the_new_cursor(self) -> None:
        config = self._config(run_all=True, update_all=False)
        pages = iter([([], alias_sync.SyncState("fresh", 800))])

        with (
            self._patched_state(stored_cursor="stored") as state,
            patch.object(pipeline, "iter_synced_pages", return_value=pages) as fetch,
        ):
            pipeline._process_timeseries_optimized(MagicMock(), self.logger, config, MagicMock(), MagicMock())

        self.assertIsNone(fetch.call_args.args[3].cursor)
        self.assertEqual(state["write_sync_state"].call_args.args[-1], alias_sync.SyncState("fresh", 800))

    def test_the_cursor_is_stored_after_every_page(self) -> None:
        """A run stopped by the function time limit must not send the next run back to the start."""
        config = self._config(run_all=False, update_all=False)
        pages = iter(
            [
                ([node()], alias_sync.SyncState("c1", 1000)),
                ([node()], alias_sync.SyncState("c2", 1000)),
            ]
        )

        with self._patched_state() as state, patch.object(pipeline, "iter_synced_pages", return_value=pages):
            pipeline._process_timeseries_optimized(MagicMock(), self.logger, config, MagicMock(), MagicMock())

        stored = [call.args[-1].cursor for call in state["write_sync_state"].call_args_list]
        self.assertEqual(stored, ["c1", "c2"])

    def test_a_failure_keeps_the_cursor_of_the_last_written_page(self) -> None:
        config = self._config(run_all=False, update_all=False)

        def pages():
            yield [node()], alias_sync.SyncState("c1", 1000)
            raise CogniteAPIError("Unavailable", code=503)

        with (
            self._patched_state() as state,
            patch.object(pipeline, "iter_synced_pages", return_value=pages()),
            self.assertRaises(CogniteAPIError),
        ):
            pipeline._process_timeseries_optimized(MagicMock(), self.logger, config, MagicMock(), MagicMock())

        self.assertEqual(state["write_sync_state"].call_args.args[-1].cursor, "c1")

    def test_an_interrupted_full_read_continues_where_it_stopped(self) -> None:
        """runAll would otherwise start from zero on every run and never finish a large view."""
        config = self._config(run_all=True, update_all=False)

        with (
            self._patched_state(stored_cursor="halfway", full_read_in_progress=True),
            patch.object(pipeline, "iter_synced_pages", return_value=iter([])) as fetch,
        ):
            pipeline._process_timeseries_optimized(MagicMock(), self.logger, config, MagicMock(), MagicMock())

        self.assertEqual(fetch.call_args.args[3].cursor, "halfway")

    def test_a_full_read_is_marked_in_progress_until_the_view_is_done(self) -> None:
        config = self._config(run_all=True, update_all=False)
        pages = iter(
            [
                ([node()], alias_sync.SyncState("c1", 1000)),
                ([node()], alias_sync.SyncState("c2", 1000)),
            ]
        )

        with self._patched_state() as state, patch.object(pipeline, "iter_synced_pages", return_value=pages):
            pipeline._process_timeseries_optimized(MagicMock(), self.logger, config, MagicMock(), MagicMock())

        stored = [call.args[-1] for call in state["write_sync_state"].call_args_list]
        self.assertEqual(
            stored,
            [
                alias_sync.SyncState("c1", 1000, True),
                alias_sync.SyncState("c2", 1000, True),
                alias_sync.SyncState("c2", 1000, False),
            ],
        )

    def test_a_failed_type_does_not_move_the_cursor(self) -> None:
        """The next run must read again what this one did not finish."""
        config = self._config(run_all=False, update_all=False)

        with (
            self._patched_state() as state,
            patch.object(pipeline, "iter_synced_pages", side_effect=CogniteAPIError("Unavailable", code=503)),
            self.assertRaises(CogniteAPIError),
        ):
            pipeline._process_timeseries_optimized(MagicMock(), self.logger, config, MagicMock(), MagicMock())

        state["write_sync_state"].assert_not_called()

    def test_the_timeseries_cursor_is_stored_before_assets_are_read(self) -> None:
        """Each type is finished, stored and released before the next one is read."""
        config = self._config(run_all=False, update_all=False)
        events: list[str] = []

        def read(client, logger, view, state, label):
            events.append(f"read {label}")
            return iter([([], alias_sync.SyncState("c", 1000))])

        with (
            self._patched_state() as state,
            patch.object(pipeline, "iter_synced_pages", side_effect=read),
            patch.object(pipeline, "update_pipeline_run"),
            patch.object(pipeline.gc, "collect", side_effect=lambda: events.append("collect")),
        ):
            state["write_sync_state"].side_effect = lambda *args: events.append(f"write {args[3]}")
            pipeline.metadata_update(MagicMock(), self.logger, {"ExtractionPipelineExtId": "ep"}, config)

        ts_key = alias_sync.sync_state_key(config.data.job.timeseries_view, TS_NODE)
        asset_key = alias_sync.sync_state_key(config.data.job.asset_view, ASSET_NODE)
        self.assertEqual(
            events,
            [f"read {TS_NODE}", f"write {ts_key}", "collect", f"read {ASSET_NODE}", f"write {asset_key}", "collect"],
        )

    def test_process_timeseries_handles_empty_fetch(self) -> None:
        config = self._config(run_all=False, update_all=False)

        with self._patched_state(), patch.object(pipeline, "iter_synced_pages", return_value=iter([])):
            total = pipeline._process_timeseries_optimized(MagicMock(), self.logger, config, MagicMock(), MagicMock())

        self.assertEqual(total, 0)

    def test_a_failed_run_fails_the_function_call(self) -> None:
        """A returned failure dict is a succeeded call to CDF, so the workflow would carry on."""
        with (
            patch("handler._report_usage"),
            patch("handler.load_config_parameters", side_effect=ValueError("bad config")),
            self.assertRaises(ValueError),
        ):
            handler.handle({"ExtractionPipelineExtId": "ep"}, MagicMock())

    def test_invalid_input_fails_before_any_cdf_call(self) -> None:
        for data in ({}, {"ExtractionPipelineExtId": ""}, {"ExtractionPipelineExtId": "ep", "logLevel": "VERBOSE"}):
            client = MagicMock()
            with self.subTest(data=data), patch("handler.load_config_parameters") as load:
                with self.assertRaises(ValidationError):
                    handler.handle(data, client)
                load.assert_not_called()
                self.assertEqual(client.mock_calls, [])


class TestSyncState(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")
        self.parameters = Parameters(runAll=False, rawDb="db", rawTableState="state")

    def test_a_first_run_starts_with_no_cursor(self) -> None:
        client = MagicMock()
        client.raw.rows.retrieve.return_value = None

        state = alias_sync.read_sync_state(client, self.parameters, self.logger, "key")

        self.assertEqual(state, alias_sync.SyncState(None, ALIAS_PAGE_SIZE))

    def test_a_missing_state_store_reads_as_a_first_run(self) -> None:
        """The database and table are created by the first write, so their absence is not a failure."""
        for error in (
            CogniteAPIError("Not found", code=404),
            CogniteAPIError("Database not found", code=400),
            CogniteAPIError("Table not found.", code=400),
        ):
            client = MagicMock()
            client.raw.rows.retrieve.side_effect = error
            with self.subTest(message=error.message, code=error.code):
                state = alias_sync.read_sync_state(client, self.parameters, self.logger, "key")
                self.assertEqual(state, alias_sync.SyncState(None, ALIAS_PAGE_SIZE))

    def test_a_state_read_error_fails_the_run(self) -> None:
        """Only a missing store is a first run. A bad key, empty message, or outage must still fail."""
        for error in (
            CogniteAPIError("Key is invalid", code=400),
            CogniteAPIError(None, code=400),
            CogniteAPIError("Unavailable", code=503),
        ):
            client = MagicMock()
            client.raw.rows.retrieve.side_effect = error
            with self.subTest(message=error.message, code=error.code), self.assertRaises(CogniteAPIError):
                alias_sync.read_sync_state(client, self.parameters, self.logger, "key")

    def test_reading_the_state_creates_nothing(self) -> None:
        client = MagicMock()
        client.raw.rows.retrieve.return_value = MagicMock(
            columns={"syncCursor": "c", "batchSize": 800, "fullReadInProgress": True}
        )

        state = alias_sync.read_sync_state(client, self.parameters, self.logger, "key")

        self.assertEqual(state, alias_sync.SyncState("c", 800, True))
        client.raw.databases.create.assert_not_called()
        client.raw.tables.create.assert_not_called()

    def test_writing_the_state_creates_a_missing_state_table(self) -> None:
        client = MagicMock()
        view = ViewPropertyConfig(schemaSpace="s", instanceSpace="i", externalId="v", version="v1")

        alias_sync.write_sync_state(client, self.parameters, self.logger, "key", view, alias_sync.SyncState("c", 1000))

        self.assertTrue(client.raw.rows.insert.call_args.kwargs.get("ensure_parent"))

    def test_a_failed_cursor_write_does_not_fail_the_run(self) -> None:
        """The previous cursor still holds, so the next run only reads more than it needs to."""
        client = MagicMock()
        client.raw.rows.insert.side_effect = CogniteAPIError("Unavailable", code=503)
        view = ViewPropertyConfig(schemaSpace="s", instanceSpace="i", externalId="v", version="v1")

        alias_sync.write_sync_state(client, self.parameters, self.logger, "key", view, alias_sync.SyncState("c", 1000))


if __name__ == "__main__":
    unittest.main()
