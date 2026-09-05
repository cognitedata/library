#!/usr/bin/env python3
"""
Comprehensive Test Suite for Metadata Update Optimizations

This script tests all optimization features for both entity matching and P&ID annotation
metadata update functions without requiring a full CDF environment setup.
"""

import sys
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes.data_modeling import NodeApply, ViewId
from cognite.client.exceptions import CogniteAPIError, CogniteConnectionError

from constants import DEFAULT_ALIAS_PATTERN  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip
from alias_optimizations import (  # isort: skip
    BatchProcessor,
    AliasRule,
    OptimizedMetadataProcessor,
    _unmanaged_aliases,
    PerformanceBenchmark,
    cleanup_memory,
    monitor_memory_usage,
    optimize_metadata_processing,
    time_operation,
)


class TestPerformanceMonitoring(unittest.TestCase):
    """Test performance monitoring utilities"""

    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")

    def test_time_operation(self) -> None:
        """Test timing context manager"""
        print("🧪 Testing time_operation...")

        with time_operation("Test operation", self.logger):
            time.sleep(0.1)

        print("✅ time_operation test passed")

    def test_memory_monitoring(self) -> None:
        """Test memory monitoring"""
        print("🧪 Testing memory monitoring...")

        monitor_memory_usage(self.logger, "Test memory check")
        cleanup_memory()

        print("✅ Memory monitoring test passed")


class TestBatchProcessing(unittest.TestCase):
    """Test batch processing utilities"""

    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")

    def test_apply_updates_chunks_by_constructor_batch_size(self) -> None:
        """Updates are applied in chunks of the batch size given to the constructor"""
        client = MagicMock()
        updates = [NodeApply(space="sp", external_id=f"item-{i}") for i in range(7)]

        applied = BatchProcessor(batch_size=3).apply_updates_in_batches(client, updates, self.logger)

        self.assertEqual(applied, 7)
        batch_sizes = [len(call.args[0]) for call in client.data_modeling.instances.apply.call_args_list]
        self.assertEqual(batch_sizes, [3, 3, 1])

    def test_apply_updates_with_no_updates(self) -> None:
        """An empty update list applies nothing"""
        client = MagicMock()

        applied = BatchProcessor().apply_updates_in_batches(client, [], self.logger)

        self.assertEqual(applied, 0)
        client.data_modeling.instances.apply.assert_not_called()

    def test_a_rejected_property_is_not_retried_or_split(self) -> None:
        """A misconfigured property fails the same way in a smaller batch, so try it once.

        Retrying and then splitting spends a minute of the function's runtime on a
        request the API can never accept, and buries the one message that explains why.
        """
        client = MagicMock()
        client.data_modeling.instances.apply.side_effect = CogniteAPIError(
            "Property 'labels' does not exist in view 'cdf_cdm:CogniteAsset/v1'", code=400
        )
        updates = [NodeApply(space="sp", external_id=f"item-{i}") for i in range(4)]

        with patch("tenacity.nap.time.sleep"), self.assertRaises(CogniteAPIError) as caught:
            BatchProcessor(batch_size=4).apply_updates_in_batches(client, updates, self.logger)

        self.assertIn("does not exist in view", str(caught.exception))
        self.assertEqual(client.data_modeling.instances.apply.call_count, 1)

    def test_a_batch_the_api_calls_too_large_is_split_at_any_batch_size(self) -> None:
        """Splitting takes a quarter of the batch size, which rounds to zero below four.

        The chunk size then has to stay at least one, or the split loop raises a
        ValueError that buries the API error it was trying to recover from.
        """
        client = MagicMock()
        client.data_modeling.instances.apply.side_effect = [
            CogniteAPIError("Request too large", code=413),
            None,
        ]
        updates = [NodeApply(space="sp", external_id="item-0")]

        with patch("tenacity.nap.time.sleep"):
            applied = BatchProcessor(batch_size=1).apply_updates_in_batches(client, updates, self.logger)

        self.assertEqual(applied, 1)
        self.assertEqual(client.data_modeling.instances.apply.call_count, 2)

    def test_a_dropped_connection_is_retried(self) -> None:
        """A connection error carries no status code, and the next attempt may connect."""
        client = MagicMock()
        client.data_modeling.instances.apply.side_effect = [
            CogniteConnectionError("Connection reset"),
            None,
        ]
        updates = [NodeApply(space="sp", external_id="item-0")]

        with patch("tenacity.nap.time.sleep"):
            applied = BatchProcessor(batch_size=1).apply_updates_in_batches(client, updates, self.logger)

        self.assertEqual(applied, 1)
        self.assertEqual(client.data_modeling.instances.apply.call_count, 2)

    def test_a_bug_is_not_retried(self) -> None:
        """A bug in our own code fails identically every attempt, so it fails once."""
        client = MagicMock()
        client.data_modeling.instances.apply.side_effect = TypeError("bad argument")
        updates = [NodeApply(space="sp", external_id="item-0")]

        with patch("tenacity.nap.time.sleep"), self.assertRaises(TypeError):
            BatchProcessor(batch_size=1).apply_updates_in_batches(client, updates, self.logger)

        self.assertEqual(client.data_modeling.instances.apply.call_count, 1)

    def test_a_rate_limited_batch_is_still_retried(self) -> None:
        """Rate limiting is transient, so it keeps its retry."""
        client = MagicMock()
        client.data_modeling.instances.apply.side_effect = [
            CogniteAPIError("Too many requests", code=429),
            None,
        ]
        updates = [NodeApply(space="sp", external_id="item-0")]

        with patch("tenacity.nap.time.sleep"):
            applied = BatchProcessor(batch_size=1).apply_updates_in_batches(client, updates, self.logger)

        self.assertEqual(applied, 1)
        self.assertEqual(client.data_modeling.instances.apply.call_count, 2)


# A site-specific pattern used to check that configuration, not the built-in default,
# decides the alias. The "_" in the separator class is deliberate: the generated alias
# joins the groups with "_", and the pattern has to match that to recognise its own work.
PUMP_PATTERN = r"([A-Z]{3})[-_]?(\d{4})"

# The document number patterns shipped in the module's default.config.yaml, for
# fileAliasPattern. Kept in sync by hand; the tests below pin the behaviour they promise.
DOCUMENT_PATTERNS = [
    r"(?<![A-Z])([A-Z]{2,4}-[0-9]+-[A-Z]-[0-9]+-[0-9]+)",
    r"(?<![A-Z])([A-Z]{2,4}-[0-9]+-[A-Z]-[0-9]+)(?:-[0-9]+)?",
    r"([0-9]{2})[-_.:]([A-Z]{2,3})[-_.:]([0-9]{4,5})",
]


class TestOptimizedMetadataProcessor(unittest.TestCase):
    """Test optimized metadata processing"""

    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")
        self.processor = OptimizedMetadataProcessor(self.logger)
        self.view_id = ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1")
        self.file_view_id = ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1")

    def test_timeseries_alias_enrichment(self) -> None:
        """Test timeseries metadata updates aliases only"""
        print("🧪 Testing timeseries alias enrichment...")

        node = MagicMock()
        node.external_id = "pi:160001"
        node.properties = {
            self.view_id: {
                "name": "VAL_23-KA-9101:X.Value",
                "description": "existing description",
                "aliases": ["existing"],
                "tags": ["existing:tag"],
            }
        }

        result = self.processor.process_timeseries_metadata(node, self.view_id, "inst_cfihos_oil_and_gas")

        self.assertIsNotNone(result)
        properties = result.sources[0].properties
        self.assertIn("aliases", properties)
        self.assertIn("23_KA_9101", properties["aliases"])
        self.assertNotIn("tags", properties)
        self.assertNotIn("description", properties)

        print("✅ Timeseries alias enrichment test passed")

    def test_every_matching_pattern_contributes_an_alias(self) -> None:
        """Names that follow two conventions at once yield an alias for each."""
        processor = OptimizedMetadataProcessor(
            self.logger,
            timeseries_alias_rule=AliasRule.from_config([DEFAULT_ALIAS_PATTERN, PUMP_PATTERN]),
        )
        node = MagicMock()
        node.external_id = "pi:160020"
        node.properties = {self.view_id: {"name": "VAL_23-KA-9101_PMP1234", "aliases": []}}

        result = processor.process_timeseries_metadata(node, self.view_id, "inst_cfihos_oil_and_gas")

        self.assertEqual(result.sources[0].properties["aliases"], ["23_KA_9101", "PMP_1234"])

    def test_longest_selection_keeps_only_the_most_specific_alias(self) -> None:
        """With overlapping conventions, the longest match is the most specific one."""
        processor = OptimizedMetadataProcessor(
            self.logger,
            timeseries_alias_rule=AliasRule.from_config(
                [DEFAULT_ALIAS_PATTERN, PUMP_PATTERN], selection="longest"
            ),
        )
        node = MagicMock()
        node.external_id = "pi:160021"
        node.properties = {self.view_id: {"name": "VAL_23-KA-9101_PMP1234", "aliases": []}}

        result = processor.process_timeseries_metadata(node, self.view_id, "inst_cfihos_oil_and_gas")

        self.assertEqual(result.sources[0].properties["aliases"], ["23_KA_9101"])

    def test_equally_long_aliases_are_resolved_by_configured_order(self) -> None:
        """A tie must not depend on dict or set ordering, so the first pattern wins."""
        first = r"([A-Z]{3})[-_]?(\d{4})"
        second = r"(\d{4})[-_]?([A-Z]{3})"
        node = MagicMock()
        node.external_id = "pi:160022"
        node.properties = {self.view_id: {"name": "PMP1234 and 5678XYZ", "aliases": []}}

        processor = OptimizedMetadataProcessor(
            self.logger, timeseries_alias_rule=AliasRule.from_config([second, first], selection="longest")
        )
        result = processor.process_timeseries_metadata(node, self.view_id, "inst_cfihos_oil_and_gas")

        self.assertEqual(result.sources[0].properties["aliases"], ["5678_XYZ"])

    def test_update_all_reclaims_aliases_from_every_configured_pattern(self) -> None:
        """Selecting only the longest must not orphan aliases an earlier run wrote."""
        processor = OptimizedMetadataProcessor(
            self.logger,
            timeseries_alias_rule=AliasRule.from_config(
                [DEFAULT_ALIAS_PATTERN, PUMP_PATTERN], selection="longest"
            ),
        )
        node = MagicMock()
        node.external_id = "pi:160023"
        node.properties = {
            self.view_id: {
                "name": "VAL_23-KA-9101_PMP1234",
                # PMP_1234 was written when the mode was "all"; it is still ours to remove.
                "aliases": ["operator note", "23_KA_9101", "PMP_1234"],
            }
        }

        result = processor.process_timeseries_metadata(
            node, self.view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertEqual(
            result.sources[0].properties["aliases"], ["operator note", "23_KA_9101"]
        )

    def test_configured_pattern_drives_timeseries_alias_generation(self) -> None:
        """A site whose tags do not follow the default shape configures its own pattern."""
        processor = OptimizedMetadataProcessor(
            self.logger, timeseries_alias_rule=AliasRule.from_config([PUMP_PATTERN])
        )
        node = MagicMock()
        node.external_id = "pi:160010"
        node.properties = {self.view_id: {"name": "PMP1234 discharge pressure", "aliases": []}}

        result = processor.process_timeseries_metadata(node, self.view_id, "inst_cfihos_oil_and_gas")

        self.assertEqual(result.sources[0].properties["aliases"], ["PMP_1234"])

    def test_an_unmatched_optional_group_is_left_out_of_the_alias(self) -> None:
        """A configured pattern may make a group optional, and then it captures None.

        Joining that straight into the alias raises a TypeError, so a name matching only
        the mandatory part of the pattern would take the whole run down.
        """
        processor = OptimizedMetadataProcessor(
            self.logger, timeseries_alias_rule=AliasRule.from_config([r"([A-Z]{3})?[-_]?(\d{4})"])
        )
        node = MagicMock()
        node.external_id = "pi:160030"
        node.properties = {self.view_id: {"name": "1234 discharge pressure", "aliases": []}}

        result = processor.process_timeseries_metadata(node, self.view_id, "inst_cfihos_oil_and_gas")

        self.assertEqual(result.sources[0].properties["aliases"], ["1234"])

    def test_each_view_uses_its_own_pattern(self) -> None:
        """The asset pattern must not be applied to timeseries names, or the reverse."""
        processor = OptimizedMetadataProcessor(
            self.logger,
            timeseries_alias_rule=AliasRule.from_config([PUMP_PATTERN]),
            asset_alias_rule=AliasRule.from_config([r"(\d{2})[-_]([A-Z]{2,3})"]),
        )
        asset_view = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {asset_view: {"name": "23-KA pump", "aliases": [], "tags": []}}

        result = processor.process_asset_metadata(node, asset_view, "inst_cfihos_oil_and_gas")

        self.assertEqual(result.sources[0].properties["aliases"], ["23_KA"])

    def test_update_all_rebuilds_only_aliases_the_configured_pattern_generates(self) -> None:
        """Managed aliases follow the configured pattern, not the built-in default shape."""
        processor = OptimizedMetadataProcessor(self.logger, timeseries_alias_rule=AliasRule.from_config([PUMP_PATTERN]))
        node = MagicMock()
        node.external_id = "pi:160011"
        node.properties = {
            self.view_id: {
                "name": "PMP1234 discharge pressure",
                # The first is what this pattern generates and is rebuilt; the second is
                # the default pattern's shape, which is now someone else's data.
                "aliases": ["PMP_1234", "23_KA_9101"],
            }
        }

        result = processor.process_timeseries_metadata(
            node, self.view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertEqual(result.sources[0].properties["aliases"], ["23_KA_9101", "PMP_1234"])

    def test_a_stale_generated_alias_is_dropped_when_the_name_changes(self) -> None:
        """The point of rebuilding: an alias from a previous name must not linger.

        This is why a pattern has to tolerate "_" between its groups - that is the
        separator the generated alias uses, and how the function recognises its own work.
        """
        processor = OptimizedMetadataProcessor(self.logger, timeseries_alias_rule=AliasRule.from_config([PUMP_PATTERN]))
        node = MagicMock()
        node.external_id = "pi:160012"
        node.properties = {self.view_id: {"name": "PMP9999 discharge pressure", "aliases": ["PMP_1234"]}}

        result = processor.process_timeseries_metadata(
            node, self.view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertEqual(result.sources[0].properties["aliases"], ["PMP_9999"])

    def test_file_aliases_cover_the_name_without_extension_and_the_tag(self) -> None:
        """A document is findable both by its bare file name and by the tag it carries."""
        node = MagicMock()
        node.external_id = "file:4001"
        node.properties = {self.file_view_id: {"name": "PID_23-KA-9101_rev3.pdf", "aliases": []}}

        result = self.processor.process_file_metadata(node, self.file_view_id, "inst_cfihos_oil_and_gas")

        self.assertEqual(
            result.sources[0].properties["aliases"],
            ["PID_23-KA-9101_rev3", "23_KA_9101"],
        )

    def test_a_file_name_without_an_extension_is_used_as_is(self) -> None:
        """Nothing to strip, so the name itself becomes the alias."""
        node = MagicMock()
        node.external_id = "file:4002"
        node.properties = {self.file_view_id: {"name": "23-KA-9101", "aliases": []}}

        result = self.processor.process_file_metadata(node, self.file_view_id, "inst_cfihos_oil_and_gas")

        self.assertEqual(result.sources[0].properties["aliases"], ["23-KA-9101", "23_KA_9101"])

    def test_files_use_their_own_configured_pattern(self) -> None:
        """Documents may be named on a different convention than the assets they describe."""
        processor = OptimizedMetadataProcessor(
            self.logger,
            asset_alias_rule=AliasRule.from_config([r"(\d{2})[-_]([A-Z]{2,3})"]),
            file_alias_rule=AliasRule.from_config([PUMP_PATTERN]),
        )
        node = MagicMock()
        node.external_id = "file:4003"
        node.properties = {self.file_view_id: {"name": "PMP1234_datasheet.pdf", "aliases": []}}

        result = processor.process_file_metadata(node, self.file_view_id, "inst_cfihos_oil_and_gas")

        self.assertEqual(result.sources[0].properties["aliases"], ["PMP1234_datasheet", "PMP_1234"])

    def test_document_number_aliases_keep_their_separators(self) -> None:
        """The shipped document patterns, which must not rewrite dashes as underscores.

        A single capture group per pattern is what preserves them, since the alias is the
        groups joined by "_".
        """
        processor = OptimizedMetadataProcessor(
            self.logger, file_alias_rule=AliasRule.from_config(DOCUMENT_PATTERNS)
        )
        node = MagicMock()
        node.external_id = "file:4010"
        node.properties = {self.file_view_id: {"name": "PH-25578-P-4110006-001.pdf", "aliases": []}}

        result = processor.process_file_metadata(node, self.file_view_id, "inst_cfihos_oil_and_gas")

        self.assertEqual(
            result.sources[0].properties["aliases"],
            ["PH-25578-P-4110006-001", "PH-25578-P-4110006"],
        )

    def test_a_document_alias_without_its_sheet_number_is_still_ours(self) -> None:
        """The sheet number sits outside the group, so the short alias must round-trip.

        Without that, updateAll would treat the alias it just wrote as hand-curated.
        """
        rule = AliasRule.from_config(DOCUMENT_PATTERNS)

        self.assertEqual(_unmanaged_aliases(["PH-25578-P-4110006", "operator note"], rule), ["operator note"])

    def test_a_longer_prefix_does_not_yield_a_truncated_document_alias(self) -> None:
        """Matching from the second letter of a prefix would write a wrong document number."""
        processor = OptimizedMetadataProcessor(
            self.logger, file_alias_rule=AliasRule.from_config(DOCUMENT_PATTERNS)
        )
        node = MagicMock()
        node.external_id = "file:4011"
        node.properties = {self.file_view_id: {"name": "SHEET-1-A-2.pdf", "aliases": []}}

        result = processor.process_file_metadata(node, self.file_view_id, "inst_cfihos_oil_and_gas")

        self.assertEqual(result.sources[0].properties["aliases"], ["SHEET-1-A-2"])

    def test_file_skips_update_when_aliases_already_present(self) -> None:
        """No write when there is nothing to add, so reruns stay cheap."""
        node = MagicMock()
        node.external_id = "file:4004"
        node.properties = {
            self.file_view_id: {
                "name": "PID_23-KA-9101_rev3.pdf",
                "aliases": ["PID_23-KA-9101_rev3", "23_KA_9101"],
            }
        }

        result = self.processor.process_file_metadata(node, self.file_view_id, "inst_cfihos_oil_and_gas")

        self.assertIsNone(result)

    def test_file_update_all_rebuilds_generated_aliases_and_keeps_curated_ones(self) -> None:
        """A tag alias left over from a previous name is replaced, a curated note is not."""
        node = MagicMock()
        node.external_id = "file:4005"
        node.properties = {
            self.file_view_id: {
                "name": "PID_23-KA-9101_rev3.pdf",
                "aliases": ["manual note", "23_AB_0001"],
            }
        }

        result = self.processor.process_file_metadata(
            node, self.file_view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertEqual(
            result.sources[0].properties["aliases"],
            ["manual note", "PID_23-KA-9101_rev3", "23_KA_9101"],
        )

    def test_timeseries_skips_update_when_aliases_unchanged(self) -> None:
        """Test timeseries processing skips DM update when aliases already match"""
        print("🧪 Testing timeseries skip unchanged aliases...")

        node = MagicMock()
        node.external_id = "pi:160002"
        node.properties = {
            self.view_id: {
                "name": "VAL_23-KA-9101:X.Value",
                "aliases": ["existing", "23_KA_9101"],
            }
        }

        result = self.processor.process_timeseries_metadata(node, self.view_id, "inst_cfihos_oil_and_gas")
        self.assertIsNone(result)

        print("✅ Timeseries skip unchanged aliases test passed")

    def test_timeseries_update_all_replaces_stale_managed_alias(self) -> None:
        """Test updateAll drops a managed alias left over from an earlier name"""
        print("🧪 Testing timeseries updateAll...")

        node = MagicMock()
        node.external_id = "pi:160003"
        node.properties = {
            self.view_id: {
                "name": "VAL_23-KA-9101:X.Value",
                "aliases": ["11_PT_2222", "23_KA_9101"],
            }
        }

        result = self.processor.process_timeseries_metadata(
            node, self.view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertIsNotNone(result)
        properties = result.sources[0].properties
        self.assertEqual(properties["aliases"], ["23_KA_9101"])

        print("✅ Timeseries updateAll test passed")

    def test_timeseries_update_all_keeps_unmanaged_aliases(self) -> None:
        """Test updateAll leaves aliases this function never generated in place"""
        print("🧪 Testing timeseries updateAll keeps unmanaged aliases...")

        node = MagicMock()
        node.external_id = "pi:160005"
        node.properties = {
            self.view_id: {
                "name": "VAL_23-KA-9101:X.Value",
                # Curated by hand: the second one embeds a tag pattern but is not a
                # value this function would ever have written.
                "aliases": ["operator note", "spare for 23-AB-1234"],
            }
        }

        result = self.processor.process_timeseries_metadata(
            node, self.view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertIsNotNone(result)
        self.assertEqual(
            result.sources[0].properties["aliases"],
            ["operator note", "spare for 23-AB-1234", "23_KA_9101"],
        )

        print("✅ Timeseries updateAll unmanaged alias test passed")

    def test_timeseries_update_all_applies_even_when_aliases_already_correct(self) -> None:
        """Test updateAll writes managed metadata even when values already match"""
        print("🧪 Testing timeseries updateAll re-apply...")

        node = MagicMock()
        node.external_id = "pi:160004"
        node.properties = {
            self.view_id: {
                "name": "VAL_23-KA-9101:X.Value",
                "aliases": ["23_KA_9101"],
            }
        }

        result = self.processor.process_timeseries_metadata(
            node, self.view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.sources[0].properties["aliases"], ["23_KA_9101"])

        print("✅ Timeseries updateAll re-apply test passed")

    def test_asset_update_all_replaces_stale_managed_alias(self) -> None:
        """Test updateAll drops managed aliases but keeps unmanaged ones"""
        print("🧪 Testing asset updateAll...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {
            asset_view_id: {
                "name": "23-KA-9101",
                "aliases": ["operator note", "11_PT_2222", "spare for 23-AB-1234"],
                "tags": ["discipline:KA", "tag", "root:old_root"],
                "root": {"space": "inst_cfihos_oil_and_gas", "externalId": "VAL-PH"},
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertIsNotNone(result)
        properties = result.sources[0].properties
        self.assertEqual(
            properties["aliases"],
            ["operator note", "spare for 23-AB-1234", "23_KA_9101"],
        )
        self.assertNotIn("tags", properties)

        print("✅ Asset updateAll test passed")

    def test_asset_update_all_applies_even_when_metadata_already_correct(self) -> None:
        """Test updateAll writes managed asset metadata even when values already match"""
        print("🧪 Testing asset updateAll re-apply...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {
            asset_view_id: {
                "name": "23-KA-9101",
                "aliases": ["23_KA_9101"],
                "tags": ["discipline:KA", "root:VAL-PH"],
                "root": {"space": "inst_cfihos_oil_and_gas", "externalId": "VAL-PH"},
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertIsNotNone(result)
        properties = result.sources[0].properties
        self.assertEqual(properties["aliases"], ["23_KA_9101"])
        self.assertNotIn("tags", properties)

        print("✅ Asset updateAll re-apply test passed")

    def test_asset_incremental_adds_missing_alias(self) -> None:
        """Test incremental asset processing adds the alias its name yields"""
        print("🧪 Testing asset incremental alias...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {
            asset_view_id: {
                "name": "23-KA-9101",
                "aliases": [],
                "root": {"space": "inst_cfihos_oil_and_gas", "externalId": "VAL-PH"},
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_cfihos_oil_and_gas", update_all=False
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.sources[0].properties["aliases"], ["23_KA_9101"])

        print("✅ Asset incremental alias test passed")

    def test_asset_tags_are_never_written(self) -> None:
        """This function manages aliases only, so tags stay exactly as the site set them.

        Writing a tag property the configured view does not have is rejected for the
        whole batch, so the safe thing is to touch none of them.
        """
        print("🧪 Testing asset tags left untouched...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {
            asset_view_id: {
                "name": "23-KA-9101",
                "aliases": [],
                "tags": ["discipline:KA", "root:OLD-PH"],
                "root": {"space": "inst_cfihos_oil_and_gas", "externalId": "VAL-PH"},
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_cfihos_oil_and_gas", update_all=False
        )

        self.assertIsNotNone(result)
        self.assertEqual(list(result.sources[0].properties), ["aliases"])

        print("✅ Asset tags untouched test passed")

    def test_asset_incremental_skips_update_when_aliases_already_complete(self) -> None:
        """Test incremental asset processing writes nothing when the alias is present"""
        print("🧪 Testing asset incremental no-op...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {
            asset_view_id: {
                "name": "23-KA-9101",
                "aliases": ["23_KA_9101"],
                "tags": ["root:VAL-PH", "discipline:KA"],
                "root": {"space": "inst_cfihos_oil_and_gas", "externalId": "VAL-PH"},
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_cfihos_oil_and_gas", update_all=False
        )

        self.assertIsNone(result)

        print("✅ Asset incremental no-op test passed")

    def test_asset_update_all_skips_node_without_view_properties(self) -> None:
        """updateAll must not blank managed properties when the view payload is empty"""
        print("🧪 Testing asset skip on empty view properties...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        other_view_id = ViewId(space="cdf_cdm", external_id="CogniteDescribable", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {other_view_id: {"name": "23-KA-9101"}}

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertIsNone(result)

        print("✅ Asset skip on empty view properties test passed")

    def test_timeseries_update_all_skips_node_without_view_properties(self) -> None:
        """updateAll must not blank managed properties when the view payload is empty"""
        print("🧪 Testing timeseries skip on empty view properties...")

        node = MagicMock()
        node.external_id = "pi:160005"
        node.properties = {self.view_id: {}}

        result = self.processor.process_timeseries_metadata(
            node, self.view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertIsNone(result)

        print("✅ Timeseries skip on empty view properties test passed")

    def test_asset_skips_node_with_none_properties(self) -> None:
        """Nodes with no properties populated must be skipped safely"""
        print("🧪 Testing asset skip on None properties...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = None

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertIsNone(result)

        print("✅ Asset skip on None properties test passed")

    def test_timeseries_skips_node_with_none_properties(self) -> None:
        """Nodes with no properties populated must be skipped safely"""
        print("🧪 Testing timeseries skip on None properties...")

        node = MagicMock()
        node.external_id = "pi:160005"
        node.properties = None

        result = self.processor.process_timeseries_metadata(
            node, self.view_id, "inst_cfihos_oil_and_gas", update_all=True
        )

        self.assertIsNone(result)

        print("✅ Timeseries skip on None properties test passed")

    def test_alias_generation_caching(self) -> None:
        """Test alias generation with caching"""
        print("🧪 Testing alias generation caching...")

        tag = "VAL_23-KA-9101:X.Value"

        # Test timeseries alias generation
        aliases1 = self.processor._get_timeseries_alias_list_optimized(
            tag, ("existing",)
        )
        aliases2 = self.processor._get_timeseries_alias_list_optimized(
            tag, ("existing",)
        )  # Should use cache

        self.assertEqual(aliases1, aliases2)
        self.assertIn("existing", aliases1)
        self.assertIn("23_KA_9101", aliases1)

        # Test asset alias generation
        asset_aliases = self.processor._get_asset_alias_list_optimized(
            "23-KA-9101", ("existing",)
        )
        self.assertIn("existing", asset_aliases)
        self.assertIn("23_KA_9101", asset_aliases)

        print("✅ Alias generation caching test passed")

    def test_processing_statistics(self) -> None:
        """Test processing statistics collection"""
        print("🧪 Testing processing statistics...")

        # Mock some processing
        self.processor.stats['processed'] = 100
        self.processor.stats['updated'] = 75

        stats = self.processor.get_stats()

        self.assertEqual(stats['processed'], 100)
        self.assertEqual(stats['updated'], 75)
        self.assertEqual(stats['update_rate'], 0.75)

        print("✅ Processing statistics test passed")


class TestPerformanceBenchmark(unittest.TestCase):
    """Test performance benchmarking"""

    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")
        self.benchmark = PerformanceBenchmark(self.logger)

    def test_function_benchmarking(self) -> None:
        """Test function benchmarking"""
        print("🧪 Testing function benchmarking...")

        def test_function(x: int, y: int) -> int:
            time.sleep(0.05)
            return x * y

        result = self.benchmark.benchmark_function(
            "Test multiplication", test_function, 5, 6
        )

        self.assertEqual(result, 30)
        self.assertIn("Test multiplication", self.benchmark.benchmarks)
        self.assertEqual(len(self.benchmark.benchmarks["Test multiplication"]), 1)

        print("✅ Function benchmarking test passed")

    def test_benchmark_summary(self) -> None:
        """Test benchmark summary logging"""
        print("🧪 Testing benchmark summary...")

        # Add some mock benchmark data
        self.benchmark.benchmarks = {
            "Operation 1": [1.0, 1.2, 0.8],
            "Operation 2": [0.5, 0.6],
        }

        # This should not raise an exception
        self.benchmark.log_summary()

        print("✅ Benchmark summary test passed")


class TestGlobalOptimizations(unittest.TestCase):
    """Test global optimization utilities"""

    def test_optimize_metadata_processing(self) -> None:
        """Test global optimization application"""
        print("🧪 Testing global optimizations...")

        result = optimize_metadata_processing()
        self.assertTrue(result)

        print("✅ Global optimizations test passed")


class TestIntegrationScenarios(unittest.TestCase):
    """Test integration scenarios and real-world usage patterns"""

    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")

    def test_full_optimization_workflow(self) -> None:
        """Test full optimization workflow"""
        print("🧪 Testing full optimization workflow...")

        # Apply global optimizations
        optimize_metadata_processing()

        # Initialize components
        processor = OptimizedMetadataProcessor(self.logger)
        benchmark = PerformanceBenchmark(self.logger)

        # Simulate processing workflow
        start_time = time.time()

        # Mock some processing operations
        def mock_operation() -> str:
            time.sleep(0.01)
            return "processed"

        result = benchmark.benchmark_function("Mock operation", mock_operation)

        end_time = time.time()

        self.assertEqual(result, "processed")
        self.assertLess(end_time - start_time, 0.1)  # Should be fast

        # Check stats
        stats = processor.get_stats()
        self.assertIsInstance(stats, dict)

        print("✅ Full optimization workflow test passed")

    def test_large_dataset_simulation(self) -> None:
        """Test optimization performance with simulated large dataset"""
        print("🧪 Testing large dataset simulation...")

        processor = OptimizedMetadataProcessor(self.logger)

        # Simulate processing many items
        start_time = time.time()

        for i in range(1000):
            # Test cached operations
            aliases = processor._get_timeseries_alias_list_optimized(
                f"test-item-{i % 10}", ("existing",)  # Reuse names to test caching
            )
            self.assertIsInstance(aliases, list)

        end_time = time.time()
        processing_time = end_time - start_time

        print(f"   Processed 1000 items in {processing_time:.3f}s")
        self.assertLess(processing_time, 1.0)  # Should be very fast due to caching

        print("✅ Large dataset simulation test passed")


if __name__ == "__main__":
    unittest.main()
