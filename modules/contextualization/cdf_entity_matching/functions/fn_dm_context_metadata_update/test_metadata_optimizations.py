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
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).parent))

from cognite.client.data_classes.data_modeling import NodeApply, ViewId

from logger import CogniteFunctionLogger  # isort: skip
from metadata_optimizations import (  # isort: skip
    BatchProcessor,
    OptimizedMetadataProcessor,
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


class TestOptimizedMetadataProcessor(unittest.TestCase):
    """Test optimized metadata processing"""

    def setUp(self) -> None:
        self.logger = CogniteFunctionLogger("DEBUG")
        self.processor = OptimizedMetadataProcessor(self.logger)
        self.view_id = ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1")

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

        result = self.processor.process_timeseries_metadata(node, self.view_id, "inst_location")

        self.assertIsNotNone(result)
        properties = result.sources[0].properties
        self.assertIn("aliases", properties)
        self.assertIn("23_KA_9101", properties["aliases"])
        self.assertNotIn("tags", properties)
        self.assertNotIn("description", properties)

        print("✅ Timeseries alias enrichment test passed")

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

        result = self.processor.process_timeseries_metadata(node, self.view_id, "inst_location")
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
            node, self.view_id, "inst_location", update_all=True
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
            node, self.view_id, "inst_location", update_all=True
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
            node, self.view_id, "inst_location", update_all=True
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
                "root": {"space": "inst_location", "externalId": "VAL-PH"},
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_location", update_all=True
        )

        self.assertIsNotNone(result)
        properties = result.sources[0].properties
        self.assertEqual(
            properties["aliases"],
            ["operator note", "spare for 23-AB-1234", "23_KA_9101"],
        )
        self.assertIn("discipline:KA", properties["tags"])
        self.assertIn("root:VAL-PH", properties["tags"])
        self.assertNotIn("tag", properties["tags"])
        self.assertNotIn("root:old_root", properties["tags"])

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
                "root": {"space": "inst_location", "externalId": "VAL-PH"},
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_location", update_all=True
        )

        self.assertIsNotNone(result)
        properties = result.sources[0].properties
        self.assertEqual(properties["aliases"], ["23_KA_9101"])
        self.assertIn("root:VAL-PH", properties["tags"])

        print("✅ Asset updateAll re-apply test passed")

    def test_asset_incremental_adds_root_tag_from_relation(self) -> None:
        """Test incremental asset processing adds root tag from relation external id"""
        print("🧪 Testing asset incremental root tag...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {
            asset_view_id: {
                "name": "23-KA-9101",
                "aliases": ["23_KA_9101"],
                "tags": ["discipline:KA"],
                "root": {"space": "inst_location", "externalId": "VAL-PH"},
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_location", update_all=False
        )

        self.assertIsNotNone(result)
        properties = result.sources[0].properties
        self.assertEqual(properties["tags"], ["discipline:KA", "root:VAL-PH"])

        print("✅ Asset incremental root tag test passed")

    def test_asset_incremental_replaces_stale_root_tag(self) -> None:
        """Test incremental asset processing replaces a root tag from an old relation"""
        print("🧪 Testing asset incremental stale root tag...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {
            asset_view_id: {
                "name": "23-KA-9101",
                "aliases": ["23_KA_9101"],
                "tags": ["root:OLD-PH", "discipline:KA"],
                "root": {"space": "inst_location", "externalId": "VAL-PH"},
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_location", update_all=False
        )

        self.assertIsNotNone(result)
        properties = result.sources[0].properties
        self.assertEqual(properties["tags"], ["discipline:KA", "root:VAL-PH"])

        print("✅ Asset incremental stale root tag test passed")

    def test_asset_incremental_removes_root_tag_when_relation_missing(self) -> None:
        """Test incremental asset processing drops the root tag when root is unset"""
        print("🧪 Testing asset incremental root tag removal...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {
            asset_view_id: {
                "name": "23-KA-9101",
                "aliases": ["23_KA_9101"],
                "tags": ["root:OLD-PH", "discipline:KA"],
                "root": None,
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_location", update_all=False
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.sources[0].properties["tags"], ["discipline:KA"])

        print("✅ Asset incremental root tag removal test passed")

    def test_asset_incremental_skips_update_when_only_tag_order_differs(self) -> None:
        """Test incremental asset processing does not rewrite reordered tags"""
        print("🧪 Testing asset incremental tag order no-op...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {
            asset_view_id: {
                "name": "23-KA-9101",
                "aliases": ["23_KA_9101"],
                "tags": ["root:VAL-PH", "discipline:KA"],
                "root": {"space": "inst_location", "externalId": "VAL-PH"},
            }
        }

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_location", update_all=False
        )

        self.assertIsNone(result)

        print("✅ Asset incremental tag order no-op test passed")

    def test_asset_update_all_skips_node_without_view_properties(self) -> None:
        """updateAll must not blank managed properties when the view payload is empty"""
        print("🧪 Testing asset skip on empty view properties...")

        asset_view_id = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        other_view_id = ViewId(space="cdf_cdm", external_id="CogniteDescribable", version="v1")
        node = MagicMock()
        node.external_id = "23-KA-9101"
        node.properties = {other_view_id: {"name": "23-KA-9101"}}

        result = self.processor.process_asset_metadata(
            node, asset_view_id, "inst_location", update_all=True
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
            node, self.view_id, "inst_location", update_all=True
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
            node, asset_view_id, "inst_location", update_all=True
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
            node, self.view_id, "inst_location", update_all=True
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
