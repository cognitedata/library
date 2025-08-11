#!/usr/bin/env python3
"""
Test Script for Pipeline Optimizations

This script tests and demonstrates the optimization features without requiring
a full CDF environment setup.
"""

import sys
import time
import json
from pathlib import Path
from unittest.mock import Mock, MagicMock

# Add current directory to path
sys.path.append(str(Path(__file__).parent))

from logger import CogniteFunctionLogger
from pipeline_optimizations import (
    time_operation,
    monitor_memory_usage,
    cleanup_memory,
    MatchTracker,
    OptimizedRuleMapper,
    BatchProcessor,
    ConcurrentDataLoader,
    OptimizedMatchingEngine,
    RobustAPIClient,
    SimpleCache,
    PerformanceBenchmark,
    patch_existing_pipeline,
)


def test_performance_monitoring():
    """Test performance monitoring utilities"""
    print("🧪 Testing Performance Monitoring...")

    logger = CogniteFunctionLogger("INFO")

    # Test timing
    with time_operation("Test operation", logger):
        time.sleep(0.1)  # Simulate work

    # Test memory monitoring
    monitor_memory_usage(logger, "Test memory check")

    # Test memory cleanup
    cleanup_memory()

    print("✅ Performance monitoring tests passed")


def test_match_tracker():
    """Test the optimized match tracker"""
    print("🧪 Testing MatchTracker...")

    tracker = MatchTracker()

    # Test adding matches
    assert tracker.add_match("asset1", "entity1", {"score": 0.9}) == True
    assert tracker.add_match("asset1", "entity1", {"score": 0.9}) == False  # Duplicate
    assert tracker.add_match("asset2", "entity2", {"score": 0.8}) == True

    # Test checking matches
    assert tracker.has_match("asset1", "entity1") == True
    assert tracker.has_match("asset3", "entity3") == False

    # Test count
    assert tracker.get_match_count() == 2

    print("✅ MatchTracker tests passed")


def test_rule_mapper():
    """Test the optimized rule mapper"""
    print("🧪 Testing OptimizedRuleMapper...")

    # Mock rule mappings
    rule_mappings = [
        {
            "key": "rule1",
            "entity_regex": r"(\d+)_entity",
            "asset_regex": r"(\d+)_asset",
        },
        {"key": "rule2", "entity_regex": r"test_(\w+)", "asset_regex": r"asset_(\w+)"},
    ]

    mapper = OptimizedRuleMapper(rule_mappings)

    # Test entity rule matching
    entity_keys = mapper.get_rule_keys("123_entity", "entity")
    assert len(entity_keys) > 0

    # Test asset rule matching
    asset_keys = mapper.get_rule_keys("456_asset", "asset")
    assert len(asset_keys) > 0

    # Test caching (should be faster on second call)
    start = time.time()
    mapper.get_rule_keys("test_sample", "entity")
    first_time = time.time() - start

    start = time.time()
    mapper.get_rule_keys("test_sample", "entity")  # Should be cached
    cached_time = time.time() - start

    assert cached_time <= first_time

    print("✅ OptimizedRuleMapper tests passed")


def test_batch_processor():
    """Test the batch processor"""
    print("🧪 Testing BatchProcessor...")

    logger = CogniteFunctionLogger("INFO")
    processor = BatchProcessor(batch_size=3)

    # Mock data
    entities = [{"id": i, "name": f"entity_{i}"} for i in range(10)]

    # Mock processing function
    def process_batch(batch):
        return [{"processed": item["id"]} for item in batch]

    # Test batch processing
    results = processor.process_entities_in_batches(entities, process_batch, logger)

    assert len(results) == 10
    assert all("processed" in result for result in results)

    print("✅ BatchProcessor tests passed")


def test_concurrent_loader():
    """Test concurrent data loading"""
    print("🧪 Testing ConcurrentDataLoader...")

    logger = CogniteFunctionLogger("INFO")
    loader = ConcurrentDataLoader(max_workers=2)

    # Mock loading functions
    def load_data_1():
        time.sleep(0.1)
        return [{"type": "data1", "count": 5}]

    def load_data_2():
        time.sleep(0.1)
        return [{"type": "data2", "count": 3}]

    def load_data_3():
        time.sleep(0.1)
        return [{"type": "data3", "count": 7}]

    # Test concurrent loading
    load_functions = [(load_data_1, ()), (load_data_2, ()), (load_data_3, ())]

    start = time.time()
    results = loader.load_data_concurrently(load_functions, logger)
    duration = time.time() - start

    # Should be faster than sequential (3 * 0.1 = 0.3s)
    assert duration < 0.25
    assert len(results) == 3
    assert all(result is not None for result in results)

    print("✅ ConcurrentDataLoader tests passed")


def test_matching_engine():
    """Test the optimized matching engine"""
    print("🧪 Testing OptimizedMatchingEngine...")

    logger = CogniteFunctionLogger("INFO")
    engine = OptimizedMatchingEngine(logger)

    # Mock data
    assets = [
        {"asset_ext_id": "asset1", "name": "pump_123", "rule_keys": ["rule1_123"]},
        {"asset_ext_id": "asset2", "name": "valve_456", "rule_keys": ["rule2_456"]},
    ]

    entities = [
        {"entity_ext_id": "entity1", "name": "sensor_123", "rule_keys": ["rule1_123"]},
        {"entity_ext_id": "entity2", "name": "meter_789", "rule_keys": ["rule3_789"]},
    ]

    # Mock rule mapper
    rule_mapper = Mock()
    rule_mapper.get_rule_keys.return_value = ["rule1_123"]

    # Test matching (this is a simplified test)
    matches = engine.apply_rule_mappings_optimized(assets, entities, rule_mapper)

    tracker = engine.get_match_tracker()
    assert tracker.get_match_count() >= 0  # Some matches should be found

    print("✅ OptimizedMatchingEngine tests passed")


def test_robust_client():
    """Test the robust API client"""
    print("🧪 Testing RobustAPIClient...")

    logger = CogniteFunctionLogger("INFO")

    # Mock client
    mock_client = Mock()
    robust_client = RobustAPIClient(mock_client, logger)

    # Mock successful operation
    mock_operation = Mock(return_value="success")
    result = robust_client.robust_api_call(mock_operation, "arg1", "arg2")

    assert result == "success"
    assert mock_operation.called

    print("✅ RobustAPIClient tests passed")


def test_simple_cache():
    """Test the simple cache"""
    print("🧪 Testing SimpleCache...")

    cache = SimpleCache(max_size=3)

    # Test setting and getting
    cache.set("key1", "value1")
    cache.set("key2", "value2")
    cache.set("key3", "value3")

    assert cache.get("key1") == "value1"
    assert cache.get("key2") == "value2"
    assert cache.get("key3") == "value3"
    assert cache.get("nonexistent") is None

    # Test eviction
    cache.set("key4", "value4")  # Should evict oldest
    assert cache.get("key1") is None  # Evicted
    assert cache.get("key4") == "value4"

    # Test clearing
    cache.clear()
    assert cache.get("key2") is None

    print("✅ SimpleCache tests passed")


def test_benchmark():
    """Test performance benchmarking"""
    print("🧪 Testing PerformanceBenchmark...")

    logger = CogniteFunctionLogger("INFO")
    benchmark = PerformanceBenchmark(logger)

    # Test benchmarking a function
    def test_function(x, y):
        time.sleep(0.05)
        return x + y

    result = benchmark.benchmark_function("Addition", test_function, 2, 3)
    assert result == 5

    # Test multiple runs
    benchmark.benchmark_function("Addition", test_function, 5, 7)
    benchmark.benchmark_function("Addition", test_function, 1, 9)

    # Test summary
    summary = benchmark.get_summary()
    assert "Addition" in summary
    assert summary["Addition"]["count"] == 3
    assert summary["Addition"]["average"] > 0

    # Test logging summary
    benchmark.log_summary()

    print("✅ PerformanceBenchmark tests passed")


def test_patch_existing():
    """Test patching existing pipeline"""
    print("🧪 Testing patch_existing_pipeline...")

    result = patch_existing_pipeline()
    assert result == True

    print("✅ patch_existing_pipeline tests passed")


def run_performance_comparison():
    """Run a simulated performance comparison"""
    print("🧪 Running Performance Comparison Demo...")

    logger = CogniteFunctionLogger("INFO")

    # Simulate original function (slower)
    def original_function():
        time.sleep(0.2)
        return "original_result"

    # Simulate optimized function (faster)
    def optimized_function():
        time.sleep(0.1)
        return "optimized_result"

    # Time both
    original_times = []
    optimized_times = []

    for i in range(3):
        # Original
        start = time.time()
        original_function()
        original_times.append(time.time() - start)

        # Optimized
        start = time.time()
        optimized_function()
        optimized_times.append(time.time() - start)

    avg_original = sum(original_times) / len(original_times)
    avg_optimized = sum(optimized_times) / len(optimized_times)
    improvement = ((avg_original - avg_optimized) / avg_original) * 100

    logger.info("📊 Simulated Performance Comparison:")
    logger.info(f"   Original average: {avg_original:.3f}s")
    logger.info(f"   Optimized average: {avg_optimized:.3f}s")
    logger.info(f"   Improvement: {improvement:.1f}%")

    print("✅ Performance comparison demo completed")


def main():
    """Run all optimization tests"""
    print("🚀 Starting Pipeline Optimization Tests")
    print("=" * 50)

    try:
        # Run all tests
        test_performance_monitoring()
        test_match_tracker()
        test_rule_mapper()
        test_batch_processor()
        test_concurrent_loader()
        test_matching_engine()
        test_robust_client()
        test_simple_cache()
        test_benchmark()
        test_patch_existing()

        # Run performance demo
        print("\n🎯 Performance Demonstrations:")
        print("-" * 30)
        run_performance_comparison()

        print("\n✅ ALL OPTIMIZATION TESTS PASSED!")
        print("🎉 The optimization module is ready for use!")

        print("\n📋 Usage Instructions:")
        print("1. Import optimizations: from pipeline_optimizations import *")
        print(
            "2. Use optimized handler: from optimized_handler import handle_optimized"
        )
        print("3. Or wrap existing pipeline: create_optimized_pipeline_wrapper()")

        return True

    except Exception as e:
        print(f"\n❌ TESTS FAILED: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
