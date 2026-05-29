#!/usr/bin/env python3
"""
Test Script for Pipeline Optimizations

Smoke tests for the helpers in `pipeline_optimizations.py`. Covers only the
helpers actually wired into production (`handler.py` and `pipeline.py`):
time_operation, monitor_memory_usage, cleanup_memory, RobustAPIClient,
patch_existing_pipeline, and PerformanceBenchmark.

Run from the function directory:

    python test_optimizations.py
"""

import sys
import time
from pathlib import Path
from unittest.mock import Mock

# Force UTF-8 stdout so the emoji-rich print()s below don't blow up on
# Windows code pages (cp1252). No-op on POSIX / Cognite Functions runtime.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Add current directory to path so the flat imports below resolve.
sys.path.append(str(Path(__file__).parent))

from logger import CogniteFunctionLogger
from pipeline_optimizations import (
    PerformanceBenchmark,
    RobustAPIClient,
    cleanup_memory,
    monitor_memory_usage,
    patch_existing_pipeline,
    time_operation,
)


def test_performance_monitoring():
    """Test the timing context manager and the memory helpers."""
    print("🧪 Testing Performance Monitoring...")

    logger = CogniteFunctionLogger("INFO")

    with time_operation("Test operation", logger):
        time.sleep(0.1)  # Simulate work

    monitor_memory_usage(logger, "Test memory check")
    cleanup_memory()

    print("✅ Performance monitoring tests passed")


def test_robust_client():
    """Test that RobustAPIClient.robust_api_call invokes the wrapped operation."""
    print("🧪 Testing RobustAPIClient...")

    logger = CogniteFunctionLogger("INFO")

    mock_client = Mock()
    robust_client = RobustAPIClient(mock_client, logger)

    mock_operation = Mock(return_value="success")
    result = robust_client.robust_api_call(mock_operation, "arg1", "arg2")

    assert result == "success"
    assert mock_operation.called
    mock_operation.assert_called_once_with("arg1", "arg2")

    print("✅ RobustAPIClient tests passed")


def test_robust_client_retry_on_failure():
    """Verify the retry wrapper actually retries before giving up."""
    print("🧪 Testing RobustAPIClient retry behaviour...")

    logger = CogniteFunctionLogger("INFO")
    mock_client = Mock()
    robust_client = RobustAPIClient(mock_client, logger)

    # Fail twice, then succeed.
    mock_operation = Mock(side_effect=[RuntimeError("boom"), RuntimeError("boom"), "ok"])

    result = robust_client.robust_api_call(mock_operation)

    assert result == "ok"
    assert mock_operation.call_count == 3

    print("✅ RobustAPIClient retry tests passed")


def test_benchmark():
    """Test the PerformanceBenchmark accumulator and summary."""
    print("🧪 Testing PerformanceBenchmark...")

    logger = CogniteFunctionLogger("INFO")
    benchmark = PerformanceBenchmark(logger)

    def test_function(x, y):
        time.sleep(0.05)
        return x + y

    result = benchmark.benchmark_function("Addition", test_function, 2, 3)
    assert result == 5

    benchmark.benchmark_function("Addition", test_function, 5, 7)
    benchmark.benchmark_function("Addition", test_function, 1, 9)

    summary = benchmark.get_summary()
    assert "Addition" in summary
    assert summary["Addition"]["count"] == 3
    assert summary["Addition"]["average"] > 0
    assert summary["Addition"]["min"] <= summary["Addition"]["average"] <= summary["Addition"]["max"]

    benchmark.log_summary()

    print("✅ PerformanceBenchmark tests passed")


def test_patch_existing():
    """Test that patch_existing_pipeline runs without error."""
    print("🧪 Testing patch_existing_pipeline...")

    result = patch_existing_pipeline()
    assert result

    print("✅ patch_existing_pipeline tests passed")


def main():
    """Run all optimization helper tests."""
    print("🚀 Starting Pipeline Optimization Tests")
    print("=" * 50)

    try:
        test_performance_monitoring()
        test_robust_client()
        test_robust_client_retry_on_failure()
        test_benchmark()
        test_patch_existing()

        print("\n✅ ALL OPTIMIZATION TESTS PASSED!")
        return True

    except Exception as e:
        print(f"\n❌ TESTS FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
