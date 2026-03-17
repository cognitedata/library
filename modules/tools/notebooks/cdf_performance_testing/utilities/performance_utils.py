"""
Performance testing utilities for CDF operations.

This module provides helper functions for measuring and analyzing
performance of various CDF operations.
"""

import time
import statistics
import functools
from typing import Callable, List, Dict
import logging
from datetime import datetime
import json
import os


class PerformanceTracker:
    """A class to track and analyze performance metrics."""

    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.measurements = []
        self.start_time = None
        self.end_time = None

    def start(self):
        """Start timing an operation."""
        self.start_time = time.perf_counter()

    def stop(self):
        """Stop timing and record the measurement."""
        if self.start_time is None:
            raise ValueError("Must call start() before stop()")

        self.end_time = time.perf_counter()
        duration = self.end_time - self.start_time
        self.measurements.append(
            {"duration": duration, "timestamp": datetime.now().isoformat()}
        )
        self.start_time = None
        return duration

    def get_stats(self) -> Dict[str, float]:
        """Get performance statistics."""
        if not self.measurements:
            return {}

        durations = [m["duration"] for m in self.measurements]
        return {
            "operation": self.operation_name,
            "count": len(durations),
            "min": min(durations),
            "max": max(durations),
            "mean": statistics.mean(durations),
            "median": statistics.median(durations),
            "std_dev": statistics.stdev(durations) if len(durations) > 1 else 0,
            "total_time": sum(durations),
        }

    def save_results(self, filepath: str):
        """Save results to JSON file."""
        results = {
            "operation": self.operation_name,
            "measurements": self.measurements,
            "statistics": self.get_stats(),
            "generated_at": datetime.now().isoformat(),
        }

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            json.dump(results, f, indent=2)


def time_function(func: Callable) -> Callable:
    """Decorator to time function execution."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()

        print(f"{func.__name__} took {end_time - start_time:.4f} seconds")
        return result

    return wrapper


def benchmark_operation(
    operation: Callable, iterations: int = 10, warmup: int = 2, *args, **kwargs
) -> Dict[str, float]:
    """
    Benchmark an operation with multiple iterations.

    Args:
        operation: Function to benchmark
        iterations: Number of iterations to run
        warmup: Number of warmup iterations (not counted in results)
        *args, **kwargs: Arguments to pass to the operation

    Returns:
        Dictionary with performance statistics
    """
    # Warmup iterations
    for _ in range(warmup):
        operation(*args, **kwargs)

    # Actual benchmark iterations
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        operation(*args, **kwargs)
        end = time.perf_counter()
        times.append(end - start)

    return {
        "iterations": iterations,
        "min_time": min(times),
        "max_time": max(times),
        "mean_time": statistics.mean(times),
        "median_time": statistics.median(times),
        "std_dev": statistics.stdev(times) if len(times) > 1 else 0,
        "total_time": sum(times),
        "operations_per_second": iterations / sum(times),
    }


def generate_test_data(size: int, data_type: str = "timeseries") -> List[Dict]:
    """Generate test data for performance testing."""
    if data_type == "timeseries":
        return [
            {"timestamp": int(time.time() * 1000) + i * 1000, "value": i * 0.1}
            for i in range(size)
        ]
    elif data_type == "events":
        return [
            {
                "startTime": int(time.time() * 1000) + i * 1000,
                "endTime": int(time.time() * 1000) + i * 1000 + 60000,
                "description": f"Test event {i}",
                "type": "test",
            }
            for i in range(size)
        ]
    else:
        raise ValueError(f"Unsupported data type: {data_type}")


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Set up logging for performance tests."""
    logger = logging.getLogger("cdf_performance")
    logger.setLevel(getattr(logging, log_level.upper()))

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def create_results_directory(base_path: str = "results") -> str:
    """Create a timestamped results directory."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = os.path.join(base_path, timestamp)
    os.makedirs(results_dir, exist_ok=True)
    return results_dir
