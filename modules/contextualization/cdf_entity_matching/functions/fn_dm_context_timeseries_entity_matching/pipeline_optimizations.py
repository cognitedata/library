"""
Pipeline Optimizations Module

Helpers used by the entity-matching function for performance monitoring,
memory hygiene, resilient API calls, and lightweight benchmarking.

This module is intentionally lean: only the helpers actually wired into
`handler.py` and `pipeline.py` belong here. Earlier exploratory classes
(MatchTracker, OptimizedRuleMapper, BatchProcessor, ConcurrentDataLoader,
OptimizedMatchingEngine, SimpleCache, optimize_pipeline_run) have been
removed because their patterns are either implemented inline in
`pipeline.py` (inverted index, sets, per-batch processing) or have a
cost/benefit profile that doesn't justify the added complexity for this
workload.
"""

import gc
import time
from contextlib import contextmanager

import psutil
from logger import CogniteFunctionLogger
from tenacity import retry, stop_after_attempt, wait_exponential

# ===== PERFORMANCE MONITORING ===============================================

@contextmanager
def time_operation(operation_name: str, logger: CogniteFunctionLogger):
    """Context manager that logs how long the wrapped block ran."""
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        logger.info(f" Time: {operation_name} took {duration:.2f} seconds")


def monitor_memory_usage(logger: CogniteFunctionLogger, operation_name: str = ""):
    """Log current resident-set memory usage (best effort)."""
    try:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        logger.info(f"Monitor Memory: {operation_name} Memory usage: {memory_mb:.1f} MB")
    except Exception as e:
        logger.debug(f"Could not monitor memory: {e}")


def cleanup_memory():
    """Force a garbage-collection pass."""
    gc.collect()


# ===== RETRY WRAPPER ========================================================

class RobustAPIClient:
    """Wrap arbitrary CDF API calls with bounded exponential-backoff retry."""

    def __init__(self, client, logger: CogniteFunctionLogger):
        self.client = client
        self.logger = logger

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def robust_api_call(self, operation, *args, **kwargs):
        """Retry the wrapped operation up to 3 times with exponential backoff.

        On each failure tenacity sleeps according to the wait policy
        (4s capped at 10s, exponentially) and re-invokes `operation`. If all
        attempts fail, the last exception is wrapped in tenacity.RetryError.
        """
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            self.logger.warning(f"API call failed, retrying: {e}")
            raise


# ===== STARTUP TUNING =======================================================

def patch_existing_pipeline():
    """Apply quick global optimizations once at function start.

    - Tighten the GC threshold so transient allocations get reaped sooner.
    - Bump process priority slightly on Unix (no-op on Windows / Cognite Functions).
    """
    gc.set_threshold(700, 10, 10)

    try:
        import os
        if hasattr(os, "nice"):
            os.nice(-5)
    except OSError:
        # Process priority adjustment is optional and may fail on some platforms.
        pass

    return True


# ===== PERFORMANCE BENCHMARKING =============================================

class PerformanceBenchmark:
    """Lightweight per-function benchmarking accumulator.

    Used by `handler.py` to time individual phases of the pipeline and emit
    a roll-up summary at the end of the run.
    """

    def __init__(self, logger: CogniteFunctionLogger):
        self.logger = logger
        self.benchmarks: dict[str, list[float]] = {}

    def benchmark_function(self, name: str, func, *args, **kwargs):
        """Time a single function call and record the duration."""
        start = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start
            self.benchmarks.setdefault(name, []).append(duration)
            self.logger.info(f"Monitor Performance: {name} took {duration:.2f}s")
            return result
        except Exception as e:
            duration = time.time() - start
            self.logger.error(f"{name} failed after {duration:.2f}s: {e}")
            raise

    def get_summary(self) -> dict[str, dict[str, float]]:
        """Return per-name count / total / average / min / max statistics."""
        summary: dict[str, dict[str, float]] = {}
        for name, times in self.benchmarks.items():
            summary[name] = {
                "count": len(times),
                "total": sum(times),
                "average": sum(times) / len(times),
                "min": min(times),
                "max": max(times),
            }
        return summary

    def log_summary(self):
        """Log the summary stats at INFO level."""
        summary = self.get_summary()
        self.logger.info("📊 Performance Summary:")
        for name, stats in summary.items():
            self.logger.info(
                f"  {name}: {stats['count']} calls, avg {stats['average']:.2f}s, "
                f"total {stats['total']:.2f}s"
            )


# ===== EXPORTS ==============================================================

__all__ = [
    "PerformanceBenchmark",
    "RobustAPIClient",
    "cleanup_memory",
    "monitor_memory_usage",
    "patch_existing_pipeline",
    "time_operation",
]
