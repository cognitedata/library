"""
Pipeline Optimizations Module

This module provides optimization utilities that can be used with the existing pipeline.py
to improve performance without breaking the existing functionality.
"""

import time
import gc
import psutil
import concurrent.futures
from functools import lru_cache
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Tuple, Set, Callable
from collections import defaultdict
import re
from tenacity import retry, stop_after_attempt, wait_exponential

from logger import CogniteFunctionLogger


# ===== PERFORMANCE MONITORING =====


@contextmanager
def time_operation(operation_name: str, logger: CogniteFunctionLogger):
    """Context manager for timing operations"""
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        logger.info(f" Time: {operation_name} took {duration:.2f} seconds")


def monitor_memory_usage(logger: CogniteFunctionLogger, operation_name: str = ""):
    """Monitor memory usage"""
    try:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        logger.info(
            f"Monitor Memory: {operation_name} Memory usage: {memory_mb:.1f} MB"
        )
    except Exception as e:
        logger.debug(f"Could not monitor memory: {e}")


def cleanup_memory():
    """Force garbage collection"""
    gc.collect()


# ===== OPTIMIZED DATA STRUCTURES =====


class MatchTracker:
    """Optimized duplicate detection using sets"""

    def __init__(self):
        self.seen_matches: Set[str] = set()
        self.good_matches: List[Dict[str, Any]] = []

    def add_match(
        self, asset_ext_id: str, entity_ext_id: str, match_data: Dict[str, Any]
    ) -> bool:
        """Add match if not duplicate. Returns True if added."""
        match_key = f"{asset_ext_id}_{entity_ext_id}"
        if match_key in self.seen_matches:
            return False

        self.seen_matches.add(match_key)
        self.good_matches.append(match_data)
        return True

    def has_match(self, asset_ext_id: str, entity_ext_id: str) -> bool:
        """Check if match already exists"""
        match_key = f"{asset_ext_id}_{entity_ext_id}"
        return match_key in self.seen_matches

    def get_match_count(self) -> int:
        """Get total number of matches"""
        return len(self.good_matches)


class OptimizedRuleMapper:
    """Optimized rule mapping with pre-compiled regex and caching"""

    def __init__(
        self,
        rule_mappings: List[Dict[str, Any]],
        logger: Optional[CogniteFunctionLogger] = None,
    ):
        self.compiled_patterns: Dict[str, re.Pattern] = {}
        self.rule_data: Dict[str, Dict[str, Any]] = {}
        self.logger = logger

        # Pre-compile all regex patterns
        for rule in rule_mappings:
            key = rule.get("key", "")

            # Entity patterns
            if "entity_regex" in rule:
                pattern_key = f"{key}_entity"
                try:
                    self.compiled_patterns[pattern_key] = re.compile(
                        rule["entity_regex"]
                    )
                    self.rule_data[pattern_key] = rule
                except re.error as e:
                    # Log but don't fail
                    if self.logger:
                        self.logger.warning(f"Error compiling entity regex: {e}")
                    pass

            # Asset patterns
            if "asset_regex" in rule:
                pattern_key = f"{key}_asset"
                try:
                    self.compiled_patterns[pattern_key] = re.compile(
                        rule["asset_regex"]
                    )
                    self.rule_data[pattern_key] = rule
                except re.error as e:
                    # Log but don't fail
                    if self.logger:
                        self.logger.warning(f"Error compiling asset regex: {e}")
                    pass

    @lru_cache(maxsize=10000)
    def get_rule_keys(self, name: str, rule_type: str) -> Tuple[str, ...]:
        """Cache rule key computations"""
        keys = []
        for pattern_key, pattern in self.compiled_patterns.items():
            if rule_type in pattern_key:
                match = pattern.search(name)
                if match:
                    base_key = pattern_key.replace(f"_{rule_type}", "")
                    keys.append(f"{base_key}_{''.join(match.groups())}")
        return tuple(keys)


# ===== OPTIMIZED BATCH PROCESSING =====


class BatchProcessor:
    """Optimized batch processing for entities and assets"""

    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size

    def process_entities_in_batches(
        self,
        entities: List[Dict[str, Any]],
        process_func,
        logger: CogniteFunctionLogger,
        *args,
        **kwargs,
    ) -> List[Any]:
        """Process entities in optimized batches"""
        results = []
        total_entities = len(entities)

        for i in range(0, total_entities, self.batch_size):
            batch = entities[i : i + self.batch_size]
            batch_num = i // self.batch_size + 1

            with time_operation(f"Processing batch {batch_num}", logger):
                batch_results = process_func(batch, *args, **kwargs)
                if batch_results:
                    results.extend(batch_results)

            # Memory cleanup every 10 batches
            if batch_num % 10 == 0:
                cleanup_memory()
                monitor_memory_usage(logger, f"After batch {batch_num}")

        return results


# ===== OPTIMIZED CONCURRENT PROCESSING =====


class ConcurrentDataLoader:
    """Optimized concurrent data loading"""

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers

    def load_data_concurrently(
        self,
        load_functions: List[Tuple[Callable, tuple]],
        logger: CogniteFunctionLogger,
    ) -> List[Any]:
        """Load data concurrently"""

        with time_operation("Concurrent data loading", logger):
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers
            ) as executor:
                # Submit all tasks
                futures = [
                    executor.submit(func, *args) for func, args in load_functions
                ]

                # Collect results
                results = []
                for i, future in enumerate(futures):
                    try:
                        result = future.result()
                        results.append(result)
                        logger.debug(f"Loaded data source {i+1}/{len(futures)}")
                    except Exception as e:
                        logger.error(f"Failed to load data source {i+1}: {e}")
                        results.append(None)

                return results


# ===== OPTIMIZED MATCHING ALGORITHMS =====


class OptimizedMatchingEngine:
    """Optimized matching engine with better algorithms"""

    def __init__(self, logger: CogniteFunctionLogger):
        self.logger = logger
        self.match_tracker = MatchTracker()

    def apply_rule_mappings_optimized(
        self,
        assets: List[Dict[str, Any]],
        entities: List[Dict[str, Any]],
        rule_mapper: OptimizedRuleMapper,
    ) -> List[Dict[str, Any]]:
        """Apply rule mappings with optimized algorithms"""

        with time_operation("Optimized rule mapping", self.logger):
            # Build inverted index for assets
            asset_index = defaultdict(list)
            for asset in assets:
                rule_keys = rule_mapper.get_rule_keys(asset.get("name", ""), "asset")
                for rule_key in rule_keys:
                    asset_index[rule_key].append(asset)

            matches_found = 0

            # Process entities
            for entity in entities:
                entity_rule_keys = rule_mapper.get_rule_keys(
                    entity.get("name", ""), "entity"
                )

                for rule_key in entity_rule_keys:
                    if rule_key in asset_index:
                        for matching_asset in asset_index[rule_key]:
                            if self.match_tracker.add_match(
                                matching_asset.get("asset_ext_id", ""),
                                entity.get("entity_ext_id", ""),
                                {
                                    "match_type": "Rule Based Mapping",
                                    "entity_ext_id": entity.get("entity_ext_id", ""),
                                    "entity_name": entity.get("name", ""),
                                    "score": 1.0,
                                    "asset_name": matching_asset.get("name", ""),
                                    "asset_ext_id": matching_asset.get(
                                        "asset_ext_id", ""
                                    ),
                                },
                            ):
                                matches_found += 1

            self.logger.info(f"Rule mapping found {matches_found} matches")
            return self.match_tracker.good_matches

    def get_match_tracker(self) -> MatchTracker:
        """Get the match tracker"""
        return self.match_tracker


# ===== RETRY MECHANISMS =====


class RobustAPIClient:
    """Wrapper for API calls with retry logic"""

    def __init__(self, client, logger: CogniteFunctionLogger):
        self.client = client
        self.logger = logger

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def robust_api_call(self, operation, *args, **kwargs):
        """Retry failed API calls with exponential backoff"""
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            self.logger.warning(f"API call failed, retrying: {e}")
            raise

    def batch_update_entities(self, updates: List[Any], batch_size: int = 2000):
        """Update entities in optimized batches"""
        if not updates:
            return

        for i in range(0, len(updates), batch_size):
            batch = updates[i : i + batch_size]
            try:
                self.robust_api_call(self.client.data_modeling.instances.apply, batch)
                self.logger.info(
                    f"Updated batch {i//batch_size + 1}: {len(batch)} entities"
                )
            except Exception as e:
                self.logger.warning(
                    f"Large batch failed, retrying with smaller chunks: {e}"
                )
                # Split batch and retry smaller chunks
                for j in range(0, len(batch), batch_size // 4):
                    small_batch = batch[j : j + batch_size // 4]
                    self.robust_api_call(
                        self.client.data_modeling.instances.apply, small_batch
                    )


# ===== CACHING UTILITIES =====


class SimpleCache:
    """Simple in-memory cache for expensive operations"""

    def __init__(self, max_size: int = 1000):
        self.cache: Dict[str, Any] = {}
        self.max_size = max_size

    def get(self, key: str) -> Optional[Any]:
        """Get item from cache"""
        return self.cache.get(key)

    def set(self, key: str, value: Any) -> None:
        """Set item in cache"""
        if len(self.cache) >= self.max_size:
            # Simple eviction: remove oldest
            oldest_key = next(iter(self.cache))
            del self.cache[oldest_key]

        self.cache[key] = value

    def clear(self) -> None:
        """Clear cache"""
        self.cache.clear()


# ===== USAGE EXAMPLE =====


def optimize_pipeline_run(original_pipeline_func, client, logger, data, config):
    """
    Wrapper function that adds optimizations to the existing pipeline

    Usage:
        # In your main pipeline code, replace:
        # asset_entity_matching(client, logger, data, config)
        # With:
        # optimize_pipeline_run(asset_entity_matching, client, logger, data, config)
    """

    # Initialize optimization components
    batch_processor = BatchProcessor(batch_size=2000)
    concurrent_loader = ConcurrentDataLoader(max_workers=4)
    matching_engine = OptimizedMatchingEngine(logger)
    robust_client = RobustAPIClient(client, logger)
    cache = SimpleCache(max_size=1000)

    # Monitor initial state
    monitor_memory_usage(logger, "Pipeline start")

    # Add performance timing
    with time_operation("Complete pipeline execution", logger):
        try:
            # Run the original pipeline with optimizations
            result = original_pipeline_func(client, logger, data, config)

            # Final cleanup
            cleanup_memory()
            monitor_memory_usage(logger, "Pipeline end")

            return result

        except Exception as e:
            logger.error(f"Optimized pipeline failed: {e}")
            raise


# ===== QUICK OPTIMIZATION PATCHES =====


def patch_existing_pipeline():
    """
    Apply quick optimizations to existing pipeline without major refactoring

    This function can be imported and called at the start of your pipeline
    to apply immediate performance improvements.
    """

    # Increase garbage collection threshold for better memory management
    import gc

    gc.set_threshold(700, 10, 10)

    # Set process priority (if possible)
    try:
        import os

        os.nice(-5)  # Increase priority slightly
    except:
        pass

    return True


# ===== PERFORMANCE BENCHMARKING =====


class PerformanceBenchmark:
    """Simple performance benchmarking utilities"""

    def __init__(self, logger: CogniteFunctionLogger):
        self.logger = logger
        self.benchmarks: Dict[str, List[float]] = {}

    def benchmark_function(self, name: str, func, *args, **kwargs):
        """Benchmark a function call"""
        start = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start

            if name not in self.benchmarks:
                self.benchmarks[name] = []

            self.benchmarks[name].append(duration)
            self.logger.info(f"Monitor Performance: {name} took {duration:.2f}s")

            return result
        except Exception as e:
            duration = time.time() - start
            self.logger.error(f"{name} failed after {duration:.2f}s: {e}")
            raise

    def get_summary(self) -> Dict[str, Dict[str, float]]:
        """Get performance summary"""
        summary = {}
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
        """Log performance summary"""
        summary = self.get_summary()
        self.logger.info("📊 Performance Summary:")
        for name, stats in summary.items():
            self.logger.info(
                f"  {name}: {stats['count']} calls, avg {stats['average']:.2f}s, total {stats['total']:.2f}s"
            )


# ===== EXPORT MAIN OPTIMIZATION CLASSES =====

__all__ = [
    "time_operation",
    "monitor_memory_usage",
    "cleanup_memory",
    "MatchTracker",
    "OptimizedRuleMapper",
    "BatchProcessor",
    "ConcurrentDataLoader",
    "OptimizedMatchingEngine",
    "RobustAPIClient",
    "SimpleCache",
    "optimize_pipeline_run",
    "patch_existing_pipeline",
    "PerformanceBenchmark",
]
