"""
Metadata Update Optimization Module

This module provides optimization utilities for metadata update functions to improve
performance, reduce memory usage, and enhance reliability.
"""

import gc
import re
import time
from collections.abc import Callable
from contextlib import contextmanager
from functools import lru_cache

import psutil
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeApply, NodeList, NodeOrEdgeData, ViewId
from cognite.client.exceptions import CogniteAPIError
from constants import MANAGED_ASSET_TAG_PREFIX
from logger import CogniteFunctionLogger
from tenacity import retry, stop_after_attempt, wait_exponential

# Tag pattern shared by timeseries and asset aliases, e.g. VAL_23-KA-9101 -> 23_KA_9101
ALIAS_PATTERN = re.compile(r"(\d{2})[-_.:]([A-Z]{2,3})[-_.:](\d{4,5})")

# ===== PERFORMANCE MONITORING =====

@contextmanager
def time_operation(operation_name: str, logger: CogniteFunctionLogger):
    """Context manager for timing operations"""
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        logger.info(f"⏱️ {operation_name} took {duration:.2f} seconds")


def monitor_memory_usage(logger: CogniteFunctionLogger, operation_name: str = ""):
    """Monitor memory usage"""
    try:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        logger.info(f"📊 Memory: {operation_name} - {memory_mb:.1f} MB")
    except Exception as e:
        logger.debug(f"Could not monitor memory: {e}")


def cleanup_memory():
    """Force garbage collection"""
    gc.collect()


# ===== BATCH PROCESSING UTILITIES =====

class BatchProcessor:
    """Optimized batch processing for metadata updates"""
    
    def __init__(self, batch_size: int = 1000, max_workers: int = 4):
        self.batch_size = batch_size
        self.max_workers = max_workers
    
    def process_nodes_in_batches(self, nodes: NodeList[Node], 
                                process_func: Callable,
                                logger: CogniteFunctionLogger,
                                *args, **kwargs) -> list[NodeApply]:
        """Process nodes in optimized batches"""
        
        results = []
        node_type = kwargs.get('node_type', 'nodes')
        total_nodes = len(nodes[node_type])
        
        with time_operation(f"Batch processing {total_nodes} {node_type}", logger):
            for i in range(0, total_nodes, self.batch_size):
                batch_end = min(i + self.batch_size, total_nodes)
                batch = nodes[node_type][i:batch_end]
                
                batch_results = process_func(batch, *args, **kwargs)
                if batch_results:
                    results.extend(batch_results)
                
                # Memory cleanup every 10 batches
                if (i // self.batch_size) % 10 == 0:
                    cleanup_memory()
                    monitor_memory_usage(logger, f"After batch {i//self.batch_size + 1}")
        
        return results
    
    def apply_updates_in_batches(self, client: CogniteClient,
                                updates: list[NodeApply],
                                logger: CogniteFunctionLogger,
                                batch_size: int = 2000) -> int:
        """Apply updates in optimized batches with retry logic"""
        
        if not updates:
            return 0
        
        total_applied = 0
        
        with time_operation(f"Applying {len(updates)} updates in batches", logger):
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                
                try:
                    self._apply_batch_with_retry(client, batch, logger)
                    total_applied += len(batch)
                    logger.info(f"Applied batch {i//batch_size + 1}: {len(batch)} updates")
                    
                except Exception as e:
                    logger.warning(f"Large batch failed, retrying with smaller chunks: {e}")
                    # Split batch and retry smaller chunks
                    small_batch_size = batch_size // 4
                    for j in range(0, len(batch), small_batch_size):
                        small_batch = batch[j:j + small_batch_size]
                        self._apply_batch_with_retry(client, small_batch, logger)
                        total_applied += len(small_batch)
        
        return total_applied
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _apply_batch_with_retry(self, client: CogniteClient, batch: list[NodeApply], logger: CogniteFunctionLogger):
        """Apply batch with retry logic"""
        try:
            client.data_modeling.instances.apply(batch)
        except CogniteAPIError as e:
            logger.warning(f"API error applying batch: {e}")
            raise


# ===== OPTIMIZED METADATA PROCESSOR =====

class OptimizedMetadataProcessor:
    """Optimized metadata processing with caching and batch operations"""
    
    def __init__(self, logger: CogniteFunctionLogger):
        self.logger = logger
        self.batch_processor = BatchProcessor()
        self.stats = {
            'processed': 0,
            'updated': 0,
        }
    
    def process_timeseries_metadata(
        self,
        node: Node,
        view_id: ViewId,
        node_space: str,
        update_all: bool = False,
    ) -> NodeApply | None:
        """Process timeseries metadata with optimizations"""
        
        try:
            ext_id = node.external_id
            properties = node.properties[view_id]
            
            name = str(properties.get("name", ""))
            aliases_raw = properties.get("aliases", [])
            org_aliases = (
                [str(x) for x in aliases_raw] if isinstance(aliases_raw, list) else []
            )
            aliases = [] if update_all else org_aliases.copy()

            upd_aliases = self._get_timeseries_alias_list_optimized(name, tuple(aliases))

            update_needed = False
            properties_dict = {}

            if update_all or upd_aliases != org_aliases:
                properties_dict["aliases"] = upd_aliases
                update_needed = True
            
            self.stats['processed'] += 1
            
            if update_needed:
                self.stats['updated'] += 1
                self.logger.debug(f"Updating TS: {name} with {len(properties_dict)} properties")
                
                return NodeApply(
                    space=node_space,
                    external_id=ext_id,
                    sources=[
                        NodeOrEdgeData(
                            source=view_id,
                            properties=properties_dict,
                        )
                    ],
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing timeseries {node.external_id}: {e}")
            return None
    
    def process_asset_metadata(
        self,
        node: Node,
        view_id: ViewId,
        node_space: str,
        update_all: bool = False,
    ) -> NodeApply | None:
        """Process asset metadata with optimizations"""
        
        try:
            ext_id = node.external_id
            properties = node.properties[view_id]
            
            name = str(properties.get("name", ""))
            aliases_raw = properties.get("aliases", [])
            tags_raw = properties.get("tags", [])
            org_aliases = (
                [str(x) for x in aliases_raw] if isinstance(aliases_raw, list) else []
            )
            org_tags = [str(x) for x in tags_raw] if isinstance(tags_raw, list) else []
            aliases = [] if update_all else org_aliases.copy()
            # Managed tags are always rebuilt so a changed or removed root relation
            # cannot leave a stale root:* tag behind.
            tags = [
                tag
                for tag in org_tags
                if not tag.startswith(MANAGED_ASSET_TAG_PREFIX) and tag != "tag"
            ]

            root_external_id = _direct_relation_external_id(properties.get("root"))
            upd_tags, upd_aliases = self._parse_asset_tag_optimized(
                name, aliases, root_external_id, tags
            )

            update_needed = False
            properties_dict = {}

            if update_all:
                properties_dict["aliases"] = upd_aliases
                properties_dict["tags"] = upd_tags
                update_needed = True
            else:
                if upd_aliases != org_aliases:
                    properties_dict["aliases"] = upd_aliases
                    update_needed = True
                if set(upd_tags) != set(org_tags):
                    properties_dict["tags"] = upd_tags
                    update_needed = True
            
            self.stats['processed'] += 1
            
            if update_needed:
                self.stats['updated'] += 1
                self.logger.debug(f"Updating asset: {ext_id} with {len(properties_dict)} properties")
                
                return NodeApply(
                    space=node_space,
                    external_id=ext_id,
                    sources=[
                        NodeOrEdgeData(
                            source=view_id,
                            properties=properties_dict,
                        )
                    ],
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing asset {node.external_id}: {e}")
            return None
    
    def process_file_metadata(self, node: Node, view_id: ViewId,
                             node_space: str) -> NodeApply | None:
        """Process file metadata with optimizations"""
        
        try:
            ext_id = node.external_id
            properties = node.properties[view_id]
            
            name = str(properties.get("name", ""))
            aliases_raw = properties.get("aliases", [])
            aliases = [str(x) for x in aliases_raw] if isinstance(aliases_raw, list) else []
            org_aliases = aliases.copy()
            # Optimized alias generation
            upd_aliases = self._get_file_alias_list_optimized(name, tuple(aliases))
            
            self.stats['processed'] += 1
            
            if upd_aliases != org_aliases:
                self.stats['updated'] += 1
                self.logger.debug(f"Updating file: {ext_id} with {len(upd_aliases)} aliases")
                
                return NodeApply(
                    space=node_space,
                    external_id=ext_id,
                    sources=[
                        NodeOrEdgeData(
                            source=view_id,
                            properties={"aliases": upd_aliases},
                        )
                    ],
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing file {node.external_id}: {e}")
            return None
    
    def _parse_asset_tag_optimized(
        self,
        name: str,
        aliases: list[str],
        root_external_id: str,
        tags: list[str],
    ) -> tuple[list[str], list[str]]:
        """Build asset aliases and root tag from the root relation external id."""
        try:
            upd_aliases = self._get_asset_alias_list_optimized(name, tuple(aliases))

            if root_external_id:
                managed_tag = f"{MANAGED_ASSET_TAG_PREFIX}{root_external_id}"
                if managed_tag not in tags:
                    tags.append(managed_tag)

            return tags, upd_aliases

        except Exception as e:
            self.logger.error(f"Error parsing asset tag {name}: {e}")
            return tags, aliases

    @lru_cache(maxsize=5000)
    def _get_timeseries_alias_list_optimized(self, name: str, aliases_tuple: tuple[str, ...] = ()) -> list[str]:
        """Optimized timeseries alias generation with caching"""
        aliases = list(aliases_tuple)

        match = ALIAS_PATTERN.search(name)

        cleaned_value = None if not match else "_".join(match.groups())

        if cleaned_value and cleaned_value not in aliases:
            aliases.append(cleaned_value)

        return aliases
    
    @lru_cache(maxsize=5000)
    def _get_asset_alias_list_optimized(self, name: str, aliases_tuple: tuple[str, ...]) -> list[str]:
        """Optimized asset alias generation with caching"""
        aliases = list(aliases_tuple)

        match = ALIAS_PATTERN.search(name)

        cleaned_value = None if not match else "_".join(match.groups())

        if cleaned_value and cleaned_value not in aliases:
            aliases.append(cleaned_value)
        
        return aliases
    
    @lru_cache(maxsize=5000)
    def _get_file_alias_list_optimized(self, name: str, aliases_tuple: tuple[str, ...]) -> list[str]:
        """Optimized file alias generation with caching"""
        aliases = list(aliases_tuple)
        
        # Add name if not in aliases
        if name not in aliases:
            aliases.append(name)
        
        # Add name without extension
        name_no_ext = name.split('.')[0] if '.' in name else name
        if name_no_ext not in aliases:
            aliases.append(name_no_ext)
        
        return aliases
    
    def get_stats(self) -> dict[str, float | int]:
        """Get processing statistics"""
        return {
            'processed': self.stats['processed'],
            'updated': self.stats['updated'],
            'update_rate': self.stats['updated'] / self.stats['processed'] if self.stats['processed'] > 0 else 0,
        }


# ===== PERFORMANCE BENCHMARK =====

class PerformanceBenchmark:
    """Performance benchmarking utilities"""
    
    def __init__(self, logger: CogniteFunctionLogger):
        self.logger = logger
        self.benchmarks: dict[str, list[float]] = {}
    
    def benchmark_function(self, name: str, func: Callable[..., object], *args: object, **kwargs: object) -> object:
        """Benchmark a function call"""
        start = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start
            
            if name not in self.benchmarks:
                self.benchmarks[name] = []
            
            self.benchmarks[name].append(duration)
            self.logger.info(f"🚀 {name} took {duration:.2f}s")
            
            return result
        except Exception as e:
            duration = time.time() - start
            self.logger.error(f"❌ {name} failed after {duration:.2f}s: {e}")
            raise
    
    def log_summary(self):
        """Log performance summary"""
        if not self.benchmarks:
            return
        
        self.logger.info("📊 Performance Summary:")
        for name, times in self.benchmarks.items():
            avg_time = sum(times) / len(times)
            total_time = sum(times)
            self.logger.info(f"  {name}: {len(times)} calls, avg {avg_time:.2f}s, total {total_time:.2f}s")


# ===== UTILITY FUNCTIONS =====

def _direct_relation_external_id(relation: object) -> str:
    """Return the external id from a direct relation property value."""
    if relation is None:
        return ""

    if isinstance(relation, dict):
        external_id = relation.get("externalId") or relation.get("external_id")
        return str(external_id) if external_id else ""

    external_id = getattr(relation, "external_id", None) or getattr(relation, "externalId", None)
    return str(external_id) if external_id else ""


def optimize_metadata_processing():
    """Apply global optimizations for metadata processing"""
    
    # Increase garbage collection threshold
    gc.set_threshold(700, 10, 10)
    
    # Set process priority if possible
    try:
        import os
        os.nice(-5)
    except Exception:
        # Process priority adjustment is optional and may fail on some platforms.
        pass
    
    return True


# ===== EXPORT MAIN CLASSES =====

__all__ = [
    'BatchProcessor',
    'OptimizedMetadataProcessor',
    'PerformanceBenchmark',
    'cleanup_memory',
    'monitor_memory_usage',
    'optimize_metadata_processing',
    'time_operation',
] 