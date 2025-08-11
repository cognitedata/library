"""
Metadata Update Optimization Module

This module provides optimization utilities for metadata update functions to improve
performance, reduce memory usage, and enhance reliability.
"""

import time
import gc
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Tuple, Callable
from functools import lru_cache
import re
import psutil
from dataclasses import dataclass

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    NodeApply,
    NodeOrEdgeData,
    ViewId,
    NodeList,
    Node,
)
from cognite.client.exceptions import CogniteAPIError
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


# ===== CACHING UTILITIES =====


@dataclass
class CacheStats:
    """Cache statistics for monitoring"""

    hits: int = 0
    misses: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class OptimizedDisciplineCache:
    """Optimized caching for discipline codes with statistics"""

    def __init__(self):
        self.stats = CacheStats()
        # Pre-populate with NORSOK discipline codes
        self._discipline_cache = {
            "KA": "Emergency Power System",
            "PI": "Pressure Instrument",
            "EL": "Electrical System",
            "PS": "Process System",
            "IN": "Instrumentation",
            "HV": "HVAC System",
            "ME": "Mechanical Equipment",
            "TE": "Telecommunication",
            "FI": "Fire & Gas Detection",
            "ES": "Emergency Shutdown System",
            "MO": "Monitoring System",
            "PU": "Pumps and Piping",
            "VA": "Valves and Actuators",
            "ST": "Structures",
            "LY": "Hydraulic System",
            "TIC": "Technical Information and Communication",
            "TT": "Temperature Transmitter",
            "PIC": "Pressure Indicating Controller",
            "PT": "Pressure Transmitter",
            "FT": "Flow Transmitter",
            "NY": "Non-standard code (project-specific or unknown)",
            "LIC": "Level Indicating Controller",
            "FE": "Fire and Gas Detection Equipment",
            "YA": "Analyzer Systems",
            "PDT": "Differential Pressure Transmitter",
            "XA": "Cathodic Protection Systems",
            "GK": "Gas Compression Systems",
            "PC": "Process Control Systems",
            "FZSL": "Fire Zone Safety Logic",
            "ESDV": "Emergency Shutdown Valve",
            "PDI": "Pressure Differential Indicator",
            "LV": "Level Valve",
            "LT": "Level Transmitter",
            "ZA": "Miscellaneous Systems",
            "YZSL": "Utility Zone Safety Logic",
            "YZSH": "Utility Zone Safety High",
            "ZS": "Safety Systems",
            "A": "Automation / Analyzer Systems",
            "B": "Building / Structural",
            "C": "Civil / Concrete / Roads",
            "D": "Drilling Equipment",
            "E": "Electrical",
            "F": "Fire & Safety Systems",
            "G": "Gas & Metering",
            "H": "HVAC (Heating, Ventilation & Air Conditioning)",
            "I": "Instrumentation & Control",
            "J": "Jacking & Lifting Equipment",
            "K": "Telecommunications",
            "L": "Loading / Lifting Equipment",
            "M": "Mechanical / Rotating Equipment",
            "N": "Naval / Marine Systems",
            "P": "Process / Piping",
            "Q": "Subsea Equipment",
            "R": "Riser System",
            "S": "Structural",
            "T": "Turbines / Power Generation",
            "U": "Umbilicals",
            "V": "Valves / Actuators",
            "W": "Water Treatment",
            "X": "Cathodic Protection / Corrosion",
            "Y": "Utility Systems",
            "Z": "Miscellaneous / Special Systems",
        }

    def get_discipline_meaning(self, code: str) -> str:
        """Get discipline meaning with caching"""
        if code in self._discipline_cache:
            self.stats.hits += 1
            return self._discipline_cache[code]

        self.stats.misses += 1
        return "Unknown Discipline"

    def get_stats(self) -> CacheStats:
        """Get cache statistics"""
        return self.stats


# ===== OPTIMIZED REGEX UTILITIES =====


class RegexPatternCache:
    """Optimized regex pattern caching"""

    def __init__(self):
        self._pattern_cache: Dict[str, re.Pattern] = {}
        self._compile_norsok_patterns()

    def _compile_norsok_patterns(self):
        """Pre-compile common NORSOK patterns"""
        patterns = {
            "norsok_split": r"[_:.]",
            "tag_validation": r"^[A-Z0-9]+-[A-Z0-9]+-[A-Z0-9]+",
            "equipment_number": r"[A-Z0-9]+-[A-Z0-9]+-(.+)",
        }

        for name, pattern in patterns.items():
            self._pattern_cache[name] = re.compile(pattern)

    @lru_cache(maxsize=1000)
    def get_pattern(self, pattern: str) -> re.Pattern:
        """Get compiled regex pattern with caching"""
        if pattern not in self._pattern_cache:
            self._pattern_cache[pattern] = re.compile(pattern)
        return self._pattern_cache[pattern]

    def split_norsok_tag(self, tag: str) -> List[str]:
        """Optimized NORSOK tag splitting"""
        pattern = self._pattern_cache.get("norsok_split")
        if pattern:
            return pattern.split(tag, 2)
        return tag.split("-")


# ===== BATCH PROCESSING UTILITIES =====


class BatchProcessor:
    """Optimized batch processing for metadata updates"""

    def __init__(self, batch_size: int = 1000, max_workers: int = 4):
        self.batch_size = batch_size
        self.max_workers = max_workers

    def process_nodes_in_batches(
        self,
        nodes: NodeList[Node],
        process_func: Callable,
        logger: CogniteFunctionLogger,
        *args,
        **kwargs,
    ) -> List[NodeApply]:
        """Process nodes in optimized batches"""

        results = []
        node_type = kwargs.get("node_type", "nodes")
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
                    monitor_memory_usage(
                        logger, f"After batch {i//self.batch_size + 1}"
                    )

        return results

    def apply_updates_in_batches(
        self,
        client: CogniteClient,
        updates: List[NodeApply],
        logger: CogniteFunctionLogger,
        batch_size: int = 2000,
    ) -> int:
        """Apply updates in optimized batches with retry logic"""

        if not updates:
            return 0

        total_applied = 0

        with time_operation(f"Applying {len(updates)} updates in batches", logger):
            for i in range(0, len(updates), batch_size):
                batch = updates[i : i + batch_size]

                try:
                    self._apply_batch_with_retry(client, batch, logger)
                    total_applied += len(batch)
                    logger.info(
                        f"Applied batch {i//batch_size + 1}: {len(batch)} updates"
                    )

                except Exception as e:
                    logger.warning(
                        f"Large batch failed, retrying with smaller chunks: {e}"
                    )
                    # Split batch and retry smaller chunks
                    small_batch_size = batch_size // 4
                    for j in range(0, len(batch), small_batch_size):
                        small_batch = batch[j : j + small_batch_size]
                        self._apply_batch_with_retry(client, small_batch, logger)
                        total_applied += len(small_batch)

        return total_applied

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _apply_batch_with_retry(
        self,
        client: CogniteClient,
        batch: List[NodeApply],
        logger: CogniteFunctionLogger,
    ):
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
        self.discipline_cache = OptimizedDisciplineCache()
        self.regex_cache = RegexPatternCache()
        self.batch_processor = BatchProcessor()
        self.stats = {"processed": 0, "updated": 0, "cache_hits": 0, "cache_misses": 0}

    def process_timeseries_metadata(
        self, node: Node, view_id: ViewId, node_space: str
    ) -> Optional[NodeApply]:
        """Process timeseries metadata with optimizations"""

        try:
            ext_id = node.external_id
            properties = node.properties[view_id]

            name = str(properties.get("name", ""))
            description = str(properties.get("description", ""))
            aliases_raw = properties.get("aliases", [])
            tags_raw = properties.get("tags", [])
            aliases = (
                [str(x) for x in aliases_raw] if isinstance(aliases_raw, list) else []
            )
            tags = [str(x) for x in tags_raw] if isinstance(tags_raw, list) else []

            # Optimized parsing
            summary, upd_tags, upd_aliases = self._parse_norsok_tag_optimized(
                name, tags, aliases
            )

            # Check if update is needed
            update_needed = False
            properties_dict = {}

            if not description and summary:
                properties_dict["description"] = summary
                update_needed = True

            if upd_tags != tags:
                properties_dict["tags"] = upd_tags
                update_needed = True

            if upd_aliases != aliases:
                properties_dict["aliases"] = upd_aliases
                update_needed = True

            self.stats["processed"] += 1

            if update_needed:
                self.stats["updated"] += 1
                self.logger.debug(
                    f"Updating TS: {name} with {len(properties_dict)} properties"
                )

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
        self, node: Node, view_id: ViewId, node_space: str
    ) -> Optional[NodeApply]:
        """Process asset metadata with optimizations"""

        try:
            ext_id = node.external_id
            properties = node.properties[view_id]

            name = str(properties.get("name", ""))
            aliases_raw = properties.get("aliases", [])
            tags_raw = properties.get("tags", [])
            aliases = (
                [str(x) for x in aliases_raw] if isinstance(aliases_raw, list) else []
            )
            tags = [str(x) for x in tags_raw] if isinstance(tags_raw, list) else []
            root_obj = properties.get("root", {})
            root = (
                str(root_obj.get("externalId", ""))
                if isinstance(root_obj, dict)
                else ""
            )

            # Optimized parsing
            upd_tags, upd_aliases = self._parse_asset_tag_optimized(
                name, aliases, root, tags
            )

            # Check if update is needed
            update_needed = False
            properties_dict = {}

            if upd_tags != tags:
                properties_dict["tags"] = upd_tags
                update_needed = True

            if upd_aliases != aliases:
                properties_dict["aliases"] = upd_aliases
                update_needed = True

            self.stats["processed"] += 1

            if update_needed:
                self.stats["updated"] += 1
                self.logger.debug(
                    f"Updating asset: {ext_id} with {len(properties_dict)} properties"
                )

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

    def process_file_metadata(
        self, node: Node, view_id: ViewId, node_space: str
    ) -> Optional[NodeApply]:
        """Process file metadata with optimizations"""

        try:
            ext_id = node.external_id
            properties = node.properties[view_id]

            name = str(properties.get("name", ""))
            aliases_raw = properties.get("aliases", [])
            aliases = (
                [str(x) for x in aliases_raw] if isinstance(aliases_raw, list) else []
            )

            # Optimized alias generation
            upd_aliases = self._get_file_alias_list_optimized(name, tuple(aliases))

            self.stats["processed"] += 1

            if upd_aliases != aliases:
                self.stats["updated"] += 1
                self.logger.debug(
                    f"Updating file: {ext_id} with {len(upd_aliases)} aliases"
                )

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

    def _parse_norsok_tag_optimized(
        self, tag: str, tags: List[str], aliases: List[str]
    ) -> Tuple[Optional[str], List[str], List[str]]:
        """Optimized NORSOK tag parsing with caching"""

        try:
            tag_elements = self.regex_cache.split_norsok_tag(tag)

            if len(tag_elements) < 3:
                return None, tags, aliases

            if len(tag_elements) > 1 and f"site:{tag_elements[0]}" not in tags:
                tags.append(f"site:{tag_elements[0]}")

            if len(tag_elements) > 2:
                aliases = self._get_timeseries_alias_list_optimized(tag_elements[1])

            pattern = r"[-]"
            split_result = re.split(pattern, tag_elements[1])
            if split_result:
                area = split_result[0]
                discipline_code = None
                if len(split_result) > 1:
                    discipline_code = split_result[1]
                equipment_number = None
                if len(split_result) > 2:
                    equipment_number = "-".join(split_result[2:])
                if discipline_code not in tags:
                    tags.append(f"discipline:{discipline_code}")
                if area not in tags:
                    tags.append(f"area:{area}")

                summary = f"Area/System Code: {area}"
                if discipline_code:
                    discipline_meaning = self.discipline_cache.get_discipline_meaning(
                        discipline_code
                    )
                    summary = f"{summary} - Discipline Code: {discipline_code} - Discipline Meaning: {discipline_meaning}"
                    if discipline_meaning == "Unknown Discipline":
                        self.logger.warning(
                            f"Unknown discipline code: {discipline_code}"
                        )

                if equipment_number:
                    summary = f"{summary} - Equipment Number: {equipment_number}"
                return summary, tags, aliases
            else:
                self.logger.error(f"Error: Invalid NORSOK tag format: {tag}")
                return None, tags, aliases
        except Exception as e:
            self.logger.error(f"Error: invalid tag : {tag} - error: {e}")
            return None, tags, aliases

    def _parse_asset_tag_optimized(
        self, name: str, aliases: List[str], root: str, tags: List[str]
    ) -> Tuple[List[str], List[str]]:
        """Optimized asset tag parsing"""

        try:
            # Simple optimization for asset parsing
            upd_aliases = self._get_asset_alias_list_optimized(name, tuple(aliases))

            # Add root tag if not present
            if root and f"root:{root}" not in tags:
                tags.append(f"root:{root}")

            return tags, upd_aliases

        except Exception as e:
            self.logger.error(f"Error parsing asset tag {name}: {e}")
            return tags, aliases

    @lru_cache(maxsize=5000)
    def _get_timeseries_alias_list_optimized(self, name: str) -> List[str]:
        """Optimized timeseries alias generation with caching"""
        aliases = []

        # Add name if not in aliases
        if name not in aliases:
            aliases.append(name)

        # Add name without dashes
        # name_no_dash = name.replace('-', '')
        # if name_no_dash not in aliases:
        #     aliases.append(name_no_dash)

        return aliases

    @lru_cache(maxsize=5000)
    def _get_asset_alias_list_optimized(
        self, name: str, aliases_tuple: Tuple[str, ...]
    ) -> List[str]:
        """Optimized asset alias generation with caching"""
        aliases = list(aliases_tuple)

        # Add name if not in aliases
        if name not in aliases:
            aliases.append(name)

        return aliases

    @lru_cache(maxsize=5000)
    def _get_file_alias_list_optimized(
        self, name: str, aliases_tuple: Tuple[str, ...]
    ) -> List[str]:
        """Optimized file alias generation with caching"""
        aliases = list(aliases_tuple)

        # Add name if not in aliases
        if name not in aliases:
            aliases.append(name)

        # Add name without extension
        name_no_ext = name.split(".")[0] if "." in name else name
        if name_no_ext not in aliases:
            aliases.append(name_no_ext)

        return aliases

    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        cache_stats = self.discipline_cache.get_stats()
        return {
            "processed": self.stats["processed"],
            "updated": self.stats["updated"],
            "update_rate": self.stats["updated"] / self.stats["processed"]
            if self.stats["processed"] > 0
            else 0,
            "cache_hit_rate": cache_stats.hit_rate,
            "cache_hits": cache_stats.hits,
            "cache_misses": cache_stats.misses,
        }


# ===== PERFORMANCE BENCHMARK =====


class PerformanceBenchmark:
    """Performance benchmarking utilities"""

    def __init__(self, logger: CogniteFunctionLogger):
        self.logger = logger
        self.benchmarks: Dict[str, List[float]] = {}

    def benchmark_function(self, name: str, func: Callable, *args, **kwargs) -> Any:
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
            self.logger.info(
                f"  {name}: {len(times)} calls, avg {avg_time:.2f}s, total {total_time:.2f}s"
            )


# ===== UTILITY FUNCTIONS =====


def optimize_metadata_processing():
    """Apply global optimizations for metadata processing"""

    # Increase garbage collection threshold
    gc.set_threshold(700, 10, 10)

    # Set process priority if possible
    try:
        import os

        os.nice(-5)
    except:
        pass

    return True


# ===== EXPORT MAIN CLASSES =====

__all__ = [
    "time_operation",
    "monitor_memory_usage",
    "cleanup_memory",
    "OptimizedDisciplineCache",
    "RegexPatternCache",
    "BatchProcessor",
    "OptimizedMetadataProcessor",
    "PerformanceBenchmark",
    "optimize_metadata_processing",
]
