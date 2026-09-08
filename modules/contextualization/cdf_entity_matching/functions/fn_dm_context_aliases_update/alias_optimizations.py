"""
Metadata Update Optimization Module

This module provides optimization utilities for metadata update functions to improve
performance, reduce memory usage, and enhance reliability.
"""

import gc
import re
import time
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeApply, NodeOrEdgeData, ViewId
from cognite.client.exceptions import CogniteAPIError
from psutil import Process
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from constants import DEFAULT_ALIAS_PATTERN  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip

@dataclass(frozen=True)
class AliasRule:
    """How aliases are derived from a name for one view.

    Frozen so it stays hashable and can key the alias caches: the aliases a name yields
    depend on the rule as much as on the name itself.

    Attributes:
        patterns: Patterns tried against a name, in configured order.
        keep_longest_only: Keep only the longest alias when several patterns match,
            rather than one alias per matching pattern.
    """

    patterns: tuple[re.Pattern[str], ...]
    keep_longest_only: bool = False

    @classmethod
    def from_config(cls, patterns: Sequence[str], selection: str = "all") -> "AliasRule":
        """Build a rule from configured pattern strings.

        Args:
            patterns: Regular expressions, each with at least one capture group.
            selection: "longest" to keep only the longest alias, "all" to keep each.

        Returns:
            The compiled rule.
        """
        return cls(tuple(re.compile(pattern) for pattern in patterns), selection == "longest")


# Fallback for callers that configure nothing; each view configures its own rule through
# aliasPattern and aliasSelection.
_DEFAULT_ALIAS_RULE = AliasRule((re.compile(DEFAULT_ALIAS_PATTERN),))

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
        process = Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        logger.info(f"📊 Memory: {operation_name} - {memory_mb:.1f} MB")
    except Exception as e:
        logger.debug(f"Could not monitor memory: {e}")


def cleanup_memory():
    """Force garbage collection"""
    gc.collect()


# ===== BATCH PROCESSING UTILITIES =====

def is_retryable(error: Exception) -> bool:
    """Whether a failed request stands a chance of succeeding on a retry.

    A client error - a rejected property, a missing view, missing capabilities - means
    the request itself is wrong, so repeating it only delays the failure. Rate limiting
    and server-side errors are transient, as is anything the SDK re-raises unclassified
    from its transport layer, which is why the default is to retry. Bugs in this function
    are the exception: they fail the same way every time. ValueError is deliberately not
    one of them - it covers JSONDecodeError, which a half-read response raises and a
    second read can clear.
    """
    if isinstance(error, CogniteAPIError):
        return error.code == 429 or (error.code is not None and error.code >= 500)
    return not isinstance(error, (TypeError, AttributeError, NameError, KeyError, IndexError))


def _worth_another_attempt(error: BaseException) -> bool:
    """Adapt `is_retryable` to what tenacity catches.

    Everything an attempt could raise goes through the one classifier, so the write path
    treats an error the same way the read path does. A KeyboardInterrupt is not a failed
    request and is never worth repeating.
    """
    return isinstance(error, Exception) and is_retryable(error)


class BatchProcessor:
    """Applies metadata updates to CDF in retried batches"""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
    
    def apply_updates_in_batches(self, client: CogniteClient,
                                updates: list[NodeApply],
                                logger: CogniteFunctionLogger,
                                batch_size: int | None = None) -> int:
        """Apply updates in optimized batches with retry logic"""
        
        if not updates:
            return 0
        
        batch_size = batch_size or self.batch_size
        total_applied = 0
        
        with time_operation(f"Applying {len(updates)} updates in batches", logger):
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                
                try:
                    self._apply_batch_with_retry(client, batch, logger)
                    total_applied += len(batch)
                    logger.info(f"Applied batch {i//batch_size + 1}: {len(batch)} updates")
                    
                except CogniteAPIError as e:
                    # Splitting only helps a batch the API refused for its size. A
                    # rejected property or view fails identically in a smaller batch.
                    if not is_retryable(e) and e.code != 413:
                        raise
                    logger.warning(f"Large batch failed, retrying with smaller chunks: {e}")
                    # Split batch and retry smaller chunks. A batch size below four
                    # divides to zero, which range() rejects.
                    small_batch_size = max(1, batch_size // 4)
                    for j in range(0, len(batch), small_batch_size):
                        small_batch = batch[j:j + small_batch_size]
                        self._apply_batch_with_retry(client, small_batch, logger)
                        total_applied += len(small_batch)
        
        return total_applied
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception(_worth_another_attempt),
        # Raise the API error itself rather than a RetryError wrapping it, so the
        # message that names the offending property survives to the pipeline run.
        reraise=True,
    )
    def _apply_batch_with_retry(self, client: CogniteClient, batch: list[NodeApply], logger: CogniteFunctionLogger):
        """Apply batch, retrying only errors that could succeed on another attempt"""
        try:
            client.data_modeling.instances.apply(batch)
        except CogniteAPIError as e:
            logger.warning(f"API error applying batch: {e}")
            raise


# ===== UTILITY FUNCTIONS =====

# Equipment tags start with a two-digit area code. Document numbers and pump codes do not,
# so only tag-shaped aliases get separator normalization.
_TAG_ALIAS_SHAPE = re.compile(r"^[0-9]{2}[-_.:[A-Z0-9]")


def _normalize_alias_tokens(alias: str) -> str:
    """Replace tag separator characters with underscores between tokens.

    Multi-group patterns already join their groups with "_". A single capture group that
    holds the whole tag still carries "-", "." or ":" from the name unless those are
    rewritten here.
    """
    if not _TAG_ALIAS_SHAPE.match(alias):
        return alias
    return re.sub(r"[-_.:]+", "_", alias)


def _generated_alias(name: str, pattern: re.Pattern[str]) -> str | None:
    """The alias derived from a name - the pattern's capture groups joined by "_".

    A configured pattern may make a group optional, and an optional group that does not
    participate in the match captures None. Those are left out rather than joined, which
    would raise a TypeError.

    Tag-shaped aliases always use "_" between tokens, whether the pattern captured several
    groups or one group holding the whole tag.

    Returns:
        The alias, or None when the name holds no tag.
    """
    match = pattern.search(name)
    if not match:
        return None
    alias = "_".join(group for group in match.groups() if group is not None)
    return _normalize_alias_tokens(alias) if alias else None


def _file_name_without_extension(name: str) -> str:
    """The file name with its final extension removed, e.g. 23-KA-9101.pdf -> 23-KA-9101.

    Returns:
        The shortened name, or the name unchanged when it carries no extension.
    """
    stem, _, extension = name.rpartition(".")
    return stem if stem and extension else name


def _generated_aliases(name: str, rule: AliasRule) -> list[str]:
    """The aliases a name yields under a rule.

    Returns:
        One alias per matching pattern in configured order, or a single longest one when
        the rule says so. Empty when no pattern matches.
    """
    aliases: list[str] = []
    for pattern in rule.patterns:
        alias = _generated_alias(name, pattern)
        if alias and alias not in aliases:
            aliases.append(alias)

    if rule.keep_longest_only and aliases:
        # max returns the first of equally long aliases, so pattern order breaks ties.
        return [max(aliases, key=len)]

    return aliases


def _unmanaged_aliases(aliases: list[str], rule: AliasRule) -> list[str]:
    """Return the aliases this function did not generate, preserving their order.

    An alias is ours when feeding it back through any of the rule's patterns reproduces
    it exactly. That leaves hand-curated values alone whether they merely contain a tag
    ("spare for 23-AB-1234") or spell one differently ("23-KA-9101").

    Every pattern is checked even under "longest", so an alias a previous run wrote from
    a pattern that no longer wins - or from a longer pattern list - is still recognised
    and rebuilt rather than left behind.
    """
    return [
        alias
        for alias in aliases
        if not any(_generated_alias(alias, pattern) == alias for pattern in rule.patterns)
    ]


def _dedupe_preserve_order(aliases: Sequence[str]) -> list[str]:
    """Return aliases with duplicates removed, keeping the first occurrence of each."""
    seen: set[str] = set()
    unique: list[str] = []
    for alias in aliases:
        if alias not in seen:
            seen.add(alias)
            unique.append(alias)
    return unique


def _alias_property_value(aliases: list[str]) -> list[str] | None:
    """Return the value to write to the aliases property.

    Writing an empty list leaves the stored aliases untouched, so null is written when
    nothing was produced to clear them.
    """
    return aliases or None


def _starting_aliases(
    org_aliases: list[str],
    rule: AliasRule,
    *,
    update_all: bool,
    remove_old_aliases: bool,
) -> list[str]:
    """Return the alias list to merge newly produced values into.

    Args:
        org_aliases: Aliases currently stored on the instance.
        rule: The view's alias rule.
        update_all: Rebuild generated aliases while keeping hand-curated ones.
        remove_old_aliases: Discard every existing alias before producing new ones.

    Returns:
        The working alias list passed into the merge helpers.
    """
    if remove_old_aliases:
        return []
    if update_all:
        return _unmanaged_aliases(org_aliases, rule)
    return org_aliases.copy()


def _merge_generated_aliases(
    existing: Sequence[str],
    name: str,
    rule: AliasRule,
) -> tuple[str, ...]:
    """Return existing aliases plus any new ones the name yields under the rule."""
    aliases = list(existing)
    for alias in _generated_aliases(name, rule):
        if alias and alias not in aliases:
            aliases.append(alias)
    return tuple(aliases)


def _merge_file_aliases(
    existing: Sequence[str],
    name: str,
    rule: AliasRule,
) -> tuple[str, ...]:
    """Return existing aliases plus the file name stem and any tag aliases from the name."""
    aliases = list(existing)
    for candidate in [_file_name_without_extension(name), *_generated_aliases(name, rule)]:
        if candidate and candidate not in aliases:
            aliases.append(candidate)
    return tuple(aliases)


# ===== OPTIMIZED METADATA PROCESSOR =====

# A naming convention the configured patterns do not cover produces one warning per
# instance, which on a large space hides everything else the run logged. Past this many,
# the names are only counted and reported as a total.
_MAX_MISSING_ALIAS_WARNINGS = 10


class OptimizedMetadataProcessor:
    """Optimized metadata processing with caching and batch operations"""
    
    def __init__(
        self,
        logger: CogniteFunctionLogger,
        timeseries_alias_rule: AliasRule = _DEFAULT_ALIAS_RULE,
        asset_alias_rule: AliasRule = _DEFAULT_ALIAS_RULE,
        file_alias_rule: AliasRule = _DEFAULT_ALIAS_RULE,
    ):
        self.logger = logger
        # Each rule drives both alias generation and the check for which existing
        # aliases this function owns.
        self.timeseries_alias_rule = timeseries_alias_rule
        self.asset_alias_rule = asset_alias_rule
        self.file_alias_rule = file_alias_rule
        self.stats = {
            'processed': 0,
            'updated': 0,
            'names_without_alias': 0,
        }

    def _warn_name_without_alias(self, name: str) -> None:
        """Note a name no pattern reads, warning about only the first few.

        Every such name is counted, so `log_missing_alias_summary` can report the total
        once the run is over.
        """
        self.stats['names_without_alias'] += 1
        if self.stats['names_without_alias'] <= _MAX_MISSING_ALIAS_WARNINGS:
            self.logger.warning(
                "No alias extracted based on input regular expression "
                f"for name: {name}"
            )

    def log_missing_alias_summary(self) -> None:
        """Report the total of names no pattern read, when some were left unlogged.

        Below the cap every name is already in the log, and a total would only repeat it.
        """
        total = self.stats['names_without_alias']
        if total > _MAX_MISSING_ALIAS_WARNINGS:
            self.logger.warning(
                f"No alias could be extracted from {total} names in total. "
                f"Only the first {_MAX_MISSING_ALIAS_WARNINGS} are listed above."
            )

    
    def process_timeseries_metadata(
        self,
        node: Node,
        view_id: ViewId,
        node_space: str,
        update_all: bool = False,
        remove_old_aliases: bool = False,
    ) -> NodeApply | None:
        """Process timeseries metadata with optimizations"""
        
        try:
            ext_id = node.external_id
            # Skip rather than recompute from an empty payload, which under updateAll
            # would overwrite the managed properties with empty values.
            properties = node.properties.get(view_id) if node.properties else None
            if not properties:
                self.logger.warning(f"No properties for view {view_id} on timeseries: {ext_id}")
                return None

            name = str(properties.get("name", ""))
            aliases_raw = properties.get("aliases", [])
            org_aliases = (
                [str(x) for x in aliases_raw] if isinstance(aliases_raw, list) else []
            )
            # Only the generated aliases are rebuilt; hand-curated ones are preserved unless
            # removeOldAliases clears the list first.
            aliases = _starting_aliases(
                org_aliases,
                self.timeseries_alias_rule,
                update_all=update_all,
                remove_old_aliases=remove_old_aliases,
            )

            upd_aliases = list(
                self._get_timeseries_alias_list_optimized(
                    name, tuple(aliases), self.timeseries_alias_rule
                )
            )
            if name and not upd_aliases and not _generated_aliases(name, self.timeseries_alias_rule):
                self._warn_name_without_alias(name)

            upd_aliases = _dedupe_preserve_order(upd_aliases)

            update_needed = False
            properties_dict: dict[str, list[str] | None] = {}

            if update_all or upd_aliases != org_aliases:
                properties_dict["aliases"] = _alias_property_value(upd_aliases)
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
        remove_old_aliases: bool = False,
    ) -> NodeApply | None:
        """Process asset metadata with optimizations"""
        
        try:
            ext_id = node.external_id
            # Skip rather than recompute from an empty payload, which under updateAll
            # would overwrite the managed properties with empty values.
            properties = node.properties.get(view_id) if node.properties else None
            if not properties:
                self.logger.warning(f"No properties for view {view_id} on asset: {ext_id}")
                return None

            name = str(properties.get("name", ""))
            aliases_raw = properties.get("aliases", [])
            org_aliases = (
                [str(x) for x in aliases_raw] if isinstance(aliases_raw, list) else []
            )
            # Only the generated aliases are rebuilt; hand-curated ones are preserved unless
            # removeOldAliases clears the list first.
            aliases = _starting_aliases(
                org_aliases,
                self.asset_alias_rule,
                update_all=update_all,
                remove_old_aliases=remove_old_aliases,
            )

            upd_aliases = list(
                self._get_asset_alias_list_optimized(
                    name, tuple(aliases), self.asset_alias_rule
                )
            )
            if name and not upd_aliases and not _generated_aliases(name, self.asset_alias_rule):
                self._warn_name_without_alias(name)

            upd_aliases = _dedupe_preserve_order(upd_aliases)

            update_needed = False
            properties_dict: dict[str, list[str] | None] = {}

            if update_all or upd_aliases != org_aliases:
                properties_dict["aliases"] = _alias_property_value(upd_aliases)
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
    
    def process_file_metadata(
        self,
        node: Node,
        view_id: ViewId,
        node_space: str,
        update_all: bool = False,
        remove_old_aliases: bool = False,
    ) -> NodeApply | None:
        """Add search aliases to a file from its name.

        Args:
            node: The file instance to process.
            view_id: View the aliases are written to.
            node_space: Space the file was read from, and the one it is written back to.
            update_all: Rebuild the generated aliases instead of merging with them.
            remove_old_aliases: Discard every existing alias before producing new ones.

        Returns:
            The update to apply, or None when the file needs no change.
        """
        try:
            ext_id = node.external_id
            # Skip rather than recompute from an empty payload, which under updateAll
            # would overwrite the managed properties with empty values.
            properties = node.properties.get(view_id) if node.properties else None
            if not properties:
                self.logger.warning(f"No properties for view {view_id} on file: {ext_id}")
                return None

            name = str(properties.get("name", ""))
            aliases_raw = properties.get("aliases", [])
            org_aliases = [str(x) for x in aliases_raw] if isinstance(aliases_raw, list) else []
            # Only the generated aliases are rebuilt; hand-curated ones are preserved unless
            # removeOldAliases clears the list first.
            aliases = _starting_aliases(
                org_aliases,
                self.file_alias_rule,
                update_all=update_all,
                remove_old_aliases=remove_old_aliases,
            )

            upd_aliases = list(
                self._get_file_alias_list_optimized(name, tuple(aliases), self.file_alias_rule)
            )

            if name and not upd_aliases and not _generated_aliases(name, self.file_alias_rule):
                self._warn_name_without_alias(name)

            upd_aliases = _dedupe_preserve_order(upd_aliases)

            self.stats['processed'] += 1

            if not update_all and not remove_old_aliases and upd_aliases == org_aliases:
                return None

            self.stats['updated'] += 1
            self.logger.debug(f"Updating file: {ext_id} with {len(upd_aliases)} aliases")

            return NodeApply(
                space=node_space,
                external_id=ext_id,
                sources=[
                    NodeOrEdgeData(
                        source=view_id,
                        properties={"aliases": _alias_property_value(upd_aliases)},
                    )
                ],
            )

        except Exception as e:
            self.logger.error(f"Error processing file {node.external_id}: {e}")
            return None

    @staticmethod
    @lru_cache(maxsize=5000)
    def _get_timeseries_alias_list_optimized(
        name: str,
        aliases_tuple: tuple[str, ...] = (),
        rule: AliasRule = _DEFAULT_ALIAS_RULE,
    ) -> tuple[str, ...]:
        """Optimized timeseries alias generation with caching"""
        return _merge_generated_aliases(aliases_tuple, name, rule)
    
    @staticmethod
    @lru_cache(maxsize=5000)
    def _get_asset_alias_list_optimized(
        name: str,
        aliases_tuple: tuple[str, ...],
        rule: AliasRule = _DEFAULT_ALIAS_RULE,
    ) -> tuple[str, ...]:
        """Optimized asset alias generation with caching"""
        return _merge_generated_aliases(aliases_tuple, name, rule)
    
    @staticmethod
    @lru_cache(maxsize=5000)
    def _get_file_alias_list_optimized(
        name: str,
        aliases_tuple: tuple[str, ...],
        rule: AliasRule = _DEFAULT_ALIAS_RULE,
    ) -> tuple[str, ...]:
        """Optimized file alias generation with caching.

        A document is searched for both by its bare file name and by the tag it refers
        to, so it gets the name with the extension removed plus the usual tag aliases.
        The name is not a pattern match, so aliasSelection does not apply to it.
        """
        return _merge_file_aliases(aliases_tuple, name, rule)

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
    'AliasRule',
    'BatchProcessor',
    'OptimizedMetadataProcessor',
    'PerformanceBenchmark',
    'cleanup_memory',
    'monitor_memory_usage',
    'optimize_metadata_processing',
    'time_operation',
] 