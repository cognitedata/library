import abc
import re
from collections import defaultdict
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import cast

from cognite.client import CogniteClient
from cognite.client.data_classes import Row, RowWrite
from cognite.client.data_classes.data_modeling import (
    NodeList,
)
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from services.ConfigService import Config, ViewPropertyConfig
from services.DataModelService import IDataModelService
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import entity


def count_pattern_sample_strings(pattern_groups: list[dict]) -> int:
    """Count sample strings across pattern-mode entity groups."""
    return sum(len(group.get("sample") or []) for group in pattern_groups)


def format_pattern_groups_for_log(pattern_groups: list[dict], *, max_samples_per_group: int = 80) -> list[str]:
    """Format pattern sample groups as indented log lines."""
    lines: list[str] = []
    for group in pattern_groups:
        samples = group.get("sample") or []
        if isinstance(samples, str):
            samples = [samples]
        lines.append(
            f"  [{group.get('resource_type')}/{group.get('annotation_type')}] {len(samples)} sample(s)"
        )
        preview = samples[:max_samples_per_group]
        for sample in preview:
            lines.append(f"    - {sample}")
        if len(samples) > len(preview):
            lines.append(f"    ... +{len(samples) - len(preview)} more")
    return lines


def split_entities_by_kind(entities: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Split diagram-detect entities into asset-like vs file-like lists.

    Uses annotation_type when present; otherwise treats unknown as assets.
    """
    assets: list[dict] = []
    files: list[dict] = []
    for row in entities:
        if row.get("annotation_type") == "diagrams.FileLink":
            files.append(row)
        else:
            assets.append(row)
    return assets, files


def entities_missing_search_property(entities: list[dict]) -> list[dict]:
    """Return entities with empty or missing search_property (aliases)."""
    missing: list[dict] = []
    for row in entities:
        search = row.get("search_property")
        if not search:
            missing.append(row)
    return missing


class ICacheService(abc.ABC):
    """
    Manages a persistent cache of entities to pass into diagram detect (e.g., assets, files)
    stored in a CDF RAW table. This avoids repeatedly fetching the same data for files
    that share the same operational context.
    """

    @abc.abstractmethod
    def get_entities(
        self,
        data_model_service: IDataModelService,
        primary_scope_value: str,
        secondary_scope_value: str | None,
    ) -> tuple[list[dict], list[dict]]:
        pass

    @abc.abstractmethod
    def _update_cache(self, row_to_write: RowWrite) -> None:
        pass

    @abc.abstractmethod
    def _validate_cache(self, last_update_datetime_str: str) -> bool:
        pass

    @abc.abstractmethod
    def _generate_tag_samples_from_entities(self, entities: list[dict], *, source_view: str) -> list[dict]:
        pass


class GeneralCacheService(ICacheService):
    """
    Manages a persistent cache of entities to pass into diagram detect (e.g., assets, files)
    stored in a CDF RAW table. This avoids repeatedly fetching the same data for files
    that share the same operational context.
    """

    def __init__(self, config: Config, client: CogniteClient, logger: CogniteFunctionLogger):
        self.client = client
        self.config = config
        self.logger = logger

        self.db_name: str = config.raw_tables.raw_db
        self.tbl_name: str = config.raw_tables.raw_table_cache
        self.manual_patterns_tbl_name: str = config.raw_tables.raw_manual_patterns_catalog
        self.cache_time_limit: int = config.launch_function.cache_service.cache_time_limit  # in hours

        self.file_view: ViewPropertyConfig = config.data_model_views.file_view
        self.target_entities_view: ViewPropertyConfig = config.data_model_views.target_entities_view

    def get_entities(
        self,
        data_model_service: IDataModelService,
        primary_scope_value: str,
        secondary_scope_value: str | None,
    ) -> tuple[list[dict], list[dict]]:
        """
        Retrieves or generates entities and pattern samples for diagram detection.

        This method orchestrates the cache lifecycle: checking validity, fetching fresh data if needed,
        generating pattern samples, and updating the cache. The cache is scoped by primary and secondary
        scope values to ensure relevant entities are used for each file context.

        Args:
            data_model_service: Service instance for querying data model instances.
            primary_scope_value: Primary scope identifier (e.g., site, facility).
            secondary_scope_value: Optional secondary scope identifier (e.g., unit, area).

        Returns:
            A tuple containing:
                - Combined list of entity dictionaries (assets + files) for diagram detection.
                - Combined list of pattern sample dictionaries for pattern mode detection.
        """
        entities: list[dict] = []
        key = f"{primary_scope_value}_{secondary_scope_value}" if secondary_scope_value else f"{primary_scope_value}"

        try:
            row: Row | None = self.client.raw.rows.retrieve(db_name=self.db_name, table_name=self.tbl_name, key=key)
        except (CogniteAPIError, CogniteNotFoundError):
            row = None

        # Attempt to retrieve from the cache
        if row and row.columns and self._validate_cache(row.columns["LastUpdateTimeUtcIso"]):
            self.logger.info(f"Cache is up-to-date for key: {key}\nEntities and patterns loaded from: CACHE.")
            asset_entities: list[dict] = row.columns.get("AssetEntities", [])
            file_entities: list[dict] = row.columns.get("FileEntities", [])
            asset_pattern_samples: list[dict] = row.columns.get("AssetPatternSamples", [])
            file_pattern_samples: list[dict] = row.columns.get("FilePatternSamples", [])
            combined_pattern_samples: list[dict] = row.columns.get("CombinedPatternSamples", [])
            entities = asset_entities + file_entities
            self._log_launch_input_summary(
                scope_key=key,
                source="CACHE",
                asset_entities=asset_entities,
                file_entities=file_entities,
                asset_pattern_samples=asset_pattern_samples,
                file_pattern_samples=file_pattern_samples,
                pattern_samples=combined_pattern_samples,
            )
            return entities, combined_pattern_samples

        self.logger.info(f"Cache is out-of-date for key: {key}\nEntities and patterns loaded from: CDF (fresh fetch)")

        # Fetch data
        asset_instances, file_instances = data_model_service.get_instances_entities(
            primary_scope_value, secondary_scope_value
        )

        # Convert to entities for diagram detect job
        asset_entities, file_entities = self._convert_instances_to_entities(asset_instances, file_instances)
        entities = asset_entities + file_entities

        # Generate pattern samples from the same entities
        asset_pattern_samples = self._generate_tag_samples_from_entities(
            asset_entities,
            source_view=f"targetEntitiesView ({self.target_entities_view.external_id})",
        )
        file_pattern_samples = self._generate_tag_samples_from_entities(
            file_entities,
            source_view=f"fileView ({self.file_view.external_id})",
        )
        auto_pattern_samples = asset_pattern_samples + file_pattern_samples

        # Grab the manual pattern samples
        manual_pattern_samples = self._get_manual_patterns(primary_scope_value, secondary_scope_value)

        # Merge the auto and manual patterns
        combined_pattern_samples = self._merge_patterns(auto_pattern_samples, manual_pattern_samples)

        self._log_launch_input_summary(
            scope_key=key,
            source="CDF",
            asset_entities=asset_entities,
            file_entities=file_entities,
            asset_pattern_samples=asset_pattern_samples,
            file_pattern_samples=file_pattern_samples,
            pattern_samples=combined_pattern_samples,
            manual_pattern_groups=len(manual_pattern_samples),
            manual_pattern_strings=count_pattern_sample_strings(manual_pattern_samples),
        )

        # Update cache
        new_row = RowWrite(
            key=key,
            columns={
                "AssetEntities": asset_entities,
                "FileEntities": file_entities,
                "AssetPatternSamples": asset_pattern_samples,
                "FilePatternSamples": file_pattern_samples,
                "ManualPatternSamples": manual_pattern_samples,
                "CombinedPatternSamples": combined_pattern_samples,
                "LastUpdateTimeUtcIso": datetime.now(UTC).isoformat(),
            },
        )
        self._update_cache(new_row)
        return entities, combined_pattern_samples

    def _log_launch_input_summary(
        self,
        *,
        scope_key: str,
        source: str,
        asset_entities: list[dict],
        file_entities: list[dict],
        pattern_samples: list[dict],
        asset_pattern_samples: list[dict] | None = None,
        file_pattern_samples: list[dict] | None = None,
        manual_pattern_groups: int | None = None,
        manual_pattern_strings: int | None = None,
    ) -> None:
        """Log INFO counts and DEBUG details for launch entity/pattern input."""
        pattern_string_count = count_pattern_sample_strings(pattern_samples)
        assets_missing = entities_missing_search_property(asset_entities)
        files_missing = entities_missing_search_property(file_entities)
        structural = self.config.launch_function.structural_auto_patterns
        pattern_mode = self.config.launch_function.pattern_mode

        target_search = self.target_entities_view.search_property
        file_search = self.file_view.search_property
        asset_patterns = asset_pattern_samples or []
        file_patterns = file_pattern_samples or []
        asset_pattern_count = count_pattern_sample_strings(asset_patterns)
        file_pattern_count = count_pattern_sample_strings(file_patterns)

        if not scope_key:
            scope_desc = (
                "unscoped — primaryScopeProperty is empty, so all DetectInDiagrams "
                "entities are loaded project-wide (cache key '')"
            )
        else:
            scope_desc = (
                f"scoped cache key {scope_key!r} — entities filtered by "
                "primaryScopeProperty / secondaryScopeProperty values on the files being annotated"
            )

        if source == "CACHE":
            source_desc = (
                f"CACHE — reused from RAW table {self.db_name}/{self.tbl_name} "
                "(still within cacheTimeLimit)"
            )
        else:
            source_desc = (
                "CDF — fresh query of targetEntitiesView + fileView instances "
                f"(then written to RAW {self.db_name}/{self.tbl_name})"
            )

        info_lines = [
            "Launch input summary:",
            f"  • Scope: {scope_desc}",
            f"  • Entity source: {source_desc}",
            f"  • Target entities ({self.target_entities_view.external_id}): {len(asset_entities)} "
            f"({len(assets_missing)} without '{target_search}' — Diagram Detect cannot match those)",
            f"  • File entities ({self.file_view.external_id}): {len(file_entities)} "
            f"({len(files_missing)} without '{file_search}' — Diagram Detect cannot match those)",
            f"  • Total entities for regular detect: {len(asset_entities) + len(file_entities)}",
            f"  • Pattern mode: {pattern_mode} | structuralAutoPatterns: {structural}",
            f"  • Auto patterns from targetEntitiesView ({self.target_entities_view.external_id}): "
            f"{asset_pattern_count} sample string(s) in {len(asset_patterns)} group(s)",
            f"  • Auto patterns from fileView ({self.file_view.external_id}): "
            f"{file_pattern_count} sample string(s) in {len(file_patterns)} group(s)",
        ]
        if manual_pattern_groups is not None:
            manual_strings = manual_pattern_strings if manual_pattern_strings is not None else 0
            info_lines.append(
                f"  • Manual patterns merged: {manual_pattern_groups} group(s), "
                f"{manual_strings} sample string(s)"
            )
        info_lines.append(
            f"  • Combined patterns sent to pattern-mode detect: "
            f"{pattern_string_count} sample string(s) in {len(pattern_samples)} group(s)"
        )
        self.logger.info("\n".join(info_lines))

        if self.logger.log_level != "DEBUG":
            return

        debug_lines = ["Launch input details (DEBUG):"]
        for label, rows, search_name in (
            ("assets", asset_entities, target_search),
            ("files", file_entities, file_search),
        ):
            debug_lines.append(f"  {label} ({len(rows)}):")
            preview = rows[:40]
            for row in preview:
                search_values = row.get("search_property") or []
                debug_lines.append(
                    f"    - {row.get('space')}/{row.get('external_id')}"
                    f" resource_type={row.get('resource_type')!r}"
                    f" {search_name}={search_values!r}"
                )
            if len(rows) > len(preview):
                debug_lines.append(f"    ... +{len(rows) - len(preview)} more")

        if assets_missing or files_missing:
            debug_lines.append(f"  Entities missing '{target_search}' / '{file_search}':")
            for row in (assets_missing + files_missing)[:30]:
                debug_lines.append(f"    - {row.get('space')}/{row.get('external_id')} name={row.get('name')!r}")

        if asset_patterns:
            debug_lines.append(
                f"  Auto patterns — targetEntitiesView ({self.target_entities_view.external_id}) "
                f"({asset_pattern_count}):"
            )
            debug_lines.extend(format_pattern_groups_for_log(asset_patterns))
        if file_patterns:
            debug_lines.append(
                f"  Auto patterns — fileView ({self.file_view.external_id}) ({file_pattern_count}):"
            )
            debug_lines.extend(format_pattern_groups_for_log(file_patterns))

        debug_lines.append(f"  Combined pattern samples ({pattern_string_count}):")
        debug_lines.extend(format_pattern_groups_for_log(pattern_samples))
        self.logger.debug("\n".join(debug_lines))

    def _update_cache(self, row_to_write: RowWrite) -> None:
        """
        Writes a cache entry to the RAW database table.

        This method's only responsibility is the database insertion. All data preparation
        and formatting should be done before calling this method.

        Args:
            row_to_write: Fully-formed RowWrite object containing cache data to persist.

        Returns:
            None
        """
        self.client.raw.rows.insert(
            db_name=self.db_name,
            table_name=self.tbl_name,
            row=row_to_write,
            ensure_parent=True,
        )
        self.logger.info("Successfully updated RAW cache")
        return

    def _validate_cache(self, last_update_datetime_str: str) -> bool:
        """
        Validates whether cached data is still fresh based on time elapsed since last update.

        Compares the cache's last update timestamp against the configured cache time limit
        to determine if a refresh is needed.

        Args:
            last_update_datetime_str: ISO-formatted datetime string of the cache's last update.

        Returns:
            True if the cache is still valid (within time limit), False if expired.
        """
        last_update_datetime_utc = datetime.fromisoformat(last_update_datetime_str)
        current_datetime_utc = datetime.now(UTC)
        time_difference: timedelta = current_datetime_utc - last_update_datetime_utc

        cache_validity_period = timedelta(hours=self.cache_time_limit)
        self.logger.debug(f"Cache time limit: {cache_validity_period}")
        self.logger.debug(f"Time difference: {time_difference}")

        return not time_difference > cache_validity_period

    def _convert_instances_to_entities(
        self, asset_instances: NodeList, file_instances: NodeList
    ) -> tuple[list[dict], list[dict]]:
        """
        Transforms data model node instances into entity dictionaries for diagram detection.

        Extracts relevant properties from asset and file nodes and formats them as entity
        dictionaries compatible with the diagram detect API.

        Args:
            asset_instances: NodeList of asset instances from the data model.
            file_instances: NodeList of file instances from the data model.

        Returns:
            A tuple containing:
                - List of target entity dictionaries (typically assets).
                - List of file entity dictionaries.
        """
        target_entities_resource_type: str | None = self.config.launch_function.target_entities_resource_property
        target_entities_search_property: str = self.config.launch_function.target_entities_search_property
        target_entities: list[dict] = []

        for instance in asset_instances:
            instance_properties = (instance.properties or {}).get(self.target_entities_view.as_view_id()) or {}
            asset_resource_type: str = (
                instance_properties.get(target_entities_resource_type) if target_entities_resource_type else None
            ) or self.target_entities_view.external_id
            if target_entities_search_property in instance_properties:
                asset_entity = entity(
                    external_id=instance.external_id,
                    name=instance_properties.get("name"),
                    space=instance.space,
                    annotation_type=self.target_entities_view.annotation_type,
                    resource_type=asset_resource_type,
                    search_property=instance_properties.get(target_entities_search_property),
                )
                target_entities.append(asset_entity.to_dict())
            else:
                search_value: list = [instance_properties.get("name")]
                asset_entity = entity(
                    external_id=instance.external_id,
                    name=instance_properties.get("name"),
                    space=instance.space,
                    annotation_type=self.target_entities_view.annotation_type,
                    resource_type=asset_resource_type,
                    search_property=search_value,
                )
                target_entities.append(asset_entity.to_dict())

        file_resource_type_prop: str | None = self.config.launch_function.file_resource_property
        file_search_property: str = self.config.launch_function.file_search_property
        file_entities: list[dict] = []

        for instance in file_instances:
            instance_properties = (instance.properties or {}).get(self.file_view.as_view_id()) or {}
            file_entity_resource_type: str = (
                instance_properties.get(file_resource_type_prop) if file_resource_type_prop else None
            ) or self.file_view.external_id
            file_entity = entity(
                external_id=instance.external_id,
                name=instance_properties.get("name"),
                space=instance.space,
                annotation_type=self.file_view.annotation_type,
                resource_type=file_entity_resource_type,
                search_property=instance_properties.get(file_search_property),
            )
            file_entities.append(file_entity.to_dict())

        return target_entities, file_entities

    def _generate_tag_samples_from_entities(self, entities: list[dict], *, source_view: str) -> list[dict]:
        """
        Generates regex-like pattern samples from entity search properties for pattern mode detection.

        Two modes (parameters.structuralAutoPatterns / launchFunction.structuralAutoPatterns):

        - structural (True): emit digit/letter *shape* templates such as ``00-AA-0000``.
          Letter codes are not enumerated, so ``23-XX-9106`` and ``23-KA-9101`` share a shape.
        - legacy (False): expand observed letter groups into required constants
          (e.g. ``[FE|KA|PC|VA]``).

        Separators are never required constants: ``_``, ``-``, ``.``, ``:``, ``;``, ``/``,
        and space are always normalized to an unbracketed ``-`` (never ``[_]``), in both modes.

        Args:
            entities: List of entity dictionaries containing search properties (aliases).
            source_view: Human-readable source for logs (e.g. ``fileView (CogniteFile)``).

        Returns:
            List of pattern sample dictionaries, each containing:
                - sample: List of pattern strings
                - resource_type: Entity resource type
                - annotation_type: Annotation type for the entity
        """
        # Structure: { resource_type: {"patterns": { template_key: [...] }, "annotation_type": "..."} }
        pattern_builders: dict[str, dict[str, object]] = defaultdict(lambda: {"patterns": {}, "annotation_type": None})
        structural = self.config.launch_function.structural_auto_patterns
        self.logger.info(
            f"Generating {'structural' if structural else 'legacy'} pattern samples "
            f"from {len(entities)} entities in {source_view}."
        )

        def _parse_alias(alias: str, resource_type_key: str) -> tuple[str, list[list[str]]]:
            """
            Parse an alias into a normalized template string and collect variable letter groups.

            - All separators normalize to unbracketed '-' (never bracketed required literals).
            - Replace digits with '0' and letters with 'A' in alphanumeric segments.
            - If an alphanumeric segment equals the resource type and is token-boundary isolated,
              wrap it in brackets to mark it constant.
            """
            tokens: list[str] = []
            current_alnum: list[str] = []
            for ch in alias:
                if ch.isalnum():
                    current_alnum.append(ch)
                else:
                    if current_alnum:
                        tokens.append("".join(current_alnum))
                        current_alnum = []
                    tokens.append(ch)
            if current_alnum:
                tokens.append("".join(current_alnum))

            full_template_key_parts: list[str] = []
            all_variable_parts: list[list[str]] = []

            def is_separator(tok: str) -> bool:
                return len(tok) == 1 and not tok.isalnum()

            for i, part in enumerate(tokens):
                if not part:
                    continue
                if is_separator(part):
                    # Bracket characters from aliases cannot match literal brackets in docs.
                    if part in ("[", "]"):
                        pass
                    else:
                        # Never emit [_] / [.] etc. — separators stay optional for detect.
                        full_template_key_parts.append("-")
                    continue

                left_ok = (i == 0) or is_separator(tokens[i - 1])
                right_ok = (i == len(tokens) - 1) or is_separator(tokens[i + 1])
                if left_ok and right_ok and part == resource_type_key:
                    full_template_key_parts.append(f"[{part}]")
                    continue

                segment_template = re.sub(r"\d", "0", part)
                segment_template = re.sub(r"[A-Za-z]", "A", segment_template)
                full_template_key_parts.append(segment_template)

                if not structural:
                    variable_letters = re.findall(r"[A-Za-z]+", part)
                    if variable_letters:
                        all_variable_parts.append(variable_letters)

            return "".join(full_template_key_parts), all_variable_parts

        for entity_row in entities:
            key = entity_row["resource_type"]
            if pattern_builders[key]["annotation_type"] is None:
                pattern_builders[key]["annotation_type"] = entity_row.get("annotation_type")

            aliases = entity_row.get("search_property", [])

            if not aliases:
                continue

            for alias in aliases:
                if not alias:
                    continue
                template_key, variable_parts_from_alias = _parse_alias(alias, key)
                resource_patterns = pattern_builders[key]["patterns"]
                if template_key in resource_patterns:
                    existing_variable_sets = resource_patterns[template_key]
                    for i, part_group in enumerate(variable_parts_from_alias):
                        for j, letter_group in enumerate(part_group):
                            existing_variable_sets[i][j].add(letter_group)
                else:
                    new_variable_sets = []
                    for part_group in variable_parts_from_alias:
                        new_variable_sets.append([{lg} for lg in part_group])
                    resource_patterns[template_key] = new_variable_sets

        result = []
        for resource_type, data in pattern_builders.items():
            final_samples = []
            templates: dict[str, list[list[set[str]]]] = data.get("patterns") or {}
            annotation_type = data["annotation_type"]
            for template_key, collected_vars in templates.items():
                if structural:
                    final_samples.append(template_key)
                    continue

                var_iter: Iterator[list[set[str]]] = iter(collected_vars)

                def build_segment(segment_template: str) -> str:
                    if "A" not in segment_template:
                        return segment_template
                    try:
                        letter_groups_for_segment: list[set[str]] = next(var_iter)
                        letter_group_iter: Iterator[set[str]] = iter(letter_groups_for_segment)

                        def replace_A(match: re.Match[str]) -> str:
                            alternatives = sorted(next(letter_group_iter))
                            return f"[{'|'.join(alternatives)}]"

                        return re.sub(r"A+", replace_A, segment_template)
                    except StopIteration:
                        return segment_template

                parts = [p for p in re.split(r"(\[[^\]]+\]|[^A-Za-z0-9])", template_key) if p != ""]
                final_pattern_parts = [build_segment(p) if re.search(r"A", p) else p for p in parts]
                final_samples.append("".join(final_pattern_parts))

            def _has_alpha_or_class(s: str) -> bool:
                if re.search(r"[A-Za-z]", s):
                    return True
                return bool(re.search(r"\[[^\]]*\|[^\]]*\]", s))

            final_samples = [s for s in final_samples if _has_alpha_or_class(s)]

            if final_samples:
                result.append(
                    {
                        "sample": sorted(final_samples),
                        "resource_type": resource_type,
                        "annotation_type": annotation_type,
                    }
                )

        if self.logger.log_level == "DEBUG":
            sample_count = count_pattern_sample_strings(result)
            debug_lines = [
                f"Generated {'structural' if structural else 'legacy'} patterns for {source_view}: "
                f"{sample_count} sample string(s) in {len(result)} group(s):",
                *format_pattern_groups_for_log(result),
            ]
            self.logger.debug("\n".join(debug_lines))

        return result

    def _get_manual_patterns(self, primary_scope: str, secondary_scope: str | None) -> list[dict]:
        """
        Retrieves manually defined pattern samples from the RAW catalog.

        Fetches patterns at three levels of specificity: global, primary scope, and combined scope,
        allowing for hierarchical pattern definitions with increasing specificity.

        Args:
            primary_scope: Primary scope identifier for fetching scope-specific patterns.
            secondary_scope: Optional secondary scope identifier for fetching more specific patterns.

        Returns:
            List of manually defined pattern dictionaries from all applicable scope levels.
        """
        keys_to_fetch = ["GLOBAL"]
        if primary_scope:
            keys_to_fetch.append(primary_scope)
        if primary_scope and secondary_scope:
            keys_to_fetch.append(f"{primary_scope}_{secondary_scope}")

        source = f"RAW {self.db_name}/{self.manual_patterns_tbl_name}"
        self.logger.info(f"Fetching manual patterns from {source} for keys: {keys_to_fetch}")
        all_manual_patterns: list[dict] = []
        per_key_counts: list[str] = []
        for key in keys_to_fetch:
            try:
                row: Row | None = self.client.raw.rows.retrieve(
                    db_name=self.db_name,
                    table_name=self.manual_patterns_tbl_name,
                    key=key,
                )
                if row:
                    patterns = (row.columns or {}).get("patterns", [])
                    if not isinstance(patterns, list):
                        patterns = []
                    group_count = len(patterns)
                    sample_count = count_pattern_sample_strings(patterns)
                    per_key_counts.append(f"{key!r}: {group_count} group(s), {sample_count} sample string(s)")
                    all_manual_patterns.extend(patterns)
                    if self.logger.log_level == "DEBUG" and patterns:
                        self.logger.debug(
                            "\n".join(
                                [
                                    f"Manual patterns from {source} key={key!r}:",
                                    *format_pattern_groups_for_log(patterns),
                                ]
                            )
                        )
                else:
                    per_key_counts.append(f"{key!r}: no row")
                    self.logger.debug(f"No manual patterns row for key={key!r} in {source}.")
            except CogniteNotFoundError:
                per_key_counts.append(f"{key!r}: not found")
                self.logger.debug(f"No manual patterns found for key={key!r} in {source}. This may be expected.")
            except CogniteAPIError as e:
                per_key_counts.append(f"{key!r}: error")
                self.logger.error(f"Failed to retrieve manual patterns for key {key} from {source}: {e}")

        total_samples = count_pattern_sample_strings(all_manual_patterns)
        self.logger.info(
            f"Loaded {len(all_manual_patterns)} manual pattern group(s) "
            f"({total_samples} sample string(s)) from {source}. "
            f"Per key: {'; '.join(per_key_counts)}"
        )
        return all_manual_patterns

    def _merge_patterns(self, auto_patterns: list[dict], manual_patterns: list[dict]) -> list[dict]:
        """
        Combines automatically generated and manually defined patterns by resource type.

        Merges pattern samples from both sources, ensuring no duplicates while preserving
        all unique patterns for each resource type. Auto-pattern annotation types take precedence.

        Args:
            auto_patterns: List of automatically generated pattern dictionaries.
            manual_patterns: List of manually defined pattern dictionaries.

        Returns:
            List of merged pattern dictionaries, deduplicated and organized by resource type.
        """
        merged: dict[str, dict[str, object]] = defaultdict(lambda: {"samples": set(), "annotation_type": None})

        # Process auto-generated patterns
        for item in auto_patterns:
            resource_type = item.get("resource_type")
            if resource_type:
                bucket = merged[resource_type]
                samples_set = cast(set[str], bucket["samples"])
                sample_list = item.get("sample") or []
                samples_set.update(sample_list)
                # Set annotation_type if not already set
                if not bucket.get("annotation_type"):
                    bucket["annotation_type"] = item.get("annotation_type")

        # Process manual patterns
        for item in manual_patterns:
            resource_type = item.get("resource_type")
            if resource_type and item.get("sample"):
                bucket = merged[resource_type]
                samples_set = cast(set[str], bucket["samples"])
                sample_val = item["sample"]
                if isinstance(sample_val, list):
                    samples_set.update(sample_val)
                else:
                    samples_set.add(cast(str, sample_val))
                # Set annotation_type if not already set (auto-patterns take precedence)
                if not bucket.get("annotation_type"):
                    # NOTE: UI that creates manual patterns will need to also have the annotation type as a required entry
                    bucket["annotation_type"] = item.get("annotation_type", "diagrams.AssetLink")

        # Convert the merged dictionary back to the required list format
        final_list = []
        for resource_type, data in merged.items():
            samples_safe: set[str] = cast(set[str], data.get("samples") or set())
            final_list.append(
                {
                    "resource_type": resource_type,
                    "sample": sorted(samples_safe),
                    "annotation_type": data.get("annotation_type"),
                }
            )

        self.logger.info(f"Merged auto and manual patterns into {len(final_list)} resource types.")
        return final_list
