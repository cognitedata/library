import abc
import re
from typing import Iterator, Any, Dict, List, Set, cast
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from cognite.client import CogniteClient
from cognite.client.data_classes import RowWrite, Row
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeList,
)
from services.ConfigService import Config, ViewPropertyConfig
from services.DataModelService import IDataModelService
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import entity


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
    def _generate_tag_samples_from_entities(self, entities: list[dict]) -> list[dict]:
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

        self.db_name: str = config.launch_function.cache_service.raw_db
        self.tbl_name: str = config.launch_function.cache_service.raw_table_cache
        self.manual_patterns_tbl_name: str = config.launch_function.cache_service.raw_manual_patterns_catalog
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
        if secondary_scope_value:
            key = f"{primary_scope_value}_{secondary_scope_value}"
        else:
            key = f"{primary_scope_value}"

        try:
            row: Row | None = self.client.raw.rows.retrieve(db_name=self.db_name, table_name=self.tbl_name, key=key)
        except:
            row = None

        # Attempt to retrieve from the cache
        if row and row.columns and self._validate_cache(row.columns["LastUpdateTimeUtcIso"]):
            self.logger.info(f"Cache is up-to-date for key: {key}\nEntities and patterns loaded from: CACHE.")
            asset_entities: list[dict] = row.columns.get("AssetEntities", [])
            file_entities: list[dict] = row.columns.get("FileEntities", [])
            combined_pattern_samples: list[dict] = row.columns.get("CombinedPatternSamples", [])
            return (asset_entities + file_entities), combined_pattern_samples

        self.logger.info(f"Cache is out-of-date for key: {key}\nEntities and patterns loaded from: CDF (Fresh Fetch)")

        # Fetch data
        asset_instances, file_instances = data_model_service.get_instances_entities(
            primary_scope_value, secondary_scope_value
        )

        # Convert to entities for diagram detect job
        asset_entities, file_entities = self._convert_instances_to_entities(asset_instances, file_instances)
        entities = asset_entities + file_entities

        # Generate pattern samples from the same entities
        asset_pattern_samples = self._generate_tag_samples_from_entities(asset_entities)
        file_pattern_samples = self._generate_tag_samples_from_entities(file_entities)
        auto_pattern_samples = asset_pattern_samples + file_pattern_samples

        # Grab the manual pattern samples
        manual_pattern_samples = self._get_manual_patterns(primary_scope_value, secondary_scope_value)

        # Merge the auto and manual patterns
        combined_pattern_samples = self._merge_patterns(auto_pattern_samples, manual_pattern_samples)

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
                "LastUpdateTimeUtcIso": datetime.now(timezone.utc).isoformat(),
            },
        )
        self._update_cache(new_row)
        return entities, combined_pattern_samples

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
        self.logger.info(f"Successfully updated RAW cache")
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
        current_datetime_utc = datetime.now(timezone.utc)
        time_difference: timedelta = current_datetime_utc - last_update_datetime_utc

        cache_validity_period = timedelta(hours=self.cache_time_limit)
        self.logger.debug(f"Cache time limit: {cache_validity_period}")
        self.logger.debug(f"Time difference: {time_difference}")

        if time_difference > cache_validity_period:
            return False

        return True

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
            instance_properties = instance.properties.get(self.target_entities_view.as_view_id())
            asset_resource_type: str = (
                instance_properties[target_entities_resource_type]
                if target_entities_resource_type
                else self.target_entities_view.external_id
            )
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
            instance_properties = instance.properties.get(self.file_view.as_view_id())
            file_entity_resource_type: str = (
                instance_properties[file_resource_type_prop]
                if target_entities_resource_type
                else self.file_view.external_id
            )
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

    def _generate_tag_samples_from_entities(self, entities: list[dict]) -> list[dict]:
        """
        Generates regex-like pattern samples from entity search properties for pattern mode detection.

        Analyzes entity aliases to extract common patterns and variations, creating consolidated
        pattern samples that can match multiple similar tags (e.g., "[FT]-000[A|B]").

        Args:
            entities: List of entity dictionaries containing search properties (aliases).

        Returns:
            List of pattern sample dictionaries, each containing:
                - sample: List of pattern strings
                - resource_type: Entity resource type
                - annotation_type: Annotation type for the entity
        """
        # Structure: { resource_type: {"patterns": { template_key: [...] }, "annotation_type": "..."} }
        pattern_builders: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"patterns": {}, "annotation_type": None})
        self.logger.info(f"Generating pattern samples from {len(entities)} entities.")

        def _parse_alias(alias: str, resource_type_key: str) -> tuple[str, list[list[str]]]:
            """
            Parse an alias into a normalized template string and collect variable letter groups.

            - Treat hyphens '-' and spaces ' ' as literal characters.
            - Wrap all other non-alphanumeric characters in brackets to mark them as required literals (e.g., [+], [.]).
            - Replace digits with '0' and letters with 'A' in alphanumeric segments.
            - If an alphanumeric segment equals the resource type and is token-boundary isolated, wrap it in brackets to mark it constant.
            """
            # Tokenize alias into alphanumeric runs and single-character separators
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
                    # Hyphen and space are plain literals; other specials must be wrapped in brackets
                    if part == "-" or part == " ":
                        full_template_key_parts.append(part)
                    else:
                        full_template_key_parts.append(f"[{part}]")
                    continue

                # Alphanumeric segment
                left_ok = (i == 0) or is_separator(tokens[i - 1])
                right_ok = (i == len(tokens) - 1) or is_separator(tokens[i + 1])
                if left_ok and right_ok and part == resource_type_key:
                    full_template_key_parts.append(f"[{part}]")
                    continue

                segment_template = re.sub(r"\d", "0", part)
                segment_template = re.sub(r"[A-Za-z]", "A", segment_template)
                full_template_key_parts.append(segment_template)

                variable_letters = re.findall(r"[A-Za-z]+", part)
                if variable_letters:
                    all_variable_parts.append(variable_letters)

            return "".join(full_template_key_parts), all_variable_parts

        for entity in entities:
            key = entity["resource_type"]
            if pattern_builders[key]["annotation_type"] is None:
                pattern_builders[key]["annotation_type"] = entity.get("annotation_type")

            aliases = entity.get("search_property", [])
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
                        new_variable_sets.append([set([lg]) for lg in part_group])
                    resource_patterns[template_key] = new_variable_sets

        result = []
        for resource_type, data in pattern_builders.items():
            final_samples = []
            templates: Dict[str, List[List[Set[str]]]] = data.get("patterns") or {}
            annotation_type = data["annotation_type"]
            for template_key, collected_vars in templates.items():
                var_iter: Iterator[List[Set[str]]] = iter(collected_vars)

                def build_segment(segment_template: str) -> str:
                    if "A" not in segment_template:
                        return segment_template
                    try:
                        letter_groups_for_segment: List[Set[str]] = next(var_iter)
                        letter_group_iter: Iterator[Set[str]] = iter(letter_groups_for_segment)

                        def replace_A(match):
                            alternatives = sorted(list(next(letter_group_iter)))
                            return f"[{'|'.join(alternatives)}]"

                        return re.sub(r"A+", replace_A, segment_template)
                    except StopIteration:
                        return segment_template

                # Split by bracketed constants or any single non-alphanumeric separator to preserve them as tokens
                parts = [p for p in re.split(r"(\[[^\]]+\]|[^A-Za-z0-9])", template_key) if p != ""]
                final_pattern_parts = [build_segment(p) if re.search(r"A", p) else p for p in parts]
                final_samples.append("".join(final_pattern_parts))

            # Sanity filter: drop overly generic numeric-only patterns (must contain a letter or a character class)
            def _has_alpha_or_class(s: str) -> bool:
                if re.search(r"[A-Za-z]", s):
                    return True
                # Character class: bracketed alternatives like [A|B] or [1|2]
                if re.search(r"\[[^\]]*\|[^\]]*\]", s):
                    return True
                return False

            final_samples = [s for s in final_samples if _has_alpha_or_class(s)]

            if final_samples:
                result.append(
                    {
                        "sample": sorted(final_samples),
                        "resource_type": resource_type,
                        "annotation_type": annotation_type,
                    }
                )
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

        self.logger.info(f"Fetching manual patterns for keys: {keys_to_fetch}")
        all_manual_patterns = []
        for key in keys_to_fetch:
            try:
                row: Row | None = self.client.raw.rows.retrieve(
                    db_name=self.db_name,
                    table_name=self.manual_patterns_tbl_name,
                    key=key,
                )
                if row:
                    patterns = (row.columns or {}).get("patterns", [])
                    all_manual_patterns.extend(patterns)
            except CogniteNotFoundError:
                self.logger.info(f"No manual patterns found for key: {key}. This may be expected.")
            except Exception as e:
                self.logger.error(f"Failed to retrieve manual patterns for key {key}: {e}")

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
        merged: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"samples": set(), "annotation_type": None})

        # Process auto-generated patterns
        for item in auto_patterns:
            resource_type = item.get("resource_type")
            if resource_type:
                bucket = merged[resource_type]
                samples_set = cast(Set[str], bucket["samples"])
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
                samples_set = cast(Set[str], bucket["samples"])
                samples_set.add(cast(str, item["sample"]))
                # Set annotation_type if not already set (auto-patterns take precedence)
                if not bucket.get("annotation_type"):
                    # NOTE: UI that creates manual patterns will need to also have the annotation type as a required entry
                    bucket["annotation_type"] = item.get("annotation_type", "diagrams.AssetLink")

        # Convert the merged dictionary back to the required list format
        final_list = []
        for resource_type, data in merged.items():
            samples_safe: Set[str] = cast(Set[str], data.get("samples") or set())
            final_list.append(
                {
                    "resource_type": resource_type,
                    "sample": sorted(list(samples_safe)),
                    "annotation_type": data.get("annotation_type"),
                }
            )

        self.logger.info(f"Merged auto and manual patterns into {len(final_list)} resource types.")
        return final_list
