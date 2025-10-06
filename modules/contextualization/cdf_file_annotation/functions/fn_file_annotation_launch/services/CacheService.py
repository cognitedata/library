import abc
import re
from typing import Iterator
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
        Returns file and asset entities for use in diagram detect job.
        Ensures that the cache is up to date and valid. This method orchestrates
        the fetching of data and the updating of the cache.
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
            self.logger.debug(f"Cache valid for key: {key}. Retrieving entities and patterns.")
            asset_entities: list[dict] = row.columns.get("AssetEntities", [])
            file_entities: list[dict] = row.columns.get("FileEntities", [])
            combined_pattern_samples: list[dict] = row.columns.get("CombinedPatternSamples", [])
            return (asset_entities + file_entities), combined_pattern_samples

        self.logger.info(f"Refreshing RAW entities cache and patterns cache for key: {key}")

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
        Writes a single, fully-formed RowWrite object to the RAW cache table.
        This method's only responsibility is the database insertion.
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
        Checks if the retrieved cache is still valid by comparing its creation
        timestamp with the 'cacheTimeLimit' from the configuration.
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
        Convert the asset and file nodes into an entity
        """
        target_entities_resource_type: str | None = self.config.launch_function.target_entities_resource_property
        target_entities_search_property: str = self.config.launch_function.target_entities_search_property
        target_entities: list[dict] = []

        for instance in asset_instances:
            instance_properties = instance.properties.get(self.target_entities_view.as_view_id())
            if target_entities_resource_type:
                resource_type: str = instance_properties[target_entities_resource_type]
            else:
                resource_type: str = self.target_entities_view.external_id
            if target_entities_search_property in instance_properties:
                asset_entity = entity(
                    external_id=instance.external_id,
                    name=instance_properties.get("name"),
                    space=instance.space,
                    annotation_type=self.target_entities_view.annotation_type,
                    resource_type=resource_type,
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
                    resource_type=resource_type,
                    search_property=search_value,
                )
                target_entities.append(asset_entity.to_dict())

        file_resource_type: str | None = self.config.launch_function.file_resource_property
        file_search_property: str = self.config.launch_function.file_search_property
        file_entities: list[dict] = []

        for instance in file_instances:
            instance_properties = instance.properties.get(self.file_view.as_view_id())
            if file_resource_type:
                resource_type: str = instance_properties[file_resource_type]
            else:
                resource_type: str = self.file_view.external_id
            file_entity = entity(
                external_id=instance.external_id,
                name=instance_properties.get("name"),
                space=instance.space,
                annotation_type=self.file_view.annotation_type,
                resource_type=resource_type,
                search_property=instance_properties.get(file_search_property),
            )
            file_entities.append(file_entity.to_dict())

        return target_entities, file_entities

    def _generate_tag_samples_from_entities(self, entities: list[dict]) -> list[dict]:
        """
        Generates pattern samples from entity search property by converting them into generalized templates.
        This version analyzes the internal structure of each segment:
        - Numbers are generalized to '0'.
        - Letters are grouped into bracketed alternatives, even when mixed with numbers.
        - Example: '629P' and '629X' will merge to create a pattern piece '000[P|X]'.
        """
        # Structure: { resource_type: { 'templates': { template_key: list_of_variable_parts }, 'annotation_type': str } }
        pattern_builders = defaultdict(lambda: {"templates": defaultdict(list), "annotation_type": None})
        self.logger.info(f"Generating pattern samples from {len(entities)} entities.")

        def _parse_alias(alias: str, resource_type_key: str) -> tuple[str, list[list[str]]]:
            """
            Parses an alias into a structural template key and its variable letter components.
            A segment '629P' yields a template '000A' and a variable part ['P'].
            """

            alias_parts = re.split(r"([ -])", alias)
            full_template_key_parts: list[str] = []
            all_variable_parts: list[list[str]] = []

            for i, part in enumerate(alias_parts):
                if not part:
                    continue
                if part in [" ", "-"]:
                    full_template_key_parts.append(part)
                    continue
                left_ok = (i == 0) or (alias_parts[i - 1] in [" ", "-"])
                right_ok = (i == len(alias_parts) - 1) or (alias_parts[i + 1] in [" ", "-"])
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
            resource_type = entity["resource_type"]
            annotation_type = entity.get("annotation_type")
            aliases = entity.get("search_property", [])

            if pattern_builders[resource_type]["annotation_type"] is None:
                pattern_builders[resource_type]["annotation_type"] = annotation_type

            for alias in aliases:
                if not alias:
                    continue
                template_key, variable_parts_from_alias = _parse_alias(alias, resource_type)

                if not pattern_builders[resource_type]["templates"][template_key]:
                    new_variable_sets = []
                    for part_group in variable_parts_from_alias:
                        new_variable_sets.append([set([lg]) for lg in part_group])
                    pattern_builders[resource_type]["templates"][template_key] = new_variable_sets
                else:
                    existing_variable_sets = pattern_builders[resource_type]["templates"][template_key]
                    for i, part_group in enumerate(variable_parts_from_alias):
                        for j, letter_group in enumerate(part_group):
                            while i >= len(existing_variable_sets):
                                existing_variable_sets.append([])
                            while j >= len(existing_variable_sets[i]):
                                existing_variable_sets[i].append(set())
                            existing_variable_sets[i][j].add(letter_group)

        result = []
        for resource_type, data in pattern_builders.items():
            final_samples = []
            annotation_type = data["annotation_type"]
            for template_key, collected_vars in data["templates"].items():
                var_iter: Iterator[list[set[str]]] = iter(collected_vars)

                def build_segment(segment_template: str) -> str:
                    if "A" not in segment_template:
                        return segment_template
                    try:
                        letter_groups_for_segment = next(var_iter)
                        letter_group_iter: Iterator[set[str]] = iter(letter_groups_for_segment)

                        def replace_A(match):
                            alternatives = sorted(list(next(letter_group_iter)))
                            return f"[{'|'.join(alternatives)}]" if len(alternatives) > 1 else alternatives[0]

                        return re.sub(r"A+", replace_A, segment_template)
                    except StopIteration:
                        return segment_template

                final_pattern_parts = [
                    build_segment(p) if p not in " -" else p for p in re.split(r"([ -])", template_key)
                ]
                final_samples.append("".join(final_pattern_parts))

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
        """Fetches and combines manual patterns from GLOBAL, primary, and secondary scopes."""
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
                    db_name=self.db_name, table_name=self.manual_patterns_tbl_name, key=key
                )
                if row:
                    patterns = (row.columns or {}).get("patterns", [])
                    all_manual_patterns.extend(patterns)
            except CogniteNotFoundError:
                self.logger.info(f"No manual patterns found for keys: {keys_to_fetch}. This may be expected.")
            except Exception as e:
                self.logger.error(f"Failed to retrieve manual patterns: {e}")
        return all_manual_patterns

    def _merge_patterns(self, auto_patterns: list[dict], manual_patterns: list[dict]) -> list[dict]:
        """Merges auto-generated and manual patterns, de-duplicating samples."""
        merged = defaultdict(lambda: {"samples": set(), "annotation_type": None})

        # Process auto-generated patterns
        for item in auto_patterns:
            resource_type = item.get("resource_type")
            samples = item.get("sample", [])
            annotation_type = item.get("annotation_type")
            if resource_type:
                merged[resource_type]["samples"].update(samples)
                if not merged[resource_type]["annotation_type"]:
                    merged[resource_type]["annotation_type"] = annotation_type

        # Process manual patterns
        for item in manual_patterns:
            resource_type = item.get("resource_type")
            sample = item.get("sample")
            annotation_type = item.get("annotation_type")
            if resource_type and sample:
                merged[resource_type]["samples"].add(sample)
                if not merged[resource_type]["annotation_type"]:
                    merged[resource_type]["annotation_type"] = annotation_type

        final_list = [
            {
                "resource_type": resource_type,
                "sample": sorted(list(data["samples"])),
                "annotation_type": data["annotation_type"],
            }
            for resource_type, data in merged.items()
        ]

        self.logger.info(f"Merged auto-generated and manual patterns into {len(final_list)} resource types.")
        return final_list
