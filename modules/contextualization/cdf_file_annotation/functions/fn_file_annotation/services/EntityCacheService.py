import abc
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator
from typing import cast

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from normalization import extract_forms
from services.ConfigService import Config, ViewPropertyConfig
from services.DataModelService import IDataModelService
from services.EntitySyncService import EntityInstance
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
        lines.append(f"  [{group.get('resource_type')}/{group.get('annotation_type')}] {len(samples)} sample(s)")
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


def detectable_entities(entities: list[dict]) -> list[dict]:
    """Return the entities diagram detect can match on.

    An instance whose aliases were never set, or were cleared, has nothing to search for
    and the API rejects the whole request over it ("must be a string or list of strings"),
    so it is left out rather than allowed to fail every file in the batch.
    """
    return [row for row in entities if row.get("search_property")]


def search_values(value: object, *, fallback: object = None) -> list[str]:
    """Return a search property as the list of non-blank strings the detect API requires.

    The property comes straight from the view, where it can be absent, null, a single
    string, or a list that a previous run left blanks in. Assets and files alike fall back
    to their name when it holds nothing to search for - aliases are cleared whenever no
    pattern matches, and the name is then all that is left to match the instance on.

    Args:
        value: The search property as read from the view.
        fallback: Used when the search property carries no usable string, normally the
            instance name.
    """
    usable = _non_blank_strings(value)
    return usable or _non_blank_strings(fallback)


def _non_blank_strings(value: object) -> list[str]:
    """The strings in a property value that are worth searching for."""
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str) and item.strip()]
    return []


class ICacheService(abc.ABC):
    """
    Builds the entities and pattern samples to pass into diagram detect (e.g., assets, files)
    for the scope of a batch. The entities come from the data model service, which keeps
    them in the entity sync cache.
    """

    @abc.abstractmethod
    def get_entities(
        self,
        data_model_service: IDataModelService,
        primary_scope_value: str,
        secondary_scope_value: str | None,
        file_space: str | None,
    ) -> tuple[list[dict], list[dict]]:
        pass

    @abc.abstractmethod
    def _generate_tag_samples_from_entities(
        self, entities: list[dict], *, source_view: str, normalize_patterns: list[str]
    ) -> list[dict]:
        pass


class GeneralCacheService(ICacheService):
    """
    Builds the entities and pattern samples to pass into diagram detect (e.g., assets, files)
    for the scope of a batch. The entities come from the data model service, which keeps
    them in the entity sync cache.
    """

    def __init__(self, config: Config, client: CogniteClient, logger: CogniteFunctionLogger):
        self.client = client
        self.config = config
        self.logger = logger

        self.db_name: str = config.raw_tables.raw_db
        self.manual_patterns_tbl_name: str = config.raw_tables.raw_manual_patterns_catalog

        self.file_view: ViewPropertyConfig = config.data_model_views.file_view
        self.target_entities_view: ViewPropertyConfig = config.data_model_views.target_entities_view

    def get_entities(
        self,
        data_model_service: IDataModelService,
        primary_scope_value: str,
        secondary_scope_value: str | None,
        file_space: str | None,
    ) -> tuple[list[dict], list[dict]]:
        """
        Builds the entities and pattern samples for diagram detection in one scope.

        The scope is the file space and the primary and secondary scope values, so each
        file context is matched only against its own entities.

        Args:
            data_model_service: Service instance for querying data model instances.
            primary_scope_value: Primary scope identifier (e.g., site, facility).
            secondary_scope_value: Optional secondary scope identifier (e.g., unit, area).
            file_space: Instance space of the files when entities are read per file space, else None.

        Returns:
            A tuple containing:
                - Combined list of entity dictionaries (assets + files) for diagram detection.
                - Combined list of pattern sample dictionaries for pattern mode detection.
        """
        key = f"{primary_scope_value}_{secondary_scope_value}" if secondary_scope_value else f"{primary_scope_value}"
        if file_space is not None:
            key = f"{file_space}:{key}"

        asset_instances, file_instances = data_model_service.get_instances_entities(
            primary_scope_value, secondary_scope_value, file_space
        )

        # Convert to entities for diagram detect job
        asset_entities, file_entities = self._convert_instances_to_entities(asset_instances, file_instances)
        entities = detectable_entities(asset_entities + file_entities)

        # Generate pattern samples from the same entities (source-specific normalize filters)
        text_norm = self.config.promote_function.entity_search_service.text_normalization
        asset_pattern_samples = self._generate_tag_samples_from_entities(
            asset_entities,
            source_view=f"targetEntitiesView ({self.target_entities_view.external_id})",
            normalize_patterns=text_norm.entity_normalization_patterns,
        )
        file_pattern_samples = self._generate_tag_samples_from_entities(
            file_entities,
            source_view=f"fileView ({self.file_view.external_id})",
            normalize_patterns=text_norm.file_normalization_patterns,
        )
        auto_pattern_samples = asset_pattern_samples + file_pattern_samples

        # Grab the manual pattern samples
        manual_pattern_samples = self._get_manual_patterns(primary_scope_value, secondary_scope_value)

        # Merge the auto and manual patterns
        combined_pattern_samples = self._merge_patterns(auto_pattern_samples, manual_pattern_samples)

        self._log_launch_input_summary(
            scope_key=key,
            asset_entities=asset_entities,
            file_entities=file_entities,
            asset_pattern_samples=asset_pattern_samples,
            file_pattern_samples=file_pattern_samples,
            pattern_samples=combined_pattern_samples,
            manual_pattern_groups=len(manual_pattern_samples),
            manual_pattern_strings=count_pattern_sample_strings(manual_pattern_samples),
        )
        return entities, combined_pattern_samples

    def _log_launch_input_summary(
        self,
        *,
        scope_key: str,
        asset_entities: list[dict],
        file_entities: list[dict],
        pattern_samples: list[dict],
        asset_pattern_samples: list[dict],
        file_pattern_samples: list[dict],
        manual_pattern_groups: int,
        manual_pattern_strings: int,
    ) -> None:
        """Log INFO counts and DEBUG details for launch entity/pattern input."""
        pattern_string_count = count_pattern_sample_strings(pattern_samples)
        assets_missing = entities_missing_search_property(asset_entities)
        files_missing = entities_missing_search_property(file_entities)
        structural = self.config.launch_function.structural_auto_patterns
        pattern_mode = self.config.launch_function.pattern_mode

        target_search = self.target_entities_view.search_property
        file_search = self.file_view.search_property
        asset_pattern_count = count_pattern_sample_strings(asset_pattern_samples)
        file_pattern_count = count_pattern_sample_strings(file_pattern_samples)

        if not scope_key:
            scope_desc = (
                "unscoped — primaryScopeProperty is empty, so all DetectInDiagrams entities are loaded project-wide"
            )
        else:
            scope_desc = (
                f"scope {scope_key!r} — entities filtered by the instance space ('<space>:' prefix) "
                "and/or primaryScopeProperty / secondaryScopeProperty values of the files being annotated"
            )

        info_lines = [
            "Launch input summary:",
            f"  • Scope: {scope_desc}",
            f"  • Target entities ({self.target_entities_view.external_id}): {len(asset_entities)} "
            f"({len(assets_missing)} without '{target_search}' — Diagram Detect cannot match those)",
            f"  • File entities ({self.file_view.external_id}): {len(file_entities)} "
            f"({len(files_missing)} without '{file_search}' — Diagram Detect cannot match those)",
            f"  • Total entities for regular detect: {len(asset_entities) + len(file_entities)}",
            f"  • Pattern mode: {pattern_mode} | structuralAutoPatterns: {structural}",
            f"  • Auto patterns from targetEntitiesView ({self.target_entities_view.external_id}): "
            f"{asset_pattern_count} sample string(s) in {len(asset_pattern_samples)} group(s)",
            f"  • Auto patterns from fileView ({self.file_view.external_id}): "
            f"{file_pattern_count} sample string(s) in {len(file_pattern_samples)} group(s)",
            f"  • Manual patterns merged: {manual_pattern_groups} group(s), {manual_pattern_strings} sample string(s)",
            f"  • Combined patterns sent to pattern-mode detect: "
            f"{pattern_string_count} sample string(s) in {len(pattern_samples)} group(s)",
        ]
        self.logger.info("\n".join(info_lines))

        if self.logger.log_level != "DEBUG":
            return

        debug_lines = ["Launch input details (DEBUG):"]
        if assets_missing or files_missing:
            debug_lines.append(f"  Entities missing '{target_search}' / '{file_search}':")
            for row in (assets_missing + files_missing)[:30]:
                debug_lines.append(f"    - {row.get('space')}/{row.get('external_id')} name={row.get('name')!r}")

        if len(debug_lines) == 1:
            return
        self.logger.debug("\n".join(debug_lines))

    def _convert_instances_to_entities(
        self, asset_instances: Iterable[EntityInstance], file_instances: Iterable[EntityInstance]
    ) -> tuple[list[dict], list[dict]]:
        """
        Transforms data model node instances into entity dictionaries for diagram detection.

        Extracts relevant properties from asset and file nodes and formats them as entity
        dictionaries compatible with the diagram detect API.

        Args:
            asset_instances: Asset instances from the data model.
            file_instances: File instances from the data model.

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
            asset_search_values = search_values(
                instance_properties.get(target_entities_search_property),
                fallback=instance_properties.get("name"),
            )
            asset_entity = entity(
                external_id=instance.external_id,
                name=instance_properties.get("name"),
                space=instance.space,
                annotation_type=self.target_entities_view.annotation_type,
                resource_type=asset_resource_type,
                search_property=asset_search_values,
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
                search_property=search_values(
                    instance_properties.get(file_search_property),
                    fallback=instance_properties.get("name"),
                ),
            )
            file_entities.append(file_entity.to_dict())

        return target_entities, file_entities

    def _generate_tag_samples_from_entities(
        self,
        entities: list[dict],
        *,
        source_view: str,
        normalize_patterns: list[str],
    ) -> list[dict]:
        """
        Generates regex-like pattern samples from entity search properties for pattern mode detection.

        Two modes (parameters.structuralAutoPatterns / launchFunction.structuralAutoPatterns):

        - structural (True): emit digit/letter *shape* templates such as ``00-AA-0000``.
          Letter codes are not enumerated, so ``23-XX-9106`` and ``23-KA-9101`` share a shape.
        - legacy (False): expand observed letter groups into required constants
          (e.g. ``[FE|KA|PC|VA]``).

        Separators are never required constants: ``_``, ``-``, ``.``, ``:``, ``;``, ``/``,
        and space are always normalized to an unbracketed ``-`` (never ``[_]``), in both modes.

        Aliases are first filtered/extracted with the source-specific normalize patterns
        (entityNormalizationPatterns for assets, fileNormalizationPatterns for files).
        Empty normalize_patterns disables filtering for that source.

        Args:
            entities: List of entity dictionaries containing search properties (aliases).
            source_view: Human-readable source for logs (e.g. ``fileView (CogniteFile)``).
            normalize_patterns: Capture-group regexes for this source; empty = no filter.

        Returns:
            List of pattern sample dictionaries, each containing:
                - sample: List of pattern strings
                - resource_type: Entity resource type
                - annotation_type: Annotation type for the entity
        """
        # Structure: { resource_type: {"patterns": { template_key: [...] }, "annotation_type": "..."} }
        pattern_builders: dict[str, dict[str, object]] = defaultdict(lambda: {"patterns": {}, "annotation_type": None})
        structural = self.config.launch_function.structural_auto_patterns
        if normalize_patterns:
            filter_note = "aliases filtered by source normalizePatterns first"
        else:
            filter_note = "source normalizePatterns empty — no alias filtering"
        self.logger.info(
            f"Generating {'structural' if structural else 'legacy'} pattern samples "
            f"from {len(entities)} entities in {source_view} ({filter_note})."
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

        aliases_kept = 0
        aliases_skipped = 0
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
                if normalize_patterns:
                    forms = extract_forms(alias, normalize_patterns)
                    if not forms:
                        aliases_skipped += 1
                        continue
                    alias_for_pattern = forms[0]
                else:
                    alias_for_pattern = alias
                aliases_kept += 1
                template_key, variable_parts_from_alias = _parse_alias(alias_for_pattern, key)
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

        if normalize_patterns:
            self.logger.info(
                f"normalizePatterns kept {aliases_kept} alias form(s) and skipped {aliases_skipped} "
                f"non-matching alias(es) for {source_view}."
            )
        else:
            self.logger.info(f"normalizePatterns empty — used all {aliases_kept} alias(es) for {source_view}.")

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
