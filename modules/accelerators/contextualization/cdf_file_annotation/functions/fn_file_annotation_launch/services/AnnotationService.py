import abc
import re
from typing import Any, Iterator
from cognite.client import CogniteClient
from services.ConfigService import Config

from cognite.client.data_classes.contextualization import (
    DiagramDetectResults,
    DiagramDetectConfig,
    FileReference,
)

from services.LoggerService import CogniteFunctionLogger


class IAnnotationService(abc.ABC):
    """
    Interface for interacting with the diagram detect and other contextualization endpoints
    """

    @abc.abstractmethod
    def run_diagram_detect(self, files: list[FileReference], entities: list[dict[str, Any]]) -> int:
        pass

    @abc.abstractmethod
    def run_pattern_mode_detect(self, files: list[FileReference], pattern_samples: list[dict[str, Any]]) -> int:
        pass


# maybe a different class for debug mode and run mode?
class GeneralAnnotationService(IAnnotationService):
    """
    Build a queue of files that are in the annotation process and return the jobId
    """

    def __init__(self, config: Config, client: CogniteClient, logger: CogniteFunctionLogger):
        self.client: CogniteClient = client
        self.config: Config = config
        self.logger: CogniteFunctionLogger = logger

        self.annotation_config = config.launch_function.annotation_service
        self.diagram_detect_config: DiagramDetectConfig | None = None
        if config.launch_function.annotation_service.diagram_detect_config:
            self.diagram_detect_config = config.launch_function.annotation_service.diagram_detect_config.as_config()

    def run_diagram_detect(self, files: list[FileReference], entities: list[dict[str, Any]]) -> int:
        detect_job: DiagramDetectResults = self.client.diagrams.detect(
            file_references=files,
            entities=entities,
            partial_match=self.annotation_config.partial_match,
            min_tokens=self.annotation_config.min_tokens,
            search_field="search_property",
            configuration=self.diagram_detect_config,
        )
        if detect_job.job_id:
            return detect_job.job_id
        else:
            raise Exception(f"API call to diagram/detect in pattern mode did not return a job ID")

    def run_pattern_mode_detect(self, files: list, pattern_samples: list[dict[str, Any]]) -> int:
        """Generates patterns and runs the diagram detection job in pattern mode."""
        self.logger.info(f"Generated {len(pattern_samples)} pattern samples for detection.")

        detect_job: DiagramDetectResults = self.client.diagrams.detect(
            file_references=files,
            entities=pattern_samples,  # Use the generated patterns
            partial_match=self.annotation_config.partial_match,
            min_tokens=self.annotation_config.min_tokens,
            search_field="sample",  # The key in your generated samples
            configuration=self.diagram_detect_config,
            pattern_mode=True,  # The crucial flag
        )
        if detect_job.job_id:
            return detect_job.job_id
        else:
            raise Exception("API call to diagram/detect in pattern mode did not return a job ID")

    def _generate_tag_samples_from_entities(self, entities: list[dict]) -> list[dict]:
        """
        Generates pattern samples from entity aliases by converting them into generalized templates.
        This version analyzes the internal structure of each segment:
        - Numbers are generalized to '0'.
        - Letters are grouped into bracketed alternatives, even when mixed with numbers.
        - Example: '629P' and '629X' will merge to create a pattern piece '000[P|X]'.
        """
        # Structure: { resource_type: { full_template_key: list_of_collected_variable_parts } }
        # where list_of_collected_variable_parts is [ [{'L1_alt1', 'L1_alt2'}], [{'L2_alt1'}], ... ]
        pattern_builders: dict[str, dict[str, list[list[set[str]]]]] = {}

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
                # Handle delimiters
                if part in [" ", "-"]:
                    full_template_key_parts.append(part)
                    continue

                # Handle fixed constants (override everything else)
                left_ok = (i == 0) or (alias_parts[i - 1] in [" ", "-"])
                right_ok = (i == len(alias_parts) - 1) or (alias_parts[i + 1] in [" ", "-"])
                if left_ok and right_ok and part == resource_type_key:
                    full_template_key_parts.append(f"[{part}]")
                    continue

                # --- Dissect the segment to create its template and find variable letters ---
                # 1. Create the structural template for the segment (e.g., '629P' -> '000A')
                segment_template = re.sub(r"\d", "0", part)
                segment_template = re.sub(r"[A-Za-z]", "A", segment_template)
                full_template_key_parts.append(segment_template)

                # 2. Extract all groups of letters from the segment
                variable_letters = re.findall(r"[A-Za-z]+", part)
                if variable_letters:
                    all_variable_parts.append(variable_letters)
            return "".join(full_template_key_parts), all_variable_parts

        for entity in entities:
            key = entity.get("resourceType") or entity.get("external_id") or "tag"
            if key not in pattern_builders:
                pattern_builders[key] = {}

            aliases = entity.get("aliases", [])
            for alias in aliases:
                if not alias:
                    continue
                # NOTE: THESE are TEMP fixes. Please do not include in the way it is now as a final soln
                if "_" in alias or "," in alias:
                    continue
                if alias[0] == ".":
                    continue
                if alias.isdigit():
                    continue
                if alias.isalpha():
                    continue
                if len(alias) <= 2:  # accounts for 'T' or 'SP'
                    continue
                if alias.count("-") == 1 and key == "Asset Annotation":
                    # accounts for 605-JT | 114-JT
                    temp = alias.split("-")
                    if temp[0].isdigit():
                        continue

                template_key, variable_parts_from_alias = _parse_alias(alias, key)

                if template_key in pattern_builders[key]:
                    # Merge with existing variable parts
                    existing_variable_sets = pattern_builders[key][template_key]
                    for i, part_group in enumerate(variable_parts_from_alias):
                        for j, letter_group in enumerate(part_group):
                            existing_variable_sets[i][j].add(letter_group)
                else:
                    # Create a new entry with the correct structure (list of lists of sets)
                    new_variable_sets = []
                    for part_group in variable_parts_from_alias:
                        new_variable_sets.append([set([lg]) for lg in part_group])
                    pattern_builders[key][template_key] = new_variable_sets

        # --- Build the final result from the processed patterns ---
        result = []
        for resource_type, templates in pattern_builders.items():
            final_samples = []
            for template_key, collected_vars in templates.items():
                # Create an iterator for the collected letter groups
                var_iter: Iterator[list[set[str]]] = iter(collected_vars)

                def build_segment(segment_template: str) -> str:
                    # This function rebuilds one segment, substituting 'A's with bracketed alternatives
                    if "A" not in segment_template:
                        return segment_template
                    try:
                        letter_groups_for_segment = next(var_iter)
                        letter_group_iter: Iterator[set[str]] = iter(letter_groups_for_segment)

                        def replace_A(match):
                            alternatives = sorted(list(next(letter_group_iter)))
                            return f"[{'|'.join(alternatives)}]"

                        return re.sub(r"A+", replace_A, segment_template)
                    except StopIteration:
                        return segment_template  # Should not happen in normal flow

                # Split the full template by delimiters, process each part, then rejoin
                final_pattern_parts = [
                    build_segment(p) if p not in " -" else p for p in re.split(r"([ -])", template_key)
                ]
                final_samples.append("".join(final_pattern_parts))

            if final_samples:
                result.append({"sample": sorted(final_samples), "resourceType": resource_type})
        return result
