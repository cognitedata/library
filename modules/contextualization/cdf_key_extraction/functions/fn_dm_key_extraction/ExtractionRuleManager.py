from utils.RegexMethodParameter import RegexMethodParameter
from utils.FixedWidthMethodParameter import FixedWidthMethodParameter
from utils.TokenReassemblyMethodParameter import TokenReassemblyMethodParameter
from utils.HeuristicMethodParameter import HeuristicMethodParameter

from typing import Dict, Literal, Union, List
import re

from logger import CogniteFunctionLogger

from config import ExtractionRuleConfig


def regex_method(input_str: str, params: RegexMethodParameter) -> List[Dict[str, Union[str, Dict[str, str]]]]:

    # Compile the pattern first
    flags = 0

    if params.regex_options.multiline:
        flags |= re.M
    if params.regex_options.dotall:
        flags |= re.S
    if params.regex_options.ignore_case:
        flags |= re.I
    if params.regex_options.unicode:
        flags |= re.U

    # Compile the patterns
    pattern = re.compile(params.pattern, flags)
    validation_pattern = re.compile(params.validation_pattern) if params.validation_pattern else None

    results = []

    # Iterate through all matches found in the input string
    for match in pattern.finditer(input_str):
        extracted_value = match.group(0)  # The full string matched by the pattern

        # 1. Post-Extraction Validation
        if validation_pattern:
            if not validation_pattern.match(extracted_value):
                # Skip this match if it fails the validation pattern
                continue

        result_data: Dict[str, Union[str, Dict[str, str]]] = {
            "match": extracted_value,
            "components": {}
        }

        # 2a. Named Capture Groups and Reassembly
        if params.capture_groups:
            # Extract components from named groups
            group_data = match.groupdict()
            component_data = {
                group_def['name']: group_data.get(group_def['name'], '')
                for group_def in params.capture_groups
            }
            result_data["components"] = component_data

            try:
                reassembled_value = params.reassemble_format.format(**component_data)
                result_data["reassembled_value"] = reassembled_value
            except KeyError:
                # Handle cases where a format placeholder doesn't match a captured group
                result_data["reassembled_value"] = "ERROR: Reassembly failed"

            results.append(result_data['reassembled_value'])
        else:
            # 2b. just add the match instead
            results.append(result_data['match'])

        # 3. Early Termination Check (Performance Optimization)
        if params.early_termination:
            break

        # 4. Max Matches Check (Performance Optimization)
        if params.max_matches_per_field is not None and len(results) >= params.max_matches_per_field:
            break

    # What do these results even look like when we get a match?
    return results

def fixed_width_method(self, params: FixedWidthMethodParameter) -> str:
    pass

def heuristic_method(self, params: HeuristicMethodParameter) -> str:
    pass

def token_reassembly_method(self, params: TokenReassemblyMethodParameter) -> str:
    pass

class ExtractionRuleManager:
    """Manages the prioritized collection of rules."""
    def __init__(self, 
                extraction_rules: list[ExtractionRuleConfig], 
                strategy: Literal["first_match", "highest_priority", "merge_all", "highest_confidence"], 
                logger: CogniteFunctionLogger):
        extraction_rules.sort(key=lambda x: x.priority)
        self.sorted_rules = {r.rule_id: r for r in extraction_rules}
        self.logger = logger
        self.strategy = strategy

        logger.info(f"Found {len(extraction_rules)} extraction rules in config")

    def get_sorted_rules(self) -> list[str]:
        return list(self.sorted_rules.keys())

    def execute_rule(self, rule_id: str, entity_source_fields: dict[str, str], entity_keys_extracted: list[str]) -> list[str]:
        rule = self.sorted_rules.get(rule_id, None)
        if not rule:
            raise Exception(f"Rule {rule_id} not found")

        value_queue = []

        # This might need to be a dict[str, float] where the key is the key extracted and the value is the confidence score
        key_results = []
        
        # Dynamic method assignment based on rule.method
        method_mapping = {
            "regex": regex_method,
            "fixed width": fixed_width_method,
            "token reassembly": token_reassembly_method,
            "heuristic": heuristic_method
        }
        
        rule_function = method_mapping.get(rule.method)
        if rule_function is None:
            raise Exception(f"Error: Unknown method '{rule.method}'")
            
        if isinstance(rule.source_fields, list):
            field_order = rule.source_fields.copy()
            field_order.sort(key=lambda x: x.priority if x.priority is not None else 100)

            for source_field in field_order:
                value = entity_source_fields.get(source_field.field_name, None)
                if value is None:
                    if source_field.required:
                        self.logger.warning(f"Missing required field '{source_field.field_name}' in entity: {entity_source_fields}")
                    continue
                value_queue.append(value)
        else:
            value_queue.append(entity_source_fields.get(rule.source_fields.field_name, None))

        while value_queue:
            # Return type of rules function is dict[str, float] where alias -> score
            key_scores = rule_function(value_queue.pop(0), rule.method_parameters)

            if key:
                # if the key already exists, aggregate the confidence score :)
                current_score = key_results.get(key, None)
                if current_score:
                    key_results[key] = (current_score + score)/2.0
                else:
                    key_results.update({key: score})

                self.logger.debug(f"Extracted key '{key}' with a score of {score} using rule '{rule_id}'")
            else:
                self.logger.warning(f"Failed to extract key using rule '{rule_id}'")

            if (self.strategy == "first_match" or self.strategy == "highest_priority") and key_results:
                # Since we already sort the rules and the fields by priority, if we found a key result, we can return it
                return key_results
        
        if self.strategy == "merge_all" and key_results:
                # We want to return all of the keys extracted with no duplicates
                return list(set(key_results))
        elif self.strategy == "highest_confidence" and key_results:
            # we want to return the key with the highest confidence score
            return max(key_results, key=key_results.get)
        else:
            self.logger.warning(f"Either no keys were extracted by the rule {rule_id}, or an unknown field selection strategy was defined.")
            return []

    def get_info(self):
        """Print a nice formatted info box about the extraction rules."""
        rules_count = len(self.sorted_rules)
        
        # Create the pretty box
        box_width = 80
        horizontal_line = "═" * box_width
        
        info_lines = [
            f"╔{horizontal_line}╗",
            f"║{' ' * ((box_width - 24) // 2)}EXTRACTION RULES INFO{' ' * ((box_width - 24) // 2)}║",
            f"╠{horizontal_line}╣",
            f"║ Total Rules: {rules_count:<{box_width - 15}}║",
            f"╠{horizontal_line}╣"
        ]
        
        if rules_count == 0:
            info_lines.extend([
                f"║{' ' * ((box_width - 16) // 2)}No rules found!{' ' * ((box_width - 16) // 2)}║",
                f"╚{horizontal_line}╝"
            ])
        else:
            info_lines.append(f"║ Rule Details:{' ' * (box_width - 15)}║")
            info_lines.append(f"╟{'─' * box_width}╢")
            
            for rule_id, rule in self.sorted_rules.items():
                rule_type = rule.method.title()
                rule_line = f" • {rule_id} ({rule_type})"
                padding = box_width - len(rule_line) - 1
                info_lines.append(f"║{rule_line}{' ' * padding}║")
            
            info_lines.append(f"╚{horizontal_line}╝")
        
        # Print the box
        for line in info_lines:
            self.logger.info(line)