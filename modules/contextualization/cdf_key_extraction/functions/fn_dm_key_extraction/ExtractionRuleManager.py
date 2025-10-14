from utils.RegexMethodParameter import RegexMethodParameter
from utils.FixedWidthMethodParameter import FixedWidthMethodParameter
from utils.TokenReassemblyMethodParameter import TokenReassemblyMethodParameter
from utils.HeuristicMethodParameter import HeuristicMethodParameter

from typing import Dict, Union, List
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

    return results

class ExtractionRuleManager:
    """Manages the prioritized collection of rules."""
    def __init__(self, extraction_rules: list[ExtractionRuleConfig], logger: CogniteFunctionLogger):
        extraction_rules.sort(key=lambda x: x.priority)
        self.sorted_rules = {r.rule_id: r for r in extraction_rules}

        logger.info(f"Found {len(extraction_rules)} extraction rules in config")

    def execute_rule(self, rule_id: str, value: str) -> str:
        rule = self.sorted_rules.get(rule_id, None)

        if not rule:
            raise Exception(f"Rule {rule_id} not found")

        match rule.method:
            case "regex":
                return regex_method(value, rule)
            case "fixed width":
                pass
            case "token reassembly":
                pass
            case "heuristic":
                pass
            case _:
                return f"Error: Unknown method '{rule.method}'"

    def fixed_width_method(self, params: FixedWidthMethodParameter) -> str:
        pass

    def heuristic_method(self, params: HeuristicMethodParameter) -> str:
        pass

    def token_reassembly_method(self, params: TokenReassemblyMethodParameter) -> str:
        pass