import ast
from utils.RegexMethodParameter import RegexMethodParameter
from utils.FixedWidthMethodParameter import FixedWidthMethodParameter
from utils.TokenReassemblyMethodParameter import TokenReassemblyMethodParameter
from utils.HeuristicMethodParameter import HeuristicMethodParameter

from typing import Dict, Literal, Union
from logger import CogniteFunctionLogger
from config import ExtractionRuleConfig
import re

def aggregate_scores(existing_scores: Dict[str, float], new_key: str, new_score: float) -> Dict[str, float]:
    # When adding a new key to the results, we want to add the new score to the existing score if the same key has already been extracted
    curr_score = existing_scores.get(new_key, None)

    if curr_score:
        existing_scores[new_key] = curr_score + new_score
    else:
        existing_scores.update({new_key: new_score})

    return existing_scores


def regex_method(input_str: str, field_name: str, params: RegexMethodParameter) -> dict:
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

    results = {}

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
            "components": {},
            "confidence": 1.0
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

            results = aggregate_scores(results, result_data['reassembled_value'], 1.0)
        else:
            # 2b. just add the match instead
            results = aggregate_scores(results, result_data['match'], 1.0)

        # 3. Early Termination Check (Performance Optimization, only do first match)
        if params.early_termination:
            break

        # 4. Max Matches Check (Performance Optimization)
        if params.max_matches_per_field is not None and len(results) >= params.max_matches_per_field:
            break

    # What do these results even look like when we get a match?
    return {'_'.join([field_name, params.name]): results}

def fixed_width_method(input_str: str, field_name: str, params: FixedWidthMethodParameter) -> dict:
    value: str = input_str

    # decode if we need to
    if params.encoding:
        actual_bytes: bytes = ast.literal_eval(value)
        value = actual_bytes.decode(params.encoding)

    if params.line_pattern:
        line_pattern = re.compile(params.line_pattern)
    else:
        line_pattern = None

    records = []

    # ===============================================================
    # |           GENERATE RECORDS VIA RULES / PRE PROCESSING       |
    # ===============================================================
    # Only use record_length if record_delimiter is not given
    if not params.record_delimiter and params.record_length and params.record_length > 0:
        split_lines = value.splitlines()

        while split_lines:
            new_record = split_lines[0:params.record_length]

            # check if this line matches the line_pattern, if provided
            if line_pattern:
                if line_pattern.match(new_record):
                    records.append(new_record)
            else:
                records.append(new_record)

            split_lines = split_lines[params.record_length:]
    elif params.record_delimiter:
        split_lines = value.split(params.record_delimiter)
        
        if line_pattern:
            for line in split_lines:
                if line_pattern.match(line):
                    records.append(line)
                else:
                    # Append a blank line, as we will not expect the user to configure multi-line patterns with line indexes in the post-processing stage. We will assume the line #s (or record #s) of the original string
                    records.append("")
        else:
            records.extend(split_lines)
    # No other rules to consider
    else:
        records = value.splitlines()

    # ===============================================================
    # |           SKIP LINES                                        |
    # ===============================================================
    if params.skip_lines and params.skip_lines > 0:
        records = records[params.skip_lines:]

    # ===============================================================
    # |           GENERATE FIELDS VIA DEFINITIONS                   |
    # ===============================================================
    results = {}

    if not params.field_definitions or params.field_definitions == []:
        raise ValueError("Field definitions are required")
    
    # Check if we are doing multi-line record parsing (every field must have a line number if so)
    if all(field.line for field in params.field_definitions):
        for f_def in params.field_definitions:
            # Get the current record based on the field line #. Reject the field if the line # is out of range
            try:
                curr_rec = records[f_def.line - params.skip_lines]

                # If we are stopping on empty records, check if the current record is empty
                if params.stop_on_empty and not curr_rec.strip():
                    break

                ext_field = curr_rec[f_def.start_position:f_def.end_position]

                # trim the extracted value if necessary
                if f_def.trim:
                    ext_field = ext_field.strip()

                # if field is required and empty, reject this record
                if f_def.required and not ext_field:
                    continue

                results = aggregate_scores(results, ext_field, 1.0)
            except:
                if params.stop_on_empty:
                    break
                else:
                    continue
    else:
        for record in records:
            record_fields = {}

            # If we are stopping on empty records, check if the current record is empty
            if params.stop_on_empty and not record.strip():
                break

            for f_def in params.field_definitions:
                ext_field = record[f_def.start_position:f_def.end_position]

                # trim the extracted value if necessary
                if f_def.trim:
                    ext_field = ext_field.strip()

                # if required field is required and empty, reject this record
                if f_def.required and not ext_field:
                    continue

                record_fields = aggregate_scores(record_fields, ext_field, 1.0)
            results.update(record_fields)
    return {'_'.join([field_name, params.name]): results}

def heuristic_method(input_str: str, field_name: str, params: HeuristicMethodParameter) -> dict:
    pass

def token_reassembly_method(input_str: str, field_name: str, params: TokenReassemblyMethodParameter) -> dict:
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

    def execute_rule(self, rule_id: str, entity_source_fields: dict[str, str], entity_keys_extracted: list[str]) -> dict[str, list[str]]:
        rule = self.sorted_rules.get(rule_id, None)
        if not rule:
            raise Exception(f"Rule {rule_id} not found")

        # Initialize the dictionary to store results for each source field
        field_results: dict[str, list[str]] = {}
        
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
            
        # TODO need a better understanding of composite field rules. Such as composite strategies and what to do with them...
        if isinstance(rule.source_fields, list):
            field_order = rule.source_fields.copy()
            field_order.sort(key=lambda x: x.priority if x.priority is not None else 100)

            for source_field in field_order:
                field_name = source_field.field_name
                value = entity_source_fields.get(field_name, None)
                if value is None:
                    if source_field.required:
                        self.logger.warning(f"Missing required field '{field_name}' in entity: {entity_source_fields}")
                    continue

                # This is where we call the rule function
                rule_results = rule_function(value, field_name, rule.method_parameters)

                highest_confidence = 0
                for field_rule_name, key_scores in rule_results.items():
                    if key_scores:
                        if self.strategy == "highest_confidence":
                            max_score_result = max(key_scores, key=lambda x: x[1])

                            # TODO Are we getting maximum score for each rule or max overall? I think for each rule
                            # if highest_confidence < max_score[1]:
                            #     highest_confidence = max_score[1]
                            #     field_results[field_rule_name] = max_score[0]
                            # elif highest_confidence == max_score[1]:
                            #     field_results[field_name].append(max_score[0])

                            field_results[field_rule_name] = [{max_score_result: key_scores[max_score_result]}]

                        else:
                            # TODO add the field_rule_name as a key if it doesn't exist in field_results
                            if field_rule_name not in field_results:
                                field_results[field_rule_name] = []
                            field_results[field_rule_name].append(key_scores)
                            if self.strategy == "first_match":
                                # We want to return all of the keys extracted with no duplicates
                                return field_results
        else:
            # This is where we call the rule function
            field_name = rule.source_fields.field_name
            value = entity_source_fields.get(field_name, None)

            # Run the function
            rule_results = rule_function(value, rule.method_parameters)

            for field_rule_name, key_scores in rule_results.items():
                if key_scores:
                    if field_rule_name not in field_results:
                        field_results[field_rule_name] = []
                    field_results[field_rule_name].append(key_scores)
                    self.logger.debug(f"Extracted keys '{key_scores}' from field '{field_name}' using rule '{rule_id}'")
                else:
                    self.logger.warning(f"Failed to extract key from field '{field_name}' using rule '{rule_id}'")
        
        return field_results

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