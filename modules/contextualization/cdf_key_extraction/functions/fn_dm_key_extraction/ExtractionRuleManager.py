import ast
from functools import lru_cache
import re
import collections
from typing import Any, Callable, Dict, Literal, Optional, Union

from config import ExtractionRuleConfig
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling.ids import NodeId
from logger import CogniteFunctionLogger
from utils.FixedWidthMethodParameter import FixedWidthMethodParameter
from utils.HeuristicMethodParameter import HeuristicMethodParameter
from utils.RegexMethodParameter import RegexMethodParameter
from utils.TokenReassemblyMethodParameter import TokenReassemblyMethodParameter


def aggregate_scores(
    existing_scores: Dict[str, float], 
    new_key: str, 
    new_score: float
) -> Dict[str, float]:
    """
    When adding a new key to the results, add the new score to the existing 
    score if the same key has already been extracted.
    """
    curr_score = existing_scores.get(new_key, None)

    if curr_score:
        existing_scores[new_key] = curr_score + new_score
    else:
        existing_scores.update({new_key: new_score})

    return existing_scores

class ExtractionRuleManager:
    """Manages the prioritized collection of rules."""
    
    def __init__(
        self, 
        extraction_rules: list[ExtractionRuleConfig], 
        strategy: Literal["first_match", "highest_priority", "merge_all", "highest_confidence"], 
        logger: CogniteFunctionLogger,
        client: CogniteClient
    ):
        extraction_rules.sort(key=lambda x: x.priority)
        self.sorted_rules = {r.rule_id: r for r in extraction_rules}
        self.logger = logger
        self.strategy = strategy
        self.client = client
        self.field_results: dict[str, list[str]] = {}

        # Dynamic method assignment based on rule.method
        self.method_mapping = {
            "regex": self.regex_method,
            "fixed width": self.fixed_width_method,
            "token reassembly": self.token_reassembly_method,
            "heuristic": self.heuristic_method
        }

        # Instantiate the cache for regex patterns
        self.regex_cache = {}

        logger.info(f"Found {len(extraction_rules)} extraction rules in config")

    def get_sorted_rules(self) -> list[str]:
        """Get list of sorted rule IDs."""
        return list(self.sorted_rules.keys())
    
  
    def parse_rule_results(self, rule_id: str, rule_results: dict) -> int:
        """Parse and store rule results based on strategy."""
        num_keys = 0
        for field_rule_name, key_scores in rule_results.items():
            if key_scores:
                if self.strategy == "highest_confidence":
                    max_score_result = max(key_scores, key=lambda x: x[1])

                    self.logger.verbose(
                        'DEBUG', 
                        f"Max score for rule {rule_id}, field_rule {field_rule_name}: "
                        f"{max_score_result}"
                    )
                    # TODO: Are we getting maximum score for each rule or max overall? 
                    # I think for each rule

                    self.field_results[field_rule_name] = [
                        {max_score_result: key_scores[max_score_result]}
                    ]
                    num_keys += 1
                else:
                    # TODO: add the field_rule_name as a key if it doesn't exist in field_results
                    if field_rule_name not in self.field_results:
                        self.field_results[field_rule_name] = []
                    self.field_results[field_rule_name].append(key_scores)
                    num_keys += len(key_scores)
                    if self.strategy == "first_match":
                        self.logger.verbose(
                            'INFO', 
                            f'Returning first match found in field_rule: {field_rule_name}'
                        )
                        # We want to return all of the keys extracted with no duplicates
                        return num_keys
            else:
                self.logger.verbose('DEBUG', f"No keys found for rule {rule_id} on last execution")
        return num_keys

    def execute_rule(
        self, 
        rule_id: str, 
        entity_source_fields: dict[str, str],
        entity_keys_extracted: dict[str, list[str]],
    ) -> dict[str, list[str]]:
        """Execute a specific rule on the provided entity source fields and may use existing keys extracted in future methods."""
        rule = self.sorted_rules.get(rule_id, None)
        if not rule:
            raise Exception(f"Rule {rule_id} not found")

        # Initialize the dictionary to store results for each source field
        self.field_results: dict[str, list[str]] = {}
        
        rule_function = self.method_mapping.get(rule.method)

        if rule.method == 'heuristic' and isinstance(rule.params, HeuristicMethodParameter):
            rule.method_parameters.current_node_id = NodeId(space=entity_source_fields.get('space', ''), external_id=entity_source_fields.get('external_id', ''))
        elif rule_function is None:
            raise Exception(f"Error: Unknown method '{rule.method}'")
        else:
            self.logger.verbose('INFO', f'Using rule function for method: {rule.method}')
            
        # TODO: need a better understanding of composite field rules. 
        # Such as composite strategies and what to do with them... 
        # same with logical OR source field operator
        if isinstance(rule.source_fields, list):
            field_order = rule.source_fields.copy()

            # TODO: This will need to change based on the difference between 
            # logical OR and composite fields...
            field_order.sort(key=lambda x: x.priority if x.priority is not None else 100)
            self.logger.verbose('DEBUG', f'Field order for rule {rule_id}: {field_order}')

            for source_field in field_order:
                field_name = source_field.field_name
                value = entity_source_fields.get(field_name, None)
                if value is None or value == "":
                    if source_field.required:
                        self.logger.verbose(
                            'WARNING', 
                            f"Missing required field '{field_name}' in entity: "
                            f"{entity_source_fields}"
                        )
                    continue

                # This is where we call the rule function
                rule_results = rule_function(value, field_name, rule_id, rule.method_parameters) #TODO we need to add the NodeId of the current entity for heuristic calls
                num_keys = self.parse_rule_results(rule_id, rule_results)

                # we already sort on field priority, so this logic works for both strategies
                if (num_keys > 0 and (self.strategy == "first_match" or self.strategy == "highest_priority")):
                    return self.field_results
        else:
            # This is where we call the rule function
            field_name = rule.source_fields.field_name
            value = entity_source_fields.get(field_name, None)

            if not value or value == "":
                self.logger.verbose(
                    'WARNING', 
                    f"Missing required field '{field_name}' in entity: "
                    f"{entity_source_fields}"
                )
                return self.field_results

            # Run the function
            rule_results = rule_function(value, rule.method_parameters) #TODO we need to add the NodeId of the current entity for heuristic calls
            num_keys = self.parse_rule_results(rule_id, rule_results)

            if (num_keys > 0 and self.strategy == "first_match"):
                return self.field_results

        # TODO different returns based on merge_all and highest_priority
        return self.field_results
    
    def regex_method(
        self,
        input_str: str, 
        field_name: str,
        rule_id: str,
        params: RegexMethodParameter
    ) -> dict:
        """
        TODO: Add the caching necessary to store the regex patterns for each rule. 
        Need to make this method non-static.
        """
        # Check if the pattern is already cached
        if rule_id + "_pattern" in self.regex_cache.keys():
            pattern = self.regex_cache[rule_id + "_pattern"]
        else:
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
            self.regex_cache[rule_id + "_pattern"] = pattern
        
        # Check if the validation pattern is already cached
        if rule_id + "_validation_pattern" in self.regex_cache.keys():
            validation_pattern = self.regex_cache[rule_id + "_validation_pattern"]
        else:
            validation_pattern = (
                re.compile(params.validation_pattern)
                if params.validation_pattern
                else None
            )

        results = {}

        # Iterate through all matches found in the input string
        for match in pattern.finditer(input_str)[:params.max_matches_per_field]:
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
                    # Handle cases where format placeholder doesn't match captured group
                    result_data["reassembled_value"] = "ERROR: Reassembly failed"

                results = aggregate_scores(results, result_data['reassembled_value'], 1.0)
            else:
                # 2b. just add the match instead
                results = aggregate_scores(results, result_data['match'], 1.0)

            # 3. Early Termination Check (Performance Optimization, only do first match)
            if params.early_termination:
                break

            # 4. Max Matches Check (Performance Optimization)
            if (params.max_matches_per_field is not None and 
                    len(results) >= params.max_matches_per_field):
                break

        # What do these results even look like when we get a match?
        return {'_'.join([field_name, params.name]): results}


    def fixed_width_method(
        self,
        input_str: str, 
        field_name: str,
        rule_id: str,
        params: FixedWidthMethodParameter
    ) -> dict:
        """Process fixed-width formatted input strings."""
        value: str = input_str

        # decode if we need to
        if params.encoding:
            actual_bytes: bytes = ast.literal_eval(value)
            value = actual_bytes.decode(params.encoding)

        if params.line_pattern:
            if rule_id + "_line_pattern" in self.regex_cache.keys():
                line_pattern = self.regex_cache[rule_id + "_line_pattern"]
            else:
                line_pattern = re.compile(params.line_pattern)
                self.regex_cache[rule_id + "_line_pattern"] = line_pattern
        else:
            line_pattern = None

        records = []

        # ===============================================================
        # |           GENERATE RECORDS VIA RULES / PRE PROCESSING       |
        # ===============================================================
        # Only use record_length if record_delimiter is not given
        if (not params.record_delimiter and 
                params.record_length and 
                params.record_length > 0):
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
                        # Append a blank line, as we will not expect the user to 
                        # configure multi-line patterns with line indexes in the 
                        # post-processing stage. We will assume the line #s 
                        # (or record #s) of the original string
                        records.append("")
            else:
                records.extend(split_lines)
        # No other rules to consider
        else:
            records = value.splitlines()

        # ===============================================================
        # |           SKIP LINES                                       |
        # ===============================================================
        if params.skip_lines and params.skip_lines > 0:
            records = records[params.skip_lines:]

        # ===============================================================
        # |           GENERATE FIELDS VIA DEFINITIONS                  |
        # ===============================================================
        results = {}

        if not params.field_definitions or params.field_definitions == []:
            raise ValueError("Field definitions are required")
        
        # Check if we are doing multi-line record parsing 
        # (every field must have a line number if so)
        if all(field.line for field in params.field_definitions):
            for f_def in params.field_definitions:
                # Get the current record based on the field line #. 
                # Reject the field if the line # is out of range
                try:
                    curr_rec = records[f_def.line - params.skip_lines]

                    # If we are stopping on empty records, 
                    # check if the current record is empty
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
                except Exception:
                    if params.stop_on_empty:
                        break
                    else:
                        continue
        else:
            continue_outer_flag = False
            for record in records:
                record_fields = {}

                # If we are stopping on empty records, check if current record is empty
                if params.stop_on_empty and record.strip() == "":
                    break

                for f_def in params.field_definitions:
                    ext_field = record[f_def.start_position:f_def.end_position]

                    # trim the extracted value if necessary
                    if f_def.trim:
                        ext_field = ext_field.strip()

                    # if required field is empty, reject this record
                    if f_def.required and not ext_field:
                        continue_outer_flag = True
                        break

                    record_fields = aggregate_scores(record_fields, ext_field, 1.0)

                # If we are rejecting this record, we need not add other fields to results
                if continue_outer_flag:
                    continue_outer_flag = False
                    continue
                
                results.update(record_fields)
        
        return {'_'.join([field_name, params.name]): results}

    def positional_detection(
        self,
        input_str: str,
        config: list[dict]
    ) -> tuple[list[str], float]:
        """
        Identifies tags based on their position relative to known markers.
        Config example:
        [
            {"position": "start_of_field", "pattern": r"^[A-Z0-9-_]{5,15}\b"},
            {"position": "after_keyword", "keywords": ["Tag:", "Equipment ID:", "Ref:"], "offset": 1},
            {"position": "between_delimiters", "start_delimiter": "[", "end_delimiter": "]", "pattern": r"[A-Z0-9-_]+"}
        ]
        """
        results = []
        score = 0.0

        for rule in config:
            match rule["position"]:
                case "start_of_field":
                    pattern = rule.get("pattern", None)

                    if not pattern:
                        continue

                    m = re.match(pattern, input_str)
                    if m:
                        results.append(m.group())
                        score += rule.get("confidence_boost", 0.0)
                case "after_keyword":
                    keywords = rule.get("keywords", [])
                    offset = rule.get("offset", 1)
                    for kw in keywords:
                        # Find the keyword in the string
                        idx = input_str.find(kw)
                        if idx != -1:
                            # Split after the keyword
                            after = input_str[idx + len(kw):].strip()
                            tokens = after.split(sep=rule.get("separator", "-"))
                            if len(tokens) >= offset:
                                results.append(tokens[offset - 1])
                                score += rule.get("confidence_boost", 0.0)
                case "between_delimiters":
                    start = rule.get("start_delimiter", "[")
                    end = rule.get("end_delimiter", "]")
                    pattern = rule.get("pattern", r"[A-Z0-9-_]+")
                    # Find all occurrences between delimiters
                    regex = re.compile(re.escape(start) + r"(" + pattern + r")" + re.escape(end))
                    matches = regex.findall(input_str)
                    results.extend(matches)
                    score += rule.get("confidence_boost", 0.0) * len(matches)
                case _:
                    continue

        return results, score

    def frequency_analysis(
        self,
        input_str: str,
        config: dict,
        current_node_id: NodeId
    ) -> tuple[list[str], float]:
        """
        Identifies tags or patterns based on statistical frequency analysis.
        Config example:
        {
            "analyze_corpus": True,
            "min_frequency": 5,
            "common_prefix_detection": {
                "enable": True,
                "min_prefix_frequency": 10,
                "prefix_length": [2, 3, 4]
            },
            "pattern_extraction": {
                "n_gram_analysis": True,
                "n_gram_sizes": [3, 4, 5],
                "structural_pattern_learning": True
            }
        }
        """
        results = []
        score = 0.0

        # If we need to get the whole corpus of thee entity, we need to grab it from CDF
        if config.get("analyze_corpus", False):
            # TODO setup a graphql query or something here to yoink literally every property
            self.client.data_modeling.instances.retrieve()
        else:
            corpus = [input_str]

        # 1. Frequency analysis: Find substrings in input_str that appear at least min_frequency times in corpus
        min_freq = config.get("min_frequency", 3)
        substr_freq = collections.Counter()
        frequent_tokens = set()
        for entry in :
            for word in entry.split():
                substr_freq[word] += 1

        for word in input_str.split():
            if substr_freq[word] >= min_freq:
                frequent_tokens.add(word)

        # 2. Common prefix detection
        prefix_conf = config.get("common_prefix_detection", {})
        if prefix_conf.get("enable", False):
            min_prefix_freq = prefix_conf.get("min_prefix_frequency", 3)
            prefix_lengths = prefix_conf.get("prefix_length", [2, 3, 4])
            prefix_counter = collections.Counter()
            for entry in frequent_tokens:
                for length in prefix_lengths:
                    if len(entry) >= length:
                        prefix = entry[:length]
                        prefix_counter[prefix] += 1
            for length in prefix_lengths:
                if len(input_str) >= length:
                    prefix = input_str[:length]
                    if prefix_counter[prefix] >= min_prefix_freq:
                        results.append(prefix)
                        score += 0.05

        # 3. Pattern extraction (n-gram analysis)
        pattern_conf = config.get("pattern_extraction", {})
        if pattern_conf.get("n_gram_analysis", False):
            n_gram_sizes = pattern_conf.get("n_gram_sizes", [3, 4, 5])
            ngram_counter = collections.Counter()
            for entry in corpus:
                for n in n_gram_sizes:
                    for i in range(len(entry) - n + 1):
                        ngram = entry[i:i+n]
                        ngram_counter[ngram] += 1
            for n in n_gram_sizes:
                for i in range(len(input_str) - n + 1):
                    ngram = input_str[i:i+n]
                    if ngram_counter[ngram] >= min_freq:
                        results.append(ngram)
                        score += 0.02

        # 4. (Optional) Structural pattern learning could be implemented with regex pattern mining or ML

        return list(set(results)), score

    def heuristic_method(
        self,
        input_str: str, 
        field_name: str,
        rule_id: str,
        params: HeuristicMethodParameter
    ) -> dict:
        """Process input using heuristic methods."""
        # ===============================================================
        # |           PRE PROCESSING                                    |
        # ===============================================================

        # Create a dictionary of each strategy configured and its resulting score
        strategy_names = [param.name for param in params]
        strategy_scores = dict.fromkeys(strategy_scores, 0.0)
        strategy_extractions = dict.fromkeys(strategy_scores, [])

        agg_method = params.scoring.aggregation_method
        results = {}

        if agg_method not in ["weighted_average", "max", "consensus"]:
            self.logger.verbose('ERROR', f'Unknown aggregation method found in config, terminating rule: {agg_method}')

        # ===============================================================
        # |           APPLY STRATEGIES                                  |
        # ===============================================================
        # Here is where we extract CANDIDATES, not the final keys. The final keys will be determined by the confidence modifiers performed later.

        for strategy in params.heuristic_strategies:
            match strategy.method:
                case 'positional_detection':
                    self.logger.verbose('DEBUG', f'Executing positional_detection strategy...')
                    # Maybe we have a Heuristic agent do this for us.....? Maybe?
                    # self.client.ai.tools.documents.ask_question()
                    keys, score = self.positional_detection(input_str, strategy.rules)

                    strategy_scores[strategy.name] += score
                    strategy_extractions[strategy.name].extend(keys)

                    pass
                case 'frequency_analysis':
                    self.logger.verbose('DEBUG', f'Executing frequency_analysis strategy...')
                    
                    keys, score = self.frequency_analysis(input_str, strategy.rules, params.current_node_id)
                    pass
                case 'context_inference':
                    self.logger.verbose('DEBUG', f'Executing context_inference strategy...')
                    pass
                case 'example_based_learning':
                    self.logger.verbose('DEBUG', f'Executing example_based_learning strategy...')
                    self.logger.verbose('DEBUG', 'Please wait to use this rule until it is implemented.....')
                    raise NotImplementedError("Example-based learning strategy is not implemented yet.")
                case _:
                    self.logger.verbose('WARNING', f'Unknown strategy found in config, skipping: {strategy.method}')
                    pass

        self.logger.verbose('INFO', f'Finished executing strategies for input: {input_str}')
        self.logger.verbose('INFO', f'Aggregating scores via {agg_method} method')

        # ===============================================================
        # |                        Confidence Adjustments               |
        # ===============================================================

        # TODO we have to apply each of these strategies to each key candidate extracted in the strategies portion

        for mod in params.confidence_modifiers:
            condition = mod.get('condition', None)

            if not condition:
                self.logger.verbose('WARNING', f'Confidence modifier {mod} is missing a condition or is invalid, skipping...')
                continue

            match condition:
                case 'multiple_strategies_agree':
                    pass
                case 'field_name_indicates_tag':
                    pass
                case 'extracted_value_length':
                    pass
                case 'extracted_value_in_known_catalog':
                    pass

        # ===============================================================
        # |           SCORE AGGREGATION                                 |
        # ===============================================================
        
        # remove any strategies that scored 0.0
        strategies = [s for s in strategies if strategy_scores[s.name] > 0.0]

        match agg_method:
            case 'weighted_average':
                pass
            case 'max':
                pass
            case 'consensus':
                pass
            case _:
                self.logger.verbose('ERROR', f'Unknown aggregation method encountered: {agg_method}')
        
        # ===============================================================
        # |           VALIDATION                                        |
        # ===============================================================
        
        if params.validation:
            self.logger.verbose('INFO', f'Running validation checks...')
            for check in params.validation:
                match check.get('check', None):
                    case 'length_range':
                        self.logger.verbose('DEBUG', f'Checking length range...')
                        # Implement length range check
                    case 'character_composition':
                        self.logger.verbose('DEBUG', f'Checking character composition...')
                        # Implement character composition check
                    case 'not_common_word':
                        self.logger.verbose('DEBUG', f'Checking for not common word...')
                        # Implement not common word check
                    case 'format_consistency':
                        self.logger.verbose('DEBUG', f'Checking format consistency...')
                        # Implement format consistency check
                    case _:
                        self.logger.verbose('WARNING', f'Unknown validation check: {check}')

        # End of validation checks, aggregate final results
        for strategy in strategy_names:
            results.update({strategy_extractions.get(strategy, ""): strategy_scores.get(strategy, 0.0)})

        # for safety
        params.current_node_id = None

        return {'_'.join([field_name, params.name]): results}

    @lru_cache(maxsize=32)
    def get_condition_lambda(self, rule_id: str, assembly_rule_name: str) -> Callable[[Dict[str, str]], bool]:
        """Create a lambda function to evaluate the conditions. Store the lambdas in cache"""
        lambdas = []

        # Get the rule params using rule id and the conditions using assembly_rule_name
        rule: TokenReassemblyMethodParameter = self.sorted_rules.get(rule_id, None).method_parameters

        # Grab the conditions for the assembly rule
        conditions = next((rule.conditions for rule in rule.assembly_rules if rule.name == assembly_rule_name), None)

        if not conditions:
            self.logger.verbose("DEBUG", f'No valid conditions found for assembly rule {assembly_rule_name} in rule {rule_id}. !!This rule will be applied unconditionally!!')
            return lambda _: True

        for condition, data in conditions.items():
            if condition.endswith('_missing'):
                cnd_prop = condition.removesuffix('_missing')

                if cnd_prop not in [p.name for p in rule.tokenization.token_patterns]:
                    raise ValueError(f"Condition '{condition}' references unknown token pattern '{cnd_prop}'")
                
                is_missing = bool(data) # boolean
                lambdas.append(lambda tokens: tokens.get(cnd_prop, "") == "" and is_missing)
                self.logger.verbose('DEBUG', f"Condition '{condition}': {cnd_prop} is {'not' if not is_missing else ''} missing added to lambdas for rule {rule_id}")
                continue
            match condition:
                case 'all_required_present':
                    if bool(data):
                        # all required tokens must be present
                        lambdas.append(lambda tokens: all(tokens.get(p.name, "") != "" for p in rule.tokenization.token_patterns if p.required))
                    else:
                        # some required tokens must be missing
                        lambdas.append(lambda tokens: not all(tokens.get(p.name, "") != "" for p in rule.tokenization.token_patterns if p.required))
                    self.logger.verbose('DEBUG', f'Condition {condition}:  {'all required tokens must be present' if bool(data) else 'some required tokens must be missing'}  added to lambdas for rule {rule_id}')
                case 'context_match':
                    # Get property and value from condition
                    cnd_prop = data.get('property', None)
                    cnd_value = data.get('value', None)

                    if cnd_prop and cnd_value:
                        lambdas.append(lambda tokens: tokens.get(cnd_prop, None) == cnd_value)
                        self.logger.verbose('DEBUG', f"Condition 'context_match': {cnd_prop} == {cnd_value} added to lambdas for rule {rule_id}")
                    else:
                        raise ValueError(f"Condition 'context_match' is missing property or value")
                case _:
                    raise NotImplementedError(f"Condition '{condition}' is not implemented")
        # for each condition, create the lambda and add it to the list of lambdas
        # if there are more than one lambdas, then we concatenate them into one lambda with 'and'
        if len(lambdas) > 1:
            return lambda tokens: all(l(tokens) for l in lambdas)
        # else we just return the lambda
        elif len(lambdas) == 1:
            return lambdas.pop(0)
        else:
            raise ValueError(f"No valid conditions found for rule {rule_id}")


    def token_reassembly_method(
        self,
        input_str: str, 
        field_name: str, 
        rule_id: str,
        params: TokenReassemblyMethodParameter
    ) -> dict:
        """Process input using token reassembly methods."""
        # ===============================================================
        # |           GENERATE PATTERNS VIA RULES / PRE PROCESSING      |
        # ===============================================================
        token_dump = input_str.split(params.tokenization.separator_pattern)
        tokens = {}
        results = {}

        # reject this input if it exceeds the max token count
        if params.tokenization.max_tokens and len(token_dump) > params.tokenization.max_tokens:
            self.logger.verbose("WARNING", f"Input exceeds max tokens when split on '{params.tokenization.separator_pattern}' separator.")
            return {}

        # reject this input if it contains fewer than the minimum token count
        if params.tokenization.min_tokens and len(token_dump) < params.tokenization.min_tokens:
            self.logger.verbose("WARNING", f"Input contains fewer tokens than the minimum required when split on '{params.tokenization.separator_pattern}' separator.")
            return {}

        # TODO check the above logic, not sure if it should apply, since we use the position field in token patterns

        for token in params.tokenization.token_patterns:
            token_regex_name = rule_id + token.name + 'regex_pattern'
            token_regex = self.regex_cache.get(token_regex_name, None)
            
            if not token_regex:
                token_regex = re.compile(token.pattern)
                self.regex_cache.update({token_regex_name: token_regex})

            try:
                if token.position < len(token_dump):        
                    ext_token = token_dump[token.position]
                    ext_token_match = token_regex.fullmatch(ext_token)
                    if ext_token_match:
                        tokens.update({token.name: ext_token_match.group()})
                    else:
                        tokens.update({token.name: ""})
                        if token.required:
                            self.logger.verbose("WARNING", f"Required token '{token.name}' is missing.")
                else:
                        tokens.update({token.name: ""})
                        if token.required:
                            self.logger.verbose("WARNING", f"Required token '{token.name}' is missing.")
            except Exception as e:
                raise ValueError(f'Unable to exract token(s), error msg: {e}')

        # ===============================================================
        # |           REASSEMBLE WITH CONDITIONS                        |
        # ===============================================================        
        for assembly_rule in params.assembly_rules:
            assembly_lambda = self.get_condition_lambda(rule_id, assembly_rule.name)
            if assembly_lambda(tokens):
                try:
                    # Replace placeholders in the format string with token values
                    assembled_key = assembly_rule.format.format(**tokens)
                    
                    # Add to results with confidence score (could be based on assembly_rule.priority)
                    results = aggregate_scores(results, assembled_key, 1.0)
                    
                    self.logger.verbose('DEBUG', f"Successfully assembled key '{assembled_key}' using rule '{assembly_rule.name}'")
                    break  # Use first matching rule (they're sorted by priority)
                    
                except KeyError as e:
                    self.logger.verbose('WARNING', f"Format string contains placeholder not found in tokens: {e}")
                    continue
                except Exception as e:
                    self.logger.verbose('WARNING', f"Failed to format assembly rule '{assembly_rule.name}': {e}")
                    continue

        # ===============================================================
        # |          VALIDATE REASSEMBLED TOKENS                        |
        # ===============================================================        
        if params.validation.validate_assembled:
            # There should be only 1 result to validate
            if len(results) != 1:
                self.logger.verbose("WARNING", f"Expected 1 assembled result for rule {rule_id}, but got {len(results)}.")
                return {}

            # Check the validation pattern
            if params.validation.validation_pattern:
                # Check if the validation pattern is already cached
                if rule_id + "_validation_pattern" in self.regex_cache.keys():
                    validation_pattern = self.regex_cache[rule_id + "_validation_pattern"]
                else:
                    validation_pattern = (
                        re.compile(params.validation_pattern)
                        if params.validation.validation_pattern
                        else None
                    )

                if validation_pattern.fullmatch(results[0]):
                    self.logger.verbose("DEBUG", f"Validation pattern matched for rule {rule_id}.")
                else:
                    self.logger.verbose("WARNING", f"Validation pattern did not match for rule {rule_id} for reassembled tokens: {results[0]}.")
                    return {}

        return {'_'.join([field_name, params.method.replace(' ', '_')]): results}

    def _calculate_confidence(
        self, key_value: str, text: str
    ) -> float:
        """Calculate confidence score for extracted key."""
        base_confidence = 0.5

        # Adjust based on key characteristics
        if len(key_value) >= 3:
            base_confidence += 0.1
        else:
            base_confidence -= 0.1

        # Check if key appears at start of field (higher confidence)
        if text.strip().startswith(key_value):
            base_confidence += 0.1
        
        if text.strip().endswith(key_value):
            base_confidence += 0.1

        # Check for word boundaries
        if re.search(r"\b" + re.escape(key_value) + r"\b", text):
            base_confidence += 0.05

        return min(base_confidence, 1.0)

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