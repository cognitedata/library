import itertools
import re
from typing import Callable, Iterator

from ...utils.DataStructures import *
from ...config import ExtractionRuleConfig
from ...utils.TokenReassemblyMethodParameter import (
    AssemblyRule,
    TokenReassemblyMethodParameter,
)
from .ExtractionMethodHandler import ExtractionMethodHandler


class TokenReassemblyExtractionHandler(ExtractionMethodHandler):
    """Handles token reassembly extraction."""

    def get_condition_lambda(
        self, assembly_rule_name: str, rule: TokenReassemblyMethodParameter
    ) -> Callable[[Dict[str, str]], bool]:
        """Create a lambda function to evaluate the conditions. Store the lambdas in cache"""
        lambdas = []

        conditions = next(
            (
                a_rule.conditions
                for a_rule in rule.assembly_rules
                if a_rule.name == assembly_rule_name
            ),
            None,
        )

        if not conditions:
            self.logger.verbose(
                "DEBUG",
                f"No valid conditions found for token reassembly. !!This rule will be applied unconditionally!!",
            )
            return lambda _: True

        for condition, data in conditions.items():
            condition = condition.lower()
            if condition.endswith("_missing"):
                cnd_prop = condition.removesuffix("_missing")

                if cnd_prop not in [p.name for p in rule.tokenization.token_patterns]:
                    raise ValueError(
                        f"Condition '{condition}' references unknown token pattern '{cnd_prop}'"
                    )

                is_missing = bool(data)  # boolean
                lambdas.append(
                    lambda tokens: tokens.get(cnd_prop, "") == "" and is_missing
                )
                self.logger.verbose(
                    "DEBUG",
                    f"Condition '{condition}': {cnd_prop} is {'not' if not is_missing else ''} missing added to lambdas for rule {assembly_rule_name}",
                )
                continue
            match condition:
                case "all_required_present":
                    if bool(data):
                        # all required tokens must be present
                        lambdas.append(
                            lambda tokens: all(
                                tokens.get(p.name, "") != ""
                                for p in rule.tokenization.token_patterns
                                if p.required
                            )
                        )
                    else:
                        # some required tokens must be missing
                        lambdas.append(
                            lambda tokens: not all(
                                tokens.get(p.name, "") != ""
                                for p in rule.tokenization.token_patterns
                                if p.required
                            )
                        )
                    self.logger.verbose(
                        "DEBUG",
                        f"Condition {condition}:  {'all required tokens must be present' if bool(data) else 'some required tokens must be missing'}  added to lambdas for rule {assembly_rule_name}",
                    )
                case "context_match":
                    # Get property and value from condition
                    cnd_prop = data.get("property", None)
                    cnd_value = data.get("value", None)

                    if cnd_prop and cnd_value:
                        lambdas.append(
                            lambda tokens: tokens.get(cnd_prop, None) == cnd_value
                        )
                        self.logger.verbose(
                            "DEBUG",
                            f"Condition 'context_match': {cnd_prop} == {cnd_value} added to lambdas for rule {assembly_rule_name}",
                        )
                    else:
                        raise ValueError(
                            f"Condition 'context_match' is missing property or value"
                        )
                case _:
                    raise NotImplementedError(
                        f"Condition '{condition}' is not implemented"
                    )
        # for each condition, create the lambda and add it to the list of lambdas
        # if there are more than one lambdas, then we concatenate them into one lambda with 'and'
        if len(lambdas) > 1:
            return lambda tokens: all(l(tokens) for l in lambdas)
        # else we just return the lambda
        elif len(lambdas) == 1:
            return lambdas.pop(0)
        else:
            raise ValueError(
                f"No valid conditions found for assembly rule {assembly_rule_name}"
            )

    def extract(
        self, text: str, rule: ExtractionRuleConfig, context: Dict[str, Any] = None
    ) -> List[ExtractedKey]:
        """Process input using token reassembly methods."""
        # ===============================================================
        # |           GENERATE PATTERNS VIA RULES / PRE PROCESSING      |
        # ===============================================================
        tkr_rule = rule.config
        token_dump = []
        tokens = {}
        results = {}

        if isinstance(tkr_rule.tokenization.separator_pattern, list):
            pattern = r"[" + "".join(tkr_rule.tokenization.separator_pattern) + r"]+"
        else:
            pattern = tkr_rule.tokenization.separator_pattern

        # 2. Use re.split()
        token_dump = re.split(pattern, text)

        # 3. validate
        if (
            tkr_rule.tokenization.max_tokens
            and len(token_dump) > tkr_rule.tokenization.max_tokens
        ):
            self.logger.verbose(
                "WARNING",
                f"Input exceeds max tokens when split on '{tkr_rule.tokenization.separator_pattern}' separator.",
            )
            return []

        # reject this input if it contains fewer than the minimum token count
        if (
            tkr_rule.tokenization.min_tokens
            and len(token_dump) < tkr_rule.tokenization.min_tokens
        ):
            self.logger.verbose(
                "WARNING",
                f"Input contains fewer tokens than the minimum required when split on '{tkr_rule.tokenization.separator_pattern}' separator.",
            )
            return []

        # ===============================================================
        # |           GENERATE TOKEN DUMP                               |
        # ===============================================================
        for token_pattern in tkr_rule.tokenization.token_patterns:
            pattern_name = token_pattern.name
            pattern_regex = token_pattern.pattern
            component_type = token_pattern.component_type
            position = token_pattern.position

            if pattern_regex:
                try:
                    compiled_pattern = re.compile(pattern_regex)
                    for i, token in enumerate(token_dump):
                        token = token.strip()
                        # Skip this pattern if we know where in the string the token sits
                        if position is not None and i != position:
                            continue
                        match = compiled_pattern.match(token)
                        if match:
                            if pattern_name not in tokens:
                                tokens[pattern_name] = []
                            tokens[pattern_name].append(
                                {
                                    "value": match.group(1) if match.lastindex else match.group(0),
                                    "field": "unknown",
                                    "position": i,
                                    "component_type": component_type,
                                    "required": token_pattern.required,
                                }
                            )
                except re.error as e:
                    self.logger.verbose(
                        "ERROR", f"Invalid token pattern '{pattern_regex}': {e}"
                    )

        # Ensure we have our required tokens - may want to add a flag as this is a pretty nuclear option
        required_token_names = [
            token_pattern.name
            for token_pattern in tkr_rule.tokenization.token_patterns
            if token_pattern.required
        ]
        missing_tokens = [
            token_name
            for token_name in required_token_names
            if tokens.get(token_name, None) is None
        ]
        if missing_tokens:
            self.logger.verbose(
                "WARNING",
                f"Missing required tokens for rule '{rule.rule_id}': {', '.join(missing_tokens)}. "
                f"Found tokens: {list(tokens.keys())}. Input text: '{text[:100]}...'",
            )

        # ===============================================================
        # |           REASSEMBLE WITH CONDITIONS                        |
        # ===============================================================
        results = []
        for assembly_rule in tkr_rule.assembly_rules:
            # We may want to score this differently?
            results.extend(
                self._assemble_tokens(
                    tokens, assembly_rule, rule
                )
            )

        # ===============================================================
        # |          VALIDATE REASSEMBLED TOKENS                        |
        # ===============================================================
        final_results = []
        for new_key in results:
            # Ensure we don't get any invalid values in required patterns
            if (not new_key.value or new_key.value == "") and new_key.required:
                self.logger.verbose(
                    "WARNING",
                    f"Got an invalid token for field {new_key.source_field}, skipping...",
                )
                return []

            if tkr_rule.validation:
                # Check the validation pattern
                if tkr_rule.validation.validation_pattern:
                    # Check if the validation pattern is already cached
                    validation_pattern = re.compile(
                        tkr_rule.validation.validation_pattern
                    )

                    # Validate with validation regex pattern
                    if not validation_pattern.fullmatch(new_key.value):
                        self.logger.verbose(
                            "WARNING",
                            f"Validation pattern did not match for rule for reassembled tokens: {results[0]}.",
                        )
                        continue

            final_results.append(new_key)
        return final_results

    def _assemble_tokens(
        self,
        all_tokens: Dict,
        assembly_rule: AssemblyRule,
        rule: ExtractionRuleConfig
    ) -> list[ExtractedKey]:
        results = []
        assembly_lambda = self.get_condition_lambda(assembly_rule.name, rule.config)

        # Create every possible token result with cartesian products
        all_keys = list(all_tokens.keys())
        all_value_lists = [[v['value'] for v in all_tokens[key]] for key in all_keys]
        all_value_combinations = [dict(zip(all_keys, combo)) for combo in itertools.product(*all_value_lists)]

        for combo in all_value_combinations:
            if assembly_lambda(combo):
                try:
                    # Replace placeholders in the format string with token values
                    assembled_key = assembly_rule.format.format(**combo)
                    self.logger.verbose(
                        "DEBUG",
                        f"Successfully assembled key '{assembled_key}' using assembly rule '{assembly_rule.name}'",
                    )
                    results.append(
                        ExtractedKey(
                            value=assembled_key,
                            method=rule.method,
                            confidence=assembly_rule.priority / 100,
                            source_field=rule.get_source_field_string,
                            rule_id=rule.rule_id,
                            extraction_type=rule.extraction_type,
                            metadata={"tokens_used": list(combo.keys())},
                        )
                    )

                except KeyError as e:
                    self.logger.verbose(
                        "WARNING",
                        f"Rule: {assembly_rule.name} - Format string contains placeholder not found in tokens: {e}",
                    )
                except Exception as e:
                    self.logger.verbose(
                        "WARNING",
                        f"Failed to format assembly rule '{assembly_rule.name}': {e}",
                    )

        return results
