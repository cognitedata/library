import collections
import re

from cognite.client.data_classes.data_modeling.ids import NodeId, ViewId

from ...utils.DataStructures import *
from ...config import ExtractionRuleConfig
from .ExtractionMethodHandler import ExtractionMethodHandler


class HeuristicExtractionHandler(ExtractionMethodHandler):
    """Handles heuristic-based key extraction."""

    def extract(
        self, text: str, rule: ExtractionRuleConfig, context: Dict[str, Any] = None
    ) -> List[ExtractedKey]:
        """Extract keys using heuristic methods."""
        if not text:
            return []

        extracted_keys = []
        config = rule.config

        # Get heuristic strategies
        strategies = config.get("heuristic_strategies", [])
        if not strategies:
            self.logger.verbose(
                "WARNING", f"No heuristic strategies found for rule '{rule.name}'"
            )
            return []

        # Apply each strategy
        candidate_scores = {}

        for strategy in strategies:
            strategy_name = strategy.get("name")
            strategy_method = strategy.get("method")
            weight = strategy.get("weight", 0.25)

            # Match by method (preferred) or name
            if (
                strategy_method == "positional_detection"
                or strategy_name == "positional_detection"
            ):
                candidates = self._positional_detection(text, strategy, rule)
            elif (
                strategy_method == "frequency_analysis"
                or strategy_name == "frequency_analysis"
            ):
                candidates = self._frequency_analysis(text, strategy, rule, context)
            elif (
                strategy_method == "context_inference"
                or strategy_name == "context_inference"
            ):
                candidates = self._context_inference(text, strategy, rule, context)
            elif (
                strategy_method == "example_based_learning"
                or strategy_name == "example_based_learning"
            ):
                candidates = self._example_based_learning(text, strategy, rule)
            else:
                self.logger.verbose(
                    "WARNING",
                    f"Unknown strategy: name={strategy_name}, method={strategy_method}",
                )
                continue

            # Update candidate scores
            for candidate, score in candidates.items():
                if candidate not in candidate_scores:
                    candidate_scores[candidate] = 0
                candidate_scores[candidate] += score * weight

        # Apply confidence modifiers
        # scoring_config = config.get("scoring", {})
        # confidence_modifiers is at config level, not under scoring
        confidence_modifiers = config.get("confidence_modifiers", [])

        for candidate, base_score in candidate_scores.items():
            adjusted_score = base_score

            for modifier in confidence_modifiers:
                condition = modifier.get("condition")
                modifier_value = modifier.get("modifier", "+0")

                if self._check_confidence_condition(
                    candidate, condition, modifier, text, context
                ):
                    if modifier_value.startswith("+"):
                        adjusted_score += float(modifier_value[1:])
                    elif modifier_value.startswith("-"):
                        adjusted_score -= float(modifier_value[1:])

            # Normalize score
            adjusted_score = max(0, min(1, adjusted_score))

            if adjusted_score >= rule.confidence_threshold:
                extracted_key = ExtractedKey(
                    value=candidate,
                    extraction_type=rule.extraction_type,
                    source_field="heuristic",
                    confidence=adjusted_score,
                    method=rule.method,
                    rule_id=rule.name,
                    metadata={
                        "base_score": base_score,
                        "adjusted_score": adjusted_score,
                        "strategies_applied": len(strategies),
                    },
                )
                extracted_keys.append(extracted_key)

        return extracted_keys

    def _positional_detection(
        self, text: str, strategy: Dict, rule: ExtractionRuleConfig
    ) -> Dict[str, float]:
        """Apply positional detection strategy."""
        candidates = {}
        rules_config = strategy.get("rules", [])

        for rule_config in rules_config:
            position = rule_config.get("position")
            pattern = rule_config.get("pattern")
            confidence_boost = rule_config.get("confidence_boost", 0)

            if position == "start_of_field":
                match = re.match(pattern, text)
                if match:
                    candidate = match.group(0)
                    if candidate not in candidates:
                        candidates[candidate] = 0.7
                    candidates[candidate] = (
                        candidates.get(candidate, 0.7) + confidence_boost
                    )

            elif position == "after_keyword":
                keywords = rule_config.get("keywords", [])
                for keyword in keywords:
                    # Match pattern immediately after keyword (with optional whitespace)
                    keyword_pattern = f"{re.escape(keyword)}\\s*({pattern})"
                    matches = re.findall(keyword_pattern, text)
                    for match in matches:
                        candidates[match] = 0.8 + confidence_boost
                    # Also try matching without keyword requirement (for cases where keyword might be optional)
                    # Extract anything matching the pattern if keywords are specified
                    if not matches:
                        # Fallback: extract pattern matches from the text
                        fallback_matches = re.findall(pattern, text)
                        for match in fallback_matches:
                            # Only include if it appears after a keyword context
                            if any(
                                keyword.lower() in text.lower() for keyword in keywords
                            ):
                                if match not in candidates:
                                    candidates[match] = 0.6
                                candidates[match] = (
                                    candidates.get(match, 0.6) + confidence_boost
                                )

            elif position == "in_parentheses":
                matches = re.findall(pattern, text)
                for match in matches:
                    if match not in candidates:
                        candidates[match] = 0.6
                    candidates[match] = candidates.get(match, 0.6) + confidence_boost

        return candidates

    def _frequency_analysis(
        self, text: str, strategy: Dict, rule: ExtractionRuleConfig, context: Dict[str, Any]
    ) -> Dict[str, float]:
        """Apply frequency analysis strategy."""
        candidates = {}

        # Simple frequency analysis - look for repeated patterns
        rules_config = strategy.get("rules", [])
        for rule_config in rules_config:
            if rule_config.get("analyze_corpus", False):
                # TODO setup a graphql query or something here to yoink literally every property
                current_node_id = NodeId(
                    context.get("instance_space", ""), context.get("entity_id", "")
                )
                sources = ViewId(
                    context.get("view_space", ""),
                    context.get("view_external_id", ""),
                    context.get("view_version", ""),
                )
                instances_result = None
                try:
                    sources = [sources]
                    instances_result = (
                        self.client.data_modeling.instances.retrieve_nodes(
                            nodes=current_node_id, sources=sources
                        )
                    )

                    corpus = flatten_dict_values(instances_result.dump())
                except Exception as e:
                    self.logger.verbose(
                        "ERROR",
                        f"Failed to retrieve instances {current_node_id} from view(s) {sources}. Continuing with input string",
                    )
                    self.logger.verbose("ERROR", f"Error was {e}")
                    corpus = [text]
            else:
                corpus = [text]

            # Extract potential candidates
            potential_candidates = re.findall(r"[A-Z0-9-_]{3,15}", " ".join(corpus))

            # 1. Frequency analysis: Find substrings in input_str that appear at least min_frequency times in corpus
            min_freq = rule_config.get("min_frequency", 1)
            substr_freq = collections.Counter()
            for word in potential_candidates:
                substr_freq[word] += 1

            for substr, count in substr_freq.items():
                if count >= min_freq:
                    if substr not in candidates:
                        candidates[substr] = 0.5
                    else:
                        candidates[substr] += 0.05
                    if 3 <= len(substr) <= 12:
                        candidates[substr] += 0.01

            # 2a. Common prefix detection
            prefix_conf = rule_config.get("common_prefix_detection", {})
            if prefix_conf.get("enable", False) and candidates:
                min_prefix_freq = prefix_conf.get("min_prefix_frequency", 3)
                prefix_lengths = prefix_conf.get("prefix_length", [2, 3, 4])
                prefix_score_modifier = prefix_conf.get("prefix_score_modifier", 0.3)
                prefix_counter = collections.Counter()

                for entry, _ in candidates.items():
                    for length in prefix_lengths:
                        if len(entry) >= length:
                            prefix = entry[:length]
                            prefix_counter[prefix] += 1

                # Perform inverse rank weighing (Harmonic series score distribution)
                prefix_ranks = [
                    p[0]
                    for p in prefix_counter.most_common()
                    if p[1] >= min_prefix_freq
                ]
                prefix_modifiers = {
                    pfx: prefix_score_modifier / (rank + 1)
                    for rank, pfx in enumerate(prefix_ranks)
                }

                for pfx in prefix_modifiers.keys():
                    for substr in candidates.keys():
                        if substr.startswith(pfx):
                            candidates[substr] += prefix_modifiers[pfx]

            # 2b. Common suffix detection
            suffix_conf = rule_config.get("common_suffix_detection", {})
            if suffix_conf.get("enable", False) and candidates:
                min_suffix_freq = suffix_conf.get("min_suffix_frequency", 3)
                suffix_lengths = suffix_conf.get("suffix_length", [2, 3, 4])
                suffix_score_modifier = suffix_conf.get("suffix_score_modifier", 0.3)
                suffix_counter = collections.Counter()

                for entry, _ in candidates.items():
                    for length in suffix_lengths:
                        if len(entry) >= length:
                            suffix = entry[-length:]
                            suffix_counter[suffix] += 1

                # Perform inverse rank weighing (Harmonic series score distribution)
                suffix_ranks = [
                    s[0]
                    for s in suffix_counter.most_common()
                    if s[1] >= min_suffix_freq
                ]
                suffix_modifiers = {
                    sfx: suffix_score_modifier / (rank + 1)
                    for rank, sfx in enumerate(suffix_ranks)
                }

                for sfx in suffix_modifiers.keys():
                    for substr in candidates.keys():
                        if substr.endswith(sfx):
                            candidates[substr] += suffix_modifiers[sfx]

            # 3. Pattern extraction (n-gram analysis)
            pattern_conf = rule_config.get("pattern_extraction", {})
            if pattern_conf.get("n_gram_analysis", False) and candidates:
                n_gram_sizes = pattern_conf.get("n_gram_sizes", [3, 4, 5])
                n_gram_score_modifier = pattern_conf.get("n_gram_score_modifier", 0.3)
                ngram_counter = collections.Counter()

                for entry in corpus:
                    for n in n_gram_sizes:
                        for i in range(len(entry) - n + 1):
                            ngram = entry[i : i + n]
                            ngram_counter[ngram] += 1

                # Perform inverse rank weighing (Harmonic series score distribution)
                ngram_ranks = [ng[0] for ng in ngram_counter.most_common()]
                ngram_modifiers = [
                    {ng: n_gram_score_modifier / (rank + 1)}
                    for rank, ng in enumerate(ngram_ranks)
                ]

                for ng in ngram_modifiers.keys():
                    for substr in candidates.keys():
                        if ng in substr:
                            candidates[substr] += ngram_modifiers[ng]

        # 4. (Optional) Structural pattern learning could be implemented with regex pattern mining or ML

        return candidates

    def _context_inference(
        self,
        text: str,
        rule_config: Dict,
        rule: ExtractionRuleConfig,
        context: Dict[str, Any],
    ) -> Dict[str, float]:
        """Apply context inference strategy."""
        candidates = {}

        if "surrounding_keywords" in rule_config:
            positive_keywords = rule_config["surrounding_keywords"].get("positive", [])
            negative_keywords = rule_config["surrounding_keywords"].get("negative", [])
            keyword_proximity_bonus = rule_config["surrounding_keywords"].get(
                "keyword_proximity_bonus", 0
            )

            # Extract potential candidates
            potential_candidates = re.findall(r"[A-Z0-9-_]{3,15}", text)

            for candidate in potential_candidates:
                score = 0.4

                # Check for positive keywords nearby
                candidate_pos = text.find(candidate)
                context_window = rule_config.get("context_window", 20)

                start_context = max(0, candidate_pos - context_window)
                end_context = min(
                    len(text), candidate_pos + len(candidate) + context_window
                )
                context_text = text[start_context:end_context].lower()

                for keyword in positive_keywords:
                    if keyword.lower() in context_text:
                        score += keyword_proximity_bonus

                for keyword in negative_keywords:
                    if keyword.lower() in context_text:
                        score -= keyword_proximity_bonus

                candidates[candidate] = max(0, score)

        return candidates

    def _example_based_learning(
        self, text: str, strategy: Dict, rule: ExtractionRuleConfig
    ) -> Dict[str, float]:
        """Apply example-based learning strategy."""
        candidates = {}

        # Simple implementation - look for patterns similar to known examples
        rules_config = strategy.get("rules", [])
        for rule_config in rules_config:
            if rule_config.get("learning_mode") == "similarity":
                # Extract potential candidates
                potential_candidates = re.findall(r"[A-Z0-9-_]{3,15}", text)

                for candidate in potential_candidates:
                    score = 0.3

                    # Simple similarity scoring based on character patterns
                    if re.match(r"^[A-Z]+[-_]?\d+[A-Z]?$", candidate):
                        score += 0.3

                    # Boost for common industrial patterns
                    if re.match(r"^[A-Z]{1,3}[-_]?\d{2,4}[A-Z]?$", candidate):
                        score += 0.2

                    candidates[candidate] = score

        return candidates

    def _check_confidence_condition(
        self,
        candidate: str,
        condition: str,
        modifier: Dict,
        text: str,
        context: Dict[str, Any],
    ) -> bool:
        """Check confidence modifier conditions."""
        if condition == "multiple_strategies_agree":
            operator = modifier.get("operator", ">=2")
            # This would need to track strategy agreement - simplified for now
            return True

        elif condition == "field_name_indicates_tag":
            field_names = modifier.get("field_names", [])
            # This would need field context - simplified for now
            return True

        elif condition == "extracted_value_length":
            length_range = modifier.get("range", [5, 20])
            return length_range[0] <= len(candidate) <= length_range[1]

        elif condition == "extracted_value_in_known_catalog":
            # This would check against a known catalog - simplified for now
            return False

        return False


def flatten_dict_values(d):
    """Recursively collect all values from a nested dictionary (including lists) into a flat list."""
    flat = []
    if isinstance(d, dict):
        for v in d.values():
            flat.extend(flatten_dict_values(v))
    elif isinstance(d, list):
        for item in d:
            flat.extend(flatten_dict_values(item))
    else:
        flat.append(d)
    return flat
