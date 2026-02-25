import re

from ...utils.confidence import compute_confidence
from ...utils.DataStructures import *
from .ExtractionMethodHandler import ExtractionMethodHandler


class RegexExtractionHandler(ExtractionMethodHandler):
    """Handles regex-based key extraction."""

    def extract(
        self, text: str, rule: ExtractionRule, context: Dict[str, Any] = None
    ) -> List[ExtractedKey]:
        """Extract keys using regex patterns."""
        if not text and not rule.config.get("pattern", None):
            return []

        extracted_keys = []

        try:
            # Compile regex pattern with appropriate flags
            flags = 0
            if not rule.case_sensitive:
                flags |= re.IGNORECASE

            pattern = re.compile(rule.config.get("pattern", None), flags)

            # Find all matches
            matches = pattern.finditer(text)

            for m in matches:
                key_value = m.group(0)

                if key_value:
                    # Apply shared confidence rules: exact -> 1.0, start/end -> 0.90, contains -> 0.80, else token-overlap (cap 0.80)
                    confidence = compute_confidence(
                        text.strip(), key_value, case_sensitive=rule.case_sensitive
                    )

                    # Blacklist: set confidence to 0.0 if extracted value contains any blacklisted keyword
                    blacklist_keywords = (context or {}).get("blacklist_keywords") or []
                    if blacklist_keywords and any(
                        kw.lower() in key_value.lower() for kw in blacklist_keywords
                    ):
                        confidence = 0.0

                    if confidence >= rule.min_confidence:
                        # Handle both dict and SourceField object for source_field
                        source_field = "unknown"
                        if rule.source_fields:
                            first_field = rule.source_fields[0]
                            if isinstance(first_field, dict):
                                source_field = first_field.get("field_name", "unknown")
                            else:
                                source_field = first_field.field_name

                        extracted_key = ExtractedKey(
                            value=key_value,
                            extraction_type=rule.extraction_type,
                            source_field=source_field,
                            confidence=confidence,
                            method=rule.method,
                            rule_name=rule.name,
                            metadata={
                                "pattern": rule.pattern,
                                "match_position": text.find(key_value),
                                "context": context or {},
                            },
                        )
                        extracted_keys.append(extracted_key)

        except re.error as e:
            self.logger.error(
                f"Invalid regex pattern '{rule.pattern}' in rule '{rule.name}': {e}"
            )

        return extracted_keys
