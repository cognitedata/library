"""
Passthrough extraction handler: use the entire field value as the extracted key.

No regex, fixed width, or other parsing is applied—the full (optionally trimmed)
value is returned as a single candidate key. Useful when the field already
contains the identifier (e.g. name, externalId) and no extraction logic is needed.
"""

from typing import Any, Dict, List

from ...utils.DataStructures import ExtractedKey, ExtractionMethod
from ...utils.rule_utils import common_extracted_key_attrs, get_min_confidence
from .ExtractionMethodHandler import ExtractionMethodHandler


class PassthroughExtractionHandler(ExtractionMethodHandler):
    """Extract the entire field value as a single key (no parsing)."""

    def extract(
        self,
        text: str,
        rule: Any,
        context: Dict[str, Any] = None,
    ) -> List[ExtractedKey]:
        """Return the full (trimmed) text as one ExtractedKey."""
        if not text or not str(text).strip():
            return []

        value = str(text).strip()
        confidence = get_min_confidence(rule)
        if confidence <= 0:
            confidence = 1.0
        common = common_extracted_key_attrs(rule, method_override=ExtractionMethod.PASSTHROUGH)
        return [
            ExtractedKey(
                value=value,
                extraction_type=common["extraction_type"],
                source_field=common["source_field"],
                confidence=confidence,
                method=common["method"],
                rule_id=common["rule_id"],
                metadata={"passthrough": True, "context": context or {}},
            )
        ]
