import re

from ...utils.confidence import compute_confidence
from ...utils.DataStructures import *
from ...utils.rule_utils import (
    common_extracted_key_attrs,
    get_config,
    get_min_confidence,
    get_rule_id,
    get_source_field_name,
)
from .ExtractionMethodHandler import ExtractionMethodHandler


class RegexExtractionHandler(ExtractionMethodHandler):
    """Handles regex-based key extraction."""

    def extract(
        self, text: str, rule: Any, context: Dict[str, Any] = None
    ) -> List[ExtractedKey]:
        """Extract keys using regex patterns. Supports ExtractionRuleConfig (typed config) or dict-like rule."""
        cfg = get_config(rule)
        pattern_str = cfg.get("pattern") or getattr(rule, "pattern", None)
        if not text or not pattern_str:
            return []

        extracted_keys = []
        try:
            flags = 0
            regex_opts = cfg.get("regex_options") or getattr(rule, "regex_options", None)
            if hasattr(regex_opts, "to_regex_flags"):
                flags = regex_opts.to_regex_flags()
            else:
                case_sens = getattr(rule, "case_sensitive", None)
                if case_sens is None and isinstance(cfg, dict):
                    case_sens = cfg.get("case_sensitive", True)
                if case_sens is None:
                    case_sens = True
                if not case_sens:
                    flags = re.IGNORECASE
            case_sensitive = not (flags & re.IGNORECASE)
            min_conf = get_min_confidence(rule)

            pattern = re.compile(pattern_str, flags)
            matches = pattern.finditer(text)

            for m in matches:
                key_value = m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)
                if not key_value:
                    continue
                confidence = compute_confidence(
                    text.strip(), key_value, case_sensitive=case_sensitive
                )
                if confidence < min_conf:
                    continue
                common = common_extracted_key_attrs(rule)
                extracted_key = ExtractedKey(
                    value=key_value,
                    extraction_type=common["extraction_type"],
                    source_field=get_source_field_name(rule),
                    confidence=confidence,
                    method=common["method"],
                    rule_id=common["rule_id"],
                    metadata={
                        "pattern": pattern_str,
                        "match_position": text.find(key_value),
                        "context": context or {},
                    },
                )
                extracted_keys.append(extracted_key)
        except re.error as e:
            self.logger.error(f"Invalid regex pattern in rule '{get_rule_id(rule)}': {e}")
        return extracted_keys
