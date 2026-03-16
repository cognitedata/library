"""
Shared rule adapter and ExtractedKey helpers.

Provides a single contract for rule-like objects (Pydantic, SimpleNamespace, or dict)
so handlers can access rule_id, config, source_field name, extraction_type, and method
without duplicating getattr/dict logic.
"""

from typing import Any, Dict, List, Optional

from .DataStructures import ExtractionMethod, ExtractionType


def get_rule_id(rule: Any) -> str:
    """Return rule identifier (name or rule_id). Works with object or dict."""
    if rule is None:
        return "unknown"
    if isinstance(rule, dict):
        return rule.get("name") or rule.get("rule_id") or "unknown"
    return getattr(rule, "name", None) or getattr(rule, "rule_id", None) or "unknown"


def get_config(rule: Any) -> Dict[str, Any]:
    """Return rule config dict. Works with object or dict."""
    if rule is None:
        return {}
    if isinstance(rule, dict):
        return rule.get("config") or {}
    cfg = getattr(rule, "config", None)
    if isinstance(cfg, dict):
        return cfg
    return {}


def get_source_field_name(rule: Any, default: str = "unknown") -> str:
    """Return the first source field's field_name. Works with object or dict."""
    if rule is None:
        return default
    fields = None
    if isinstance(rule, dict):
        fields = rule.get("source_fields")
    else:
        fields = getattr(rule, "source_fields", None)
    if not fields:
        return default
    first = fields[0] if isinstance(fields, list) else fields
    if isinstance(first, dict):
        return first.get("field_name", default)
    return getattr(first, "field_name", default)


def get_min_confidence(rule: Any, default: float = 0.3) -> float:
    """Return rule min_confidence. Works with object or dict."""
    if rule is None:
        return default
    if isinstance(rule, dict):
        return rule.get("min_confidence", default)
    return getattr(rule, "min_confidence", default)


def normalize_extraction_type(extraction_type: Any) -> ExtractionType:
    """Return ExtractionType enum. Accepts string or enum."""
    if extraction_type is None:
        return ExtractionType.CANDIDATE_KEY
    if isinstance(extraction_type, ExtractionType):
        return extraction_type
    if isinstance(extraction_type, str) and extraction_type in [
        e.value for e in ExtractionType
    ]:
        return ExtractionType(extraction_type)
    return ExtractionType.CANDIDATE_KEY


def normalize_method(method: Any) -> ExtractionMethod:
    """Return ExtractionMethod enum. Accepts string or enum."""
    if method is None:
        return ExtractionMethod.REGEX
    if isinstance(method, ExtractionMethod):
        return method
    if isinstance(method, str):
        normalized = method.replace("_", " ").strip()
        for e in ExtractionMethod:
            if e.value == normalized or e.value == method:
                return e
        if method == "fixed_width":
            return ExtractionMethod.FIXED_WIDTH
        if method == "token_reassembly":
            return ExtractionMethod.TOKEN_REASSEMBLY
    return ExtractionMethod.REGEX


def get_extraction_type_from_rule(rule: Any) -> ExtractionType:
    """Return rule extraction_type as enum. Works with object or dict."""
    if rule is None:
        return ExtractionType.CANDIDATE_KEY
    if isinstance(rule, dict):
        return normalize_extraction_type(rule.get("extraction_type"))
    return normalize_extraction_type(getattr(rule, "extraction_type", None))


def get_method_from_rule(rule: Any) -> ExtractionMethod:
    """Return rule method as enum. Works with object or dict."""
    if rule is None:
        return ExtractionMethod.REGEX
    if isinstance(rule, dict):
        return normalize_method(rule.get("method"))
    return normalize_method(getattr(rule, "method", None))


def common_extracted_key_attrs(
    rule: Any,
    source_field_override: Optional[str] = None,
    method_override: Optional[ExtractionMethod] = None,
) -> Dict[str, Any]:
    """
    Return a dict of common ExtractedKey attributes derived from rule.
    Handlers can pass value, confidence, metadata and merge with this.
    """
    return {
        "source_field": source_field_override
        if source_field_override is not None
        else get_source_field_name(rule),
        "extraction_type": get_extraction_type_from_rule(rule),
        "method": method_override if method_override is not None else get_method_from_rule(rule),
        "rule_id": get_rule_id(rule),
    }
