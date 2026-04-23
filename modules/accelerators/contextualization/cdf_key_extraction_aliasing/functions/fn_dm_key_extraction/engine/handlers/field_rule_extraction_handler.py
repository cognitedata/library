"""Regex-handler field extraction: fields[], template, merge modes (handler: regex_handler)."""

from __future__ import annotations

import itertools
import re
import string
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set

from ...utils.confidence import compute_confidence
from ...utils.DataStructures import ExtractedKey, ExtractionMethod
from ...utils.RegexMethodParameter import RegexOptions
from ...utils.rule_utils import get_extraction_type_from_rule, get_rule_id
from .ExtractionMethodHandler import ExtractionMethodHandler


def _rule_attr(rule: Any, key: str, default: Any = None) -> Any:
    if rule is None:
        return default
    if isinstance(rule, dict):
        return rule.get(key, default)
    return getattr(rule, key, default)


def _spec_attr(spec: Any, key: str, default: Any = None) -> Any:
    if spec is None:
        return default
    if isinstance(spec, dict):
        return spec.get(key, default)
    return getattr(spec, key, default)


def _rule_fields(rule: Any) -> List[Any]:
    raw = _rule_attr(rule, "fields")
    if not raw:
        return []
    return list(raw)


def _min_conf_from_rule(rule: Any) -> float:
    v = _rule_attr(rule, "validation")
    if v is None:
        return 0.1
    mc = getattr(v, "min_confidence", None) if not isinstance(v, dict) else v.get("min_confidence")
    return float(mc if mc is not None else 0.1)


class FieldRuleExtractionHandler(ExtractionMethodHandler):
    """Declarative field rules with optional result_template (Cartesian) or flat merge."""

    HANDLER_METHOD = ExtractionMethod.REGEX_HANDLER

    def extract_from_entity(
        self,
        entity: Dict[str, Any],
        rule: Any,
        context: Dict[str, Any],
        *,
        get_field_value: Callable[..., Optional[str]],
    ) -> List[ExtractedKey]:
        if not self._rule_applies_to_entity_types(rule, context):
            return []

        fields = _rule_fields(rule)
        if not fields:
            return []

        var_lists: Dict[str, List[str]] = defaultdict(list)
        field_text_by_variable: Dict[str, str] = {}

        for spec in fields:
            raw_text = get_field_value(
                entity,
                spec,
                _rule_attr(rule, "rule_id") or _rule_attr(rule, "name"),
            )
            if raw_text is None or str(raw_text).strip() == "":
                if _spec_attr(spec, "required"):
                    self.logger.verbose(
                        "DEBUG",
                        f"Required field {_spec_attr(spec, 'field_name')} missing for rule {get_rule_id(rule)}",
                    )
                continue
            text = str(raw_text).strip()
            try:
                vals = self._extract_values_for_spec(text, spec, rule)
            except ValueError as e:
                self.logger.verbose(
                    "WARNING",
                    f"Rule {get_rule_id(rule)} field {_spec_attr(spec, 'field_name')}: {e}",
                )
                continue
            if not vals:
                continue
            var_name = _spec_attr(spec, "variable") or _spec_attr(spec, "field_name") or "field"
            var_lists[var_name].extend(vals)
            field_text_by_variable[var_name] = text

        outputs = self._build_output_strings(rule, var_lists)
        return self._to_extracted_keys(rule, outputs, context, field_text_by_variable)

    def _rule_applies_to_entity_types(self, rule: Any, _context: Dict[str, Any]) -> bool:
        """Rule applicability is graph-driven (associations); ``entity_types`` on rules is ignored."""
        return True

    def _extract_values_for_spec(self, text: str, spec: Any, rule: Any) -> List[str]:
        """regex_handler: regex or trim passthrough."""
        rx = _spec_attr(spec, "regex")
        if rx:
            return self._regex_values(text, spec, rule)
        if not text:
            return []
        return [text.strip()] if text.strip() else []

    def _regex_values(self, text: str, spec: Any, rule: Any) -> List[str]:
        pattern_str = _spec_attr(spec, "regex") or ""
        if not pattern_str:
            return []
        ro = _spec_attr(spec, "regex_options")
        if ro is None:
            flags = RegexOptions().to_regex_flags()
        elif isinstance(ro, dict):
            flags = RegexOptions.model_validate(ro).to_regex_flags()
        else:
            flags = ro.to_regex_flags()

        min_conf = _min_conf_from_rule(rule)
        max_m = int(_spec_attr(spec, "max_matches_per_field") or 100)
        out: List[str] = []
        try:
            case_sensitive = not (flags & re.IGNORECASE)
            pat = re.compile(pattern_str, flags)
            for m in pat.finditer(text):
                key_value = m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)
                if not key_value:
                    continue
                conf = compute_confidence(text.strip(), key_value, case_sensitive=case_sensitive)
                if conf < min_conf:
                    continue
                out.append(key_value.strip())
                if len(out) >= max_m:
                    break
        except re.error as e:
            self.logger.error(f"Invalid regex in rule {get_rule_id(rule)}: {e}")
        return out

    def _build_output_strings(self, rule: Any, var_lists: Dict[str, List[str]]) -> List[str]:
        tpl = _rule_attr(rule, "result_template")
        if tpl:
            return self._apply_template(rule, var_lists, str(tpl))
        order: List[str] = []
        seen: Set[str] = set()
        for spec in _rule_fields(rule):
            vn = _spec_attr(spec, "variable") or _spec_attr(spec, "field_name")
            if vn and vn not in seen:
                seen.add(vn)
                order.append(vn)
        flat: List[str] = []
        for vn in order:
            flat.extend(var_lists.get(vn, []))
        for k, vs in var_lists.items():
            if k not in seen:
                flat.extend(vs)
        return flat

    def _apply_template(self, rule: Any, var_lists: Dict[str, List[str]], template: str) -> List[str]:
        formatter = string.Formatter()
        field_names = [fn for _, fn, _, _ in formatter.parse(template) if fn is not None]
        if not field_names:
            return [template]
        cap = int(_rule_attr(rule, "max_template_combinations") or 10000)
        lists = [var_lists.get(fn, [""]) for fn in field_names]
        out: List[str] = []
        count = 0
        for combo in itertools.product(*lists):
            if count >= cap:
                self.logger.warning(
                    f"Rule {get_rule_id(rule)}: Cartesian cap {cap} reached for result_template"
                )
                break
            mapping = {field_names[i]: combo[i] for i in range(len(field_names))}
            try:
                out.append(template.format(**mapping))
            except KeyError as e:
                self.logger.warning(f"Template placeholder error in rule {get_rule_id(rule)}: {e}")
            count += 1
        return out

    def _to_extracted_keys(
        self,
        rule: Any,
        values: List[str],
        context: Dict[str, Any],
        field_text_by_variable: Optional[Dict[str, str]] = None,
    ) -> List[ExtractedKey]:
        ext_type = get_extraction_type_from_rule(rule)
        rid = get_rule_id(rule)
        sf = ",".join(
            (_spec_attr(s, "field_name") or "") for s in _rule_fields(rule)
        )[:500] or "fields"
        src_in = dict(field_text_by_variable) if field_text_by_variable else {}
        keys: List[ExtractedKey] = []
        for v in values:
            if not v:
                continue
            keys.append(
                ExtractedKey(
                    value=v,
                    extraction_type=ext_type,
                    source_field=sf,
                    confidence=1.0,
                    method=self.HANDLER_METHOD,
                    rule_id=rid,
                    metadata={"context": context, "handler": self.HANDLER_METHOD.value},
                    source_inputs=src_in,
                )
            )
        return keys
