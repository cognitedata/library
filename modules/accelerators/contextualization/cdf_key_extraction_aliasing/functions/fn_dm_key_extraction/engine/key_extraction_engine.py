"""
Key Extraction Engine for Cognite Data Fusion (CDF)

Extracts candidate keys and foreign key references using regex_handler
and heuristic handlers (extract_from_entity).

Features:
- Declarative fields[], optional result_template (Cartesian cap); all field specs are merged
- Shared validation (validation_rules, min_confidence) aligned with aliasing
- Configurable extraction rules with priority ordering

Author: Darren Downtain
Version: 2.0.0
"""

import re
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from cdf_fn_common.confidence_match_eval import (
    apply_confidence_match_rules_mutating,
)
from cdf_fn_common.confidence_match_rule_refs import (
    dedupe_confidence_match_rules_by_name,
    validation_rules_list_get,
    validation_rules_list_set,
)
from cdf_fn_common.pipeline_io import pipeline_io_dict_for_engine
from cdf_fn_common.workflow_associations import parse_source_view_to_extraction_pairs
from ..common.logger import CogniteFunctionLogger
from ..config import Config
from ..utils.DataStructures import *
from ..utils.rule_utils import (
    get_extraction_type_from_rule,
    normalize_extraction_type,
    normalize_method,
)
from .handlers import (
    ExtractionMethodHandler,
    FieldRuleExtractionHandler,
    HeuristicExtractionHandler,
)


def _validation_dict_from_any(validation: Any) -> Dict[str, Any]:
    """Normalize validation (dict, SimpleNamespace, Pydantic) to a plain dict."""
    if validation is None:
        return {}
    if hasattr(validation, "model_dump"):
        return dict(validation.model_dump(mode="python"))
    if isinstance(validation, SimpleNamespace):
        return {
            k: getattr(validation, k)
            for k in vars(validation)
            if not k.startswith("_")
        }
    if isinstance(validation, dict):
        d = dict(validation)
        vr = validation_rules_list_get(d)
        if vr is not None:
            validation_rules_list_set(d, list(vr) if isinstance(vr, list) else [])
        return d
    return {}


def _validation_ns_from_flat_dict(d: Dict[str, Any]) -> SimpleNamespace:
    """Build SimpleNamespace for validation from a flat dict."""
    rules = validation_rules_list_get(d)
    if rules is None:
        rules = []
    elif not isinstance(rules, list):
        rules = list(rules)
    else:
        rules = list(rules)
    ns = SimpleNamespace(
        min_confidence=d.get("min_confidence", 0.1),
        regexp_match=d.get("regexp_match"),
        expression_match=d.get("expression_match"),
        validation_rules=rules,
    )
    for k, v in d.items():
        if k in (
            "min_confidence",
            "regexp_match",
            "expression_match",
            "validation_rules",
            "confidence_match_rules",
        ):
            continue
        setattr(ns, k, v)
    return ns


def merge_validation_overlay_onto_global(global_v: Any, overlay: Any) -> SimpleNamespace:
    """Merge per-extraction-rule ``validation`` YAML onto ``data.validation`` (concatenate ``validation_rules``)."""
    base_d = _validation_dict_from_any(global_v)
    over_d = _validation_dict_from_any(overlay)
    merged = dict(base_d)
    brules = list(validation_rules_list_get(merged) or [])
    orules = validation_rules_list_get(over_d)
    if isinstance(orules, list) and orules:
        validation_rules_list_set(
            merged,
            dedupe_confidence_match_rules_by_name(brules + list(orules)),
        )
    else:
        validation_rules_list_set(merged, dedupe_confidence_match_rules_by_name(brules))
    for k, v in over_d.items():
        if k in ("validation_rules", "validation_rules"):
            continue
        if v is not None:
            merged[k] = v
    return _validation_ns_from_flat_dict(merged)


class KeyExtractionEngine:
    """Main engine for key extraction operations."""

    def __init__(
        self,
        config: Union[Config, Dict[str, Any]],
        logger: CogniteFunctionLogger = CogniteFunctionLogger("INFO", False),
    ):
        """Initialize the key extraction engine with configuration (Config or dict from scope YAML / workflow input)."""
        self.logger = logger
        if isinstance(config, dict):
            params = config.get("parameters", {})
            if isinstance(params, dict):
                params = SimpleNamespace(min_key_length=params.get("min_key_length", 3), **params)
            data = SimpleNamespace(
                extraction_rules=config.get("extraction_rules", []),
            )
            self.config = SimpleNamespace(parameters=params, data=data)
            vd = dict(config.get("validation") or {})
            _vr = validation_rules_list_get(vd)
            vd.pop("validation_rules", None)
            vd["validation_rules"] = list(_vr or []) if isinstance(_vr, list) else []
            validation_default = SimpleNamespace(**vd) if vd else SimpleNamespace()
            if not hasattr(validation_default, "min_confidence"):
                validation_default.min_confidence = 0.1
            if not hasattr(validation_default, "regexp_match"):
                validation_default.regexp_match = None
            if not hasattr(validation_default, "expression_match"):
                validation_default.expression_match = None
            if not hasattr(validation_default, "validation_rules"):
                validation_default.validation_rules = []
            self._data_validation = validation_default
            self._source_views = list(config.get("source_views") or [])
            _defaults = {
                "scope_filters": {},
                "validation": validation_default,
                "handler": "regex_handler",
                "pipeline_input": "cumulative",
                "pipeline_output": "merge",
            }
            _sf_defaults = {"table_id": None, "role": "target", "field_name": "", "required": False, "priority": 1}
            self.rules = []
            for rule_data in config.get("extraction_rules", []):
                if isinstance(rule_data, dict):
                    had_validation = "validation" in rule_data
                    r = dict(_defaults, **rule_data)
                    if not had_validation:
                        r["validation"] = validation_default
                    else:
                        r["validation"] = merge_validation_overlay_onto_global(
                            validation_default, r["validation"]
                        )
                    if not str(r.get("handler") or "").strip():
                        r["handler"] = "regex_handler"
                    if not r.get("rule_id") and r.get("name"):
                        r["rule_id"] = str(r["name"])
                    if r.get("name") is None and r.get("rule_id"):
                        r["name"] = str(r["rule_id"])
                    flds = r.get("fields")
                    if flds and isinstance(flds, list) and flds and isinstance(flds[0], dict):
                        r["fields"] = [SimpleNamespace(**{**_sf_defaults, **f}) for f in flds]
                    elif not flds:
                        r["fields"] = []
                    if r.get("config") is None and r.get("parameters") is not None:
                        p = r["parameters"]
                        r["config"] = dict(p) if isinstance(p, dict) else p
                    elif r.get("config") is None:
                        r["config"] = {}
                    _pio = pipeline_io_dict_for_engine(r)
                    r["pipeline_input"] = _pio["pipeline_input"]
                    r["pipeline_output"] = _pio["pipeline_output"]
                    self.rules.append(SimpleNamespace(**r))
                else:
                    self.rules.append(r)
            raw_assoc = config.get("associations")
            if raw_assoc is None and isinstance(config.get("data"), dict):
                raw_assoc = config["data"].get("associations")
            self._association_pairs: Set[Tuple[int, str]] = set()
            if isinstance(raw_assoc, list):
                for i, n in parse_source_view_to_extraction_pairs({"associations": raw_assoc}):
                    nn = str(n).strip()
                    if nn:
                        self._association_pairs.add((int(i), nn))
        else:
            self.config = config
            self.rules = config.data.extraction_rules
            self._data_validation = getattr(config.data, "validation", None)
            sv = getattr(config.data, "source_views", None)
            if sv:
                self._source_views = [
                    v.model_dump(mode="python", exclude_none=False)
                    if hasattr(v, "model_dump")
                    else v
                    for v in sv
                ]
            elif getattr(config.data, "source_view", None) is not None:
                sv_one = config.data.source_view
                self._source_views = [
                    sv_one.model_dump(mode="python", exclude_none=False)
                    if hasattr(sv_one, "model_dump")
                    else sv_one
                ]
            else:
                self._source_views = []
            self._association_pairs = set()
            raw_assoc = getattr(config.data, "associations", None)
            if isinstance(raw_assoc, list):
                for i, n in parse_source_view_to_extraction_pairs({"associations": raw_assoc}):
                    nn = str(n).strip()
                    if nn:
                        self._association_pairs.add((int(i), nn))
        self.method_handlers = self._initialize_method_handlers()

    def _categorize_keys_into_result(
        self,
        keys: List[ExtractedKey],
        rule: Any,
        result: ExtractionResult,
    ) -> None:
        """Append keys to candidate_keys / foreign_key_references / document_references by rule type."""
        rtype = normalize_extraction_type(get_extraction_type_from_rule(rule))
        for key in keys:
            if rtype == ExtractionType.CANDIDATE_KEY:
                result.candidate_keys.append(key)
            elif rtype == ExtractionType.FOREIGN_KEY_REFERENCE:
                result.foreign_key_references.append(key)
            elif rtype == ExtractionType.DOCUMENT_REFERENCE:
                result.document_references.append(key)

    def _initialize_method_handlers(
        self,
    ) -> Dict[ExtractionMethod, ExtractionMethodHandler]:
        """Initialize method handler instances."""
        return {
            ExtractionMethod.REGEX_HANDLER.value: FieldRuleExtractionHandler(self.logger),
            ExtractionMethod.HEURISTIC.value: HeuristicExtractionHandler(self.logger),
        }

    def extract_keys(
        self,
        entity: Dict[str, Any],
        entity_type: str = "",
        *,
        exclude_self_referencing_keys: Optional[bool] = None,
        source_view_index: Optional[int] = None,
    ) -> ExtractionResult:
        """
        Extract keys from entity metadata.

        Args:
            entity: Entity data with metadata fields
            entity_type: Stored on the result / metadata only (not used to select rules).
            exclude_self_referencing_keys: If set, overrides parameters.exclude_self_referencing_keys
                for this extraction (e.g. from source_views config).
            source_view_index: Required when ``associations`` define source-view → extraction edges;
                rules run only for pairs ``(source_view_index, rule_name)`` in that graph.

        Returns:
            ExtractionResult with extracted keys and metadata
        """
        et_meta = (
            str(entity_type).strip()
            or str(entity.get("entity_type") or "").strip()
            or "instance"
        )
        result = ExtractionResult(
            entity_id=entity.get("id", entity.get("externalId", "unknown")),
            entity_type=et_meta,
        )

        # Build context for extraction (entity_type is diagnostic only; rules are gated by associations)
        context = self._build_context(entity, et_meta)

        sorted_rules = sorted(
            [r for r in self.rules if getattr(r, "enabled", True)],
            key=lambda r: getattr(r, "priority", 100),
        )

        pair_gating = bool(self._association_pairs)
        for rule in sorted_rules:
            if pair_gating:
                rid = (
                    getattr(rule, "name", None)
                    or getattr(rule, "rule_id", None)
                    or ""
                )
                rid = str(rid).strip()
                if source_view_index is None or not rid:
                    continue
                if (int(source_view_index), rid) not in self._association_pairs:
                    continue
            # No associations (empty canvas wiring in scope): run all enabled rules so
            # YAML-only / local runs still extract keys; when associations exist, gate above.
            if not self._check_scope_filters(rule, context):
                continue

            method_enum = normalize_method(getattr(rule, "handler", None))
            method_key = method_enum.value
            if method_key == ExtractionMethod.UNSUPPORTED.value:
                self.logger.warning(
                    f"Skipping rule {getattr(rule, 'rule_id', getattr(rule, 'name', '?'))}: "
                    f"unsupported or legacy handler {getattr(rule, 'handler', None)!r}"
                )
                continue

            method_handler = self.method_handlers.get(method_key)
            if not method_handler:
                self.logger.warning(f"No handler registered for method {method_key}")
                continue

            rule_label = getattr(rule, "name", None) or getattr(rule, "rule_id", None)
            rule_id_for_lookup = getattr(rule, "rule_id", None) or getattr(rule, "name", None)

            def _gv(
                ent: Dict[str, Any],
                spec: Any,
                rule_name: Optional[str] = None,
            ) -> Optional[str]:
                raw = self._get_field_value(ent, spec, rule_name or rule_id_for_lookup)
                if raw is None:
                    return None
                return self._preprocess_field_value(str(raw), spec)

            try:
                extracted_keys = method_handler.extract_from_entity(
                    entity,
                    rule,
                    context,
                    get_field_value=_gv,
                )
            except Exception as e:
                self.logger.verbose(
                    "ERROR",
                    f"Error extracting keys with rule '{rule_label}': {e}",
                )
                continue

            if extracted_keys:
                self._categorize_keys_into_result(extracted_keys, rule, result)

        # Apply validation per key: global + per-extraction-rule (source_views rows do not carry validation)
        rule_by_id: Dict[str, Any] = {}
        for rule in self.rules:
            rid = getattr(rule, "rule_id", None) or getattr(rule, "name", None)
            if rid is not None:
                rule_by_id[str(rid)] = rule

        result = self._validate_extraction_result(
            result,
            entity,
            et_meta,
            rule_by_id,
            exclude_self_referencing_keys=exclude_self_referencing_keys,
        )

        return result

    def _build_context(
        self, entity: Dict[str, Any], entity_type: str
    ) -> Dict[str, Any]:
        """Build context information for extraction."""
        context = {
            "entity_type": entity_type,
            "entity_id": entity.get("id", entity.get("externalId", "unknown")),
        }

        # Add common context fields
        if "metadata" in entity and entity["metadata"] is not None:
            metadata = entity["metadata"]
            context.update(
                {
                    "site": metadata.get("site"),
                    "unit": metadata.get("unit"),
                    "equipment_type": metadata.get("equipmentType"),
                    "document_type": metadata.get("documentType"),
                }
            )

        view_id = entity.get(entity.get("externalId", "unknown"), {})
        if view_id:
            context.update(
                {
                    "view_space": view_id.get("view_space", ""),
                    "view_external_id": view_id.get("view_external_id"),
                    "view_version": view_id.get("view_version"),
                    "instance_space": view_id.get("instance_space"),
                }
            )

        return context

    def _check_scope_filters(
        self, rule: Any, context: Dict[str, Any]
    ) -> bool:
        """Check if rule scope filters are satisfied."""
        scope_filters = getattr(rule, "scope_filters", None) or {}
        if not scope_filters:
            return True

        for filter_key, filter_values in scope_filters.items():
            fk = str(filter_key).strip().lower()
            if fk in ("entity_type", "entity_types"):
                raw_vals = (
                    filter_values if isinstance(filter_values, list) else [filter_values]
                )
                allowed = {
                    str(v).strip().lower()
                    for v in raw_vals
                    if v is not None and str(v).strip() != ""
                }
                if not allowed:
                    continue
                et = str(context.get("entity_type") or "").strip().lower()
                if et not in allowed:
                    return False
                continue
            context_value = context.get(filter_key)
            if isinstance(filter_values, list):
                if context_value not in filter_values:
                    return False
            else:
                if context_value != filter_values:
                    return False

        return True

    def _get_field_value(
        self,
        entity: Dict[str, Any],
        source_field: Any,
        rule_name: Optional[str] = None,
    ) -> Optional[str]:
        """Get field value from entity data. Uses rule_name for pipeline-style keys (rule_name_field_name)."""
        field_name = getattr(source_field, "field_name", source_field)
        if not isinstance(field_name, str):
            field_name = str(field_name) if field_name is not None else ""

        # Prefer pipeline-style prefixed keys (supports dotted field_name) before
        # walking nested dicts on the entity root.
        if rule_name and getattr(source_field, "table_id", None):
            lookup = "_".join([rule_name, source_field.table_id, field_name])
            got = entity.get("table_data", {}).get(lookup) or entity.get(lookup)
            if got is not None:
                return str(got)
        if rule_name:
            lookup = "_".join([rule_name, field_name])
            if lookup in entity:
                value = entity[lookup]
                return str(value) if value is not None else None
        if field_name in entity:
            value = entity[field_name]
            return str(value) if value is not None else None
        if "." in field_name:
            parts = field_name.split(".")
            value = entity
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return str(entity[field_name]) if field_name in entity else None
            return str(value) if value is not None else None
        return None

    def _preprocess_field_value(
        self, field_value: str, source_field: Any
    ) -> str:
        """Apply preprocessing to field value."""
        processed = field_value
        preprocessing = getattr(source_field, "preprocessing", None) or []
        if isinstance(preprocessing, str):
            preprocessing = [preprocessing]
        for step in preprocessing:
            if step == "trim":
                processed = processed.strip()
            elif step == "lowercase":
                processed = processed.lower()
            elif step == "uppercase":
                processed = processed.upper()
            elif step == "remove_special_chars":
                processed = re.sub(r"[^\w\s-]", "", processed)
        max_len = getattr(source_field, "max_length", None) or 1000
        if len(processed) > max_len:
            processed = processed[:max_len]
        return processed

    def _should_exclude_self_referencing_keys(
        self,
        entity_type: str,
        source_override: Optional[bool] = None,
    ) -> bool:
        """Whether to drop FKs whose value matches a candidate on the same instance."""
        if source_override is not None:
            return bool(source_override)
        raw = getattr(
            self.config.parameters, "exclude_self_referencing_keys", True
        )
        if isinstance(raw, dict):
            # Legacy per-entity_type maps are ignored; use ``default`` only.
            return bool(raw.get("default", True))
        return bool(raw)

    def _exclude_self_referencing_keys(
        self,
        result: ExtractionResult,
        *,
        source_override: Optional[bool] = None,
    ) -> None:
        """Drop foreign keys whose value matches any candidate key on the same instance."""
        if not self._should_exclude_self_referencing_keys(
            result.entity_type, source_override=source_override
        ):
            return
        candidate_values = {k.value for k in result.candidate_keys}
        if not candidate_values:
            return
        result.foreign_key_references = [
            k for k in result.foreign_key_references if k.value not in candidate_values
        ]

    def _confidence_rule_as_dict(self, raw: Any) -> Optional[Dict[str, Any]]:
        if raw is None:
            return None
        if hasattr(raw, "model_dump"):
            return raw.model_dump(mode="python")
        if isinstance(raw, SimpleNamespace):
            return {
                k: getattr(raw, k)
                for k in vars(raw)
                if not k.startswith("_")
            }
        if isinstance(raw, dict):
            return dict(raw)
        return None

    def _apply_validation_confidence_rules(
        self, validation: Any, keys: List[ExtractedKey]
    ) -> None:
        rules_raw = (
            getattr(validation, "validation_rules", None)
            or getattr(validation, "validation_rules", None)
            or []
        )
        if not isinstance(rules_raw, list):
            rules_raw = list(rules_raw)
        apply_confidence_match_rules_mutating(
            keys,
            rules_raw=rules_raw,
            default_expression_match=validation,
            log_warning=self.logger.warning,
            log_verbose=self.logger.verbose,
        )

    def _validation_to_metadata_dict(self, validation: Any) -> Dict[str, Any]:
        if validation is None:
            return {}
        if hasattr(validation, "model_dump"):
            return validation.model_dump(mode="python")
        if isinstance(validation, SimpleNamespace):
            d = vars(validation).copy()
            # Serialize nested list of SimpleNamespace if present
            rules = validation_rules_list_get(d)
            if isinstance(rules, list):
                validation_rules_list_set(
                    d,
                    [self._confidence_rule_as_dict(r) or r for r in rules],
                )
            return d
        if isinstance(validation, dict):
            d = dict(validation)
            vr = validation_rules_list_get(d)
            if vr is not None:
                validation_rules_list_set(d, list(vr) if isinstance(vr, list) else [])
            return d
        return {}

    def _validate_extraction_result(
        self,
        result: ExtractionResult,
        entity: Dict[str, Any],
        entity_type: str,
        rule_by_id: Dict[str, Any],
        *,
        exclude_self_referencing_keys: Optional[bool] = None,
    ) -> ExtractionResult:
        """Apply validation_rules and min_confidence per key (global + extraction rule only)."""
        result.candidate_keys = self._remove_duplicate_keys(result.candidate_keys)
        result.foreign_key_references = self._remove_duplicate_keys(
            result.foreign_key_references
        )
        result.document_references = self._remove_duplicate_keys(
            result.document_references
        )

        if not self.rules:
            self._exclude_self_referencing_keys(
                result, source_override=exclude_self_referencing_keys
            )
            result.metadata = {
                "extraction_timestamp": datetime.now().isoformat(),
                "total_candidate_keys": len(result.candidate_keys),
                "total_foreign_keys": len(result.foreign_key_references),
                "total_document_references": len(result.document_references),
            }
            return result

        def _filter_keys(keys: List[ExtractedKey]) -> List[ExtractedKey]:
            out: List[ExtractedKey] = []
            for key in keys:
                rule = rule_by_id.get(key.rule_id)
                base_rv = self._data_validation
                if rule is not None and getattr(rule, "validation", None) is not None:
                    base_rv = getattr(rule, "validation")
                val = base_rv
                self._apply_validation_confidence_rules(val, [key])
                mc = float(getattr(val, "min_confidence", 0) or 0)
                if key.confidence >= mc:
                    out.append(key)
            return out

        result.candidate_keys = _filter_keys(result.candidate_keys)
        result.foreign_key_references = _filter_keys(result.foreign_key_references)
        result.document_references = _filter_keys(result.document_references)

        self._exclude_self_referencing_keys(
            result, source_override=exclude_self_referencing_keys
        )
        meta_val = self._data_validation
        validation_config_dict = self._validation_to_metadata_dict(meta_val)
        result.metadata = {
            "extraction_timestamp": datetime.now().isoformat(),
            "total_candidate_keys": len(result.candidate_keys),
            "total_foreign_keys": len(result.foreign_key_references),
            "total_document_references": len(result.document_references),
            "validation_config": validation_config_dict,
        }

        return result

    def _remove_duplicate_keys(self, keys: List[ExtractedKey]) -> List[ExtractedKey]:
        """Remove duplicate keys, keeping the one with highest confidence."""
        seen = {}
        for key in keys:
            if key.value not in seen or key.confidence > seen[key.value].confidence:
                seen[key.value] = key
        return list(seen.values())

# TODO this method is never called?
# def load_config_from_yaml(file_path: str) -> Dict[str, Any]:
#     """Load configuration from YAML file."""
#     try:
#         with open(file_path, "r") as f:
#             return yaml.safe_load(f)
#     except FileNotFoundError:
#         logger.verbose("ERROR", f"Configuration file not found: {file_path}")
#         return {}
#     except yaml.YAMLError as e:
#         logger.verbose("ERROR", f"Error parsing YAML configuration: {e}")
#         return {}


def main():
    """Example usage of the KeyExtractionEngine."""

    config = {
        "associations": [
            {
                "kind": "source_view_to_extraction",
                "source_view_index": 0,
                "extraction_rule_name": "standard_pump_tag",
            },
            {
                "kind": "source_view_to_extraction",
                "source_view_index": 0,
                "extraction_rule_name": "flow_instrument_tag",
            },
        ],
        "extraction_rules": [
            {
                "rule_id": "standard_pump_tag",
                "description": "Extracts standard pump tags from equipment descriptions",
                "extraction_type": "candidate_key",
                "handler": "regex_handler",
                "priority": 50,
                "enabled": True,
                "field_results_mode": "merge_all",
                "fields": [
                    {
                        "field_name": "name",
                        "required": True,
                        "priority": 1,
                        "regex": r"\bP[-_]?\d{2,4}[A-Z]?\b",
                        "regex_options": {"ignore_case": True},
                    },
                    {
                        "field_name": "description",
                        "required": False,
                        "priority": 2,
                        "regex": r"\bP[-_]?\d{2,4}[A-Z]?\b",
                        "regex_options": {"ignore_case": True},
                    },
                ],
                "validation": {"min_confidence": 0.7},
            },
            {
                "rule_id": "flow_instrument_tag",
                "description": "Extracts ISA flow instrument tags",
                "extraction_type": "foreign_key_reference",
                "handler": "regex_handler",
                "priority": 30,
                "enabled": True,
                "field_results_mode": "merge_all",
                "fields": [
                    {
                        "field_name": "description",
                        "regex": r"\bFIC[-_]?\d{4}[A-Z]?\b",
                        "regex_options": {"ignore_case": True},
                    }
                ],
                "validation": {"min_confidence": 0.8},
            },
        ],
        "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
    }

    # Initialize engine
    engine = KeyExtractionEngine(config)

    # Test cases
    test_entities = [
        {
            "id": "asset_001",
            "name": "P-101",
            "description": "Main feed pump for Tank T-301, controlled by FIC-2001",
            "metadata": {"site": "Plant_A", "equipmentType": "pump"},
        },
        {
            "id": "asset_002",
            "name": "FCV-2001A",
            "description": "Flow control valve for reactor feed system",
            "metadata": {"site": "Plant_B", "equipmentType": "valve"},
        },
    ]

    print("Key Extraction Engine - Test Results")
    print("=" * 50)

    for i, entity in enumerate(test_entities, 1):
        print(f"\nTest Case {i}:")
        print(f"Entity: {entity['name']}")
        print(f"Description: {entity['description']}")

        result = engine.extract_keys(entity, "asset", source_view_index=0)

        print(f"\nCandidate Keys ({len(result.candidate_keys)}):")
        for key in result.candidate_keys:
            print(
                f"  - {key.value} (confidence: {key.confidence:.2f}, method: {key.method.value})"
            )

        print(f"\nForeign Key References ({len(result.foreign_key_references)}):")
        for key in result.foreign_key_references:
            print(
                f"  - {key.value} (confidence: {key.confidence:.2f}, method: {key.method.value})"
            )

        print(f"\nDocument References ({len(result.document_references)}):")
        for key in result.document_references:
            print(
                f"  - {key.value} (confidence: {key.confidence:.2f}, method: {key.method.value})"
            )


if __name__ == "__main__":
    main()
