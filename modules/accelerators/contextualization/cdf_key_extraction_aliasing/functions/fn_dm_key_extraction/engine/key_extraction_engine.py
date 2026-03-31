"""
Key Extraction Engine for Cognite Data Fusion (CDF)

This module implements the core key extraction functionality as described in the
key_extraction.md specification. It provides methods for extracting candidate keys
and foreign key references from entity metadata using regex, fixed width parsing,
token reassembly, and heuristic approaches.

Features:
- 5 extraction methods: passthrough (default when omitted), regex, fixed width, token reassembly, heuristic
- Support for candidate keys and foreign key references
- Configurable extraction rules with priority ordering
- Integration with CDF data model views
- Comprehensive validation and error handling

Author: Darren Downtain
Version: 1.0.1
"""

import re
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Union

from ..utils.TokenReassemblyMethodParameter import (
    AssemblyRule,
    TokenReassemblyMethodParameter,
)

from ..common.logger import CogniteFunctionLogger
from ..config import Config, ExtractionRuleConfig
from ..utils.DataStructures import *
from ..utils.rule_utils import (
    get_extraction_type_from_rule,
    get_method_from_rule,
    normalize_extraction_type,
    normalize_method,
)
from .handlers import (
    ExtractionMethodHandler,
    FixedWidthExtractionHandler,
    HeuristicExtractionHandler,
    PassthroughExtractionHandler,
    RegexExtractionHandler,
    TokenReassemblyExtractionHandler,
)


class KeyExtractionEngine:
    """Main engine for key extraction operations."""

    def __init__(
        self,
        config: Union[Config, Dict[str, Any]],
        logger: CogniteFunctionLogger = CogniteFunctionLogger("INFO", False),
    ):
        """Initialize the key extraction engine with configuration (Config or dict from main.py)."""
        self.logger = logger
        if isinstance(config, dict):
            params = config.get("parameters", {})
            if isinstance(params, dict):
                params = SimpleNamespace(min_key_length=params.get("min_key_length", 3), **params)
            data = SimpleNamespace(
                extraction_rules=config.get("extraction_rules", []),
                field_selection_strategy=config.get("field_selection_strategy", "first_match"),
            )
            self.config = SimpleNamespace(parameters=params, data=data)
            vd = dict(config.get("validation") or {})
            validation_default = SimpleNamespace(**vd) if vd else SimpleNamespace()
            if not hasattr(validation_default, "min_confidence"):
                validation_default.min_confidence = 0.1
            if not hasattr(validation_default, "regexp_match"):
                validation_default.regexp_match = None
            if not hasattr(validation_default, "confidence_match_rules"):
                validation_default.confidence_match_rules = []
            self._data_validation = validation_default
            self._source_views = list(config.get("source_views") or [])
            _defaults = {"scope_filters": {}, "validation": validation_default}
            _sf_defaults = {"table_id": None, "role": "target", "field_name": "", "required": False, "priority": 1}
            self.rules = []
            for r in config.get("extraction_rules", []):
                if isinstance(r, dict):
                    r = dict(_defaults, **r)
                    r.setdefault("validation", validation_default)
                    # Normalize method name for handler lookup (e.g. fixed_width -> fixed width)
                    method = r.get("method", "")
                    if method == "fixed_width":
                        r["method"] = ExtractionMethod.FIXED_WIDTH.value
                    elif method == "token_reassembly":
                        r["method"] = ExtractionMethod.TOKEN_REASSEMBLY.value
                    sf = r.get("source_fields", [])
                    if sf and isinstance(sf[0], dict):
                        r["source_fields"] = [SimpleNamespace(**{**_sf_defaults, **f}) for f in sf]
                    self.rules.append(SimpleNamespace(**r))
                else:
                    self.rules.append(r)
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
        self.method_handlers = self._initialize_method_handlers()
        self.field_selection_strategy = self.config.data.field_selection_strategy

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
            ExtractionMethod.PASSTHROUGH.value: PassthroughExtractionHandler(self.logger),
            ExtractionMethod.REGEX.value: RegexExtractionHandler(self.logger),
            ExtractionMethod.FIXED_WIDTH.value: FixedWidthExtractionHandler(self.logger),
            ExtractionMethod.TOKEN_REASSEMBLY.value: TokenReassemblyExtractionHandler(
                self.logger
            ),
            ExtractionMethod.HEURISTIC.value: HeuristicExtractionHandler(self.logger),
        }

    def extract_keys(
        self,
        entity: Dict[str, Any],
        entity_type: str = "asset",
        *,
        exclude_self_referencing_keys: Optional[bool] = None,
    ) -> ExtractionResult:
        """
        Extract keys from entity metadata.

        Args:
            entity: Entity data with metadata fields
            entity_type: Type of entity (asset, file, timeseries, etc.)
            exclude_self_referencing_keys: If set, overrides parameters.exclude_self_referencing_keys
                for this extraction (e.g. from source_views config).

        Returns:
            ExtractionResult with extracted keys and metadata
        """
        result = ExtractionResult(
            entity_id=entity.get("id", entity.get("externalId", "unknown")),
            entity_type=entity_type,
        )

        # Build context for extraction
        context = self._build_context(entity, entity_type)

        # Apply each rule
        for rule in self.rules:
            if not getattr(rule, "enabled", True):
                continue

            # Handle composite field extraction (cross-field merging)
            if getattr(rule, "composite_strategy", None):
                extracted_keys = self._extract_from_composite_fields(
                    entity, rule, context, result
                )
                if extracted_keys:
                    self._categorize_keys_into_result(extracted_keys, rule, result)
                continue

            # Check scope filters
            if not self._check_scope_filters(rule, context):
                continue

            # Extract from source fields honoring field_selection_strategy
            strategy = self.field_selection_strategy

            # Normalize source_fields to list (can be single SourceFieldParameter or list)
            source_fields_list = (
                list(rule.source_fields)
                if isinstance(rule.source_fields, list)
                else ([rule.source_fields] if rule.source_fields else [])
            )
            fields = sorted(
                source_fields_list, key=lambda f: getattr(f, "priority", 1)
            )

            collected_for_rule: List[ExtractedKey] = []

            for source_field in fields:
                field_value = self._get_field_value(entity, source_field, rule.name)
                if not isinstance(field_value, str) or field_value == "":
                    if getattr(source_field, "required", False):
                        self.logger.verbose(
                            "DEBUG",
                            f"Required field '{source_field.field_name}' missing for entity {result.entity_id}",
                        )
                        continue
                    else:
                        continue

                if getattr(source_field, "preprocessing", None):
                    processed_value = self._preprocess_field_value(
                        field_value, source_field
                    )
                else:
                    processed_value = field_value

                # Extract keys using appropriate method (canonical method name from one place)
                method_key = normalize_method(
                    getattr(rule, "method", None)
                ).value
                method_handler = self.method_handlers.get(method_key)
                if not method_handler:
                    self.logger.warning(f"No handler for method {method_key}")
                    continue

                try:
                    extracted_keys = method_handler.extract(
                        processed_value, rule, context
                    )

                    # Attribute extracted keys to the specific field we extracted from.
                    # Handlers may not know which field produced the match.
                    for k in extracted_keys:
                        if getattr(source_field, "field_name", None):
                            k.source_field = source_field.field_name

                    collected_for_rule.extend(extracted_keys)

                    # field selection strategies short-circuiting
                    # first_match: stop at first field (in priority order) that produces results
                    if strategy == "first_match" and extracted_keys:
                        break

                except Exception as e:
                    self.logger.verbose(
                        "ERROR", f"Error extracting keys with rule '{rule.name}': {e}"
                    )

            # Post-process collected keys per strategy
            if collected_for_rule:
                self._categorize_keys_into_result(collected_for_rule, rule, result)

        # Apply validation using last rule's validation + optional source_view overlay
        rule_ref = self.rules[-1] if self.rules else None
        base_validation = self._resolve_validation(rule_ref)
        merged_validation = self._merge_validation_for_entity(
            entity, base_validation, entity_type
        )
        if self.rules:
            result = self._validate_extraction_result(
                rule_ref,
                result,
                exclude_self_referencing_keys=exclude_self_referencing_keys,
                validation_override=merged_validation,
            )
        else:
            result = self._validate_extraction_result(
                None,
                result,
                exclude_self_referencing_keys=exclude_self_referencing_keys,
                validation_override=merged_validation,
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

    def _extract_from_composite_fields(
        self,
        entity: Dict[str, Any],
        rule: ExtractionRuleConfig,
        context: Dict[str, Any],
        result: ExtractionResult,
    ) -> List[ExtractedKey]:
        """
        Extract keys by merging multiple source fields based on composite_strategy.

        Args:
            entity: Entity data with metadata fields
            rule: Extraction rule with composite_strategy configured
            context: Extraction context
            result: Current extraction result (for metadata)

        Returns:
            List of extracted keys from merged fields
        """
        extracted_keys = []

        # Separate fields by role (target/context used by composite strategies below)
        target_fields = [f for f in rule.source_fields if f.role == "target"]
        context_fields = [f for f in rule.source_fields if f.role == "context"]

        # Collect field values
        field_values = {}
        _rnm = getattr(rule, "name", None) or getattr(rule, "rule_id", None)
        for source_field in rule.source_fields:
            field_value = self._get_field_value(entity, source_field, _rnm)
            if field_value:
                # Apply preprocessing
                processed_value = self._preprocess_field_value(
                    field_value, source_field
                )
                field_values[source_field.field_name] = {
                    "value": processed_value,
                    "field": source_field,
                }
            elif source_field.required:
                # Required field missing, skip this rule
                self.logger.verbose(
                    "WARNING",
                    f"Required field '{source_field.field_name}' missing for composite extraction in rule '{rule.name}'",
                )
                return []

        # Apply composite strategy
        if rule.composite_strategy == "concatenate":
            # Simple concatenation of fields
            separator = rule.config.get("field_separator", "-")
            field_order = rule.config.get(
                "field_order", [f.field_name for f in target_fields]
            )

            composite_value_parts = []
            for field_name in field_order:
                if field_name in field_values:
                    composite_value_parts.append(field_values[field_name]["value"])

            if composite_value_parts:
                composite_value = separator.join(composite_value_parts)

                # Extract using the configured method (canonical method name)
                method_key = normalize_method(getattr(rule, "method", None)).value
                method_handler = self.method_handlers.get(method_key)
                if method_handler:
                    try:
                        extracted_keys = method_handler.extract(
                            composite_value, rule, context
                        )
                        # Update source_field metadata to indicate composite extraction
                        for key in extracted_keys:
                            key.source_field = "+".join(field_order)
                            key.metadata["composite_extraction"] = True
                            key.metadata["composite_fields"] = field_order
                            key.metadata["composite_strategy"] = "concatenate"
                    except Exception as e:
                        self.logger.verbose(
                            "ERROR",
                            f"Error in composite extraction for rule '{rule.name}': {e}",
                        )

        elif rule.composite_strategy == "token_reassembly":
            # Cross-field token reassembly
            extracted_keys = self._extract_cross_field_token_reassembly(
                field_values, rule, context
            )

        elif rule.composite_strategy == "context_aware":
            # Use context fields to inform extraction from target fields
            # Merge context into context dict and extract from target fields
            enhanced_context = context.copy()
            for context_field in context_fields:
                if context_field.field_name in field_values:
                    enhanced_context[context_field.field_name] = field_values[
                        context_field.field_name
                    ]["value"]

            # Extract from target fields with enhanced context
            method_key = normalize_method(getattr(rule, "method", None)).value
            method_handler = self.method_handlers.get(method_key)
            if method_handler:
                for target_field in target_fields:
                    if target_field.field_name in field_values:
                        try:
                            field_keys = method_handler.extract(
                                field_values[target_field.field_name]["value"],
                                rule,
                                enhanced_context,
                            )
                            for key in field_keys:
                                key.metadata["context_aware"] = True
                                key.metadata["context_fields"] = [
                                    f.field_name for f in context_fields
                                ]
                            extracted_keys.extend(field_keys)
                        except Exception as e:
                            self.logger.verbose(
                                "ERROR",
                                f"Error in context-aware extraction for rule '{rule.name}': {e}",
                            )

        return extracted_keys

    def _extract_cross_field_token_reassembly(
        self,
        field_values: Dict[str, Dict[str, Any]],
        rule: ExtractionRule,
        context: Dict[str, Any],
    ) -> List[ExtractedKey]:
        """
        Extract tokens from multiple fields and reassemble them into complete tags.

        Args:
            field_values: Dictionary mapping field names to their processed values
            rule: Extraction rule with token_reassembly method
            context: Extraction context

        Returns:
            List of extracted and reassembled keys
        """
        extracted_keys = []
        config = rule.config
        tokenization = config.get("tokenization", {})

        extract_from_multiple_fields = tokenization.get(
            "extract_from_multiple_fields", []
        )

        if not extract_from_multiple_fields:
            # Fallback: tokenize each field separately and combine
            # Use first token from each field as potential components
            separator_patterns = tokenization.get(
                "separator_patterns", ["-", "_", "/", " "]
            )
            all_tokens = {}

            for field_name, field_data in field_values.items():
                tokens = self._tokenize_field_value(
                    field_data["value"], separator_patterns
                )
                # Store tokens keyed by field name (for assembly format like {siteCode}-{unitNumber})
                if tokens:
                    all_tokens[field_name] = [
                        {
                            "value": tokens[0],  # Use first token from field
                            "field": field_name,
                            "position": 0,
                            "required": True,
                        }
                    ]
        else:
            # Extract tokens from specific fields based on configuration
            all_tokens = {}
            separator_patterns = tokenization.get(
                "separator_patterns", ["-", "_", "/", " "]
            )
            token_patterns = tokenization.get("token_patterns", [])

            # Map token types to fields
            for field_config in rule.source_fields:
                field_name = field_config.field_name
                token_type = field_config.role

                if field_name in field_values:
                    field_value = field_values[field_name]["value"]
                    tokens = self._tokenize_field_value(field_value, separator_patterns)

                    # Match tokens against patterns if specified
                    for token_pattern in token_patterns:
                        pattern_name = token_pattern.get("name")
                        pattern_regex = token_pattern.get("pattern")
                        component_type = token_pattern.get("component_type", "")

                        if component_type == token_type and pattern_regex:
                            try:
                                compiled_pattern = re.compile(pattern_regex)
                                for i, token in enumerate(tokens):
                                    if compiled_pattern.match(token):
                                        if pattern_name not in all_tokens:
                                            all_tokens[pattern_name] = []
                                        all_tokens[pattern_name].append(
                                            {
                                                "value": token,
                                                "field": field_name,
                                                "position": i,
                                                "component_type": component_type,
                                                "required": token_pattern.get(
                                                    "required", False
                                                ),
                                            }
                                        )
                            except re.error as e:
                                self.logger.verbose(
                                    "ERROR",
                                    f"Invalid token pattern '{pattern_regex}': {e}",
                                )

        # Apply assembly rules
        assembly_rules = config.get("assembly_rules", [])
        handler = TokenReassemblyExtractionHandler()
        tkr_param = TokenReassemblyMethodParameter(**rule.config)

        for assembly_rule in assembly_rules:
            assembled_keys = handler._assemble_tokens(
                all_tokens,
                AssemblyRule(**assembly_rule),
                tkr_param,
                get_extraction_type_from_rule(rule),
                get_method_from_rule(rule),
                context,
            )
            if assembled_keys:
                for assembled_key in assembled_keys:
                    assembled_key.metadata["cross_field_extraction"] = True
                    assembled_key.metadata["source_fields"] = list(field_values.keys())
                    assembled_key.source_field = "+".join(field_values.keys())
                    assembled_key.extraction_type = config.get(
                        "extraction_type", rule.extraction_type
                    )
                extracted_keys.append(assembled_key)

        return extracted_keys

    def _tokenize_field_value(
        self, text: str, separator_patterns: List[str]
    ) -> List[str]:
        """Tokenize text using separator patterns."""
        separator_regex = "|".join(re.escape(sep) for sep in separator_patterns)
        tokens = re.split(separator_regex, text)
        return [token.strip() for token in tokens if token.strip()]

    def _check_scope_filters(
        self, rule: ExtractionRuleConfig, context: Dict[str, Any]
    ) -> bool:
        """Check if rule scope filters are satisfied."""
        if not rule.scope_filters:
            return True

        for filter_key, filter_values in rule.scope_filters.items():
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
        if isinstance(field_name, str) and "." in field_name:
            parts = field_name.split(".")
            value = entity
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return None
            return str(value) if value is not None else None
        if rule_name and getattr(source_field, "table_id", None):
            lookup = "_".join([rule_name, source_field.table_id, field_name])
            return entity.get("table_data", {}).get(lookup) or entity.get(lookup)
        if rule_name:
            lookup = "_".join([rule_name, field_name])
            if lookup in entity:
                value = entity[lookup]
                return str(value) if value is not None else None
        if field_name in entity:
            value = entity[field_name]
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
            if entity_type in raw:
                return bool(raw[entity_type])
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

    def _resolve_validation(self, rule: Optional[Any]) -> Optional[Any]:
        if rule is not None and getattr(rule, "validation", None) is not None:
            return rule.validation
        return getattr(self, "_data_validation", None)

    @staticmethod
    def _source_view_entry_as_dict(entry: Any) -> Dict[str, Any]:
        if isinstance(entry, dict):
            return entry
        if hasattr(entry, "model_dump"):
            return entry.model_dump(mode="python", exclude_none=False)
        return {}

    def _match_source_view_for_entity(
        self, entity: Dict[str, Any], entity_type_fallback: str
    ) -> Optional[Dict[str, Any]]:
        """First matching source_views[] entry (list order) using stamped view + entity_type."""
        if not getattr(self, "_source_views", None):
            return None
        esp = entity.get("view_space")
        eext = entity.get("view_external_id")
        ever = entity.get("view_version")
        et = entity.get("entity_type")
        if et is None or (isinstance(et, str) and not str(et).strip()):
            et = entity_type_fallback
        et_norm = str(et).strip().lower()

        def _norm_str(x: Any) -> Optional[str]:
            if x is None:
                return None
            return str(x)

        for raw in self._source_views:
            v = self._source_view_entry_as_dict(raw)
            if _norm_str(v.get("view_space")) != _norm_str(esp):
                continue
            if _norm_str(v.get("view_external_id")) != _norm_str(eext):
                continue
            if _norm_str(v.get("view_version")) != _norm_str(ever):
                continue
            cfg_et = v.get("entity_type")
            if hasattr(cfg_et, "value"):
                cfg_et = cfg_et.value
            if str(cfg_et or "").strip().lower() != et_norm:
                continue
            return v
        return None

    def _validation_shallow_copy_ns(self, base: Any) -> SimpleNamespace:
        """Copy validation into a mutable SimpleNamespace for merge (preserves extra keys, e.g. max_keys_per_type)."""
        d = self._validation_to_metadata_dict(base) if base is not None else {}
        rules = d.get("confidence_match_rules")
        if rules is None:
            rules = []
        elif not isinstance(rules, list):
            rules = list(rules)
        else:
            rules = list(rules)
        ns = SimpleNamespace(
            min_confidence=d.get("min_confidence", 0.1),
            regexp_match=d.get("regexp_match"),
            confidence_match_rules=rules,
        )
        for k, v in d.items():
            if k in ("min_confidence", "regexp_match", "confidence_match_rules"):
                continue
            setattr(ns, k, v)
        return ns

    def _merge_validation_for_entity(
        self,
        entity: Dict[str, Any],
        base_validation: Any,
        entity_type_fallback: str,
    ) -> Any:
        """Merge optional source_views[].validation with base (rule or global) validation."""
        matched = self._match_source_view_for_entity(entity, entity_type_fallback)
        if not matched:
            return base_validation
        vd = matched.get("validation")
        if vd is None:
            return base_validation
        view_d: Dict[str, Any]
        if isinstance(vd, dict):
            view_d = dict(vd)
        elif hasattr(vd, "model_dump"):
            view_d = vd.model_dump(mode="python", exclude_none=False)
        else:
            view_d = self._source_view_entry_as_dict(vd)
        if not view_d:
            return base_validation

        merged = self._validation_shallow_copy_ns(base_validation)

        if "confidence_match_rules" in view_d:
            vrules = view_d["confidence_match_rules"]
            base_rules = list(merged.confidence_match_rules or [])
            if isinstance(vrules, list) and len(vrules) > 0:
                merged.confidence_match_rules = base_rules + list(vrules)

        for key, val in view_d.items():
            if key == "confidence_match_rules":
                continue
            if val is not None:
                setattr(merged, key, val)

        return merged

    def _expression_items_to_pattern_strings(self, expressions: Any) -> List[str]:
        """Normalize match.expressions entries to regex strings (str or {pattern, ...} dict/model)."""
        out: List[str] = []
        if not expressions:
            return out
        for e in expressions:
            if isinstance(e, str):
                s = str(e).strip()
                if s:
                    out.append(s)
            elif isinstance(e, dict):
                p = str(e.get("pattern", "") or "").strip()
                if p:
                    out.append(p)
            elif hasattr(e, "pattern"):
                p = str(getattr(e, "pattern", "") or "").strip()
                if p:
                    out.append(p)
        return out

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

    def _build_sorted_confidence_runtime(
        self, rules_raw: List[Any]
    ) -> List[tuple]:
        """Return list of (priority, list_index, name, compiled_regexes, keywords, mode, value)."""
        runtime: List[tuple] = []
        for idx, raw in enumerate(rules_raw or []):
            rd = self._confidence_rule_as_dict(raw)
            if not rd:
                continue
            if not rd.get("enabled", True):
                continue
            match = rd.get("match") or {}
            if hasattr(match, "model_dump"):
                match = match.model_dump(mode="python")
            elif isinstance(match, SimpleNamespace):
                match = vars(match)
            exprs = self._expression_items_to_pattern_strings(match.get("expressions"))
            kws = [k for k in (match.get("keywords") or []) if str(k).strip()]
            if not exprs and not kws:
                self.logger.verbose(
                    "WARNING",
                    "Skipping confidence_match_rule with empty match (no expressions or keywords)",
                )
                continue
            mod = rd.get("confidence_modifier") or {}
            if hasattr(mod, "model_dump"):
                mod = mod.model_dump(mode="python")
            elif isinstance(mod, SimpleNamespace):
                mod = vars(mod)
            mode = mod.get("mode")
            if mode not in ("explicit", "offset"):
                self.logger.warning(
                    "Skipping confidence_match_rule %r: invalid confidence_modifier.mode %r",
                    rd.get("name", idx),
                    mode,
                )
                continue
            try:
                value = float(mod.get("value", 0.0))
            except (TypeError, ValueError):
                self.logger.warning(
                    "Skipping confidence_match_rule %r: invalid confidence_modifier.value",
                    rd.get("name", idx),
                )
                continue
            pri = rd.get("priority")
            if pri is None:
                pri = idx * 10
            compiled: List[re.Pattern] = []
            for pat in exprs:
                try:
                    compiled.append(re.compile(pat))
                except re.error as e:
                    self.logger.warning(
                        "Invalid regex in confidence_match_rule %r pattern %r: %s",
                        rd.get("name", idx),
                        pat,
                        e,
                    )
            name = rd.get("name")
            runtime.append((int(pri), idx, name, compiled, kws, mode, value))
        runtime.sort(key=lambda t: (t[0], t[1]))
        return runtime

    def _key_matches_confidence_rule(
        self,
        key_value: str,
        compiled: List[re.Pattern],
        keywords: List[str],
    ) -> bool:
        kv_lower = key_value.lower()
        if any(kw.lower() in kv_lower for kw in keywords):
            return True
        for cre in compiled:
            if cre.search(key_value):
                return True
        return False

    @staticmethod
    def _apply_confidence_modifier_value(
        confidence: float, mode: str, value: float
    ) -> float:
        if mode == "explicit":
            out = value
        else:
            out = confidence + value
        return max(0.0, min(1.0, out))

    def _apply_confidence_match_rules(
        self, keys: List[ExtractedKey], runtime: List[tuple]
    ) -> None:
        if not runtime:
            return
        for key in keys:
            for _pri, _idx, name, compiled, kws, mode, mod_val in runtime:
                if self._key_matches_confidence_rule(key.value, compiled, kws):
                    key.confidence = self._apply_confidence_modifier_value(
                        key.confidence, mode, mod_val
                    )
                    self.logger.verbose(
                        "DEBUG",
                        f"confidence_match_rule {name or _idx!r} applied to key {key.value!r} -> {key.confidence:.4f}",
                    )
                    break

    def _validation_to_metadata_dict(self, validation: Any) -> Dict[str, Any]:
        if validation is None:
            return {}
        if hasattr(validation, "model_dump"):
            return validation.model_dump(mode="python")
        if isinstance(validation, SimpleNamespace):
            d = vars(validation).copy()
            # Serialize nested list of SimpleNamespace if present
            rules = d.get("confidence_match_rules")
            if isinstance(rules, list):
                d["confidence_match_rules"] = [
                    self._confidence_rule_as_dict(r) or r for r in rules
                ]
            return d
        if isinstance(validation, dict):
            return dict(validation)
        return {}

    def _validate_extraction_result(
        self,
        rule: Optional[ExtractionRuleConfig],
        result: ExtractionResult,
        *,
        exclude_self_referencing_keys: Optional[bool] = None,
        validation_override: Optional[Any] = None,
    ) -> ExtractionResult:
        """Apply validation to extraction result (uses rule.validation and config.parameters)."""
        min_key_length = getattr(
            self.config.parameters, "min_key_length", 3
        ) or 3
        result.candidate_keys = [
            k for k in result.candidate_keys if len(k.value) >= min_key_length
        ]
        result.foreign_key_references = [
            k for k in result.foreign_key_references if len(k.value) >= min_key_length
        ]
        result.document_references = [
            k for k in result.document_references if len(k.value) >= min_key_length
        ]
        result.candidate_keys = self._remove_duplicate_keys(result.candidate_keys)
        result.foreign_key_references = self._remove_duplicate_keys(
            result.foreign_key_references
        )
        result.document_references = self._remove_duplicate_keys(
            result.document_references
        )

        if validation_override is not None:
            validation = validation_override
        else:
            validation = self._resolve_validation(rule)
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

        if validation is None:
            validation = SimpleNamespace(
                min_confidence=0.1,
                regexp_match=None,
                confidence_match_rules=[],
            )

        rules_raw = getattr(validation, "confidence_match_rules", None) or []
        runtime = self._build_sorted_confidence_runtime(
            list(rules_raw) if not isinstance(rules_raw, list) else rules_raw
        )
        self._apply_confidence_match_rules(result.candidate_keys, runtime)
        self._apply_confidence_match_rules(result.foreign_key_references, runtime)
        self._apply_confidence_match_rules(result.document_references, runtime)

        min_confidence = getattr(validation, "min_confidence", 0) or 0
        result.candidate_keys = [
            k for k in result.candidate_keys if k.confidence >= min_confidence
        ]
        result.foreign_key_references = [
            k for k in result.foreign_key_references if k.confidence >= min_confidence
        ]
        result.document_references = [
            k for k in result.document_references if k.confidence >= min_confidence
        ]
        rgx_patterns = None
        regexp_match = getattr(validation, "regexp_match", None)
        if isinstance(regexp_match, list):
            rgx_patterns = regexp_match
        elif isinstance(regexp_match, str):
            rgx_patterns = [regexp_match]
        if rgx_patterns:
            result.candidate_keys = [
                k for k in result.candidate_keys
                if any(re.search(p, k.value) for p in rgx_patterns)
            ]
            result.foreign_key_references = [
                k for k in result.foreign_key_references
                if any(re.search(p, k.value) for p in rgx_patterns)
            ]
            result.document_references = [
                k for k in result.document_references
                if any(re.search(p, k.value) for p in rgx_patterns)
            ]
        self._exclude_self_referencing_keys(
            result, source_override=exclude_self_referencing_keys
        )
        validation_config_dict = self._validation_to_metadata_dict(validation)
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

    # Example configuration
    config = {
        "extraction_rules": [
            {
                "name": "standard_pump_tag",
                "description": "Extracts standard pump tags from equipment descriptions",
                "extraction_type": "candidate_key",
                "method": "regex",
                "pattern": r"\bP[-_]?\d{2,4}[A-Z]?\b",
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.7,
                "case_sensitive": False,
                "source_fields": [
                    {
                        "field_name": "name",
                        "field_type": "string",
                        "required": True,
                        "priority": 1,
                    },
                    {
                        "field_name": "description",
                        "field_type": "string",
                        "required": False,
                        "priority": 2,
                    },
                ],
            },
            {
                "name": "flow_instrument_tag",
                "description": "Extracts ISA flow instrument tags",
                "extraction_type": "foreign_key_reference",
                "method": "regex",
                "pattern": r"\bFIC[-_]?\d{4}[A-Z]?\b",
                "priority": 30,
                "enabled": True,
                "min_confidence": 0.8,
                "source_fields": [
                    {
                        "field_name": "description",
                        "field_type": "string",
                        "required": False,
                    }
                ],
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

        result = engine.extract_keys(entity, "asset")

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
