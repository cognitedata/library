"""
Tag Aliasing Engine for Cognite Data Fusion (CDF)

This module implements a comprehensive tag aliasing system that generates multiple
alternative representations of asset tags, equipment identifiers, and document names
to improve entity matching and contextualization accuracy.

Features:
- 13+ transformation types (including alias_mapping_table from Cognite RAW, character substitution, prefix/suffix, regex, case, equipment expansion, related instruments, hierarchical expansion, document aliases, leading zero normalization, pattern recognition, pattern-based expansion, composite)
- Support for related instrument tag generation
- Semantic expansion (equipment letter codes) for semantic matching
- Hierarchical tag expansion
- Document-specific aliasing
- Composite transformations

Author: Darren Downtain
Version: 1.0.0
"""

import re
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union

import yaml

from cdf_fn_common.confidence_match_eval import (
    _parse_group_node,
    _sort_concurrent_children,
    apply_confidence_match_rules_to_float_scores,
    normalize_confidence_match_step,
)
from cdf_fn_common.confidence_match_rule_refs import merge_validation_dict_overlay
from cdf_fn_common.pipeline_io import (
    normalize_pipeline_input,
    parse_rule_pipeline_io,
)
from ..common.logger import CogniteFunctionLogger
from .transformer_utils import (
    STANDARD_TAG_PATTERN,
    PatternMatchMixin,
    extract_equipment_number,
    extract_tag_structure,
    generate_separator_variants,
)

# Import pattern library components
try:
    from .tag_pattern_library import (
        DocumentPatternRegistry,
        DocumentType,
        EquipmentType,
        InstrumentType,
        PatternValidator,
        StandardTagPatternRegistry,
    )

    PATTERN_LIBRARY_AVAILABLE = True
except ImportError:
    PATTERN_LIBRARY_AVAILABLE = False
    # Logger will be initialized in __init__, use print for now
    print(
        "[WARNING] Pattern library not available. Pattern-based transformations disabled."
    )

# Import handlers
from .handlers import (
    AliasTransformerHandler,
    CaseTransformationHandler,
    CharacterSubstitutionHandler,
    DocumentAliasesHandler,
    SemanticExpansionHandler,
    HierarchicalExpansionHandler,
    LeadingZeroNormalizationHandler,
    PatternBasedExpansionHandler,
    PatternRecognitionHandler,
    PrefixSuffixHandler,
    RegexSubstitutionHandler,
    RelatedInstrumentsHandler,
    AliasMappingTableHandler,
)


class TransformationType(Enum):
    """Enumeration of available aliasing transformation types."""

    CHARACTER_SUBSTITUTION = "character_substitution"
    PREFIX_SUFFIX = "prefix_suffix"
    REGEX_SUBSTITUTION = "regex_substitution"
    CASE_TRANSFORMATION = "case_transformation"
    SEMANTIC_EXPANSION = "semantic_expansion"
    RELATED_INSTRUMENTS = "related_instruments"
    HIERARCHICAL_EXPANSION = "hierarchical_expansion"
    DOCUMENT_ALIASES = "document_aliases"
    LEADING_ZERO_NORMALIZATION = "leading_zero_normalization"
    COMPOSITE = "composite"
    PATTERN_RECOGNITION = "pattern_recognition"
    PATTERN_BASED_EXPANSION = "pattern_based_expansion"
    ALIAS_MAPPING_TABLE = "alias_mapping_table"


@dataclass
class AliasRule:
    """Configuration for an individual aliasing rule."""

    name: str
    handler: TransformationType
    enabled: bool = True
    priority: int = 50
    preserve_original: bool = True
    #: ``cumulative`` = feed full working set; ``previous`` = feed only last handler output.
    pipeline_input: str = "cumulative"
    #: ``merge`` / ``replace`` — informational; merged into ``preserve_original`` at parse time.
    pipeline_output: str = "merge"
    config: Dict[str, Any] = field(default_factory=dict)
    scope_filters: Dict[str, Any] = field(default_factory=dict)
    conditions: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    #: Optional ``validation`` block merged onto global ``data.validation`` when this rule runs (same semantics as extraction).
    validation: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AliasingResult:
    """Result of aliasing operation."""

    original_tag: str
    aliases: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SequentialPathwayStep:
    """Ordered rules applied to the current alias set (same semantics as legacy flat rules)."""

    rules: List[AliasRule]


@dataclass
class ParallelPathwayStep:
    """Independent sequential branches, each starting from the alias set at the fork."""

    branches: List[List[AliasRule]]


PathwayStep = Union[SequentialPathwayStep, ParallelPathwayStep]


# Transformer classes have been moved to handlers module
# All transformer functionality is now in separate handler files under engine/handlers/


class AliasingEngine:
    """Main engine for generating tag aliases."""

    def __init__(
        self,
        config: Dict[str, Any],
        logger: Optional[CogniteFunctionLogger] = None,
        client: Optional[Any] = None,
    ):
        """Initialize the aliasing engine with configuration."""
        self.config = config
        self.logger = logger or CogniteFunctionLogger("INFO", True)
        self.client = client
        self.pathway_steps: Optional[List[PathwayStep]] = None
        self._pathways_config_active = self._config_has_pathways(config)
        if self._pathways_config_active:
            if config.get("rules"):
                self.logger.info(
                    "Aliasing: `pathways` is set; ignoring legacy flat `rules` for execution"
                )
            self.pathway_steps = self._load_pathways(config.get("pathways") or {})
            self.rules = self._flatten_pathway_rules(self.pathway_steps)
        else:
            self.rules = self._load_rules()
        self.transformers = self._initialize_transformers()
        self.validation_config = config.get("validation", {})
        raw_ep = config.get("extraction_aliasing_pipelines")
        self._extraction_aliasing_pipelines: Dict[str, List[Any]] = (
            dict(raw_ep) if isinstance(raw_ep, dict) else {}
        )
        self._rule_by_name: Dict[str, AliasRule] = {}
        for r in self.rules:
            if r.name not in self._rule_by_name:
                self._rule_by_name[r.name] = r
        self._index_rules_from_extraction_pipelines()
        self._hydrate_alias_mapping_table_rules()

    @staticmethod
    def _config_has_pathways(config: Dict[str, Any]) -> bool:
        pw = config.get("pathways")
        if not isinstance(pw, dict):
            return False
        steps = pw.get("steps")
        return isinstance(steps, list) and len(steps) > 0

    def _flatten_pathway_rules(self, steps: List[PathwayStep]) -> List[AliasRule]:
        out: List[AliasRule] = []
        for step in steps:
            if isinstance(step, SequentialPathwayStep):
                out.extend(step.rules)
            elif isinstance(step, ParallelPathwayStep):
                for branch in step.branches:
                    out.extend(branch)
        return out

    def _walk_pipeline_node_leaves(self, node: Any) -> List[Dict[str, Any]]:
        """Collect leaf transformation dicts from an ``aliasing_pipeline`` subtree."""
        node = normalize_confidence_match_step(node)
        grouped = _parse_group_node(node)
        if grouped is not None:
            _mode, children = grouped
            acc: List[Dict[str, Any]] = []
            for c in children:
                acc.extend(self._walk_pipeline_node_leaves(c))
            return acc
        if isinstance(node, dict) and (node.get("handler") or node.get("type")):
            return [node]
        return []

    def _index_rules_from_extraction_pipelines(self) -> None:
        """Register pipeline leaf rules on ``_rule_by_name`` for validation overlay lookup."""
        for nodes in self._extraction_aliasing_pipelines.values():
            if not isinstance(nodes, list):
                continue
            for n in nodes:
                for leaf in self._walk_pipeline_node_leaves(n):
                    ar = self._parse_rule_dict(leaf)
                    if ar and ar.name not in self._rule_by_name:
                        self._rule_by_name[ar.name] = ar

    def _exec_pipeline_node(
        self,
        node: Any,
        aliases: Set[str],
        entity_type: Optional[str],
        context: Optional[Dict[str, Any]],
        applied_rules: List[str],
        applied_rules_set: Set[str],
    ) -> Set[str]:
        """Execute one pipeline node: ``hierarchy`` ordered|concurrent or a single transform leaf."""
        node = normalize_confidence_match_step(node)
        grouped = _parse_group_node(node)
        if grouped is not None:
            mode, children = grouped
            if mode == "concurrent":
                merged: Set[str] = set()
                fork = set(aliases)
                for child in _sort_concurrent_children(children):
                    merged.update(
                        self._exec_pipeline_node(
                            child,
                            fork,
                            entity_type,
                            context,
                            applied_rules,
                            applied_rules_set,
                        )
                    )
                return merged
            cur = set(aliases)
            for child in children:
                cur = self._exec_pipeline_node(
                    child,
                    cur,
                    entity_type,
                    context,
                    applied_rules,
                    applied_rules_set,
                )
            return cur
        if isinstance(node, dict) and (node.get("handler") or node.get("type")):
            pr = self._parse_rule_dict(node)
            if not pr:
                return set(aliases)
            return self._apply_rules_sequential(
                [pr],
                aliases,
                entity_type,
                context,
                applied_rules,
                applied_rules_set,
            )
        return set(aliases)

    def _run_extraction_aliasing_pipeline(
        self,
        nodes: List[Any],
        tag: str,
        entity_type: Optional[str],
        context: Optional[Dict[str, Any]],
        applied_rules: List[str],
        applied_rules_set: Set[str],
    ) -> Set[str]:
        """Top-level pipeline: sequential chain (each item feeds the next)."""
        cur: Set[str] = {tag}
        for item in nodes:
            cur = self._exec_pipeline_node(
                item,
                cur,
                entity_type,
                context,
                applied_rules,
                applied_rules_set,
            )
        return cur

    def _parse_rule_dict(self, rule_config: Dict[str, Any]) -> Optional[AliasRule]:
        """Parse a single rule mapping into AliasRule (list order preserved; no priority sort)."""
        try:
            handler_key = rule_config.get("handler") or rule_config.get("type")
            if not handler_key:
                self.logger.error(
                    f"Invalid rule configuration (missing handler/type): {rule_config!r}"
                )
                return None
            pin, pout, preserve = parse_rule_pipeline_io(rule_config)
            val = rule_config.get("validation")
            validation = val if isinstance(val, dict) else {}
            rname = (
                rule_config.get("name")
                or rule_config.get("rule_id")
                or "unnamed_aliasing_rule"
            )
            return AliasRule(
                name=str(rname),
                handler=TransformationType(handler_key),
                enabled=rule_config.get("enabled", True),
                priority=rule_config.get("priority", 50),
                preserve_original=preserve,
                pipeline_input=pin,
                pipeline_output=pout,
                config=rule_config.get("config", {}),
                scope_filters=rule_config.get("scope_filters", {}),
                conditions=rule_config.get("conditions", {}),
                description=rule_config.get("description", ""),
                validation=validation,
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.error(f"Invalid rule configuration: {e}")
            return None

    def _parse_rule_dict_list(self, rules_config: List[Dict[str, Any]]) -> List[AliasRule]:
        rules: List[AliasRule] = []
        for rule_config in rules_config:
            if not isinstance(rule_config, dict):
                self.logger.error(f"Skipping non-dict aliasing rule: {rule_config!r}")
                continue
            r = self._parse_rule_dict(rule_config)
            if r:
                rules.append(r)
        return rules

    def _load_pathways(self, pathways_cfg: Dict[str, Any]) -> List[PathwayStep]:
        """Build sequential/parallel steps from ``pathways.steps`` (see module docs)."""
        raw_steps = pathways_cfg.get("steps")
        if not isinstance(raw_steps, list):
            self.logger.error("pathways.steps must be a list; using empty pipeline")
            return []
        out: List[PathwayStep] = []
        for i, step in enumerate(raw_steps):
            if not isinstance(step, dict):
                self.logger.error(f"pathways.steps[{i}] must be a mapping; skipping")
                continue
            mode = str(step.get("mode") or "sequential").strip().lower()
            if mode == "sequential":
                rules_raw = step.get("rules")
                if not isinstance(rules_raw, list):
                    self.logger.error(
                        f"pathways.steps[{i}] sequential requires `rules` list; skipping"
                    )
                    continue
                rules = self._parse_rule_dict_list(rules_raw)
                if rules:
                    out.append(SequentialPathwayStep(rules=rules))
            elif mode == "parallel":
                branches_raw = step.get("branches")
                if not isinstance(branches_raw, list) or not branches_raw:
                    self.logger.error(
                        f"pathways.steps[{i}] parallel requires non-empty `branches`; skipping"
                    )
                    continue
                branches: List[List[AliasRule]] = []
                for j, br in enumerate(branches_raw):
                    rules_raw: List[Dict[str, Any]] = []
                    if isinstance(br, dict):
                        rr = br.get("rules")
                        if isinstance(rr, list):
                            rules_raw = rr
                    elif isinstance(br, list):
                        rules_raw = br
                    else:
                        self.logger.error(
                            f"pathways.steps[{i}].branches[{j}] must be a mapping "
                            f"with `rules` or a rules list; skipping branch"
                        )
                        continue
                    parsed = self._parse_rule_dict_list(rules_raw)
                    if parsed:
                        branches.append(parsed)
                if branches:
                    out.append(ParallelPathwayStep(branches=branches))
            else:
                self.logger.error(
                    f"pathways.steps[{i}] has unknown mode {mode!r}; "
                    f"expected 'sequential' or 'parallel'"
                )
        if not out:
            self.logger.warning("pathways produced no executable steps; aliasing is a no-op")
        return out

    def _hydrate_alias_mapping_table_rules(self) -> None:
        """Load RAW mapping rows for alias_mapping_table rules (or use injected resolved_rows)."""
        from .alias_mapping_table_raw_loader import load_alias_mapping_table_from_client

        for rule in self._rule_by_name.values():
            if rule.handler != TransformationType.ALIAS_MAPPING_TABLE:
                continue
            cfg = rule.config
            default_sm = str(cfg.get("source_match") or "exact").strip().lower()
            if default_sm not in ("exact", "glob", "regex"):
                self.logger.error(
                    f"Rule {rule.name}: invalid source_match default {default_sm!r}; disabling"
                )
                rule.enabled = False
                continue

            injected = cfg.get("resolved_rows")
            if isinstance(injected, list) and len(injected) > 0:
                continue

            raw_table = cfg.get("raw_table")
            if not raw_table or not isinstance(raw_table, dict):
                self.logger.error(
                    f"Rule {rule.name}: alias_mapping_table requires non-empty resolved_rows "
                    f"or raw_table; disabling"
                )
                rule.enabled = False
                continue

            if self.client is None:
                self.logger.error(
                    f"Rule {rule.name}: Cognite client required to load alias_mapping_table "
                    f"from RAW; disabling"
                )
                rule.enabled = False
                continue

            rows, errs = load_alias_mapping_table_from_client(
                self.client,
                raw_table,
                rule_default_source_match=default_sm,
                case_insensitive=bool(cfg.get("case_insensitive", False)),
            )
            for msg in errs:
                self.logger.warning(msg)
            if not rows:
                self.logger.warning(
                    f"Rule {rule.name}: no mapping rows loaded from RAW; disabling"
                )
                rule.enabled = False
                continue
            cfg["resolved_rows"] = rows

    def _load_rules(self) -> List[AliasRule]:
        """Load and parse flat ``rules``; execution order is ascending ``priority``."""
        rules_config = self.config.get("rules", [])
        if not isinstance(rules_config, list):
            self.logger.error("config.rules must be a list")
            return []
        parsed = self._parse_rule_dict_list(rules_config)
        return sorted(parsed, key=lambda r: r.priority)

    def _record_applied_rule(
        self,
        rule: AliasRule,
        applied_rules: List[str],
        applied_rules_set: Set[str],
    ) -> None:
        if rule.name not in applied_rules_set:
            applied_rules.append(rule.name)
            applied_rules_set.add(rule.name)

    def _apply_rules_sequential(
        self,
        rules: List[AliasRule],
        aliases: Set[str],
        entity_type: Optional[str],
        context: Optional[Dict[str, Any]],
        applied_rules: List[str],
        applied_rules_set: Set[str],
    ) -> Set[str]:
        """Apply an ordered rule list to an alias set (legacy semantics per rule)."""
        initial = set(aliases)
        out = set(initial)
        prev_transform_output: Optional[Set[str]] = None

        for rule in rules:
            if not rule.enabled:
                continue
            if not self._check_conditions(rule, entity_type, context):
                continue
            transformer = self.transformers.get(rule.handler)
            if not transformer:
                self.logger.verbose(
                    "WARNING", f"No transformer for rule handler {rule.handler}"
                )
                continue
            pin = normalize_pipeline_input(getattr(rule, "pipeline_input", None))
            try:
                if pin == "previous":
                    aliases_in = (
                        set(prev_transform_output)
                        if prev_transform_output is not None
                        else set(initial)
                    )
                else:
                    aliases_in = set(out)
                new_aliases = transformer.transform(
                    aliases_in, rule.config, context
                )
                prev_transform_output = set(new_aliases)
                if rule.preserve_original:
                    out.update(new_aliases)
                else:
                    out = set(new_aliases)
                self._record_applied_rule(rule, applied_rules, applied_rules_set)
            except Exception as e:
                self.logger.verbose("ERROR", f"Error applying rule {rule.name}: {e}")
        return out

    def _initialize_transformers(
        self,
    ) -> Dict[TransformationType, AliasTransformerHandler]:
        """Initialize transformer handler instances."""
        transformers = {
            TransformationType.CHARACTER_SUBSTITUTION: CharacterSubstitutionHandler(
                self.logger
            ),
            TransformationType.PREFIX_SUFFIX: PrefixSuffixHandler(self.logger),
            TransformationType.REGEX_SUBSTITUTION: RegexSubstitutionHandler(
                self.logger
            ),
            TransformationType.CASE_TRANSFORMATION: CaseTransformationHandler(
                self.logger
            ),
            TransformationType.LEADING_ZERO_NORMALIZATION: LeadingZeroNormalizationHandler(
                self.logger
            ),
            TransformationType.SEMANTIC_EXPANSION: SemanticExpansionHandler(
                self.logger
            ),
            TransformationType.RELATED_INSTRUMENTS: RelatedInstrumentsHandler(
                self.logger
            ),
            TransformationType.HIERARCHICAL_EXPANSION: HierarchicalExpansionHandler(
                self.logger
            ),
            TransformationType.DOCUMENT_ALIASES: DocumentAliasesHandler(self.logger),
            TransformationType.ALIAS_MAPPING_TABLE: AliasMappingTableHandler(
                self.logger
            ),
        }

        # Add pattern-based transformers if pattern library is available
        if PATTERN_LIBRARY_AVAILABLE:
            transformers.update(
                {
                    TransformationType.PATTERN_RECOGNITION: PatternRecognitionHandler(
                        self.logger
                    ),
                    TransformationType.PATTERN_BASED_EXPANSION: PatternBasedExpansionHandler(
                        self.logger
                    ),
                }
            )

        return transformers

    def generate_aliases(
        self, tag: str, entity_type: str = None, context: Dict[str, Any] = None
    ) -> AliasingResult:
        """
        Generate all aliases for a given tag.

        Args:
            tag: Base tag to generate aliases for
            entity_type: Type of entity (asset, file, etc.)
            context: Additional context (site, unit, equipment_type, etc.)

        Returns:
            AliasingResult with generated aliases and metadata
        """
        aliases = {tag}  # Start with original
        applied_rules: List[str] = []
        applied_rules_set: Set[str] = set()
        ctx = context or {}
        ext_key = (
            ctx.get("extraction_rule_name")
            or ctx.get("rule_name")
            or ctx.get("extraction_rule_id")
        )

        if self._extraction_aliasing_pipelines:
            if ext_key:
                rid = str(ext_key).strip()
                nodes = self._extraction_aliasing_pipelines.get(rid)
                if isinstance(nodes, list):
                    aliases = self._run_extraction_aliasing_pipeline(
                        nodes,
                        tag,
                        entity_type,
                        context,
                        applied_rules,
                        applied_rules_set,
                    )
                else:
                    self.logger.warning(
                        "Aliasing: no aliasing_pipeline for extraction rule %r; identity only",
                        rid,
                    )
            else:
                self.logger.warning(
                    "Aliasing: extraction_aliasing_pipelines configured but context missing "
                    "extraction_rule_name / rule_name; identity only"
                )
        elif self._pathways_config_active:
            for step in self.pathway_steps or []:
                if isinstance(step, SequentialPathwayStep):
                    aliases = self._apply_rules_sequential(
                        step.rules,
                        aliases,
                        entity_type,
                        context,
                        applied_rules,
                        applied_rules_set,
                    )
                elif isinstance(step, ParallelPathwayStep):
                    fork = set(aliases)
                    merged = set(fork)
                    for branch in step.branches:
                        branch_out = self._apply_rules_sequential(
                            branch,
                            fork,
                            entity_type,
                            context,
                            applied_rules,
                            applied_rules_set,
                        )
                        merged.update(branch_out)
                    aliases = merged
        else:
            aliases = self._apply_rules_sequential(
                self.rules,
                aliases,
                entity_type,
                context,
                applied_rules,
                applied_rules_set,
            )

        validated_aliases = self._validate_aliases(
            list(aliases), self._effective_validation(applied_rules)
        )

        return AliasingResult(
            original_tag=tag,
            aliases=validated_aliases,
            metadata={
                "applied_rules": applied_rules,
                "total_aliases": len(validated_aliases),
                "entity_type": entity_type,
                "context": context,
            },
        )

    def _check_conditions(
        self, rule: AliasRule, entity_type: str, context: Dict[str, Any]
    ) -> bool:
        """Check optional non-entity_type scope_filters / conditions against *context*."""
        if rule.scope_filters:
            if context:
                for key, expected_value in rule.scope_filters.items():
                    if key == "entity_type":
                        continue
                    actual_value = context.get(key)
                    if isinstance(expected_value, list):
                        if actual_value not in expected_value:
                            return False
                    else:
                        if actual_value != expected_value:
                            return False

        if rule.conditions:
            if context:
                for key, expected_value in rule.conditions.items():
                    if key == "entity_type":
                        continue

                    actual_value = context.get(key)
                    if isinstance(expected_value, list):
                        if actual_value not in expected_value:
                            return False
                    else:
                        if actual_value != expected_value:
                            return False

        return True

    def _effective_validation(self, applied_rule_names: List[str]) -> Dict[str, Any]:
        """Global ``data.validation`` merged with each applied rule's ``validation`` (order = first rule first)."""
        base: Dict[str, Any] = (
            dict(self.validation_config) if isinstance(self.validation_config, dict) else {}
        )
        for nm in applied_rule_names:
            rule = self._rule_by_name.get(nm)
            if not rule:
                continue
            ov = getattr(rule, "validation", None)
            if isinstance(ov, dict) and ov:
                base = merge_validation_dict_overlay(base, ov)
        return base

    def _validate_aliases(self, aliases: List[str], validation: Dict[str, Any]) -> List[str]:
        """Apply confidence_match_rules, min_confidence, dedupe, and max_aliases_per_tag."""
        max_aliases = int(validation.get("max_aliases_per_tag", 50) or 50)
        min_confidence = float(validation.get("min_confidence", 0.0) or 0.0)
        rules_raw = validation.get("confidence_match_rules") or []

        scored = [(str(a), 1.0) for a in aliases if a]
        scored = apply_confidence_match_rules_to_float_scores(
            scored,
            rules_raw=list(rules_raw) if isinstance(rules_raw, list) else list(rules_raw),
            default_expression_match=validation,
            log_warning=self.logger.warning,
            log_verbose=self.logger.verbose,
        )
        validated = [v for v, c in scored if c >= min_confidence]

        seen: Set[str] = set()
        validated = [x for x in validated if not (x in seen or seen.add(x))]

        if len(validated) > max_aliases:
            validated = sorted(validated)[:max_aliases]

        return validated


def load_config_from_yaml(file_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    from ..common.config_utils import load_config_from_yaml as _load_config_from_yaml

    return _load_config_from_yaml(file_path)


def main():
    """Example usage of the AliasingEngine."""
    import copy

    from cdf_adapter import _DEFAULT_ALIASING_VALIDATION

    # Example configuration
    config = {
        "rules": [
            {
                "name": "normalize_separators",
                "type": "character_substitution",
                "enabled": True,
                "priority": 10,
                "preserve_original": True,
                "config": {"substitutions": {"_": "-", " ": "-"}},
            },
            {
                "name": "generate_separator_variants",
                "type": "character_substitution",
                "enabled": True,
                "priority": 15,
                "preserve_original": True,
                "config": {
                    "substitutions": {"-": ["_", " ", ""]},
                    "cascade_substitutions": False,
                    "max_aliases_per_input": 20,
                },
            },
            {
                "name": "add_site_prefix",
                "type": "prefix_suffix",
                "enabled": True,
                "priority": 20,
                "preserve_original": True,
                "config": {
                    "operation": "add_prefix",
                    "context_mapping": {
                        "Plant_A": {"prefix": "PA-"},
                        "Plant_B": {"prefix": "PB-"},
                    },
                    "resolve_from": "site",
                    "conditions": {"missing_prefix": True},
                },
            },
            {
                "name": "semantic_expansion",
                "type": "semantic_expansion",
                "enabled": True,
                "priority": 30,
                "preserve_original": True,
                "config": {
                    "type_mappings": {
                        "P": ["PUMP", "PMP"],
                        "V": ["VALVE", "VLV"],
                        "T": ["TANK", "TNK"],
                    },
                    "format_templates": ["{type}-{tag}", "{type}_{tag}"],
                    "auto_detect": True,
                },
            },
            {
                "name": "generate_instruments",
                "type": "related_instruments",
                "enabled": True,
                "priority": 40,
                "preserve_original": True,
                "config": {
                    "applicable_equipment_types": ["pump", "compressor", "tank"],
                    "instrument_types": [
                        {"prefix": "FIC", "applicable_to": ["pump", "compressor"]},
                        {
                            "prefix": "PI",
                            "applicable_to": ["pump", "compressor", "tank"],
                        },
                        {"prefix": "TI", "applicable_to": ["pump"]},
                        {"prefix": "LIC", "applicable_to": ["tank"]},
                    ],
                    "format_rules": {"separator": "-", "case": "upper"},
                },
            },
        ],
        "validation": {
            **copy.deepcopy(_DEFAULT_ALIASING_VALIDATION),
            "max_aliases_per_tag": 30,
        },
    }

    # Initialize engine with default logger
    default_logger = CogniteFunctionLogger("INFO", True)
    engine = AliasingEngine(config, default_logger)

    # Test cases
    test_cases = [
        {
            "tag": "P_101",
            "entity_type": "asset",
            "context": {"site": "Plant_A", "equipment_type": "pump"},
        },
        {
            "tag": "FCV-2001-A",
            "entity_type": "asset",
            "context": {"site": "Plant_B", "equipment_type": "valve"},
        },
        {
            "tag": "T-201",
            "entity_type": "asset",
            "context": {"site": "Plant_A", "equipment_type": "tank"},
        },
    ]

    print("Tag Aliasing Engine - Test Results")
    print("=" * 50)

    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest Case {i}:")
        print(f"Input: {test_case['tag']}")
        print(f"Type: {test_case['entity_type']}")
        print(f"Context: {test_case['context']}")

        result = engine.generate_aliases(**test_case)

        print(f"Generated {len(result.aliases)} aliases:")
        for alias in sorted(result.aliases):
            print(f"  - {alias}")

        print(f"Applied rules: {result.metadata['applied_rules']}")


if __name__ == "__main__":
    main()
