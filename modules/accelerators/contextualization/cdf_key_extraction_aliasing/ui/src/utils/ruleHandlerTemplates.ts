import type { MessageKey } from "../i18n/types";

/** Canonical buckets for extraction handler UI (defaults + docs). */
export type DiscoveryHandlerKind = "regex_handler" | "heuristic";

const REGEX_HANDLER_YAML = `# Optional rule-level keys (advanced):
# result_template: "{unit}-{name}"
# max_template_combinations: 10000
`;

const HEURISTIC_YAML = `strategies:
  - id: delimiter_split
    weight: 1.0
  - id: sliding_token
    weight: 0.5
max_candidates_per_field: 20
`;

const EXTRACTION_DEFAULTS: Record<DiscoveryHandlerKind, string> = {
  regex_handler: REGEX_HANDLER_YAML,
  heuristic: HEURISTIC_YAML,
};

const DISCOVERY_DOC: Record<DiscoveryHandlerKind, MessageKey> = {
  regex_handler: "discoveryRules.handlerDoc.regex_handler",
  heuristic: "discoveryRules.handlerDoc.heuristic",
};

/**
 * Map raw handler value (from config or select) to a canonical UI bucket.
 */
export function discoveryHandlerKind(handler: string): DiscoveryHandlerKind {
  const h = handler.trim().toLowerCase().replace(/-/g, "_").replace(/\s+/g, "_");
  if (
    h === "regex_handler" ||
    h === "regexhandler" ||
    h === "field_rule" ||
    h === "fieldrule" ||
    h === "field_rule_fixed_width" ||
    h === "fixed_width" ||
    h === "fixedwidth"
  )
    return "regex_handler";
  if (h === "heuristic") return "heuristic";
  // Legacy → regex_handler (patterns live under fields[].regex)
  return "regex_handler";
}

/** Single canonical `handler` string for the discovery UI and serialized YAML. */
export function canonicalDiscoveryHandlerForUi(handler: string): string {
  const k = discoveryHandlerKind(handler);
  const map: Record<DiscoveryHandlerKind, string> = {
    regex_handler: "regex_handler",
    heuristic: "heuristic",
  };
  return map[k];
}

export function defaultParametersYamlForDiscoveryHandler(handler: string): string {
  return EXTRACTION_DEFAULTS[discoveryHandlerKind(handler)];
}

export function discoveryParametersDocKey(handler: string): MessageKey {
  return DISCOVERY_DOC[discoveryHandlerKind(handler)];
}

/** Aliasing handlers supported by the structured editor (must match TRANSFORMATION_TYPES). */
export type AliasingHandlerId =
  | "character_substitution"
  | "prefix_suffix"
  | "regex_substitution"
  | "case_transformation"
  | "semantic_expansion"
  | "related_instruments"
  | "hierarchical_expansion"
  | "document_aliases"
  | "leading_zero_normalization"
  | "composite"
  | "pattern_recognition"
  | "pattern_based_expansion"
  | "alias_mapping_table";

const ALIASING_DEFAULTS: Record<AliasingHandlerId, string> = {
  character_substitution: `substitutions:
  "_": "-"
`,
  prefix_suffix: `operation: add_prefix
prefix: ""
suffix: ""
`,
  regex_substitution: `patterns:
  - pattern: "^OLD"
    replacement: "NEW"
`,
  case_transformation: `operations:
  - upper
  - lower
`,
  semantic_expansion: `type_mappings:
  P:
    - PUMP
format_templates:
  - "{type}-{tag}"
auto_detect: true
`,
  related_instruments: `applicable_equipment_types:
  - pump
instrument_types: []
`,
  hierarchical_expansion: `hierarchy_levels:
  - level: 1
    format: "{plant}-{area}"
generate_partial_paths: true
`,
  document_aliases: `pid_rules: {}
drawing_rules: {}
`,
  leading_zero_normalization: `min_length: 4
preserve_single_zero: false
`,
  composite: `strategies: []
`,
  pattern_recognition: `patterns: []
`,
  pattern_based_expansion: `similarity_threshold: 0.8
`,
  alias_mapping_table: `raw_table: {}
source_match: exact
`,
};

const ALIASING_DOC: Record<AliasingHandlerId, MessageKey> = {
  character_substitution: "aliasingRules.handlerDoc.character_substitution",
  prefix_suffix: "aliasingRules.handlerDoc.prefix_suffix",
  regex_substitution: "aliasingRules.handlerDoc.regex_substitution",
  case_transformation: "aliasingRules.handlerDoc.case_transformation",
  semantic_expansion: "aliasingRules.handlerDoc.semantic_expansion",
  related_instruments: "aliasingRules.handlerDoc.related_instruments",
  hierarchical_expansion: "aliasingRules.handlerDoc.hierarchical_expansion",
  document_aliases: "aliasingRules.handlerDoc.document_aliases",
  leading_zero_normalization: "aliasingRules.handlerDoc.leading_zero_normalization",
  composite: "aliasingRules.handlerDoc.composite",
  pattern_recognition: "aliasingRules.handlerDoc.pattern_recognition",
  pattern_based_expansion: "aliasingRules.handlerDoc.pattern_based_expansion",
  alias_mapping_table: "aliasingRules.handlerDoc.alias_mapping_table",
};

export function isAliasingHandlerId(h: string): h is AliasingHandlerId {
  return Object.prototype.hasOwnProperty.call(ALIASING_DEFAULTS, h);
}

export function defaultConfigYamlForAliasingHandler(handler: string): string {
  if (isAliasingHandlerId(handler)) return ALIASING_DEFAULTS[handler];
  return "{}\n";
}

export function aliasingConfigDocKey(handler: string): MessageKey {
  if (isAliasingHandlerId(handler)) return ALIASING_DOC[handler];
  return "aliasingRules.handlerDoc.generic";
}

/** Handlers with structured config sub-panels in the aliasing rules editor. */
export type AliasingStructuredId =
  | "character_substitution"
  | "prefix_suffix"
  | "regex_substitution"
  | "semantic_expansion"
  | "case_transformation"
  | "leading_zero_normalization"
  | "hierarchical_expansion"
  | "alias_mapping_table";

const ALIASING_STRUCTURED_SET = new Set<AliasingStructuredId>([
  "character_substitution",
  "prefix_suffix",
  "regex_substitution",
  "semantic_expansion",
  "case_transformation",
  "leading_zero_normalization",
  "hierarchical_expansion",
  "alias_mapping_table",
]);

export function aliasingStructuredKind(handler: string): AliasingStructuredId | null {
  return ALIASING_STRUCTURED_SET.has(handler as AliasingStructuredId)
    ? (handler as AliasingStructuredId)
    : null;
}
