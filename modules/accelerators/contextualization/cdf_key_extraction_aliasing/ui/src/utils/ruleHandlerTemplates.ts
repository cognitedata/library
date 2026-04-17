import type { MessageKey } from "../i18n/types";

/** Canonical buckets for extraction handler UI (defaults + docs). */
export type DiscoveryHandlerKind = "passthrough" | "regex" | "fixedWidth" | "tokenReassembly" | "heuristic";

const PASSTHROUGH_YAML = `min_confidence: 1.0
`;

const REGEX_YAML = `pattern: ""
max_matches_per_field: 10
regex_options:
  ignore_case: false
  multiline: false
  dotall: false
  unicode: true
early_termination: false
`;

const FIXED_WIDTH_YAML = `field_definitions:
  - name: field1
    start_position: 0
    end_position: 8
    field_type: string
    required: true
    trim: true
encoding: utf-8
`;

const TOKEN_REASSEMBLY_YAML = `tokenization:
  separator_patterns:
    - "-"
    - "_"
    - "/"
    - " "
  token_patterns: []
assembly_rules:
  - format: "{site}-{unit}"
    conditions: {}
`;

const HEURISTIC_YAML = `heuristic_strategies:
  - method: positional_detection
    name: primary_segments
    config: {}
scoring:
  min_confidence: 0.7
`;

const EXTRACTION_DEFAULTS: Record<DiscoveryHandlerKind, string> = {
  passthrough: PASSTHROUGH_YAML,
  regex: REGEX_YAML,
  fixedWidth: FIXED_WIDTH_YAML,
  tokenReassembly: TOKEN_REASSEMBLY_YAML,
  heuristic: HEURISTIC_YAML,
};

const DISCOVERY_DOC: Record<DiscoveryHandlerKind, MessageKey> = {
  passthrough: "discoveryRules.handlerDoc.passthrough",
  regex: "discoveryRules.handlerDoc.regex",
  fixedWidth: "discoveryRules.handlerDoc.fixedWidth",
  tokenReassembly: "discoveryRules.handlerDoc.tokenReassembly",
  heuristic: "discoveryRules.handlerDoc.heuristic",
};

/**
 * Map raw handler value (from config or select) to a canonical UI bucket.
 */
export function discoveryHandlerKind(handler: string): DiscoveryHandlerKind {
  const h = handler.trim().toLowerCase().replace(/_/g, " ");
  if (h === "passthrough") return "passthrough";
  if (h === "fixed width") return "fixedWidth";
  if (h === "token reassembly") return "tokenReassembly";
  if (h === "heuristic") return "heuristic";
  return "regex";
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
