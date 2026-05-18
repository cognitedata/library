/**
 * Canonical handler ids for palette presets (aligns with Python engines).
 */

export const EXTRACTION_HANDLER_IDS = ["regex_handler"] as const;
export type ExtractionHandlerId = (typeof EXTRACTION_HANDLER_IDS)[number];

/** Aliasing TransformationType string values (tag_aliasing_engine.TransformationType). */
export const ALIASING_HANDLER_IDS = [
  "character_substitution",
  "prefix_suffix",
  "regex_substitution",
  "case_transformation",
  "semantic_expansion",
  "related_instruments",
  "hierarchical_expansion",
  "document_aliases",
  "leading_zero_normalization",
  "composite",
  "pattern_recognition",
  "pattern_based_expansion",
  "alias_mapping_table",
] as const;
export type AliasingHandlerId = (typeof ALIASING_HANDLER_IDS)[number];

/** Core discovery transform handlers (tag normalization / fan-out). */
export const CORE_TRANSFORM_HANDLER_IDS = [
  "regex_substitution",
  "leading_zero_normalize",
  "sequential_literal_replace",
  "substitution_variants",
] as const;
export type CoreTransformHandlerId = (typeof CORE_TRANSFORM_HANDLER_IDS)[number];

/** ELT catalog handlers (string/scalar/light struct transforms). */
export const ELT_TRANSFORM_HANDLER_IDS = [
  "trim_whitespace",
  "change_case",
  "coerce_scalar",
  "default_if_empty",
  "split_string",
  "split_join",
  "parse_json_extract",
  "format_datetime",
  "hash_stable",
  "mask_string",
  "static_lookup_map",
  "heuristic_sampler",
] as const;
export type EltTransformHandlerId = (typeof ELT_TRANSFORM_HANDLER_IDS)[number];

export const DISCOVERY_TRANSFORM_HANDLER_IDS = [
  ...CORE_TRANSFORM_HANDLER_IDS,
  ...ELT_TRANSFORM_HANDLER_IDS,
] as const;
export type DiscoveryTransformHandlerId = (typeof DISCOVERY_TRANSFORM_HANDLER_IDS)[number];

export const TRANSFORM_HANDLER_IDS = DISCOVERY_TRANSFORM_HANDLER_IDS;
export type TransformHandlerId = DiscoveryTransformHandlerId;

/** Annotation / validation step presets for the canvas (layout-only until compile phase). */
export const ANNOTATION_KINDS = ["global_validation", "edge_validation"] as const;
export type AnnotationKind = (typeof ANNOTATION_KINDS)[number];
