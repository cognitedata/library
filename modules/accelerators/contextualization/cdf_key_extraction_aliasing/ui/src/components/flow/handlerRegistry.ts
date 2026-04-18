/**
 * Canonical handler ids for palette presets (aligns with Python engines).
 */

export const EXTRACTION_HANDLER_IDS = ["regex_handler", "heuristic"] as const;
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

/** Annotation / validation step presets for the canvas (layout-only until compile phase). */
export const ANNOTATION_KINDS = ["global_validation", "edge_validation"] as const;
export type AnnotationKind = (typeof ANNOTATION_KINDS)[number];
