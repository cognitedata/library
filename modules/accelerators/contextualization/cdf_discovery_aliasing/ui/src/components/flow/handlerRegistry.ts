/**
 * Canonical handler ids for palette presets (aligns with Python engines).
 */

import type { MessageKey } from "../../i18n";

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

export type TransformHandlerDefinition = {
  id: DiscoveryTransformHandlerId;
  /** i18n key for human-readable label (palette tree, menus, new node labels). */
  nameKey: MessageKey;
};

export const TRANSFORM_HANDLER_DEFINITIONS: readonly TransformHandlerDefinition[] = [
  { id: "regex_substitution", nameKey: "transforms.handlerName.regex_substitution" },
  { id: "leading_zero_normalize", nameKey: "transforms.handlerName.leading_zero_normalize" },
  { id: "sequential_literal_replace", nameKey: "transforms.handlerName.sequential_literal_replace" },
  { id: "substitution_variants", nameKey: "transforms.handlerName.substitution_variants" },
  { id: "trim_whitespace", nameKey: "transforms.handlerName.trim_whitespace" },
  { id: "change_case", nameKey: "transforms.handlerName.change_case" },
  { id: "coerce_scalar", nameKey: "transforms.handlerName.coerce_scalar" },
  { id: "default_if_empty", nameKey: "transforms.handlerName.default_if_empty" },
  { id: "split_string", nameKey: "transforms.handlerName.split_string" },
  { id: "split_join", nameKey: "transforms.handlerName.split_join" },
  { id: "parse_json_extract", nameKey: "transforms.handlerName.parse_json_extract" },
  { id: "format_datetime", nameKey: "transforms.handlerName.format_datetime" },
  { id: "hash_stable", nameKey: "transforms.handlerName.hash_stable" },
  { id: "mask_string", nameKey: "transforms.handlerName.mask_string" },
  { id: "static_lookup_map", nameKey: "transforms.handlerName.static_lookup_map" },
  { id: "heuristic_sampler", nameKey: "transforms.handlerName.heuristic_sampler" },
] as const;

export const TRANSFORM_HANDLER_IDS = TRANSFORM_HANDLER_DEFINITIONS.map((d) => d.id);
export type TransformHandlerId = DiscoveryTransformHandlerId;

const _DEF_BY_ID = new Map(
  TRANSFORM_HANDLER_DEFINITIONS.map((d) => [d.id, d] as const)
);

export function transformHandlerDefinition(id: string): TransformHandlerDefinition | undefined {
  return _DEF_BY_ID.get(id as DiscoveryTransformHandlerId);
}

export function transformHandlerDisplayName(
  id: DiscoveryTransformHandlerId,
  t: (key: MessageKey, vars?: Record<string, string | number>) => string
): string {
  const def = _DEF_BY_ID.get(id);
  return def ? t(def.nameKey) : id;
}

/** Annotation / validation step presets for the canvas (layout-only until compile phase). */
export const ANNOTATION_KINDS = ["global_validation", "edge_validation"] as const;
export type AnnotationKind = (typeof ANNOTATION_KINDS)[number];
