/**
 * Canonical handler ids for palette presets (aligns with Python engines).
 */

import type { MessageKey } from "../../i18n/types";

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

/** UI / palette grouping for transform handlers (ordered in pickers). */
export type TransformHandlerCategoryId = "core" | "string" | "structure" | "derive";

export const TRANSFORM_HANDLER_CATEGORY_DEFS: {
  readonly id: TransformHandlerCategoryId;
  readonly labelKey: MessageKey;
}[] = [
  { id: "core", labelKey: "transforms.handlerGroup.core" },
  { id: "string", labelKey: "transforms.handlerGroup.string" },
  { id: "structure", labelKey: "transforms.handlerGroup.structure" },
  { id: "derive", labelKey: "transforms.handlerGroup.derive" },
] as const;

export const DISCOVERY_TRANSFORM_HANDLER_IDS = [
  ...CORE_TRANSFORM_HANDLER_IDS,
  ...ELT_TRANSFORM_HANDLER_IDS,
] as const;
export type DiscoveryTransformHandlerId = (typeof DISCOVERY_TRANSFORM_HANDLER_IDS)[number];

export type TransformHandlerDefinition = {
  id: DiscoveryTransformHandlerId;
  /** i18n key for human-readable label (palette tree, menus, new node labels). */
  nameKey: MessageKey;
  category: TransformHandlerCategoryId;
};

export const TRANSFORM_HANDLER_DEFINITIONS: readonly TransformHandlerDefinition[] = [
  {
    id: "regex_substitution",
    nameKey: "transforms.handlerName.regex_substitution",
    category: "string",
  },
  {
    id: "leading_zero_normalize",
    nameKey: "transforms.handlerName.leading_zero_normalize",
    category: "string",
  },
  {
    id: "sequential_literal_replace",
    nameKey: "transforms.handlerName.sequential_literal_replace",
    category: "string",
  },
  {
    id: "substitution_variants",
    nameKey: "transforms.handlerName.substitution_variants",
    category: "string",
  },
  {
    id: "trim_whitespace",
    nameKey: "transforms.handlerName.trim_whitespace",
    category: "string",
  },
  { id: "change_case", nameKey: "transforms.handlerName.change_case", category: "string" },
  {
    id: "default_if_empty",
    nameKey: "transforms.handlerName.default_if_empty",
    category: "string",
  },
  { id: "mask_string", nameKey: "transforms.handlerName.mask_string", category: "string" },
  { id: "split_string", nameKey: "transforms.handlerName.split_string", category: "structure" },
  { id: "split_join", nameKey: "transforms.handlerName.split_join", category: "structure" },
  {
    id: "parse_json_extract",
    nameKey: "transforms.handlerName.parse_json_extract",
    category: "structure",
  },
  { id: "coerce_scalar", nameKey: "transforms.handlerName.coerce_scalar", category: "structure" },
  {
    id: "format_datetime",
    nameKey: "transforms.handlerName.format_datetime",
    category: "derive",
  },
  { id: "hash_stable", nameKey: "transforms.handlerName.hash_stable", category: "derive" },
  {
    id: "static_lookup_map",
    nameKey: "transforms.handlerName.static_lookup_map",
    category: "derive",
  },
  {
    id: "heuristic_sampler",
    nameKey: "transforms.handlerName.heuristic_sampler",
    category: "derive",
  },
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
