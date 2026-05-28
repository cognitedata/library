import type { MessageKey } from "../i18n/types";
import type { DiscoveryTransformHandlerId as EtlTransformHandlerId } from "../components/transform/etlHandlerRegistry";
import { transformHandlerDescription } from "./transformHandlerCatalog";

export type TransformFieldRow = Record<string, unknown>;

const TRANSFORM_DEFAULTS: Record<EtlTransformHandlerId, Record<string, unknown>> = {
  regex_substitution: { patterns: [{ pattern: "^OLD", replacement: "NEW" }] },
  leading_zero_normalize: { minimum_width: 0 },
  sequential_literal_replace: { replacements: [{ from: "OLD", to: "NEW" }] },
  substitution_variants: { match_literal: "FIC", variants: ["FIC", "FI", "FT", "FC"] },
  trim_whitespace: { mode: "ends_only" },
  change_case: { case: "lower" },
  coerce_scalar: { type: "int", empty_as_null: true, strict: false },
  default_if_empty: { literal: "", field: "" },
  split_string: { delimiter: ",", trim: true, max_splits: -1 },
  split_join: { delimiter: "-", trim: true, max_splits: -1, template: "{0}-{1}" },
  parse_json_extract: { path: "meta.id" },
  format_datetime: { input_format: "", output_format: "%Y-%m-%dT%H:%M:%SZ" },
  hash_stable: { algorithm: "sha256", salt: "" },
  mask_string: { keep_last: 4, mask_char: "*" },
  static_lookup_map: { map: {} },
  heuristic_sampler: {
    samples: ["P-101"],
    on_no_match: "keep_working",
  },
};

const TRANSFORM_DOC: Record<EtlTransformHandlerId, MessageKey> = {
  regex_substitution: "transforms.handlerDoc.regex_substitution",
  leading_zero_normalize: "transforms.handlerDoc.leading_zero_normalize",
  sequential_literal_replace: "transforms.handlerDoc.sequential_literal_replace",
  substitution_variants: "transforms.handlerDoc.substitution_variants",
  trim_whitespace: "transforms.handlerDoc.trim_whitespace",
  change_case: "transforms.handlerDoc.change_case",
  coerce_scalar: "transforms.handlerDoc.coerce_scalar",
  default_if_empty: "transforms.handlerDoc.default_if_empty",
  split_string: "transforms.handlerDoc.split_string",
  split_join: "transforms.handlerDoc.split_join",
  parse_json_extract: "transforms.handlerDoc.parse_json_extract",
  format_datetime: "transforms.handlerDoc.format_datetime",
  hash_stable: "transforms.handlerDoc.hash_stable",
  mask_string: "transforms.handlerDoc.mask_string",
  static_lookup_map: "transforms.handlerDoc.static_lookup_map",
  heuristic_sampler: "transforms.handlerDoc.heuristic_sampler",
};

export function isEtlTransformHandlerId(h: string): h is EtlTransformHandlerId {
  return Object.prototype.hasOwnProperty.call(TRANSFORM_DEFAULTS, h);
}

/** When `output_multi_value` is omitted from transform config (UI + engine). */
export function defaultOutputMultiValueForHandler(handlerId: string): "array_json" | "explode_rows" {
  return handlerId === "split_string" ? "array_json" : "explode_rows";
}

export function defaultTransformHandlerBlock(handler: EtlTransformHandlerId): Record<string, unknown> {
  return JSON.parse(JSON.stringify(TRANSFORM_DEFAULTS[handler])) as Record<string, unknown>;
}

export function transformHandlerDocKey(handler: string): MessageKey {
  if (isEtlTransformHandlerId(handler)) return TRANSFORM_DOC[handler];
  return "transforms.handlerDoc.generic";
}

export type DefaultTransformNodeConfigOptions = {
  /**
   * When set, `fields` / `output_template` read this RAW property and `output_field` matches it
   * (prior transform's sink key).
   */
  previousOutputField?: string | null;
};

/** When the node is a canvas transform, return its config `output_field` (trimmed), else null. */
export function readTransformOutputFieldForChainedTransform(
  node: { type?: string | null; data?: unknown } | undefined | null
): string | null {
  if (!node || node.type !== "discoveryTransform") return null;
  const data = (node.data ?? {}) as Record<string, unknown>;
  const cfg = data.config;
  if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) return null;
  const of = String((cfg as Record<string, unknown>).output_field ?? "").trim();
  return of || null;
}

export function defaultTransformNodeConfig(
  handler: EtlTransformHandlerId = "regex_substitution",
  options?: DefaultTransformNodeConfigOptions
): Record<string, unknown> {
  const prev = String(options?.previousOutputField ?? "").trim();
  const cfg: Record<string, unknown> = {
    description: transformHandlerDescription(handler),
    handler_id: handler,
    enabled: true,
    fields: [{ field_name: prev }],
    output_field: prev,
    output_template: prev ? `{${prev}}` : "",
    output_mode: "append",
    [handler]: defaultTransformHandlerBlock(handler),
  };
  if (handler === "split_string") {
    cfg.output_multi_value = "array_json";
  }
  return cfg;
}

/** CogniteAsset name → assetTag extraction (regex on fields[].name). */
export const ASSET_TAG_FROM_NAME_REGEX =
  String.raw`(?<![\d-])(?:\b|(?<=_))(?:\d{1,8}-?)?[A-Z]{1,8}-?\d{1,10}(?:-\d{1,6})*[A-Z]?\b`;

export function defaultAssetTagFromNameTransformConfig(): Record<string, unknown> {
  return {
    description: "Extract asset tag from CogniteAsset name",
    handler_id: "regex_substitution",
    enabled: true,
    fields: [{ field_name: "name", regex: ASSET_TAG_FROM_NAME_REGEX }],
    output_field: "assetTag",
    output_template: "{name}",
    output_mode: "overwrite",
    regex_substitution: { patterns: [] },
  };
}

const TAG_FIELD_REGEX_OPTIONS = {
  ignore_case: false,
  multiline: false,
  dotall: false,
  unicode: true,
};

/** Strip leading ``unit-`` style numeric prefixes (e.g. ``10-P-1234`` → ``P-1234``). */
export const STRIP_NUMERIC_UNIT_PREFIX_PATTERN = String.raw`^\d+-`;

/**
 * ``Aliases: Tag Based`` — extract instrument tag from ``name`` (optional ``unit`` prefix),
 * strip leading numeric unit prefixes, append to existing ``aliases``.
 */
export function defaultTagBasedAliasesTransformConfig(options?: {
  includeUnit?: boolean;
}): Record<string, unknown> {
  const includeUnit = options?.includeUnit !== false;
  const tagField: TransformFieldRow = {
    field_name: "name",
    required: true,
    priority: 1,
    regex: ASSET_TAG_FROM_NAME_REGEX,
    max_matches_per_field: 10,
    regex_options: TAG_FIELD_REGEX_OPTIONS,
  };
  const extractFields: TransformFieldRow[] = includeUnit
    ? [tagField, { field_name: "unit" }]
    : [tagField];
  const extractStep: Record<string, unknown> = {
    description: "Extract tag alias from name (and unit when present)",
    handler_id: "regex_substitution",
    enabled: true,
    fields: extractFields,
    output_template: includeUnit ? "{unit}-{name}" : "{name}",
    output_field: "_tagAliasDraft",
    output_mode: "overwrite",
    regex_substitution: { patterns: [] },
  };
  const stripStep: Record<string, unknown> = {
    description: "Strip numeric unit prefix and append to aliases",
    handler_id: "regex_substitution",
    enabled: true,
    fields: [{ field_name: "_tagAliasDraft" }],
    output_template: "{_tagAliasDraft}",
    output_field: "aliases",
    output_mode: "append",
    regex_substitution: {
      patterns: [{ pattern: STRIP_NUMERIC_UNIT_PREFIX_PATTERN, replacement: "" }],
    },
  };
  return {
    description: "Extract tag alias from name; strip leading numeric prefix; append to aliases",
    enabled: true,
    execution: { mode: "ordered" },
    steps: [extractStep, stripStep],
  };
}

/** Drop unused top-level transform node `priority` (stage order is workflow DAG only). */
export function sanitizeTransformNodeConfig(cfg: Record<string, unknown>): Record<string, unknown> {
  const next = { ...cfg };
  delete next.priority;
  return next;
}

export function readTransformHandlerId(cfg: Record<string, unknown>): string {
  return String(cfg.handler_id ?? cfg.handler ?? "").trim();
}

export function readTransformHandlerBlock(
  cfg: Record<string, unknown>,
  handlerId: string
): Record<string, unknown> {
  const nested = cfg[handlerId];
  if (nested && typeof nested === "object" && !Array.isArray(nested)) {
    return { ...(nested as Record<string, unknown>) };
  }
  const legacy = cfg.config;
  if (legacy && typeof legacy === "object" && !Array.isArray(legacy)) {
    return { ...(legacy as Record<string, unknown>) };
  }
  return {};
}

export function patchTransformHandlerBlock(
  cfg: Record<string, unknown>,
  handlerId: string,
  block: Record<string, unknown>
): Record<string, unknown> {
  const next = { ...cfg, [handlerId]: block };
  if ("config" in next) delete next.config;
  if ("handler" in next) delete next.handler;
  return next;
}

/** Full ``fields[]`` rows (preserves ``regex_options``, ``priority``, …). */
export function readTransformFields(cfg: Record<string, unknown>): TransformFieldRow[] {
  const raw = cfg.fields;
  if (!Array.isArray(raw)) return [];
  return raw
    .filter((item) => item && typeof item === "object" && !Array.isArray(item))
    .map((item) => ({ ...(item as Record<string, unknown>) }));
}
