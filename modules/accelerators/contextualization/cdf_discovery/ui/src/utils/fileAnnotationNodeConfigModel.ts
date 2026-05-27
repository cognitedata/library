import {
  DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC,
  DEFAULT_MAX_PAGES_PER_DETECT_REQUEST,
  DEFAULT_MAX_PAGES_PER_FILE_REFERENCE,
  DEFAULT_MAX_PATTERN_SAMPLES,
  DEFAULT_MIN_TOKENS,
  readOptionalPositiveInt,
} from "./fanoutNodeConfigModel";

export const DEFAULT_CHILD_FUNCTION = "fn_etl_file_annotation";

export type EntityTargetPreset = "asset" | "file" | "custom";

const PRESET_DEFAULTS: Record<EntityTargetPreset, { patterns_entity_property: string; pattern_resource_type: string }> = {
  asset: { patterns_entity_property: "aliases", pattern_resource_type: "equipment" },
  file: { patterns_entity_property: "name", pattern_resource_type: "file" },
  custom: { patterns_entity_property: "", pattern_resource_type: "equipment" },
};

export function defaultFileAnnotationNodeConfig(): Record<string, unknown> {
  return {
    description: "File annotation",
    entity_target: "asset",
    entities_input_mode: "auto",
    patterns_entity_property: "aliases",
    pattern_mode: true,
    pattern_normalization: "file_annotation",
    pattern_resource_type: "equipment",
    max_pattern_samples: DEFAULT_MAX_PATTERN_SAMPLES,
    partial_match: true,
    min_tokens: DEFAULT_MIN_TOKENS,
    max_pages_per_file_reference: DEFAULT_MAX_PAGES_PER_FILE_REFERENCE,
    max_pages_per_detect_request: DEFAULT_MAX_PAGES_PER_DETECT_REQUEST,
    max_detect_jobs_per_invocation: 1,
    diagram_poll_timeout_sec: DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC,
    child_function_external_id: DEFAULT_CHILD_FUNCTION,
  };
}

export function applyEntityTargetPreset(
  cfg: Record<string, unknown>,
  preset: EntityTargetPreset
): Record<string, unknown> {
  const d = PRESET_DEFAULTS[preset];
  return {
    ...cfg,
    entity_target: preset,
    patterns_entity_property: d.patterns_entity_property,
    pattern_resource_type: d.pattern_resource_type,
  };
}

export function fileAnnotationConfigSummary(config: Record<string, unknown>): string {
  const parts: string[] = [];
  const target = String(config.entity_target ?? "asset");
  parts.push(target);
  if (config.pattern_mode !== false) parts.push("pattern");
  const pagesCall = readOptionalPositiveInt(config.max_pages_per_detect_request);
  if (pagesCall) parts.push(`${pagesCall} pp/call`);
  const child = String(config.child_function_external_id ?? DEFAULT_CHILD_FUNCTION).trim();
  if (child) parts.push(child.replace(/^fn_/, ""));
  const desc = String(config.description ?? "").trim();
  return parts.length ? parts.join(" · ") : desc;
}
