import type { JsonObject } from "../types/jsonConfig";
import { readTransformHandlerId } from "./etlTransformHandlerTemplates";
import {
  parseExecutionMode,
  parseStepsArray,
  serializeExecution,
  serializeSteps,
  type ExecutionMode,
} from "./etlPipelineStepsModel";

const PIPELINE_KEYS = new Set(["execution", "steps", "field_policies"]);

const STEP_STRIP_KEYS = new Set([...PIPELINE_KEYS, "description"]);

function extrasFrom(value: JsonObject): JsonObject {
  if (!isMultiStepTransformConfig(value)) return {};
  const o: JsonObject = {};
  for (const [k, v] of Object.entries(value)) {
    if (!STEP_STRIP_KEYS.has(k)) o[k] = v;
  }
  return o;
}

/** One transform step (handler + fields + handler block). */
export function legacyConfigToStep(cfg: JsonObject): JsonObject {
  const step: JsonObject = {};
  for (const [k, v] of Object.entries(cfg)) {
    if (!PIPELINE_KEYS.has(k)) step[k] = v;
  }
  return step;
}

export function materializeTransformSteps(cfg: JsonObject): JsonObject[] {
  const explicit = parseStepsArray(cfg);
  if (explicit.length > 0) return explicit.map((s) => ({ ...s }));
  const handler = readTransformHandlerId(cfg as Record<string, unknown>);
  if (!handler) return [];
  return [legacyConfigToStep(cfg)];
}

export function isMultiStepTransformConfig(cfg: JsonObject): boolean {
  if (cfg.execution != null) return true;
  if (Array.isArray(cfg.steps) && cfg.steps.length > 0) return true;
  return parseStepsArray(cfg).length > 1;
}

export type ParsedTransformNodeConfig = {
  description: string;
  executionMode: ExecutionMode;
  steps: JsonObject[];
  fieldPolicies: unknown;
  multiStep: boolean;
  extras: JsonObject;
};

export function parseTransformNodeConfig(value: JsonObject): ParsedTransformNodeConfig {
  const steps = materializeTransformSteps(value);
  return {
    description: String(value.description ?? "").trim(),
    executionMode: parseExecutionMode(value),
    steps,
    fieldPolicies: value.field_policies,
    multiStep: isMultiStepTransformConfig(value),
    extras: extrasFrom(value),
  };
}

/** Flatten first step to top-level when not multi-step (engine legacy shape). */
export function serializeTransformNodeConfig(parts: {
  description: string;
  executionMode: ExecutionMode;
  steps: JsonObject[];
  fieldPolicies: unknown;
  multiStep: boolean;
  extras: JsonObject;
}): JsonObject {
  const desc = parts.description.trim();
  if (parts.multiStep && parts.steps.length > 0) {
    const out: JsonObject = {
      ...parts.extras,
      ...serializeExecution(parts.executionMode),
      ...serializeSteps(parts.steps),
    };
    if (desc) out.description = desc;
    if (parts.executionMode === "parallel" && parts.fieldPolicies != null) {
      out.field_policies = parts.fieldPolicies;
    }
    return out;
  }

  const step = parts.steps[0] ?? {};
  const out: JsonObject = { ...parts.extras, ...step };
  if (desc) out.description = desc;
  delete out.execution;
  delete out.steps;
  if ("field_policies" in out && parts.executionMode !== "parallel") {
    delete out.field_policies;
  }
  return out;
}

export function defaultTransformStep(existing: JsonObject[]): JsonObject {
  const last = existing.length > 0 ? existing[existing.length - 1]! : null;
  const handler = last
    ? readTransformHandlerId(last as Record<string, unknown>) || "trim_whitespace"
    : "trim_whitespace";
  return {
    description: "",
    handler_id: handler,
    enabled: true,
    fields: [{ field_name: "" }],
    output_field: "",
    output_template: "",
    output_mode: "append",
    // Runtime default input_mode is cumulative (merge predecessor + sink cohort row).
    [handler]: {},
  };
}
