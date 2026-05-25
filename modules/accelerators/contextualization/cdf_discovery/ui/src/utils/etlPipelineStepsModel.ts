import type { JsonObject } from "../types/jsonConfig";

export type ExecutionMode = "ordered" | "parallel";

export type ParsedPipelineExecution = {
  mode: ExecutionMode;
};

const EXECUTION_MODES = new Set<ExecutionMode>(["ordered", "parallel"]);

export function parseExecutionMode(value: JsonObject): ExecutionMode {
  const exec = value.execution;
  if (exec && typeof exec === "object" && !Array.isArray(exec)) {
    const mode = String((exec as JsonObject).mode ?? "").trim().toLowerCase();
    if (EXECUTION_MODES.has(mode as ExecutionMode)) return mode as ExecutionMode;
  }
  return "ordered";
}

export function parseStepsArray(value: JsonObject): JsonObject[] {
  const raw = value.steps;
  if (!Array.isArray(raw)) return [];
  return raw.filter((s): s is JsonObject => s !== null && typeof s === "object" && !Array.isArray(s));
}

export function serializeExecution(mode: ExecutionMode): JsonObject {
  return { execution: { mode } };
}

export function serializeSteps(steps: JsonObject[]): JsonObject {
  return steps.length > 0 ? { steps } : {};
}
