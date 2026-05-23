export type RunStepId = "extract" | "create" | "write";

export type StepRunStatus = "idle" | "running" | "succeeded" | "failed";

export type ProgressEvent = {
  event?: string;
  task_id?: string;
  workflow_step?: string;
  function_external_id?: string;
  code?: number;
  level?: string;
  message?: string;
  status?: string;
  error?: string;
};

export const PIPELINE_STEP_ORDER: RunStepId[] = ["extract", "create", "write"];

export function stepsForRun(step: RunStepId | "all"): RunStepId[] {
  return step === "all" ? [...PIPELINE_STEP_ORDER] : [step];
}

export function initialStepStatuses(step: RunStepId | "all"): Record<RunStepId, StepRunStatus> {
  const planned = new Set(stepsForRun(step));
  const out = {} as Record<RunStepId, StepRunStatus>;
  for (const s of PIPELINE_STEP_ORDER) {
    out[s] = planned.has(s) ? "idle" : "idle";
  }
  return out;
}

export function parseProgressLine(line: string): ProgressEvent | null {
  if (!line.trim()) return null;
  try {
    return JSON.parse(line) as ProgressEvent;
  } catch {
    return null;
  }
}
