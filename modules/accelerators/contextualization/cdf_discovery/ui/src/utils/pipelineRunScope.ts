import { useCallback, useEffect, useState } from "react";
import type { TransformPipelineParameters } from "../types/transformCanvas";

export type PipelineRunScope = "incremental" | "all";

const STORAGE_PREFIX = "cdf_discovery:etl_run_scope:";

export function pipelineIncrementalEnabled(
  parameters?: TransformPipelineParameters | null
): boolean {
  if (!parameters) return false;
  return Boolean(parameters.incremental || parameters.incremental_change_processing);
}

export function defaultPipelineRunScope(
  parameters?: TransformPipelineParameters | null
): PipelineRunScope {
  return pipelineIncrementalEnabled(parameters) ? "incremental" : "all";
}

export function pipelineRunScopeStorageKey(resourceId: string): string {
  return `${STORAGE_PREFIX}${resourceId}`;
}

export function readStoredPipelineRunScope(resourceId: string): PipelineRunScope | null {
  if (typeof sessionStorage === "undefined") return null;
  try {
    const raw = sessionStorage.getItem(pipelineRunScopeStorageKey(resourceId));
    return raw === "incremental" || raw === "all" ? raw : null;
  } catch {
    return null;
  }
}

export function storePipelineRunScope(resourceId: string, scope: PipelineRunScope): void {
  if (typeof sessionStorage === "undefined") return;
  try {
    sessionStorage.setItem(pipelineRunScopeStorageKey(resourceId), scope);
  } catch {
    // ignore quota / private browsing
  }
}

export function resolvePipelineRunScope(
  resourceId: string,
  parameters?: TransformPipelineParameters | null
): PipelineRunScope {
  return readStoredPipelineRunScope(resourceId) ?? defaultPipelineRunScope(parameters);
}

/** Run scope for a pipeline/template tab; persists per resource across runs and remounts. */
export function usePipelineRunScope(
  resourceId: string,
  parameters?: TransformPipelineParameters | null
): [PipelineRunScope, (scope: PipelineRunScope) => void] {
  const incrementalDefault = pipelineIncrementalEnabled(parameters);
  const [runScope, setRunScopeState] = useState<PipelineRunScope>(() =>
    resolvePipelineRunScope(resourceId, parameters)
  );

  useEffect(() => {
    setRunScopeState(resolvePipelineRunScope(resourceId, parameters));
  }, [resourceId]);

  useEffect(() => {
    if (readStoredPipelineRunScope(resourceId) != null) return;
    setRunScopeState(incrementalDefault ? "incremental" : "all");
  }, [resourceId, incrementalDefault]);

  const setRunScope = useCallback(
    (scope: PipelineRunScope) => {
      storePipelineRunScope(resourceId, scope);
      setRunScopeState(scope);
    },
    [resourceId]
  );

  return [runScope, setRunScope];
}

const DRY_RUN_STORAGE_PREFIX = "cdf_discovery:etl_dry_run:";

export function pipelineDryRunStorageKey(resourceId: string): string {
  return `${DRY_RUN_STORAGE_PREFIX}${resourceId}`;
}

export function readStoredPipelineDryRun(resourceId: string): boolean | null {
  if (typeof sessionStorage === "undefined") return null;
  try {
    const raw = sessionStorage.getItem(pipelineDryRunStorageKey(resourceId));
    if (raw === "1" || raw === "true") return true;
    if (raw === "0" || raw === "false") return false;
    return null;
  } catch {
    return null;
  }
}

export function storePipelineDryRun(resourceId: string, dryRun: boolean): void {
  if (typeof sessionStorage === "undefined") return;
  try {
    sessionStorage.setItem(pipelineDryRunStorageKey(resourceId), dryRun ? "1" : "0");
  } catch {
    // ignore quota / private browsing
  }
}

/** Dry-run toggle for local pipeline runs; persists per resource in session storage. */
export function usePipelineDryRun(resourceId: string): [boolean, (dryRun: boolean) => void] {
  const [dryRun, setDryRunState] = useState(
    () => readStoredPipelineDryRun(resourceId) ?? false
  );

  useEffect(() => {
    setDryRunState(readStoredPipelineDryRun(resourceId) ?? false);
  }, [resourceId]);

  const setDryRun = useCallback(
    (value: boolean) => {
      storePipelineDryRun(resourceId, value);
      setDryRunState(value);
    },
    [resourceId]
  );

  return [dryRun, setDryRun];
}
