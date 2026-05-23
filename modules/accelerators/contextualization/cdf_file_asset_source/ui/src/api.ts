import { apiUrl } from "./utils/apiBase";

export type ConfigStepMeta = {
  id: string;
  label: string;
};

export async function fetchHealth(): Promise<{ status: string }> {
  const r = await fetch(apiUrl("/api/health"));
  if (!r.ok) throw new Error(`health ${r.status}`);
  return r.json();
}

export async function fetchConfigSteps(): Promise<{
  default_config: string;
  steps: ConfigStepMeta[];
}> {
  const r = await fetch(apiUrl("/api/config-steps"));
  if (!r.ok) throw new Error(`config-steps ${r.status}`);
  return r.json();
}

export async function fetchDefaultConfig(): Promise<{ path: string; content: string }> {
  const r = await fetch(apiUrl("/api/default-config"));
  if (!r.ok) throw new Error(`default-config ${r.status}`);
  return r.json();
}

export async function saveDefaultConfig(content: string): Promise<void> {
  const r = await fetch(apiUrl("/api/default-config"), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content }),
  });
  if (!r.ok) {
    const err = await r.json().catch(() => ({}));
    throw new Error((err as { detail?: string }).detail ?? `save default ${r.status}`);
  }
}

export async function validateConfigs(steps?: string[]): Promise<{
  valid: boolean;
  results: Array<{
    step?: string;
    path: string;
    valid: boolean;
    errors: string[];
    warnings: string[];
    messages: string[];
  }>;
}> {
  const r = await fetch(apiUrl("/api/validate"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(steps?.length ? { steps } : {}),
  });
  if (!r.ok) throw new Error(`validate ${r.status}`);
  return r.json();
}

export async function runPipeline(step: "extract" | "create" | "write" | "all"): Promise<{
  exit_code: number;
  stdout: string;
  stderr: string;
  run: unknown;
}> {
  const r = await fetch(apiUrl("/api/run"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ step }),
  });
  if (!r.ok) throw new Error(`run ${r.status}`);
  return r.json();
}

export async function fetchRunResults(): Promise<{
  items: Array<{ path: string; run_scope?: unknown }>;
}> {
  const r = await fetch(apiUrl("/api/run-results"));
  if (!r.ok) throw new Error(`run-results ${r.status}`);
  return r.json();
}

export async function previewRunResult(rel: string): Promise<{ path: string; data: unknown }> {
  const r = await fetch(
    apiUrl(`/api/run-results/preview?${new URLSearchParams({ rel })}`)
  );
  if (!r.ok) throw new Error(`preview ${r.status}`);
  return r.json();
}

export async function fetchModuleFile(rel: string): Promise<{ path: string; content: string }> {
  const r = await fetch(apiUrl(`/api/file?${new URLSearchParams({ rel })}`));
  if (!r.ok) throw new Error(`file ${r.status}`);
  return r.json();
}
