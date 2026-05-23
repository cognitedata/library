import { fetchJson } from "./fetchJson";
import type { GovernanceDocument } from "../types/governanceConfig";

const BASE = "/api/governance/declared";

export type BuildTarget = "spaces" | "groups" | "all";

export async function fetchGovernanceHealth(): Promise<{
  ok: boolean;
  declared_root: string;
  config_exists: boolean;
}> {
  return fetchJson(`${BASE}/health`);
}

export async function fetchGovernanceModel(): Promise<GovernanceDocument> {
  return fetchJson<GovernanceDocument>(`${BASE}/config/model`);
}

export async function saveGovernanceModel(model: GovernanceDocument): Promise<void> {
  await fetchJson(`${BASE}/config/model`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(model),
  });
}

export async function fetchGovernanceConfigRaw(): Promise<string> {
  const data = await fetchJson<{ content: string }>(`${BASE}/config`);
  return data.content ?? "";
}

export async function saveGovernanceConfigRaw(content: string): Promise<void> {
  await fetchJson(`${BASE}/config`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content }),
  });
}

export async function mirrorGovernanceModel(model: GovernanceDocument): Promise<{
  written: string[];
  skipped: string[];
}> {
  return fetchJson(`${BASE}/config/mirror`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model }),
  });
}

export async function runGovernanceBuild(opts: {
  target: BuildTarget;
  force?: boolean;
  dryRun?: boolean;
}): Promise<{ ok: boolean; exit_code: number; stdout: string; stderr: string }> {
  return fetchJson(`${BASE}/build`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      target: opts.target,
      force: opts.force ?? false,
      dry_run: opts.dryRun ?? false,
    }),
  });
}

export async function listGovernanceArtifacts(
  kind: "spaces" | "groups"
): Promise<string[]> {
  const data = await fetchJson<{ spaces: string[]; groups: string[] }>(
    `${BASE}/artifacts?kind=${kind}`
  );
  return kind === "spaces" ? data.spaces : data.groups;
}

export async function readGovernanceFile(rel: string): Promise<string> {
  const data = await fetchJson<{ content: string }>(
    `${BASE}/file?rel=${encodeURIComponent(rel)}`
  );
  return data.content ?? "";
}

export async function writeGovernanceFile(
  rel: string,
  content: string
): Promise<{ source_ids_synced?: boolean }> {
  return fetchJson(`${BASE}/file?rel=${encodeURIComponent(rel)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content }),
  });
}

export async function fetchSourceIdHint(sourceId: string): Promise<{
  valid: boolean;
  empty: boolean;
}> {
  return fetchJson(`${BASE}/source-id-hint?source_id=${encodeURIComponent(sourceId)}`);
}
