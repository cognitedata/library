import mixpanel from "mixpanel-browser";

export const DEPLOYMENT_PACK_USAGE_MIXPANEL_EVENT = "deployment_pack_usage";

export type DeploymentPackUsageMixpanelRow = Readonly<{
  packId: string;
  inUse: boolean;
}>;

function sortJsonKeys(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(sortJsonKeys);
  const entries = Object.entries(value as Record<string, unknown>)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => [k, sortJsonKeys(v)] as const);
  return Object.fromEntries(entries);
}

function calendarDayUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

function throttleStorageKey(project: string): string {
  return `qualitizer.mixpanel.deployment_pack_usage_day.${encodeURIComponent(project)}`;
}

function readThrottleDay(project: string): string | null {
  try {
    return localStorage.getItem(throttleStorageKey(project));
  } catch {
    return null;
  }
}

function writeThrottleDay(project: string, day: string): void {
  try {
    localStorage.setItem(throttleStorageKey(project), day);
  } catch {
    /* ignore quota / private mode */
  }
}

export function buildDeploymentPackUsageMixpanelProperties(
  rows: ReadonlyArray<DeploymentPackUsageMixpanelRow>,
  project: string | undefined
): Record<string, unknown> {
  const deployment_pack_in_use: Record<string, boolean> = {};
  for (const r of rows) {
    deployment_pack_in_use[r.packId] = r.inUse;
  }
  const deployment_packs_in_use_count = rows.filter((r) => r.inUse).length;
  const utilized_deployment_packs = rows
    .filter((r) => r.inUse)
    .map((r) => r.packId)
    .sort((a, b) => a.localeCompare(b));

  const payload: Record<string, unknown> = {
    deployment_packs_in_use_count,
    deployment_pack_in_use,
    utilized_deployment_packs,
  };
  const p = project?.trim();
  if (p) payload.cdf_project = p;
  return sortJsonKeys(payload) as Record<string, unknown>;
}

export function stringifyDeploymentPackUsageMixpanelPayload(
  rows: ReadonlyArray<DeploymentPackUsageMixpanelRow>,
  project: string | undefined
): string {
  return JSON.stringify(buildDeploymentPackUsageMixpanelProperties(rows, project), null, 2);
}

/**
 * @param force When true (e.g. user opened the internal DP Usage page), always sends. When false,
 *   sends at most once per UTC calendar day per CDF project (localStorage), shared across all
 *   Qualitizer pages (see `useDailyDeploymentPackUsageMixpanel` in `src/deployment-pack-usage/`).
 */
export function trackDeploymentPackUsageMixpanel(
  project: string,
  rows: ReadonlyArray<DeploymentPackUsageMixpanelRow>,
  options: { force: boolean }
): void {
  const day = calendarDayUtc();
  if (!options.force && readThrottleDay(project) === day) {
    return;
  }
  const props = buildDeploymentPackUsageMixpanelProperties(rows, project);
  mixpanel.track(DEPLOYMENT_PACK_USAGE_MIXPANEL_EVENT, props);
  writeThrottleDay(project, day);
}
