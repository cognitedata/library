export type ResourceKind = "extractionPipelines" | "workflows" | "transformations" | "functions";

const STORAGE_KEY = "qualitizer.runHealth.thresholds";

export const DEFAULT_THRESHOLDS: Record<ResourceKind, number> = {
  extractionPipelines: 75,
  workflows: 75,
  transformations: 75,
  functions: 75,
};

export function loadThresholds(): Record<ResourceKind, number> {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return { ...DEFAULT_THRESHOLDS };
    const parsed = JSON.parse(raw) as Partial<Record<ResourceKind, unknown>>;
    const result: Record<ResourceKind, number> = { ...DEFAULT_THRESHOLDS };
    (Object.keys(DEFAULT_THRESHOLDS) as ResourceKind[]).forEach((k) => {
      const v = Number(parsed[k]);
      if (Number.isFinite(v) && v >= 0 && v <= 100) result[k] = v;
    });
    return result;
  } catch {
    return { ...DEFAULT_THRESHOLDS };
  }
}

export function saveThresholds(value: Record<ResourceKind, number>) {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(value));
  } catch {
    // ignore
  }
}
