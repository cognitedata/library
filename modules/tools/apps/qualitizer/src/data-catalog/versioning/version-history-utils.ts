export function formatTs(ms?: number): string | null {
  if (ms == null || Number.isNaN(ms)) return null;
  try {
    return new Date(ms).toLocaleString();
  } catch {
    return null;
  }
}

export function shouldShowUpdatedSeparate(created?: number, updated?: number): boolean {
  if (created == null || updated == null) return false;
  return updated !== created;
}

export function truncJson(v: unknown, max = 280): string {
  try {
    const s = JSON.stringify(v);
    if (s.length <= max) return s;
    return `${s.slice(0, max)}…`;
  } catch {
    return String(v);
  }
}
