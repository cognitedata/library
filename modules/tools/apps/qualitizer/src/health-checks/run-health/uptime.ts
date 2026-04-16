export const SUCCESS_STATUSES = new Set(["success", "completed", "ready", "seen"]);
export const FAILED_STATUSES = new Set([
  "failed",
  "failure",
  "error",
  "timed_out",
  "timeout",
]);

export const MAX_RECENT_RUNS = 5;

export function normalize(status?: string | null): string {
  return (status ?? "").toLowerCase();
}

export function isSuccess(status?: string | null): boolean {
  return SUCCESS_STATUSES.has(normalize(status));
}

export function isFailed(status?: string | null): boolean {
  return FAILED_STATUSES.has(normalize(status));
}

export function uptimePercentage(successful: number, failed: number): number {
  const total = successful + failed;
  if (total <= 0) return 100;
  return (successful / total) * 100;
}

export type HealthClass = "healthy" | "unhealthy" | "no_runs";

export function classifyHealth(
  runs: number,
  uptime: number,
  thresholdPct: number
): HealthClass {
  if (runs <= 0) return "no_runs";
  return uptime >= thresholdPct ? "healthy" : "unhealthy";
}

export function uptimeColor(uptime: number): string {
  if (uptime >= 90) return "text-emerald-700";
  if (uptime >= 70) return "text-amber-700";
  if (uptime >= 50) return "text-orange-700";
  return "text-red-700";
}

export function uptimeBg(uptime: number): string {
  if (uptime >= 90) return "bg-emerald-50 border-emerald-200";
  if (uptime >= 70) return "bg-amber-50 border-amber-200";
  if (uptime >= 50) return "bg-orange-50 border-orange-200";
  return "bg-red-50 border-red-200";
}
