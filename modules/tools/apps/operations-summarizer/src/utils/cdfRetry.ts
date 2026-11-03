import { getAppConfig } from "@/config/appConfig";

export { getHttpStatusFromError } from "@/utils/cdfRequest";

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface RetryDelayOptions {
  attempt: number;
  error?: unknown;
  errorTimestamp?: Date;
  baseMs?: number;
}

export function computeUserRetryDelayMs({
  attempt,
  error,
  baseMs,
}: RetryDelayOptions): number {
  const resolvedBase = baseMs ?? getAppConfig().limits.retry.baseMs;
  const maxDelayMs = getAppConfig().limits.retry.maxDelayMs;
  // Honor Retry-After header when present
  if (error !== null && typeof error === "object") {
    const rec = error as Record<string, unknown>;
    const response = rec.response as Record<string, unknown> | undefined;
    const headers = response?.headers ?? rec.headers;
    if (headers && typeof headers === "object") {
      const h = headers as Record<string, unknown>;
      const retryAfter =
        h["retry-after"] ?? h["Retry-After"] ?? h["x-retry-after"];
      if (typeof retryAfter === "string") {
        const parsed = Number(retryAfter);
        if (!Number.isNaN(parsed) && parsed > 0) {
          return Math.min(parsed * 1000, maxDelayMs);
        }
      }
    }
  }

  const exponential = resolvedBase * 2 ** attempt;
  const jittered = exponential * (0.5 + 0.5 * Math.random());
  return Math.min(jittered, maxDelayMs);
}
