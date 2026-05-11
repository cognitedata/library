import { CogniteError, HttpError } from "@cognite/sdk";

export function getHttpStatus(reason: unknown): number | undefined {
  if (reason instanceof CogniteError || reason instanceof HttpError) {
    return reason.status;
  }
  if (reason && typeof reason === "object" && "status" in reason) {
    const s = (reason as { status?: unknown }).status;
    if (typeof s === "number") return s;
  }
  return undefined;
}

export function isTransientHttpError(reason: unknown): boolean {
  const status = getHttpStatus(reason);
  if (status === 503 || status === 502 || status === 429) return true;
  if (reason instanceof Error) {
    return /\b503\b|\b502\b|\b429\b|service unavailable|bad gateway|too many requests/i.test(
      reason.message
    );
  }
  return false;
}

function delay(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

export async function withTransientRetries<T>(
  fn: () => Promise<T>,
  options?: { maxAttempts?: number; baseDelayMs?: number }
): Promise<T> {
  const maxAttempts = options?.maxAttempts ?? 4;
  const baseDelayMs = options?.baseDelayMs ?? 400;
  let lastError: unknown;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (e) {
      lastError = e;
      if (!isTransientHttpError(e) || attempt === maxAttempts - 1) throw e;
      await delay(baseDelayMs * 2 ** attempt);
    }
  }
  throw lastError;
}
