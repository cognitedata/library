import { isForbiddenError } from "@/shared/cdf-errors";
import type { ProcessingRequestStats } from "./types";

export function processingRequestStats(
  failed: number,
  total: number,
  permissionsDenied: boolean
): ProcessingRequestStats | null {
  if (failed <= 0) return null;
  return {
    failed,
    total,
    ...(permissionsDenied ? { permissionsDenied: true } : {}),
  };
}

export function noteForbiddenFailure(
  permissionsDenied: { current: boolean },
  error: unknown
): void {
  if (isForbiddenError(error)) {
    permissionsDenied.current = true;
  }
}
