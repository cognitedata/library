import { CogniteError, HttpError } from "@cognite/sdk";
import { getHttpStatus } from "@/shared/transient-http-retry";

export function isForbiddenError(reason: unknown): boolean {
  if (getHttpStatus(reason) === 403) return true;
  if (reason instanceof CogniteError && reason.status === 403) return true;
  if (reason instanceof HttpError && reason.status === 403) return true;
  if (reason instanceof Error && /\b403\b|forbidden/i.test(reason.message)) return true;
  return false;
}
