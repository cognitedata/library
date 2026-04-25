import { CogniteError, HttpError } from "@cognite/sdk";

export function isForbiddenError(reason: unknown): boolean {
  if (reason instanceof CogniteError && reason.status === 403) return true;
  if (reason instanceof HttpError && reason.status === 403) return true;
  return false;
}
