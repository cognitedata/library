import { useLayoutEffect, useRef, type Dispatch, type SetStateAction } from "react";
import { isForbiddenError } from "@/shared/cdf-errors";
import type { LoadState, ProcessingRequestStats } from "./types";

export function isStaleProcessingFetch(
  fetchGeneration: number,
  startedGeneration: number
): boolean {
  return fetchGeneration !== startedGeneration;
}

export function processingWindowKey(
  windowRange: { start: number; end: number } | null | undefined
): string | undefined {
  if (!windowRange) return undefined;
  return `${windowRange.start}:${windowRange.end}`;
}

/** Clears series state when the hour window changes (not on serial phase advances). */
export function useProcessingWindowSessionReset(
  windowSessionKey: string,
  reset: () => void
) {
  const lastKeyRef = useRef<string | null>(null);

  useLayoutEffect(() => {
    if (!windowSessionKey) return;
    if (lastKeyRef.current === windowSessionKey) return;
    lastKeyRef.current = windowSessionKey;
    reset();
  }, [windowSessionKey, reset]);
}

/** Marks a series loading in the same commit before serial phase-advance effects run. */
export function useProcessingSeriesFetchLoading(
  fetchEnabled: boolean,
  isSdkLoading: boolean,
  windowRange: { start: number; end: number } | null | undefined,
  fetchGeneration: number,
  setStatus: Dispatch<SetStateAction<LoadState>>
) {
  useLayoutEffect(() => {
    if (!fetchEnabled) return;
    if (isSdkLoading || !windowRange) return;
    setStatus("loading");
  }, [
    fetchEnabled,
    fetchGeneration,
    isSdkLoading,
    setStatus,
    windowRange?.end,
    windowRange?.start,
  ]);
}

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
