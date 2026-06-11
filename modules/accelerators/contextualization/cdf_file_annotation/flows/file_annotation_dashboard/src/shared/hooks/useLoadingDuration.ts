import { useEffect, useMemo, useRef, useState } from "react";

function formatDuration(ms: number) {
  if (ms < 1000) {
    return `${ms}ms`;
  }
  return `${(ms / 1000).toFixed(1)}s`;
}

interface UseLoadingDurationOptions {
  keepRunningWhile?: boolean;
}

export function useLoadingDuration(
  isLoading: boolean,
  resetKey?: string,
  options?: UseLoadingDurationOptions
) {
  const [elapsedMs, setElapsedMs] = useState(0);
  const [lastDurationMs, setLastDurationMs] = useState<number | null>(null);
  const startedAtRef = useRef<number | null>(null);
  const keepRunningWhile = options?.keepRunningWhile ?? false;

  useEffect(() => {
    startedAtRef.current = null;
    setElapsedMs(0);
    setLastDurationMs(null);
  }, [resetKey]);

  useEffect(() => {
    if (isLoading && startedAtRef.current == null) {
      startedAtRef.current = Date.now();
      setElapsedMs(0);
    }

    if (!isLoading && startedAtRef.current != null && !keepRunningWhile) {
      const completedIn = Date.now() - startedAtRef.current;
      setLastDurationMs(completedIn);
      setElapsedMs(0);
      startedAtRef.current = null;
    }
  }, [isLoading, keepRunningWhile]);

  useEffect(() => {
    if (startedAtRef.current == null) return;

    const interval = setInterval(() => {
      if (startedAtRef.current != null) {
        setElapsedMs(Date.now() - startedAtRef.current);
      }
    }, 100);

    return () => clearInterval(interval);
  }, [isLoading, keepRunningWhile]);

  const elapsedLabel = useMemo(() => formatDuration(elapsedMs), [elapsedMs]);
  const lastDurationLabel = useMemo(
    () => (lastDurationMs != null ? formatDuration(lastDurationMs) : null),
    [lastDurationMs]
  );

  return {
    elapsedMs,
    elapsedLabel,
    lastDurationMs,
    lastDurationLabel,
  };
}
