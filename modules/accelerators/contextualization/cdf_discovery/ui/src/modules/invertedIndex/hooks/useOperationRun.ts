import { useCallback, useRef, useState } from "react";
import { consumeOperationStream, OperationCancelledError } from "../utils/operationStream";

export function useOperationRun() {
  const [loading, setLoading] = useState(false);
  const [cancelled, setCancelled] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<unknown>(null);
  const [log, setLog] = useState("");
  const abortRef = useRef<AbortController | null>(null);

  const appendLog = useCallback((line: string) => {
    setLog((prev) => (prev ? `${prev}\n${line}` : line));
  }, []);

  const run = useCallback(
    async (streamUrl: string, body: unknown) => {
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      setLoading(true);
      setCancelled(false);
      setError(null);
      setResult(null);
      setLog("");
      let streamError: string | null = null;

      try {
        const exitCode = await consumeOperationStream(
          streamUrl,
          body,
          {
            onLog: appendLog,
            onResult: (data) => setResult(data),
            onError: (detail) => {
              streamError = detail;
              setError(detail);
            },
          },
          controller.signal
        );
        if (exitCode !== 0 && !streamError) {
          setError("Operation failed");
        }
      } catch (e) {
        if (e instanceof OperationCancelledError) {
          appendLog("[operation] cancelled");
          setCancelled(true);
        } else {
          setError(String(e));
          setResult(null);
        }
      } finally {
        if (abortRef.current === controller) {
          abortRef.current = null;
        }
        setLoading(false);
      }
    },
    [appendLog]
  );

  const cancel = useCallback(() => {
    abortRef.current?.abort();
  }, []);

  const clearLog = useCallback(() => {
    setLog("");
    setError(null);
    setResult(null);
    setCancelled(false);
  }, []);

  return { loading, cancelled, error, result, log, run, cancel, clearLog };
}
