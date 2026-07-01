import { useEffect, useRef, useState } from "react";

interface UseClipboardOptions {
  resetAfterMs?: number;
}

export function useClipboard(options?: UseClipboardOptions) {
  const resetAfterMs = options?.resetAfterMs ?? 1200;
  const [copiedValue, setCopiedValue] = useState<string | null>(null);
  const timeoutRef = useRef<number | null>(null);

  const clearTimer = () => {
    if (timeoutRef.current !== null) {
      window.clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }
  };

  useEffect(() => {
    return () => {
      clearTimer();
    };
  }, []);

  const copyValue = async (value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      setCopiedValue(value);
      clearTimer();
      timeoutRef.current = window.setTimeout(() => {
        setCopiedValue((current) => (current === value ? null : current));
        timeoutRef.current = null;
      }, resetAfterMs);
    } catch {
      setCopiedValue(null);
    }
  };

  return {
    copiedValue,
    copyValue,
    setCopiedValue,
  };
}
