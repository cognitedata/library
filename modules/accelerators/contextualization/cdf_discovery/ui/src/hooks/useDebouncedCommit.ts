import { useEffect, useState } from "react";

/** Local draft with debounced upstream commit (avoids parent re-render on every keystroke). */
export function useDebouncedCommit<T>(
  committed: T,
  onCommit: (value: T) => void,
  delayMs = 400,
  syncKey?: string | number
): readonly [T, (value: T) => void] {
  const [draft, setDraft] = useState(committed);

  useEffect(() => {
    setDraft(committed);
  }, [committed, syncKey]);

  useEffect(() => {
    if (draft === committed) return;
    const id = window.setTimeout(() => onCommit(draft), delayMs);
    return () => window.clearTimeout(id);
  }, [draft, committed, onCommit, delayMs]);

  return [draft, setDraft] as const;
}
