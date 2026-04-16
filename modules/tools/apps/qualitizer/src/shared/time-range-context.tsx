import { createContext, useCallback, useContext, useMemo, useState } from "react";

export type TimeRangePreset = "12h" | "1d" | "7d" | "30d";
export type TimeRangeKind = TimeRangePreset | "custom";

export type TimeRangeValue =
  | { kind: TimeRangePreset }
  | { kind: "custom"; startMs: number; endMs: number };

const PRESET_HOURS: Record<TimeRangePreset, number> = {
  "12h": 12,
  "1d": 24,
  "7d": 24 * 7,
  "30d": 24 * 30,
};

const STORAGE_KEY = "qualitizer.timeRange";
const DEFAULT: TimeRangeValue = { kind: "1d" };

function load(): TimeRangeValue {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return DEFAULT;
    const parsed = JSON.parse(raw) as Partial<TimeRangeValue> & { kind?: string };
    if (parsed.kind === "custom") {
      const s = Number((parsed as { startMs?: unknown }).startMs);
      const e = Number((parsed as { endMs?: unknown }).endMs);
      if (Number.isFinite(s) && Number.isFinite(e) && e > s) {
        return { kind: "custom", startMs: s, endMs: e };
      }
      return DEFAULT;
    }
    if (parsed.kind === "12h" || parsed.kind === "1d" || parsed.kind === "7d" || parsed.kind === "30d") {
      return { kind: parsed.kind };
    }
    return DEFAULT;
  } catch {
    return DEFAULT;
  }
}

function save(value: TimeRangeValue) {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(value));
  } catch {
    // ignore
  }
}

export function resolveRange(value: TimeRangeValue, now = Date.now()): { startMs: number; endMs: number } {
  if (value.kind === "custom") {
    return { startMs: value.startMs, endMs: value.endMs };
  }
  const hours = PRESET_HOURS[value.kind];
  return { startMs: now - hours * 60 * 60 * 1000, endMs: now };
}

type TimeRangeContextValue = {
  range: TimeRangeValue;
  setRange: (next: TimeRangeValue) => void;
  startMs: number;
  endMs: number;
};

const TimeRangeContext = createContext<TimeRangeContextValue | null>(null);

export function TimeRangeProvider({ children }: { children: React.ReactNode }) {
  const [range, setRangeState] = useState<TimeRangeValue>(load);

  const setRange = useCallback((next: TimeRangeValue) => {
    setRangeState(next);
    save(next);
  }, []);

  const value = useMemo<TimeRangeContextValue>(() => {
    const { startMs, endMs } = resolveRange(range);
    return { range, setRange, startMs, endMs };
  }, [range, setRange]);

  return <TimeRangeContext.Provider value={value}>{children}</TimeRangeContext.Provider>;
}

export function useTimeRange() {
  const ctx = useContext(TimeRangeContext);
  if (!ctx) throw new Error("useTimeRange must be used within TimeRangeProvider");
  return ctx;
}

export function formatRangeLabel(range: TimeRangeValue): string {
  if (range.kind === "custom") {
    const s = new Date(range.startMs).toISOString().slice(0, 16).replace("T", " ");
    const e = new Date(range.endMs).toISOString().slice(0, 16).replace("T", " ");
    return `${s} → ${e} UTC`;
  }
  return range.kind;
}
