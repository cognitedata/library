export type CanvasNodeRunProgress = {
  current: number;
  total?: number;
  label?: string;
  /** Wall-clock start for live elapsed display (ms since epoch). */
  startedAtMs?: number;
  /** Fixed elapsed duration once the task completes (ms). */
  elapsedMs?: number;
};

export function canvasNodeProgressPercent(progress: CanvasNodeRunProgress | undefined): number | null {
  if (!progress) return null;
  const { current, total } = progress;
  if (total == null || total <= 0) return null;
  return Math.min(100, Math.round((Math.max(0, current) / total) * 100));
}

export function canvasNodeProgressVisible(progress: CanvasNodeRunProgress | undefined): boolean {
  return progress != null;
}

/** Format elapsed milliseconds as M:SS or H:MM:SS (tabular-friendly). */
export function formatNodeRunElapsedMs(ms: number): string {
  const totalSec = Math.max(0, Math.floor(ms / 1000));
  const hours = Math.floor(totalSec / 3600);
  const minutes = Math.floor((totalSec % 3600) / 60);
  const seconds = totalSec % 60;
  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

export function resolveNodeRunElapsedMs(
  progress: CanvasNodeRunProgress,
  nowMs: number = Date.now()
): number | undefined {
  if (typeof progress.elapsedMs === "number" && Number.isFinite(progress.elapsedMs)) {
    return Math.max(0, Math.floor(progress.elapsedMs));
  }
  if (typeof progress.startedAtMs === "number" && Number.isFinite(progress.startedAtMs)) {
    return Math.max(0, nowMs - progress.startedAtMs);
  }
  return undefined;
}
