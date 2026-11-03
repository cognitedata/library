/**
 * Map summarizer function log lines and elapsed time onto the progress bar.
 * The function call itself only reports Running until it finishes.
 */

const DATA_TYPE_PROGRESS: Record<string, number> = {
  assets: 32,
  files: 44,
  work_orders: 56,
  time_series: 68,
  equipment: 80,
};

export interface SummaryLogProgress {
  message: string;
  progress: number;
  currentStep: string;
}

export function progressFromSummaryLog(
  line: string,
): SummaryLogProgress | null {
  const text = line.trim();
  if (text.includes("OPERATIONS SUMMARIZER v2 STARTED")) {
    return {
      message: "Starting summary",
      progress: 12,
      currentStep: "Starting",
    };
  }
  if (text.includes("Ensuring agent") || text.startsWith("Using agent")) {
    return {
      message: "Preparing the agent",
      progress: 20,
      currentStep: "Agent",
    };
  }
  const processing = /Processing ([a-z_]+)/.exec(text);
  if (processing) {
    const name = processing[1];
    const label = name.replaceAll("_", " ");
    return {
      message: `Summarizing ${label}`,
      progress: DATA_TYPE_PROGRESS[name] ?? 48,
      currentStep: label,
    };
  }
  if (text.includes("Saving summary")) {
    return { message: "Saving summary", progress: 92, currentStep: "Saving" };
  }
  return null;
}

/** Creeps from 12 toward 88 so the bar keeps moving when logs have not arrived yet. */
export function creptGenerationProgress(elapsedSeconds: number): number {
  const crept = 12 + 76 * (1 - Math.exp(-elapsedSeconds / 75));
  return Math.min(88, Math.round(crept));
}
