import { SCORE_THRESHOLDS } from "./scoreExtraction";

/**
 * Aura Badge variant for a numeric score (semantic status tokens).
 */
export function getScoreBadgeVariant(
  score: number,
): "success" | "warning" | "error" {
  if (score >= SCORE_THRESHOLDS.EXCELLENT) return "success";
  if (score >= SCORE_THRESHOLDS.GOOD) return "warning";
  return "error";
}

/**
 * Strong fill colors for test score badges (excellent / good / needs attention).
 */
export function vividScoreBadgeClassName(
  variant: "success" | "warning" | "error",
): string {
  switch (variant) {
    case "success":
      return "!border-transparent !bg-emerald-600 !text-white";
    case "warning":
      return "!border-transparent !bg-amber-600 !text-white";
    case "error":
      return "!border-transparent !bg-red-600 !text-white";
  }
}

/**
 * Get the score category based on the score value.
 */
export function getScoreCategory(score: number): "excellent" | "good" | "poor" {
  if (score >= SCORE_THRESHOLDS.EXCELLENT) return "excellent";
  if (score >= SCORE_THRESHOLDS.GOOD) return "good";
  return "poor";
}
