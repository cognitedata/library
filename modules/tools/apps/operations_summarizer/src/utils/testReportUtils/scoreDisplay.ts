import { SCORE_THRESHOLDS } from "./scoreExtraction";

/**
 * Aura Badge variant for a numeric score (semantic status tokens).
 */
export function getScoreBadgeVariant(
	score: number
): "success" | "warning" | "error" {
	if (score >= SCORE_THRESHOLDS.EXCELLENT) return "success";
	if (score >= SCORE_THRESHOLDS.GOOD) return "warning";
	return "error";
}

/**
 * Strong fill colors for test score badges (excellent / good / needs attention).
 * Success and warning use Aura status tokens (dark fill, light text) which reach 4.5:1;
 * emerald-600 / amber-600 with white text measured 3.65 / 3.57.
 */
export function vividScoreBadgeClassName(
	variant: "success" | "warning" | "error"
): string {
	switch (variant) {
		case "success":
			return "!border-transparent !bg-success-foreground !text-success-background";
		case "warning":
			return "!border-transparent !bg-warning-foreground !text-warning-background";
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
