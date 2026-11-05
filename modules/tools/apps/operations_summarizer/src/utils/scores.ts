/**
 * Summary text data extraction and comparison score helpers.
 */

import type {
  DataCounts,
  ScoreCategory,
  ScoreDisplay,
} from "@/types/operations";

/**
 * Regex patterns for extracting structured data counts from AI-generated summary text.
 *
 * Summaries are free-form natural language produced by an LLM, so the same count
 * can appear in many different phrasings (e.g. "Assets (42)", "42 new assets",
 * "a total of 42 new assets"). Each entry lists the phrasings we expect for one
 * data type, ordered from most-specific to least-specific so that an earlier,
 * unambiguous match wins over a generic fallback.
 */
const DATA_COUNT_PATTERNS: Array<{
  key: keyof DataCounts;
  patterns: RegExp[];
}> = [
  {
    key: "assets",
    patterns: [
      /Assets\s*\((\d+)\)/i,
      /(\d+)\s*new\s+assets/i,
      /total\s+of\s+(\d+)\s+new\s+assets/i,
      /(\d+)\s+assets/i,
      /asset\s+count[:\s]*(\d+)/i,
    ],
  },
  {
    key: "files",
    patterns: [
      /Files\s*\((\d+)\)/i,
      /(\d+)\s*technical\s+documents/i,
      /total\s+of\s+(\d+)\s+technical\s+documents/i,
      /(\d+)\s+files/i,
      /file\s+count[:\s]*(\d+)/i,
    ],
  },
  {
    key: "time_series",
    patterns: [
      /Time\s+Series\s*\((\d+)\)/i,
      /(\d+)\s*time\s+series/i,
      /total\s+of\s+(\d+)\s+time\s+series/i,
      /time\s+series\s+count[:\s]*(\d+)/i,
    ],
  },
  {
    key: "equipment",
    patterns: [
      /Equipment\s*\((\d+)\)/i,
      /(\d+)\s*equipment/i,
      /total\s+of\s+(\d+)\s+equipment/i,
      /equipment\s+count[:\s]*(\d+)/i,
    ],
  },
  {
    key: "work_orders",
    patterns: [
      /Work\s+Orders\s*\((\d+)\)/i,
      /(\d+)\s*work\s+orders/i,
      /total\s+of\s+(\d+)\s+work\s+orders/i,
      /work\s+order\s+count[:\s]*(\d+)/i,
    ],
  },
];

/**
 * Extract data type counts from summary text using regex patterns
 */
export function extractDataCounts(summaryText: string): DataCounts {
  const counts: DataCounts = {
    assets: 0,
    files: 0,
    work_orders: 0,
    time_series: 0,
    equipment: 0,
  };

  for (const { key, patterns } of DATA_COUNT_PATTERNS) {
    for (const pattern of patterns) {
      const match = summaryText.match(pattern);
      if (match) {
        const parsed = Number.parseInt(match[1], 10);
        if (!Number.isNaN(parsed)) {
          counts[key] = parsed;
          break;
        }
      }
    }
  }

  return counts;
}

/** Normalize summary body so two identical documents compare equal across CRLF / trailing space. */
export function normalizeSummaryTextForComparison(text: string): string {
  return text.replace(/\r\n/g, "\n").trim();
}

/**
 * Extract similarity score from LLM comparison text.
 * Tries explicit "overall similarity" lines first, then common shorthands.
 */
export function extractSimilarityScore(comparisonText: string): number | null {
  const patterns = [
    /\bSIMILARITY_SCORE\s*:\s*(\d+(?:\.\d+)?)\b/i,
    /\bOVERALL\s+SIMILARITY\s+SCORE\s*(?:\([^)]*\))?\s*:?\s*(\d+(?:\.\d+)?)\b/i,
    /(?:^|\n)\s*\d+\.\s*OVERALL\s+SIMILARITY\s+SCORE[^:]*:\s*(\d+(?:\.\d+)?)\b/im,
    /\bsimilarity\s+score\s*(?:\([^)]*\))?\s*:?\s*(\d+(?:\.\d+)?)\b/i,
    /\b(\d+(?:\.\d+)?)\s+out\s+of\s+100\b/i,
    /\bsimilarity\s+score\s*[:\s]+\s*(\d+(?:\.\d+)?)\b/i,
    /\b(?:final\s+)?score\s*[:\s]+\s*(\d+(?:\.\d+)?)\b/i,
    /(\d+(?:\.\d+)?)\s*(?:\/\s*100|\s*%)/,
    /\bsimilarity\s*[:\s]+\s*(\d+(?:\.\d+)?)\b/i,
  ];

  for (const pattern of patterns) {
    const match = comparisonText.match(pattern);
    if (match) {
      const score = Number.parseFloat(match[1]);
      if (!Number.isFinite(score)) continue;
      return Math.min(100, Math.max(0, Math.round(score)));
    }
  }

  return null;
}

/**
 * Get score category based on similarity score
 */
export function getScoreCategory(score: number | null): ScoreCategory {
  if (score === null) return "poor";
  if (score >= 85) return "excellent";
  if (score >= 75) return "good";
  if (score >= 60) return "fair";
  return "poor";
}

/**
 * Get score display information
 */
export function getScoreDisplay(score: number | null): ScoreDisplay {
  const category = getScoreCategory(score);

  const displays: Record<ScoreCategory, Omit<ScoreDisplay, "category">> = {
    excellent: {
      emoji: "🎉",
      description: "Excellent Consistency",
      className: "bg-green-100 text-green-800 border-green-500",
    },
    good: {
      emoji: "✅",
      description: "Good Consistency",
      className: "bg-blue-100 text-blue-800 border-blue-500",
    },
    fair: {
      emoji: "⚠️",
      description: "Fair Consistency",
      className: "bg-yellow-100 text-yellow-800 border-yellow-500",
    },
    poor: {
      emoji: "❌",
      description: "Poor Consistency",
      className: "bg-red-100 text-red-800 border-red-500",
    },
  };

  return {
    category,
    ...displays[category],
  };
}

/**
 * Calculate data accuracy score based on data counts and content coverage
 */
export function calculateDataAccuracyScore(
  originalSummary: string,
  newSummary: string,
): number {
  const originalCounts = extractDataCounts(originalSummary);
  const newCounts = extractDataCounts(newSummary);

  let countScore = 0;
  let totalDataTypes = 0;

  const allTypes = new Set([
    ...Object.keys(originalCounts),
    ...Object.keys(newCounts),
  ]) as Set<keyof DataCounts>;

  for (const dataType of allTypes) {
    const originalCount = originalCounts[dataType] || 0;
    const newCount = newCounts[dataType] || 0;
    totalDataTypes += 1;

    if (originalCount === newCount) {
      countScore += 100;
    } else if (originalCount === 0 && newCount === 0) {
      countScore += 100;
    } else if (originalCount === 0 || newCount === 0) {
      countScore += 0;
    } else {
      const diffPercent =
        Math.abs(originalCount - newCount) / Math.max(originalCount, newCount);
      if (diffPercent <= 0.05) {
        countScore += 95;
      } else if (diffPercent <= 0.1) {
        countScore += 90;
      } else if (diffPercent <= 0.2) {
        countScore += 75;
      } else if (diffPercent <= 0.5) {
        countScore += 50;
      } else {
        countScore += 20;
      }
    }
  }

  const countAccuracy = totalDataTypes > 0 ? countScore / totalDataTypes : 0;

  const dataTypesWithData = new Set<string>();
  for (const dataType of allTypes) {
    if ((originalCounts[dataType] || 0) > 0 || (newCounts[dataType] || 0) > 0) {
      dataTypesWithData.add(dataType);
    }
  }

  let coverageScore = 100;
  if (dataTypesWithData.size > 0) {
    const originalLower = originalSummary.toLowerCase();
    const newLower = newSummary.toLowerCase();

    const originalMentions = new Set<string>();
    const newMentions = new Set<string>();

    for (const dataType of dataTypesWithData) {
      if (originalLower.includes(dataType.replace(/_/g, " "))) {
        originalMentions.add(dataType);
      }
      if (newLower.includes(dataType.replace(/_/g, " "))) {
        newMentions.add(dataType);
      }
    }

    if (
      originalMentions.size === newMentions.size &&
      [...originalMentions].every((m) => newMentions.has(m))
    ) {
      coverageScore = 100;
    } else {
      const overlap = [...originalMentions].filter((m) =>
        newMentions.has(m),
      ).length;
      const total = new Set([...originalMentions, ...newMentions]).size;
      coverageScore = total > 0 ? (overlap / total) * 100 : 0;
    }
  }

  const finalScore = countAccuracy * 0.8 + coverageScore * 0.2;
  return Math.round(finalScore);
}
