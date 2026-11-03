import { describe, expect, it } from "vitest";
import {
  calculateDataAccuracyScore,
  extractDataCounts,
  extractSimilarityScore,
  getScoreCategory,
  getScoreDisplay,
  normalizeSummaryTextForComparison,
} from "./scores";

describe("extractDataCounts", () => {
  it("extracts counts from summary text", () => {
    const text = "Assets (10). Files (5). Work Orders (3).";
    const counts = extractDataCounts(text);
    expect(counts.assets).toBe(10);
    expect(counts.files).toBe(5);
    expect(counts.work_orders).toBe(3);
  });
  it("returns zeros when no patterns match", () => {
    const counts = extractDataCounts("no numbers here");
    expect(counts).toEqual({
      assets: 0,
      files: 0,
      work_orders: 0,
      time_series: 0,
      equipment: 0,
    });
  });
});

describe("extractSimilarityScore", () => {
  it("extracts score from comparison text", () => {
    expect(extractSimilarityScore("similarity score: 85")).toBe(85);
    expect(extractSimilarityScore("Score: 90")).toBe(90);
    expect(extractSimilarityScore("85%")).toBe(85);
  });
  it("parses overall similarity line without percent sign", () => {
    expect(
      extractSimilarityScore(
        "5. OVERALL SIMILARITY SCORE (0-100): 100\nSome notes.",
      ),
    ).toBe(100);
    expect(extractSimilarityScore("OVERALL SIMILARITY SCORE (0-100): 98")).toBe(
      98,
    );
  });
  it("parses SIMILARITY_SCORE machine line", () => {
    expect(extractSimilarityScore("Analysis.\nSIMILARITY_SCORE: 100")).toBe(
      100,
    );
  });
  it("clamps to 0-100", () => {
    expect(extractSimilarityScore("150%")).toBe(100);
  });
  it("returns null when no match", () => {
    expect(extractSimilarityScore("no score")).toBe(null);
  });
});

describe("normalizeSummaryTextForComparison", () => {
  it("normalizes CRLF and trims", () => {
    expect(normalizeSummaryTextForComparison("  a\r\nb  ")).toBe("a\nb");
  });
});

describe("getScoreCategory", () => {
  it("returns excellent for >= 85", () => {
    expect(getScoreCategory(85)).toBe("excellent");
    expect(getScoreCategory(100)).toBe("excellent");
  });
  it("returns good for 75-84", () => {
    expect(getScoreCategory(75)).toBe("good");
    expect(getScoreCategory(84)).toBe("good");
  });
  it("returns fair for 60-74", () => {
    expect(getScoreCategory(60)).toBe("fair");
    expect(getScoreCategory(74)).toBe("fair");
  });
  it("returns poor for < 60 or null", () => {
    expect(getScoreCategory(59)).toBe("poor");
    expect(getScoreCategory(null)).toBe("poor");
  });
});

describe("getScoreDisplay", () => {
  it("returns display with category, emoji, description", () => {
    const display = getScoreDisplay(90);
    expect(display.category).toBe("excellent");
    expect(display.emoji).toBeTruthy();
    expect(display.description).toBeTruthy();
    expect(display.className).toBeTruthy();
  });
});

describe("calculateDataAccuracyScore", () => {
  it("returns high score when counts match", () => {
    const orig = "Assets (10). Files (5).";
    const same = "Assets (10). Files (5).";
    const score = calculateDataAccuracyScore(orig, same);
    expect(score).toBeGreaterThanOrEqual(90);
  });
  it("returns lower score when counts differ", () => {
    const orig = "Assets (10). Files (5).";
    const diff = "Assets (5). Files (20).";
    const score = calculateDataAccuracyScore(orig, diff);
    expect(score).toBeLessThan(100);
  });
});
