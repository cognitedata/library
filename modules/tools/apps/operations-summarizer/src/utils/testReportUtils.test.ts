import { describe, expect, it } from "vitest";
import type { SummaryInstance } from "@/types/operations";
import {
  calculateTestReportStats,
  extractAccuracyFromContent,
  extractDataTypesFromContent,
  extractDataVolumeFromContent,
  extractPeriodFromContent,
  extractScoreFromContent,
  extractTestReportData,
  getScoreBadgeVariant,
  getScoreCategory,
  SCORE_THRESHOLDS,
  vividScoreBadgeClassName,
} from "./testReportUtils";

describe("SCORE_THRESHOLDS", () => {
  it("has EXCELLENT and GOOD thresholds", () => {
    expect(SCORE_THRESHOLDS.EXCELLENT).toBe(90);
    expect(SCORE_THRESHOLDS.GOOD).toBe(70);
  });
});

describe("extractScoreFromContent", () => {
  it("extracts Similarity Score: N/100", () => {
    expect(extractScoreFromContent("Similarity Score: 85/100")).toBe(85);
  });
  it("extracts emoji score format", () => {
    expect(extractScoreFromContent("🎉 90/100")).toBe(90);
  });
  it("extracts Score: N", () => {
    expect(extractScoreFromContent("Score: 75")).toBe(75);
  });
  it("extracts similarity_score: N", () => {
    expect(extractScoreFromContent("similarity_score: 80")).toBe(80);
  });
  it("extracts percentage", () => {
    expect(extractScoreFromContent("**Similarity Score:** 70%")).toBe(70);
  });
  it("returns 0 when no match", () => {
    expect(extractScoreFromContent("no score here")).toBe(0);
  });
});

describe("extractAccuracyFromContent", () => {
  it("extracts Data Accuracy Score N/100", () => {
    expect(extractAccuracyFromContent("Data Accuracy Score: 88/100")).toBe(88);
  });
  it("falls back to similarity when no accuracy pattern", () => {
    expect(extractAccuracyFromContent("Similarity Score: 90/100")).toBe(90);
  });
});

describe("extractDataVolumeFromContent", () => {
  it("extracts Total Data: N", () => {
    expect(extractDataVolumeFromContent("Total Data: 250")).toBe(250);
  });
  it("sums data type counts when patterns match", () => {
    const content = "10 assets. 5 files. 3 equipment.";
    expect(extractDataVolumeFromContent(content)).toBe(18);
  });
});

describe("extractPeriodFromContent", () => {
  it("extracts date range format", () => {
    expect(
      extractPeriodFromContent("Date Range: 2024-01-01 to 2024-01-31"),
    ).toContain("2024-01-01");
  });
  it("extracts from external ID format", () => {
    expect(
      extractPeriodFromContent("test_report_2024-01-01_2024-01-31"),
    ).toContain("2024-01-01");
  });
  it("returns empty string when no match", () => {
    expect(extractPeriodFromContent("no dates")).toBe("");
  });
});

describe("extractDataTypesFromContent", () => {
  it("detects data types from content", () => {
    const types = extractDataTypesFromContent("Assets (10). Files (5).");
    expect(types).toContain("Assets");
    expect(types).toContain("Files");
  });
});

describe("getScoreBadgeVariant", () => {
  it("returns success for >= 90", () => {
    expect(getScoreBadgeVariant(90)).toBe("success");
  });
  it("returns warning for >= 70", () => {
    expect(getScoreBadgeVariant(75)).toBe("warning");
  });
  it("returns error for < 70", () => {
    expect(getScoreBadgeVariant(50)).toBe("error");
  });
});

describe("vividScoreBadgeClassName", () => {
  it("returns emerald for success", () => {
    expect(vividScoreBadgeClassName("success")).toContain("emerald");
  });
  it("returns amber for warning", () => {
    expect(vividScoreBadgeClassName("warning")).toContain("amber");
  });
  it("returns red for error", () => {
    expect(vividScoreBadgeClassName("error")).toContain("red");
  });
});

describe("getScoreCategory", () => {
  it("returns excellent for >= 90", () => {
    expect(getScoreCategory(90)).toBe("excellent");
  });
  it("returns good for >= 70", () => {
    expect(getScoreCategory(70)).toBe("good");
  });
  it("returns poor for < 70", () => {
    expect(getScoreCategory(69)).toBe("poor");
  });
});

describe("calculateTestReportStats", () => {
  it("returns zeros for empty reports", () => {
    const stats = calculateTestReportStats([]);
    expect(stats.totalTests).toBe(0);
    expect(stats.averageScore).toBe(0);
  });

  it("computes stats from report instances", () => {
    const reports: SummaryInstance[] = [
      {
        externalId: "r1",
        space: "s",
        createdTime: 1,
        properties: {
          "cdf_cdm/CogniteFile/v1": {
            content: "Similarity Score: 80/100. Assets: 10. Files: 5.",
          },
        },
      },
      {
        externalId: "r2",
        space: "s",
        createdTime: 2,
        properties: {
          "cdf_cdm/CogniteFile/v1": {
            content: "Similarity Score: 100/100. Assets: 20.",
          },
        },
      },
    ];
    const stats = calculateTestReportStats(reports);
    expect(stats.totalTests).toBe(2);
    expect(stats.averageScore).toBe(90);
  });
});

describe("extractTestReportData", () => {
  it("extracts name, content, score, date from report", () => {
    const report: SummaryInstance = {
      externalId: "test_1",
      space: "s",
      createdTime: Date.now(),
      properties: {
        "cdf_cdm/CogniteFile/v1": {
          name: "Report 1",
          content: "Similarity Score: 85/100. Data Accuracy: 90/100.",
        },
      },
    };
    const data = extractTestReportData(report, "Jan 1, 2024");
    expect(data.name).toBe("Report 1");
    expect(data.content).toContain("85");
    expect(data.score).toBe(85);
    expect(data.date).toBe("Jan 1, 2024");
  });
});
