import { describe, expect, it } from "vitest";
import type { ComparisonTestResult } from "@/types/operations";
import { buildTestingPageYaml } from "./testingYaml";

describe("buildTestingPageYaml", () => {
  it("builds multi-line yaml with indented content", () => {
    const ts = new Date("2024-03-01T12:00:00.000Z");
    const result: ComparisonTestResult = {
      originalSummary: {
        externalId: "s",
        space: "ops_summaries",
        createdTime: 1,
        properties: {},
      },
      originalContent: "line1\nline2",
      newSummary: "new",
      comparisonAnalysis: "ok",
      timestamp: ts,
      originalDataCounts: {
        assets: 1,
        files: 0,
        work_orders: 0,
        time_series: 0,
        equipment: 0,
      },
      newDataCounts: {
        assets: 1,
        files: 0,
        work_orders: 0,
        time_series: 0,
        equipment: 0,
      },
      similarityScore: 92,
      dataAccuracyScore: 90,
    };

    const yaml = buildTestingPageYaml(result, "2024-01-01", "2024-01-31", [
      "assets",
      "files",
    ]);

    expect(yaml).toContain("test_name:");
    expect(yaml).toContain("line1");
    expect(yaml).toContain("similarity_score: 92");
    expect(yaml).toContain("- assets");
  });
});
