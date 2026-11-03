import { describe, expect, it, vi } from "vitest";
import type { ComparisonTestResult } from "@/types/operations";
import { parseTestReportMetrics, saveTestReportToCdf } from "./testingSave";

function makeResult(): ComparisonTestResult {
  return {
    originalSummary: {
      externalId: "s",
      space: "ops_summaries",
      createdTime: 1,
      properties: {},
    },
    originalContent: "# Orig",
    newSummary: "# New",
    comparisonAnalysis: "Analysis text",
    similarityScore: 88,
    dataAccuracyScore: 90,
    originalDataCounts: {
      assets: 2,
      files: 0,
      work_orders: 0,
      time_series: 0,
      equipment: 0,
    },
    newDataCounts: {
      assets: 2,
      files: 1,
      work_orders: 0,
      time_series: 0,
      equipment: 0,
    },
    timestamp: new Date("2024-06-01T10:00:00.000Z"),
  };
}

describe("saveTestReportToCdf", () => {
  it("calls instances.upsert with correct externalId", async () => {
    const upsert = vi.fn().mockResolvedValue(undefined);
    const sdk = { project: "proj", instances: { upsert } };

    await saveTestReportToCdf({
      sdk,
      testResult: makeResult(),
      testStartDate: "2024-01-01",
      testEndDate: "2024-01-31",
    });

    expect(upsert).toHaveBeenCalledTimes(1);
    const payload = upsert.mock.calls[0][0] as {
      items: Array<{ externalId: string }>;
    };
    expect(payload.items[0].externalId).toMatch(/^test_report_/);
  });

  it("writes structured metrics to sourceContext", async () => {
    const upsert = vi.fn().mockResolvedValue(undefined);
    const sdk = { project: "proj", instances: { upsert } };
    const result = makeResult();

    await saveTestReportToCdf({
      sdk,
      testResult: result,
      testStartDate: "2024-01-01",
      testEndDate: "2024-01-31",
    });

    const props = (
      upsert.mock.calls[0][0] as {
        items: Array<{
          sources: Array<{ properties: Record<string, unknown> }>;
        }>;
      }
    ).items[0].sources[0].properties;

    const metrics = parseTestReportMetrics(props.sourceContext as string);
    expect(metrics).not.toBeNull();
    expect(metrics?.similarityScore).toBe(88);
    expect(metrics?.dataAccuracyScore).toBe(90);
    expect(metrics?.periodStart).toBe("2024-01-01");
    expect(metrics?.periodEnd).toBe("2024-01-31");
    expect(metrics?.originalDataCounts.assets).toBe(2);
  });
});

describe("parseTestReportMetrics", () => {
  it("returns null for missing or non-JSON sourceContext", () => {
    expect(parseTestReportMetrics(undefined)).toBeNull();
    expect(parseTestReportMetrics("")).toBeNull();
    expect(parseTestReportMetrics("not json")).toBeNull();
    expect(parseTestReportMetrics('{"no":"score"}')).toBeNull();
  });

  it("parses valid metrics JSON", () => {
    const m = parseTestReportMetrics(
      JSON.stringify({
        similarityScore: 95,
        dataAccuracyScore: 92,
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
        periodStart: "2024-02-01",
        periodEnd: "2024-02-28",
        timestamp: "2024-02-28T12:00:00.000Z",
      }),
    );
    expect(m?.similarityScore).toBe(95);
    expect(m?.periodStart).toBe("2024-02-01");
  });
});
