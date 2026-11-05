import { describe, expect, it, vi } from "vitest";
import {
  applyRollupDelta,
  applyTestReportStatsDelta,
  emptyTestReportStatsRollup,
  readOrBackfillTestReportStats,
  TEST_REPORT_STATS_EXTERNAL_ID,
} from "./testReportStatsRollup";

const VIEW_KEY = "cdf_cdm/CogniteFile/v1";

function rollupNode(sourceContext: string) {
  return {
    instanceType: "node" as const,
    externalId: TEST_REPORT_STATS_EXTERNAL_ID,
    space: "ops_summaries",
    properties: { [VIEW_KEY]: { sourceContext } },
  };
}

const stored = {
  totalTests: 2,
  averageScore: 80,
  excellentCount: 1,
  goodCount: 1,
  poorCount: 0,
  averageDataVolume: 15,
  totalScore: 160,
  totalVolume: 30,
};

describe("test report stats rollup", () => {
  it("keeps averages exact when a report is added", () => {
    const next = applyRollupDelta(stored, {
      score: 50,
      volume: 30,
      direction: "add",
    });
    expect(next.totalTests).toBe(3);
    expect(next.totalScore).toBe(210);
    expect(next.totalVolume).toBe(60);
    expect(next.averageScore).toBeCloseTo(70, 5);
    expect(next.averageDataVolume).toBeCloseTo(20, 5);
    expect(next.poorCount).toBe(1);
  });

  it("reads the stored node without listing test reports", async () => {
    const retrieve = vi
      .fn()
      .mockResolvedValue({ items: [rollupNode(JSON.stringify(stored))] });
    const list = vi.fn();
    const stats = await readOrBackfillTestReportStats({
      instances: { retrieve, list, upsert: vi.fn() },
    });
    expect(stats.totalTests).toBe(2);
    expect(stats.averageScore).toBe(80);
    expect(list).not.toHaveBeenCalled();
    expect(retrieve).toHaveBeenCalledTimes(1);
  });

  it("scans once and upserts when the rollup node is missing", async () => {
    const retrieve = vi.fn().mockResolvedValue({ items: [] });
    const list = vi
      .fn()
      .mockResolvedValue({ items: [], nextCursor: undefined });
    const upsert = vi.fn().mockResolvedValue({});
    const stats = await readOrBackfillTestReportStats({
      instances: { retrieve, list, upsert },
    });
    expect(stats).toEqual({
      totalTests: 0,
      averageScore: 0,
      excellentCount: 0,
      goodCount: 0,
      poorCount: 0,
      averageDataVolume: 0,
    });
    expect(list).toHaveBeenCalledTimes(1);
    const payload = upsert.mock.calls[0][0] as {
      items: Array<{ externalId: string }>;
    };
    expect(payload.items[0].externalId).toBe(TEST_REPORT_STATS_EXTERNAL_ID);
    expect(payload.items[0].externalId.startsWith("test_report_")).toBe(false);
    expect(payload.items[0].externalId.startsWith("ops_summary_")).toBe(false);
  });

  it("skips the delta when the rollup node does not exist", async () => {
    const retrieve = vi.fn().mockResolvedValue({ items: [] });
    const upsert = vi.fn();
    await applyTestReportStatsDelta(
      { instances: { retrieve, upsert } },
      { score: 90, volume: 1, direction: "add" },
    );
    expect(upsert).not.toHaveBeenCalled();
  });

  it("clears the rollup when the last test is removed", () => {
    expect(
      applyRollupDelta(
        {
          ...emptyTestReportStatsRollup(),
          totalTests: 1,
          totalScore: 90,
          totalVolume: 4,
          excellentCount: 1,
          averageScore: 90,
          averageDataVolume: 4,
        },
        { score: 90, volume: 4, direction: "remove" },
      ),
    ).toEqual(emptyTestReportStatsRollup());
  });
});
