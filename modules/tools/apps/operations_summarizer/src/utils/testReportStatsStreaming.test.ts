import { describe, expect, it, vi } from "vitest";
import { cogniteClientInstancesMock } from "@/test-utils/testDoubles";
import { computeTestReportStatsStreaming } from "./testReportStatsStreaming";

const VIEW_KEY = "cdf_cdm/CogniteFile/v1";

function testNode(
  externalId: string,
  content: string,
): {
  instanceType: "node";
  externalId: string;
  space: string;
  createdTime: number;
  properties: Record<string, Record<string, unknown>>;
} {
  return {
    instanceType: "node",
    externalId,
    space: "ops_summaries",
    createdTime: 1,
    properties: {
      [VIEW_KEY]: { content, name: externalId },
    },
  };
}

describe("computeTestReportStatsStreaming", () => {
  it("returns zeros when no reports", async () => {
    const list = vi
      .fn()
      .mockResolvedValue({ items: [], nextCursor: undefined });
    const stats = await computeTestReportStatsStreaming(
      cogniteClientInstancesMock({ instances: { list } }),
    );
    expect(stats).toEqual({
      totalTests: 0,
      averageScore: 0,
      excellentCount: 0,
      goodCount: 0,
      poorCount: 0,
      averageDataVolume: 0,
    });
    expect(list).toHaveBeenCalled();
  });

  it("aggregates across paginated list", async () => {
    const n1 = testNode(
      "test_report_1",
      "Similarity Score: 95/100\nTotal Data: 10",
    );
    const n2 = testNode(
      "test_report_2",
      "Similarity Score: 75/100\nTotal Data: 20",
    );
    const n3 = testNode(
      "test_report_3",
      "Similarity Score: 50/100\nTotal Data: 30",
    );

    const list = vi
      .fn()
      .mockResolvedValueOnce({
        items: [n1, n2],
        nextCursor: "c2",
      })
      .mockResolvedValueOnce({
        items: [n3],
        nextCursor: undefined,
      });

    const stats = await computeTestReportStatsStreaming(
      cogniteClientInstancesMock({ instances: { list } }),
    );

    expect(stats.totalTests).toBe(3);
    expect(stats.excellentCount).toBe(1);
    expect(stats.goodCount).toBe(1);
    expect(stats.poorCount).toBe(1);
    expect(stats.averageScore).toBeCloseTo((95 + 75 + 50) / 3, 5);
    expect(stats.averageDataVolume).toBeCloseTo((10 + 20 + 30) / 3, 5);
    expect(list).toHaveBeenCalledTimes(2);
  });
});
