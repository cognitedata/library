/**
 * Walk all test_report_* pages and aggregate stats without holding every instance in memory.
 */

import type { CogniteClient } from "@cognite/sdk";
import { getInstanceQueryLimit } from "@/config/dataTypes";
import { withCdfConcurrency } from "@/utils/cdfRequest";
import { extractSummaryContent } from "@/utils/summaryDownload";
import { mapNodesToSummaryInstances } from "@/utils/summaryInstanceMapping";
import {
  buildSummaryListRequest,
  type SummaryServerListFilters,
} from "@/utils/summaryListQuery";
import { parseTestReportMetrics } from "@/utils/testingSave";
import {
  extractDataVolumeFromContent,
  extractScoreFromContent,
  getScoreCategory,
  type TestReportStats,
} from "@/utils/testReportUtils";

const EMPTY_FILTERS: SummaryServerListFilters = {
  dateFilterEnabled: false,
  dateRange: null,
  contentTypeFilterEnabled: false,
  selectedContentTypes: [],
};

/**
 * Aggregate test report statistics by streaming DMS pages (O(1) retained instances).
 */
export async function computeTestReportStatsStreaming(
  sdk: Pick<CogniteClient, "instances">,
): Promise<TestReportStats> {
  let totalTests = 0;
  let totalScore = 0;
  let totalVolume = 0;
  let excellent = 0;
  let good = 0;
  let poor = 0;

  let cursor: string | undefined;
  do {
    const req = buildSummaryListRequest({
      variant: "test",
      limit: getInstanceQueryLimit(),
      cursor,
      filters: EMPTY_FILTERS,
    });
    const response = await withCdfConcurrency(() => sdk.instances.list(req));
    const items = mapNodesToSummaryInstances(response.items);
    for (const report of items) {
      const metrics = parseTestReportMetrics(report.sourceContext);
      let score: number;
      let volume: number;
      if (metrics) {
        score = metrics.similarityScore;
        const counts = metrics.newDataCounts;
        volume = Object.values(counts).reduce((s, v) => s + (v ?? 0), 0);
      } else {
        // Legacy nodes written before structured metrics: fall back to regex.
        const { content } = extractSummaryContent(report);
        score = extractScoreFromContent(content);
        volume = extractDataVolumeFromContent(content);
      }

      totalTests += 1;
      totalScore += score;
      totalVolume += volume;

      const category = getScoreCategory(score);
      if (category === "excellent") excellent += 1;
      else if (category === "good") good += 1;
      else poor += 1;
    }
    cursor = response.nextCursor;
  } while (cursor);

  if (totalTests === 0) {
    return {
      totalTests: 0,
      averageScore: 0,
      excellentCount: 0,
      goodCount: 0,
      poorCount: 0,
      averageDataVolume: 0,
    };
  }

  return {
    totalTests,
    averageScore: totalScore / totalTests,
    excellentCount: excellent,
    goodCount: good,
    poorCount: poor,
    averageDataVolume: totalVolume / totalTests,
  };
}
