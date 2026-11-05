/**
 * Walk all test_report_* pages and aggregate stats without holding every instance in memory.
 * The Tests page read path uses the persisted rollup; this scan runs only to backfill it.
 */

import type { CogniteClient } from "@cognite/sdk";
import { getInstanceQueryLimit } from "@/config/dataTypes";
import type { SummaryInstance } from "@/types/operations";
import { VIEW_IDS } from "@/types/operations";
import { withCdfConcurrency } from "@/utils/cdfRequest";
import { extractFileProperties } from "@/utils/nodeProperties";
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

/** Raw counters from a full test-report scan. Averages are derived when persisting. */
export interface TestReportStatSums {
  totalTests: number;
  totalScore: number;
  totalVolume: number;
  excellentCount: number;
  goodCount: number;
  poorCount: number;
}

const EMPTY_FILTERS: SummaryServerListFilters = {
  dateFilterEnabled: false,
  dateRange: null,
  contentTypeFilterEnabled: false,
  selectedContentTypes: [],
};

function sourceContextOf(report: SummaryInstance): string | undefined {
  if (report.sourceContext) return report.sourceContext;
  const props = extractFileProperties(report, VIEW_IDS.SUMMARY_OUTPUT_FILE);
  const raw = props?.sourceContext;
  return typeof raw === "string" ? raw : undefined;
}

/** Score and data volume for one test report (structured metrics, else legacy markdown). */
export function scoreVolumeFromSummary(report: SummaryInstance): {
  score: number;
  volume: number;
} {
  const metrics = parseTestReportMetrics(sourceContextOf(report));
  if (metrics) {
    const volume = Object.values(metrics.newDataCounts).reduce(
      (sum, value) => sum + (value ?? 0),
      0,
    );
    return { score: metrics.similarityScore, volume };
  }
  const { content } = extractSummaryContent(report);
  return {
    score: extractScoreFromContent(content),
    volume: extractDataVolumeFromContent(content),
  };
}

/**
 * Aggregate test report statistics by streaming DMS pages (O(1) retained instances).
 * Includes raw sums so a persisted rollup can apply later deltas exactly.
 */
export async function computeTestReportStatsRollup(sdk: {
  instances: Pick<CogniteClient["instances"], "list">;
}): Promise<TestReportStatSums> {
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
      const { score, volume } = scoreVolumeFromSummary(report);
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

  return {
    totalTests,
    totalScore,
    totalVolume,
    excellentCount: excellent,
    goodCount: good,
    poorCount: poor,
  };
}

/**
 * Aggregate test report statistics by streaming DMS pages (O(1) retained instances).
 */
export async function computeTestReportStatsStreaming(sdk: {
  instances: Pick<CogniteClient["instances"], "list">;
}): Promise<TestReportStats> {
  const sums = await computeTestReportStatsRollup(sdk);
  if (sums.totalTests === 0) {
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
    totalTests: sums.totalTests,
    averageScore: sums.totalScore / sums.totalTests,
    excellentCount: sums.excellentCount,
    goodCount: sums.goodCount,
    poorCount: sums.poorCount,
    averageDataVolume: sums.totalVolume / sums.totalTests,
  };
}
