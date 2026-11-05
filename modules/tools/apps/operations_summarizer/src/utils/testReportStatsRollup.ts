/**
 * One CogniteFile node holds test-report dashboard stats so the Tests page
 * does not scan every test_report_* node on open.
 * External id is fixed and must not use the test_report_ or ops_summary_ prefixes.
 */

import type { CogniteClient } from "@cognite/sdk";
import { getSummarySpace } from "@/config/dataTypes";
import { VIEW_IDS } from "@/types/operations";
import { withCdfConcurrency } from "@/utils/cdfRequest";
import { extractFileProperties } from "@/utils/nodeProperties";
import { computeTestReportStatsRollup } from "@/utils/testReportStatsStreaming";
import {
  getScoreCategory,
  type TestReportStats,
} from "@/utils/testReportUtils";

/** Stored in getSummarySpace(). Not a test report and not an operation summary. */
export const TEST_REPORT_STATS_EXTERNAL_ID = "ops_summarizer_test_report_stats";

/** TestReportStats plus raw sums so later add/remove deltas keep averages exact. */
export interface TestReportStatsRollup extends TestReportStats {
  totalScore: number;
  totalVolume: number;
}

export type TestReportStatsDelta = {
  score: number;
  volume: number;
  direction: "add" | "remove";
};

type RetrieveUpsertClient = {
  instances: Pick<CogniteClient["instances"], "retrieve" | "upsert">;
};

type RetrieveUpsertListClient = RetrieveUpsertClient & {
  instances: Pick<CogniteClient["instances"], "retrieve" | "upsert" | "list">;
};

const ROLLUP_KEYS = [
  "totalTests",
  "averageScore",
  "excellentCount",
  "goodCount",
  "poorCount",
  "averageDataVolume",
  "totalScore",
  "totalVolume",
] as const;

function fileSource() {
  const viewId = VIEW_IDS.SUMMARY_OUTPUT_FILE;
  return {
    type: "view" as const,
    space: viewId.space,
    externalId: viewId.externalId,
    version: viewId.version,
  };
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

export function emptyTestReportStatsRollup(): TestReportStatsRollup {
  return {
    totalTests: 0,
    averageScore: 0,
    excellentCount: 0,
    goodCount: 0,
    poorCount: 0,
    averageDataVolume: 0,
    totalScore: 0,
    totalVolume: 0,
  };
}

export function rollupFromSums(sums: {
  totalTests: number;
  totalScore: number;
  totalVolume: number;
  excellentCount: number;
  goodCount: number;
  poorCount: number;
}): TestReportStatsRollup {
  if (sums.totalTests <= 0) return emptyTestReportStatsRollup();
  return {
    totalTests: sums.totalTests,
    totalScore: sums.totalScore,
    totalVolume: sums.totalVolume,
    excellentCount: sums.excellentCount,
    goodCount: sums.goodCount,
    poorCount: sums.poorCount,
    averageScore: sums.totalScore / sums.totalTests,
    averageDataVolume: sums.totalVolume / sums.totalTests,
  };
}

export function parseTestReportStatsRollup(
  sourceContext: string | undefined,
): TestReportStatsRollup | null {
  if (!sourceContext?.trim()) return null;
  try {
    const parsed = JSON.parse(sourceContext) as unknown;
    if (typeof parsed !== "object" || parsed === null) return null;
    const record = parsed as Record<string, unknown>;
    for (const key of ROLLUP_KEYS) {
      if (!isFiniteNumber(record[key])) return null;
    }
    return {
      totalTests: record.totalTests as number,
      averageScore: record.averageScore as number,
      excellentCount: record.excellentCount as number,
      goodCount: record.goodCount as number,
      poorCount: record.poorCount as number,
      averageDataVolume: record.averageDataVolume as number,
      totalScore: record.totalScore as number,
      totalVolume: record.totalVolume as number,
    };
  } catch {
    return null;
  }
}

export function applyRollupDelta(
  current: TestReportStatsRollup,
  delta: TestReportStatsDelta,
): TestReportStatsRollup {
  const sign = delta.direction === "add" ? 1 : -1;
  const category = getScoreCategory(delta.score);
  const totalTests = Math.max(0, current.totalTests + sign);
  const totalScore = current.totalScore + sign * delta.score;
  const totalVolume = current.totalVolume + sign * delta.volume;
  const excellentCount = Math.max(
    0,
    current.excellentCount + (category === "excellent" ? sign : 0),
  );
  const goodCount = Math.max(
    0,
    current.goodCount + (category === "good" ? sign : 0),
  );
  const poorCount = Math.max(
    0,
    current.poorCount + (category === "poor" ? sign : 0),
  );
  if (totalTests === 0) return emptyTestReportStatsRollup();
  return {
    totalTests,
    totalScore,
    totalVolume,
    excellentCount,
    goodCount,
    poorCount,
    averageScore: totalScore / totalTests,
    averageDataVolume: totalVolume / totalTests,
  };
}

function toStats(rollup: TestReportStatsRollup): TestReportStats {
  return {
    totalTests: rollup.totalTests,
    averageScore: rollup.averageScore,
    excellentCount: rollup.excellentCount,
    goodCount: rollup.goodCount,
    poorCount: rollup.poorCount,
    averageDataVolume: rollup.averageDataVolume,
  };
}

function isMissingNodeError(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const status = (error as { status?: unknown }).status;
  return status === 400 || status === 404;
}

function sourceContextFromRetrieved(node: {
  properties?: Record<string, Record<string, unknown>>;
}): string | undefined {
  const props = extractFileProperties(node, VIEW_IDS.SUMMARY_OUTPUT_FILE);
  const raw = props?.sourceContext;
  return typeof raw === "string" ? raw : undefined;
}

async function retrieveRollup(
  sdk: RetrieveUpsertClient,
): Promise<TestReportStatsRollup | null> {
  try {
    const response = await withCdfConcurrency(() =>
      sdk.instances.retrieve({
        items: [
          {
            instanceType: "node",
            space: getSummarySpace(),
            externalId: TEST_REPORT_STATS_EXTERNAL_ID,
          },
        ],
        sources: [{ source: fileSource() }],
      }),
    );
    const node = response.items[0];
    if (!node) return null;
    return parseTestReportStatsRollup(sourceContextFromRetrieved(node));
  } catch (error: unknown) {
    if (isMissingNodeError(error)) return null;
    throw error;
  }
}

async function upsertRollup(
  sdk: RetrieveUpsertClient,
  rollup: TestReportStatsRollup,
): Promise<void> {
  await withCdfConcurrency(() =>
    sdk.instances.upsert({
      items: [
        {
          instanceType: "node",
          space: getSummarySpace(),
          externalId: TEST_REPORT_STATS_EXTERNAL_ID,
          sources: [
            {
              source: fileSource(),
              properties: {
                name: "Test report statistics",
                mimeType: "application/json",
                sourceContext: JSON.stringify(rollup),
              },
            },
          ],
        },
      ],
    }),
  );
}

/**
 * One retrieve. When the rollup node is missing, scan test reports once and upsert it.
 */
export async function readOrBackfillTestReportStats(
  sdk: RetrieveUpsertListClient,
): Promise<TestReportStats> {
  const existing = await retrieveRollup(sdk);
  if (existing) return toStats(existing);

  const sums = await computeTestReportStatsRollup(sdk);
  const rollup = rollupFromSums(sums);
  await upsertRollup(sdk, rollup);
  return toStats(rollup);
}

/**
 * Adjust the rollup after a test report is saved or deleted.
 * Skips when the node is absent so an in-flight backfill scan is not overwritten.
 */
export async function applyTestReportStatsDelta(
  sdk: RetrieveUpsertClient,
  delta: TestReportStatsDelta,
): Promise<void> {
  const existing = await retrieveRollup(sdk);
  if (!existing) return;
  await upsertRollup(sdk, applyRollupDelta(existing, delta));
}
