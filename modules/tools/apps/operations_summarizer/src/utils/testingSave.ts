/**
 * Save test report to CDF. Used by Testing page; keeps save logic out of the component.
 */

import type { CogniteClient } from "@cognite/sdk";
import { getDisplayName, getSummarySpace } from "@/config/dataTypes";
import type {
  ComparisonTestResult,
  DataCounts,
  DataTypeName,
} from "@/types/operations";
import { DATA_COUNT_KEYS, VIEW_IDS } from "@/types/operations";
import { withCdfConcurrency } from "@/utils/cdfRequest";
import { formatDateRangeDisplay } from "@/utils/formatting";
import {
  formatSummaryContentTypeBitmapLine,
  getTestReportExternalIdPrefix,
} from "@/utils/summaryListQuery";
import { applyTestReportStatsDelta } from "@/utils/testReportStatsRollup";

/**
 * Structured metrics stored in sourceContext so readers can avoid regex parsing.
 * The description field keeps the human-readable markdown as a rendering artifact.
 */
export interface TestReportMetrics {
  similarityScore: number;
  dataAccuracyScore: number;
  originalDataCounts: DataCounts;
  newDataCounts: DataCounts;
  periodStart: string;
  periodEnd: string;
  timestamp: string;
}

/** Minimal SDK surface for `saveTestReportToCdf` (full `CogniteClient` satisfies this). */
export type SaveTestReportClient = Pick<CogniteClient, "project"> & {
  instances: Pick<CogniteClient["instances"], "upsert" | "retrieve">;
};

interface SaveTestReportParams {
  sdk: SaveTestReportClient;
  testResult: ComparisonTestResult;
  testStartDate: string;
  testEndDate: string;
}

function dataTypesPresentInTestResult(
  tr: ComparisonTestResult,
): DataTypeName[] {
  const out: DataTypeName[] = [];
  for (const k of Object.keys(tr.originalDataCounts) as DataTypeName[]) {
    if ((tr.originalDataCounts[k] ?? 0) > 0 || (tr.newDataCounts[k] ?? 0) > 0) {
      out.push(k);
    }
  }
  return out;
}

/**
 * Parse TestReportMetrics from the sourceContext field of a stored test report node.
 * Returns null for legacy nodes written before structured metrics were added.
 */
export function parseTestReportMetrics(
  sourceContext: string | undefined,
): TestReportMetrics | null {
  if (!sourceContext?.trim()) return null;
  try {
    const parsed = JSON.parse(sourceContext) as unknown;
    if (typeof parsed !== "object" || parsed === null) return null;
    const m = parsed as Record<string, unknown>;
    if (
      typeof m.similarityScore !== "number" ||
      typeof m.dataAccuracyScore !== "number"
    ) {
      return null;
    }
    return parsed as TestReportMetrics;
  } catch {
    return null;
  }
}

/**
 * Save a test comparison report to CDF (getSummarySpace()) as a markdown node.
 */
export async function saveTestReportToCdf(
  params: SaveTestReportParams,
): Promise<void> {
  const { sdk, testResult, testStartDate, testEndDate } = params;

  const now = new Date();
  const currentDate = now.toISOString().slice(0, 10);
  const currentTime = now.toISOString().slice(11, 19).replace(/:/g, "-");
  const externalId = `${getTestReportExternalIdPrefix()}${testStartDate}_${testEndDate}_${currentDate}_${currentTime}`;
  const humanReadableName = `Test report, ${formatDateRangeDisplay(testStartDate, testEndDate)}`;

  const reportContent = `# Test Comparison Report

**Test Run:** ${testResult.timestamp.toISOString().slice(0, 19).replace("T", " ")}
**Date Range:** ${testStartDate} to ${testEndDate}
**Similarity Score:** ${testResult.similarityScore.toFixed(1)}/100
**Data Accuracy Score:** ${testResult.dataAccuracyScore.toFixed(1)}/100

## Detailed Analysis

${testResult.comparisonAnalysis}

## Data Count Comparison

<table border="1" class="dataframe">
<thead>
<tr style="text-align: right;">
<th>Data Type</th>
<th>Original Count</th>
<th>New Count</th>
<th>Change</th>
<th>Status</th>
</tr>
</thead>
<tbody>
${DATA_COUNT_KEYS.map((k) => {
  const orig = testResult.originalDataCounts[k] || 0;
  const newVal = testResult.newDataCounts[k] || 0;
  const diff = newVal - orig;
  const status =
    orig === newVal
      ? "✅ Same count"
      : diff > 0
        ? "⚠️ Increased"
        : "⚠️ Decreased";
  return `<tr>
<td>${getDisplayName(k)}</td>
<td>${orig}</td>
<td>${newVal}</td>
<td>${diff}</td>
<td>${status}</td>
</tr>`;
}).join("\n")}
</tbody>
</table>

## Original Summary

${testResult.originalContent || "No original summary content available."}

## New Summary

${testResult.newSummary || "No new summary content available."}
`;

  // The OPS:XXXXX\n prefix must lead the description field so instances.list
  // can filter by content type via a `prefix` search. Use
  // stripSummaryContentTypeBitmapLine when displaying the description.
  const typeBitmap = formatSummaryContentTypeBitmapLine(
    dataTypesPresentInTestResult(testResult),
  );

  // Structured metrics in sourceContext so stat readers can skip regex parsing.
  // The description keeps the human-readable markdown as a display artifact.
  const metrics: TestReportMetrics = {
    similarityScore: testResult.similarityScore,
    dataAccuracyScore: testResult.dataAccuracyScore,
    originalDataCounts: testResult.originalDataCounts,
    newDataCounts: testResult.newDataCounts,
    periodStart: testStartDate,
    periodEnd: testEndDate,
    timestamp: testResult.timestamp.toISOString(),
  };

  await withCdfConcurrency(() =>
    sdk.instances.upsert({
      items: [
        {
          instanceType: "node",
          space: getSummarySpace(),
          externalId,
          sources: [
            {
              source: {
                type: "view",
                space: VIEW_IDS.SUMMARY_OUTPUT_FILE.space,
                externalId: VIEW_IDS.SUMMARY_OUTPUT_FILE.externalId,
                version: VIEW_IDS.SUMMARY_OUTPUT_FILE.version,
              },
              properties: {
                name: humanReadableName,
                description: `${typeBitmap}${reportContent}`,
                mimeType: "text/markdown",
                sourceContext: JSON.stringify(metrics),
              },
            },
          ],
        },
      ],
    }),
  );

  const volume = Object.values(testResult.newDataCounts).reduce(
    (sum, value) => sum + (value ?? 0),
    0,
  );
  await applyTestReportStatsDelta(sdk, {
    score: testResult.similarityScore,
    volume,
    direction: "add",
  });
}
