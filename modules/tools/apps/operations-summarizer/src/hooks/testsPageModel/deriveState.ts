import type {
  ComparisonTestResult,
  SummaryInstance,
  TestResultData,
} from "@/types/operations";
import { extractSummaryContent } from "@/utils/summaryDownload";
import { extractScoreFromContent } from "@/utils/testReportUtils";

export function deriveComparisonTestResult(
  testResultData: TestResultData | null,
  summaries: SummaryInstance[],
): ComparisonTestResult | null {
  if (!testResultData) return null;
  const originalSummary = summaries.find(
    (s) =>
      s.externalId === testResultData.originalSummaryId &&
      s.space === testResultData.originalSummarySpace,
  );
  if (!originalSummary) return null;
  return {
    originalSummary,
    originalContent: testResultData.originalContent,
    newSummary: testResultData.newSummary,
    similarityScore: testResultData.similarityScore,
    dataAccuracyScore: testResultData.dataAccuracyScore,
    originalDataCounts: testResultData.originalDataCounts,
    newDataCounts: testResultData.newDataCounts,
    comparisonAnalysis: testResultData.comparisonAnalysis,
    timestamp: new Date(testResultData.timestamp),
  };
}

export function filterTestReportsByScore(
  reports: SummaryInstance[],
  minScore: number,
  maxScore: number,
): SummaryInstance[] {
  return reports.filter((report) => {
    const { content } = extractSummaryContent(report);
    const score = extractScoreFromContent(content);
    return score >= minScore && score <= maxScore;
  });
}

export function filterSummariesByCreatedDateRange(
  summaries: SummaryInstance[],
  filterStartDate: string,
  filterEndDate: string,
): SummaryInstance[] {
  if (!filterStartDate || !filterEndDate) return summaries;
  const start = new Date(filterStartDate);
  const end = new Date(filterEndDate);
  return summaries.filter((summary) => {
    const createdTime = new Date(summary.createdTime);
    if (Number.isNaN(createdTime.getTime())) {
      return false;
    }
    return createdTime >= start && createdTime <= end;
  });
}

export type TestsPageStats = {
  totalTests: number;
  averageScore: number;
  excellentCount: number;
  goodCount: number;
  poorCount: number;
  unavailable: boolean;
};

export function deriveTestsPageStats(
  streamedTestStats: {
    totalTests: number;
    averageScore: number;
    excellentCount: number;
    goodCount: number;
    poorCount: number;
  } | null,
  isTestStatsError: boolean,
): TestsPageStats {
  if (isTestStatsError) {
    return {
      totalTests: 0,
      averageScore: 0,
      excellentCount: 0,
      goodCount: 0,
      poorCount: 0,
      unavailable: true,
    };
  }
  if (!streamedTestStats) {
    return {
      totalTests: 0,
      averageScore: 0,
      excellentCount: 0,
      goodCount: 0,
      poorCount: 0,
      unavailable: false,
    };
  }
  return {
    totalTests: streamedTestStats.totalTests,
    averageScore: streamedTestStats.averageScore,
    excellentCount: streamedTestStats.excellentCount,
    goodCount: streamedTestStats.goodCount,
    poorCount: streamedTestStats.poorCount,
    unavailable: false,
  };
}
