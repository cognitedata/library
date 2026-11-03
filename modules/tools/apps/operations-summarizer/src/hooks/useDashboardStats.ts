/**
 * Aggregated stats and derived data for dashboard cards.
 */

import { useCallback, useMemo } from "react";
import { getSelectedContentTypeKeys } from "@/config/dataTypes";
import { useSummaryGenerationContext } from "@/contexts/OperationsSummarizerContext";
import {
  useLatestSummaryInstance,
  useTestReportStatsStreaming,
} from "@/hooks/useSummariesCdf";
import type { SummaryInstance } from "@/types/operations";
import { extractSummaryContent } from "@/utils/summaryDownload";
import { extractScoreFromContent } from "@/utils/testReportUtils";

interface DashboardStats {
  /** Latest operation summary only (bounded). */
  summaries: SummaryInstance[];
  /** Not used for full history; kept empty for backward compatibility. */
  testReports: SummaryInstance[];
  testStats: {
    total: number;
    avgScore: number;
    excellent: number;
    good: number;
    poor: number;
  };
  latestSummary: SummaryInstance | null;
  latestTest: { report: SummaryInstance; score: number } | null;
  selectedContentTypes: string[];
  isLoading: boolean;
  lastUpdated: Date | null;
  refetch: () => Promise<void>;
}

export function useDashboardStats(): DashboardStats {
  const { summaryContents } = useSummaryGenerationContext();
  const {
    data: latestSummary = null,
    isLoading: latestSummaryLoading,
    refetch: refetchLatestSummary,
  } = useLatestSummaryInstance("operation");
  const {
    data: latestTestReport = null,
    isLoading: latestTestLoading,
    refetch: refetchLatestTest,
  } = useLatestSummaryInstance("test");
  const {
    data: streamedTestStats,
    isLoading: statsLoading,
    refetch: refetchTestStats,
  } = useTestReportStatsStreaming();

  const testStats = useMemo(() => {
    const s = streamedTestStats;
    if (!s) {
      return {
        total: 0,
        avgScore: 0,
        excellent: 0,
        good: 0,
        poor: 0,
      };
    }
    return {
      total: s.totalTests,
      avgScore: s.averageScore,
      excellent: s.excellentCount,
      good: s.goodCount,
      poor: s.poorCount,
    };
  }, [streamedTestStats]);

  const latestTest = useMemo(() => {
    if (!latestTestReport) return null;
    const { content } = extractSummaryContent(latestTestReport);
    const score = extractScoreFromContent(content);
    return { report: latestTestReport, score };
  }, [latestTestReport]);

  const selectedContentTypes = useMemo(
    () => getSelectedContentTypeKeys(summaryContents),
    [summaryContents],
  );

  const isLoading = latestSummaryLoading || latestTestLoading || statsLoading;

  const lastUpdated = useMemo(() => {
    if (!latestSummary && !latestTestReport) return null;
    return new Date();
  }, [latestSummary, latestTestReport]);

  const refetch = useCallback(async () => {
    await Promise.all([
      refetchLatestSummary(),
      refetchLatestTest(),
      refetchTestStats(),
    ]);
  }, [refetchLatestSummary, refetchLatestTest, refetchTestStats]);

  return {
    summaries: latestSummary ? [latestSummary] : [],
    testReports: [],
    testStats,
    latestSummary,
    latestTest,
    selectedContentTypes,
    isLoading,
    lastUpdated,
    refetch,
  };
}
