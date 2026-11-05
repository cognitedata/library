import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getEnabledDataTypes } from "@/config/dataTypes";
import type { SummaryInstance } from "@/types/operations";
import { extractDateRangeFromContent } from "@/utils/formatting";
import { extractSummaryContent } from "@/utils/summaryDownload";
import { resolveSummaryFromTestSelection } from "@/utils/summaryInstanceMapping";
import { formatDateForInput, parseDatesFromExternalId } from "./dateUtils";
import {
  deriveTestsPageStats,
  filterSummariesByCreatedDateRange,
  filterTestReportsByScore,
} from "./deriveState";

export function useTestsPageFilters(
  testReports: SummaryInstance[],
  summaries: SummaryInstance[],
  streamedTestStats: {
    totalTests: number;
    averageScore: number;
    excellentCount: number;
    goodCount: number;
    poorCount: number;
  } | null,
  isTestStatsError: boolean,
) {
  const [minScore, setMinScore] = useState(0);
  const [maxScore, setMaxScore] = useState(100);
  const [filterStartDate, setFilterStartDate] = useState("");
  const [filterEndDate, setFilterEndDate] = useState("");

  const filteredReports = useMemo(
    () => filterTestReportsByScore(testReports, minScore, maxScore),
    [testReports, minScore, maxScore],
  );

  const filteredSummaries = useMemo(
    () =>
      filterSummariesByCreatedDateRange(
        summaries,
        filterStartDate,
        filterEndDate,
      ),
    [summaries, filterStartDate, filterEndDate],
  );

  const stats = useMemo(
    () => deriveTestsPageStats(streamedTestStats, isTestStatsError),
    [streamedTestStats, isTestStatsError],
  );

  const hasActiveHistoryFilters = minScore > 0 || maxScore < 100;

  const clearHistoryFilters = useCallback(() => {
    setMinScore(0);
    setMaxScore(100);
  }, []);

  return {
    minScore,
    setMinScore,
    maxScore,
    setMaxScore,
    filterStartDate,
    setFilterStartDate,
    filterEndDate,
    setFilterEndDate,
    filteredReports,
    filteredSummaries,
    stats,
    hasActiveHistoryFilters,
    clearHistoryFilters,
  };
}

export function useAutoPopulateTestDates(
  selectedSummaryId: string | null,
  summaries: SummaryInstance[],
  setTestStartDate: (value: string) => void,
  setTestEndDate: (value: string) => void,
  setSelectedDataTypes: (types: ReturnType<typeof getEnabledDataTypes>) => void,
) {
  const lastProcessedSummaryIdRef = useRef<string | null>(null);

  useEffect(() => {
    if (
      !selectedSummaryId ||
      selectedSummaryId === lastProcessedSummaryIdRef.current
    ) {
      return;
    }

    const summary = resolveSummaryFromTestSelection(
      summaries,
      selectedSummaryId,
    );
    if (!summary) return;

    lastProcessedSummaryIdRef.current = selectedSummaryId;

    let startDateStr = "";
    let endDateStr = "";

    const externalIdDates = parseDatesFromExternalId(summary.externalId);
    if (externalIdDates) {
      startDateStr = externalIdDates.start;
      endDateStr = externalIdDates.end;
    } else {
      const { content } = extractSummaryContent(summary);
      const extractedDates = extractDateRangeFromContent(content);
      if (extractedDates) {
        startDateStr = formatDateForInput(extractedDates.start);
        endDateStr = formatDateForInput(extractedDates.end);
      }
    }

    if (startDateStr && endDateStr) {
      setTestStartDate(startDateStr);
      setTestEndDate(endDateStr);
    }

    setSelectedDataTypes(getEnabledDataTypes());
  }, [
    selectedSummaryId,
    summaries,
    setTestStartDate,
    setTestEndDate,
    setSelectedDataTypes,
  ]);
}
