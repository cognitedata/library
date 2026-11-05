import {
  type SummaryServerListFilters,
  useDeleteSummary,
  useOperationSummariesPicker,
  useSummaryListPage,
  useTestReportStatsStreaming,
} from "@/hooks/useCdfData";
import type { SummaryInstance } from "@/types/operations";

const EMPTY_SUMMARY_SERVER_FILTERS: SummaryServerListFilters = {
  dateFilterEnabled: false,
  dateRange: null,
  contentTypeFilterEnabled: false,
  selectedContentTypes: [],
};

/** ViewModel surface for delete in tests page CDF bundle (tests can satisfy this shape). */
export interface TestsPageDeleteMutationRef {
  mutateAsync: (summary: SummaryInstance) => Promise<boolean>;
  isPending: boolean;
}

function useDefaultTestsPageCdfBundleImpl() {
  const {
    items: testReports,
    isLoading: testReportsLoading,
    isError: isTestReportsError,
    refetch: refetchTestReports,
    pageDisplay: testReportPageDisplay,
    hasNextPage: testReportsHasNext,
    hasPreviousPage: testReportsHasPrev,
    goToNextPage: testReportsGoNext,
    goToPreviousPage: testReportsGoPrev,
  } = useSummaryListPage({
    variant: "test",
    serverFilters: EMPTY_SUMMARY_SERVER_FILTERS,
  });
  const {
    data: summaries = [],
    isLoading: summariesPickerLoading,
    isError: isSummariesPickerError,
    refetch: refetchSummariesPicker,
  } = useOperationSummariesPicker();
  const {
    data: streamedTestStats,
    isLoading: testStatsLoading,
    isError: isTestStatsError,
    refetch: refetchTestStats,
  } = useTestReportStatsStreaming();

  const deleteMutation = useDeleteSummary();

  return {
    testReports,
    testReportsLoading,
    isTestReportsError,
    refetchTestReports,
    testReportPageDisplay,
    testReportsHasNext,
    testReportsHasPrev,
    testReportsGoNext,
    testReportsGoPrev,
    summaries,
    summariesPickerLoading,
    isSummariesPickerError,
    refetchSummariesPicker,
    streamedTestStats,
    testStatsLoading,
    isTestStatsError,
    refetchTestStats,
    deleteMutation,
  };
}

type TestsPageCdfBundleBase = ReturnType<
  typeof useDefaultTestsPageCdfBundleImpl
>;

export type TestsPageCdfBundleValue = Omit<
  TestsPageCdfBundleBase,
  "deleteMutation"
> & {
  deleteMutation: TestsPageDeleteMutationRef;
};

/**
 * Default CDF/query hooks bundle for {@link TestsPageModelDepsContext}.
 */
export function useDefaultTestsPageCdfBundle(): TestsPageCdfBundleValue {
  return useDefaultTestsPageCdfBundleImpl();
}
