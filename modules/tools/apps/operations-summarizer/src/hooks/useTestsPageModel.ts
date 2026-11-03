import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { useNavigation } from "@/contexts/NavigationContext";
import { useTestsPageModelDeps } from "@/contexts/TestsPageModelDepsContext";
import { useEnhancedRefresh } from "@/hooks/useEnhancedRefresh";
import { useTestingState } from "@/hooks/useTestingState";
import type { DataTypeName, SummaryInstance } from "@/types/operations";
import { resolveSummaryFromTestSelection } from "@/utils/summaryInstanceMapping";
import { deriveComparisonTestResult } from "./testsPageModel/deriveState";
import {
  copyYamlToClipboard,
  deleteTestReportAction,
  downloadYamlFile,
  exportTestResultToYaml,
  saveTestResultsAction,
} from "./testsPageModel/testActions";
import {
  useAutoPopulateTestDates,
  useTestsPageFilters,
} from "./testsPageModel/useTestsPageFilters";

type UseTestsPageClientDeps = {
  useCdfClient: typeof useCdfClient;
};

const defaultTestsPageClientDeps: UseTestsPageClientDeps = {
  useCdfClient,
};

export const UseTestsPageClientContext = createContext<UseTestsPageClientDeps>(
  defaultTestsPageClientDeps,
);

/**
 * Tests page ViewModel: reports list, picker, comparison run, YAML export, history filters.
 */
export function useTestsPageModel() {
  const deps = useTestsPageModelDeps();
  const { saveTestReportToCdf } = deps;
  const { useCdfClient: useClient } = useContext(UseTestsPageClientContext);
  const { sdk } = useClient();
  const { navigate } = useNavigation();

  const {
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
  } = deps.useTestsPageCdfBundle();

  const isLoading =
    testReportsLoading || summariesPickerLoading || testStatsLoading;
  const isFetchError =
    isTestReportsError || isSummariesPickerError || isTestStatsError;
  const refetch = useCallback(async () => {
    await Promise.all([
      refetchTestReports(),
      refetchSummariesPicker(),
      refetchTestStats(),
    ]);
  }, [refetchTestReports, refetchSummariesPicker, refetchTestStats]);

  const {
    selectedSummaryId,
    setSelectedSummaryId,
    testStartDate,
    setTestStartDate,
    testEndDate,
    setTestEndDate,
    selectedDataTypes,
    setSelectedDataTypes,
    generationProgress,
    testError,
    setTestError,
    yamlContent,
    setYamlContent,
    yamlExported,
    setYamlExported,
    testResultData,
    setTestResultData,
    setGenerationProgress,
  } = useTestingState();
  const { runComparisonTest, isRunning } = deps.useComparisonTest();

  const [showCreateForm, setShowCreateForm] = useState(false);
  const [showInfoDialog, setShowInfoDialog] = useState(false);
  const [selectedReport, setSelectedReport] = useState<SummaryInstance | null>(
    null,
  );
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<SummaryInstance | null>(
    null,
  );
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [savingError, setSavingError] = useState<string | null>(null);

  const testResult = useMemo(
    () => deriveComparisonTestResult(testResultData, summaries),
    [testResultData, summaries],
  );

  const filters = useTestsPageFilters(
    testReports,
    summaries,
    streamedTestStats ?? null,
    isTestStatsError,
  );

  useAutoPopulateTestDates(
    selectedSummaryId,
    summaries,
    setTestStartDate,
    setTestEndDate,
    setSelectedDataTypes,
  );

  const {
    refresh: enhancedRefresh,
    isRefreshing: isRefreshingData,
    showSuccess: refreshSuccess,
    showError: refreshError,
    message: refreshMessage,
  } = useEnhancedRefresh({
    onRefresh: () => {
      void refetch();
    },
    successMessage: "Tests refreshed",
    errorMessage: "Failed to refresh tests",
  });

  const selectedSummary = resolveSummaryFromTestSelection(
    summaries,
    selectedSummaryId,
  );

  const toggleDataType = (dataType: DataTypeName) => {
    const newTypes = selectedDataTypes.includes(dataType)
      ? selectedDataTypes.filter((t) => t !== dataType)
      : [...selectedDataTypes, dataType];
    setSelectedDataTypes(newTypes);
  };

  const handleRunTest = () => {
    if (selectedSummary) runComparisonTest(selectedSummary);
  };

  const clearResults = () => {
    setTestResultData(null);
    setYamlContent("");
    setYamlExported(false);
    setTestError(null);
    setGenerationProgress({ status: "idle", message: "", progress: 0 });
  };

  const saveTestResults = () =>
    saveTestResultsAction({
      testResult,
      sdk,
      testStartDate,
      testEndDate,
      selectedDataTypes,
      yamlContent,
      saveTestReportToCdf,
      refetch,
      setIsSaving,
      setSavingError,
      setShowCreateForm,
      setYamlContent,
      setYamlExported,
      clearResults,
    });

  const exportToYaml = () =>
    exportTestResultToYaml(
      testResult,
      testStartDate,
      testEndDate,
      selectedDataTypes,
      setYamlContent,
      setYamlExported,
    );

  const downloadYaml = () => downloadYamlFile(yamlContent);

  const handleDelete = useCallback(
    () =>
      deleteTestReportAction(
        deleteTarget,
        deleteMutation,
        setDeleteError,
        setDeleteTarget,
      ),
    [deleteTarget, deleteMutation],
  );

  const openReportModal = useCallback((report: SummaryInstance) => {
    setSelectedReport(report);
    setIsModalOpen(true);
  }, []);

  const handleCreateSummaryFirst = () => {
    navigate("summaries");
  };

  return {
    sdk,
    navigate,
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
    isLoading,
    isFetchError,
    refetch,
    deleteMutation,
    selectedSummaryId,
    setSelectedSummaryId,
    testStartDate,
    setTestStartDate,
    testEndDate,
    setTestEndDate,
    selectedDataTypes,
    setSelectedDataTypes,
    generationProgress,
    testError,
    setTestError,
    yamlContent,
    setYamlContent,
    yamlExported,
    setYamlExported,
    testResultData,
    setTestResultData,
    setGenerationProgress,
    runComparisonTest,
    isRunning,
    showCreateForm,
    setShowCreateForm,
    showInfoDialog,
    setShowInfoDialog,
    ...filters,
    selectedReport,
    setSelectedReport,
    isModalOpen,
    setIsModalOpen,
    deleteTarget,
    setDeleteTarget,
    deleteError,
    isSaving,
    savingError,
    testResult,
    selectedSummary,
    toggleDataType,
    handleRunTest,
    saveTestResults,
    exportToYaml,
    downloadYaml,
    copyYamlToClipboard: () => copyYamlToClipboard(yamlContent),
    clearResults,
    handleDelete,
    openReportModal,
    handleCreateSummaryFirst,
    enhancedRefresh,
    isRefreshingData,
    refreshSuccess,
    refreshError,
    refreshMessage,
  };
}

export type TestsPageViewModel = ReturnType<typeof useTestsPageModel>;
