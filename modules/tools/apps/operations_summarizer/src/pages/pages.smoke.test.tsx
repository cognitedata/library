import { QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import { NavigationProvider } from "@/contexts/NavigationContext";
import { OperationsProvider } from "@/contexts/OperationsSummarizerContext";
import { createTestQueryClient } from "@/test-utils/wrappers";
import type { DataTypeName, SummaryInstance } from "@/types/operations";

// react-pdf loads a PDF.js worker and canvas that jsdom does not provide.
vi.mock("react-pdf", () => ({
  pdfjs: { GlobalWorkerOptions: { workerSrc: "" } },
  Document: ({ children }: { children?: ReactNode }) => (
    <div data-testid="mock-pdf-doc">{children}</div>
  ),
  Page: () => <div data-testid="mock-pdf-page" />,
}));

const hoisted = vi.hoisted(() => {
  const today = new Date("2024-06-15T12:00:00.000Z");
  const summary: SummaryInstance = {
    externalId: "ops_summary_2024-01-01_2024-01-31_x",
    space: "ops_summaries",
    createdTime: 1,
    properties: {},
  };
  const noop = () => {};
  const noopAsync = async () => {};

  const summariesModel = {
    state: {
      summaryPeriodStart: today,
      summaryPeriodEnd: today,
      summaryContents: {
        assets: true,
        files: false,
        work_orders: false,
        time_series: false,
        equipment: false,
      },
      selectedAgent: "agent-1",
      showSummary: false,
      generatedSummary: null as null,
    },
    dispatch: vi.fn(),
    toggleSummaryContent: vi.fn(),
    navigate: vi.fn(),
    summaries: [summary],
    isLoading: false,
    isFetchError: false,
    pageDisplay: 1,
    serverHasNext: false,
    serverHasPrev: false,
    goToNextPage: noop,
    goToPreviousPage: noop,
    deleteMutation: { isPending: false, mutateAsync: vi.fn() },
    clearFilters: noop,
    showCreateForm: false,
    setShowCreateForm: vi.fn(),
    deleteError: null as string | null,
    selectedSummary: null as SummaryInstance | null,
    isModalOpen: false,
    deleteCandidate: null as SummaryInstance | null,
    setDeleteCandidate: vi.fn(),
    savedPdfInfo: null,
    isSaving: false,
    saveError: null as string | null,
    startDate: "",
    endDate: "",
    setStartDate: vi.fn(),
    setEndDate: vi.fn(),
    enabledDataTypes: [
      "assets",
      "files",
      "work_orders",
      "time_series",
      "equipment",
    ] as DataTypeName[],
    enhancedRefresh: noopAsync,
    isRefreshingData: false,
    refreshSuccess: false,
    refreshError: false,
    refreshMessage: null as string | null,
    robustDownloadMarkdown: vi.fn().mockResolvedValue(undefined),
    isDownloadingFile: false,
    downloadError: null as string | null,
    lastFailedSummary: null as SummaryInstance | null,
    retryLastDownload: noop,
    clearError: noop,
    canRetry: false,
    selectedCount: 1,
    daysSelected: 1,
    handleStartDateChange: vi.fn(),
    handleEndDateChange: vi.fn(),
    handleGenerateSummary: noopAsync,
    handleSave: noopAsync,
    handleDownloadMarkdown: noop,
    handleDateFilterApply: noop,
    handleViewSummary: vi.fn(),
    handleCloseModal: vi.fn(),
    handleDeleteConfirm: noopAsync,
    handleDeleteCancel: noop,
    handleDownloadMd: vi.fn().mockResolvedValue(undefined),
    handleDownloadPdf: vi.fn().mockResolvedValue(undefined),
    formatDateForInput: (d: Date) => d.toISOString().split("T")[0],
    handleCreateTest: noop,
    preservedConfig: undefined as undefined,
    handleErrorRetry: noopAsync,
    handleResetToIdle: noop,
    progress: {
      progress: 0,
      message: "",
      currentStep: undefined as string | undefined,
    },
    isGenerating: false,
    generationError: null as string | null,
    recoverFromGenerationError: noop,
    canRetryGeneration: false,
    contentTypeFilterEnabled: false,
    selectedContentTypes: [] as DataTypeName[],
    toggleContentType: vi.fn(),
    toggleContentTypeFilter: vi.fn(),
    setSelectedContentTypes: vi.fn(),
    hasActiveFilters: false,
    generate: noopAsync,
    saveMutation: { mutateAsync: vi.fn(), isPending: false },
    dateFilterEnabled: false,
    dateRange: null as { start: Date; end: Date } | null,
    setDateRange: vi.fn(),
    toggleDateFilter: vi.fn(),
    refetch: vi.fn(),
    setSelectedSummary: vi.fn(),
    setIsModalOpen: vi.fn(),
    retryGeneration: noopAsync,
  };

  const testReport: SummaryInstance = {
    externalId: "test_report_1",
    space: "ops_summaries",
    createdTime: 1,
    properties: {
      "cdf_cdm/CogniteFile/v1": {
        content: "Similarity Score: 80/100",
        name: "t",
      },
    },
  };

  const testsModel = {
    sdk: { project: "p" },
    navigate: vi.fn(),
    testReports: [testReport],
    testReportPageDisplay: 1,
    testReportsHasNext: false,
    testReportsHasPrev: false,
    testReportsGoNext: noop,
    testReportsGoPrev: noop,
    summaries: [summary],
    streamedTestStats: {
      totalTests: 1,
      averageScore: 80,
      excellentCount: 0,
      goodCount: 1,
      poorCount: 0,
      averageDataVolume: 0,
    },
    testStatsLoading: false,
    isTestReportsError: false,
    isSummariesPickerError: false,
    isTestStatsError: false,
    isLoading: false,
    isFetchError: false,
    deleteMutation: { isPending: false, mutateAsync: vi.fn() },
    selectedSummaryId: "",
    setSelectedSummaryId: vi.fn(),
    testStartDate: "",
    setTestStartDate: vi.fn(),
    testEndDate: "",
    setTestEndDate: vi.fn(),
    selectedDataTypes: ["assets"] as DataTypeName[],
    setSelectedDataTypes: vi.fn(),
    generationProgress: {
      status: "idle" as const,
      message: "",
      progress: 0,
    },
    testError: null as string | null,
    yamlContent: "",
    yamlExported: false,
    isRunning: false,
    showCreateForm: false,
    setShowCreateForm: vi.fn(),
    showInfoDialog: false,
    setShowInfoDialog: vi.fn(),
    minScore: 0,
    setMinScore: vi.fn(),
    maxScore: 100,
    setMaxScore: vi.fn(),
    filterStartDate: "",
    setFilterStartDate: vi.fn(),
    filterEndDate: "",
    setFilterEndDate: vi.fn(),
    isModalOpen: false,
    setIsModalOpen: vi.fn(),
    selectedReport: null as SummaryInstance | null,
    setSelectedReport: vi.fn(),
    deleteTarget: null as SummaryInstance | null,
    setDeleteTarget: vi.fn(),
    deleteError: null as string | null,
    setDeleteError: vi.fn(),
    isSaving: false,
    setIsSaving: vi.fn(),
    savingError: null as string | null,
    setSavingError: vi.fn(),
    testResult: null,
    filteredReports: [testReport],
    filteredSummaries: [summary],
    stats: {
      totalTests: 1,
      averageScore: 80,
      excellentCount: 0,
      goodCount: 1,
      poorCount: 0,
      unavailable: false as const,
    },
    selectedSummary: undefined as SummaryInstance | undefined,
    toggleDataType: vi.fn(),
    handleRunTest: noop,
    saveTestResults: noopAsync,
    exportToYaml: noop,
    downloadYaml: noop,
    copyYamlToClipboard: noopAsync,
    clearResults: noop,
    handleDelete: noopAsync,
    openReportModal: vi.fn(),
    hasActiveHistoryFilters: false,
    clearHistoryFilters: noop,
    handleCreateSummaryFirst: noop,
    enhancedRefresh: noopAsync,
    isRefreshingData: false,
    refreshSuccess: false,
    refreshError: false,
    refreshMessage: null as string | null,
    refetchTestReports: vi.fn(),
    summariesPickerLoading: false,
    refetchSummariesPicker: vi.fn(),
    refetchTestStats: vi.fn(),
    refetch: vi.fn(),
    testResultData: null,
    setTestResultData: vi.fn(),
    setGenerationProgress: vi.fn(),
    setTestError: vi.fn(),
    setYamlContent: vi.fn(),
    setYamlExported: vi.fn(),
    runComparisonTest: vi.fn(),
  };

  return { summariesModel, testsModel };
});

// Page tests render layout. The view model is stubbed so CDF is not called.
vi.mock("@/hooks/useSummariesPageModel", () => ({
  useSummariesPageModel: () => hoisted.summariesModel,
}));

// Page tests render layout. The view model is stubbed so CDF is not called.
vi.mock("@/hooks/useTestsPageModel", () => ({
  useTestsPageModel: () => hoisted.testsModel,
}));

import { SummariesPage } from "./SummariesPage";
import { TestsPage } from "./TestsPage";

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
// Stub the Fusion host so unit tests do not boot the real App SDK.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => ({
    project: "p",
    get: vi.fn().mockResolvedValue({ data: { items: [], invalidAgents: [] } }),
    post: vi.fn().mockResolvedValue({ data: {} }),
    instances: { list: vi.fn().mockResolvedValue({ items: [] }) },
  }),
}));

function AllProviders({ children }: { children: ReactNode }) {
  const qc = createTestQueryClient();
  return (
    <QueryClientProvider client={qc}>
      <CdfClientProvider>
        <NavigationProvider>
          <OperationsProvider>{children}</OperationsProvider>
        </NavigationProvider>
      </CdfClientProvider>
    </QueryClientProvider>
  );
}

describe("pages smoke", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders SummariesPage", () => {
    render(
      <AllProviders>
        <SummariesPage />
      </AllProviders>,
    );
    expect(
      screen.getByRole("heading", { name: /summaries/i }),
    ).toBeInTheDocument();
  });

  it("renders TestsPage", () => {
    render(
      <AllProviders>
        <TestsPage />
      </AllProviders>,
    );
    expect(screen.getByRole("heading", { name: /tests/i })).toBeInTheDocument();
  });
});
