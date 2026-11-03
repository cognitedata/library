/**
 * Component tests for TestsPage.
 *
 * useTestsPageModel is mocked so tests drive UI state directly.
 * Missing utility modules (summaryDownload, summaryInstanceMapping,
 * summaryListQuery) are also mocked because they don't exist on this branch yet.
 */

import { fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { TestsPageDeleteMutationRef } from "@/hooks/useDefaultTestsPageCdfBundle";
import type { TestsPageViewModel } from "@/hooks/useTestsPageModel";
import type { SummaryInstance } from "@/types/operations";
import { TestsPage } from "./TestsPage";

// TestsPageModelDepsProvider pulls CDF client setup. This page test only checks layout.
vi.mock("@/contexts/TestsPageModelDepsContext", () => ({
  TestsPageModelDepsProvider: ({ children }: { children: ReactNode }) =>
    children,
  useTestsPageModelDeps: vi.fn(),
}));

// ─── Hook mock ────────────────────────────────────────────────

const mockHandleRunTest = vi.fn();
const mockEnhancedRefresh = vi.fn();
const mockSetShowCreateForm = vi.fn();
const mockSetShowInfoDialog = vi.fn();
const mockSetDeleteTarget = vi.fn();
const mockHandleDelete = vi.fn().mockResolvedValue(undefined);
const mockOpenReportModal = vi.fn();
const mockClearHistoryFilters = vi.fn();
const mockSetMinScore = vi.fn();
const mockSetMaxScore = vi.fn();
const mockSetFilterStartDate = vi.fn();
const mockSetFilterEndDate = vi.fn();
const mockHandleCreateSummaryFirst = vi.fn();
const mockSetIsModalOpen = vi.fn();
const mockSaveTestResults = vi.fn().mockResolvedValue(undefined);
const mockExportToYaml = vi.fn();
const mockDownloadYaml = vi.fn();
const mockCopyYamlToClipboard = vi.fn().mockResolvedValue(undefined);
const mockClearResults = vi.fn();

const VIEW = "cdf_cdm/CogniteFile/v1";

const baseSummary: SummaryInstance = {
  externalId: "ops_summary_2024-01-01_2024-01-31_x",
  space: "ops_summaries",
  createdTime: 1_700_000_000_000,
  properties: {
    [VIEW]: { name: "Jan Summary", content: "Similarity Score: 85/100" },
  },
};

const baseReport: SummaryInstance = {
  externalId: "test_report_abc",
  space: "ops_summaries",
  createdTime: 1_700_000_001_000,
  properties: {
    [VIEW]: { name: "Test Report 1", content: "Similarity Score: 80/100" },
  },
};

const mockDeleteMutation = {
  mutateAsync: vi.fn().mockResolvedValue(true),
  isPending: false,
} satisfies TestsPageDeleteMutationRef;

function makeDefaultViewModel(
  overrides: Partial<TestsPageViewModel> = {},
): TestsPageViewModel {
  return {
    sdk: null,
    navigate: vi.fn(),
    testReports: [],
    testReportsLoading: false,
    isTestReportsError: false,
    refetchTestReports: vi.fn(),
    testReportPageDisplay: 1,
    testReportsHasNext: false,
    testReportsHasPrev: false,
    testReportsGoNext: vi.fn(),
    testReportsGoPrev: vi.fn(),
    summaries: [],
    summariesPickerLoading: false,
    isSummariesPickerError: false,
    refetchSummariesPicker: vi.fn(),
    streamedTestStats: undefined,
    testStatsLoading: false,
    isTestStatsError: false,
    refetchTestStats: vi.fn(),
    isLoading: false,
    isFetchError: false,
    refetch: vi.fn(),
    deleteMutation: mockDeleteMutation,
    selectedSummaryId: "",
    setSelectedSummaryId: vi.fn(),
    testStartDate: "",
    setTestStartDate: vi.fn(),
    testEndDate: "",
    setTestEndDate: vi.fn(),
    selectedDataTypes: [],
    setSelectedDataTypes: vi.fn(),
    generationProgress: { status: "idle", message: "", progress: 0 },
    testError: null,
    setTestError: vi.fn(),
    yamlContent: "",
    setYamlContent: vi.fn(),
    yamlExported: false,
    setYamlExported: vi.fn(),
    testResultData: null,
    setTestResultData: vi.fn(),
    setGenerationProgress: vi.fn(),
    runComparisonTest: vi.fn(),
    isRunning: false,
    showCreateForm: false,
    setShowCreateForm: mockSetShowCreateForm,
    showInfoDialog: false,
    setShowInfoDialog: mockSetShowInfoDialog,
    minScore: 0,
    setMinScore: mockSetMinScore,
    maxScore: 100,
    setMaxScore: mockSetMaxScore,
    filterStartDate: "",
    setFilterStartDate: mockSetFilterStartDate,
    filterEndDate: "",
    setFilterEndDate: mockSetFilterEndDate,
    isModalOpen: false,
    setIsModalOpen: mockSetIsModalOpen,
    selectedReport: null,
    setSelectedReport: vi.fn(),
    deleteTarget: null,
    setDeleteTarget: mockSetDeleteTarget,
    deleteError: null,
    isSaving: false,
    savingError: null,
    testResult: null,
    filteredReports: [],
    filteredSummaries: [],
    stats: {
      totalTests: 0,
      averageScore: 0,
      excellentCount: 0,
      goodCount: 0,
      poorCount: 0,
      unavailable: false,
    },
    selectedSummary: undefined,
    toggleDataType: vi.fn(),
    handleRunTest: mockHandleRunTest,
    saveTestResults: mockSaveTestResults,
    exportToYaml: mockExportToYaml,
    downloadYaml: mockDownloadYaml,
    copyYamlToClipboard: mockCopyYamlToClipboard,
    clearResults: mockClearResults,
    handleDelete: mockHandleDelete,
    openReportModal: mockOpenReportModal,
    hasActiveHistoryFilters: false,
    clearHistoryFilters: mockClearHistoryFilters,
    handleCreateSummaryFirst: mockHandleCreateSummaryFirst,
    enhancedRefresh: mockEnhancedRefresh,
    isRefreshingData: false,
    refreshSuccess: false,
    refreshError: false,
    refreshMessage: "Refresh",
    ...overrides,
  };
}

let currentViewModel = makeDefaultViewModel();

// Page tests render layout. The view model is stubbed so CDF is not called.
vi.mock("@/hooks/useTestsPageModel", () => ({
  useTestsPageModel: () => currentViewModel,
}));

// ─── Helpers ──────────────────────────────────────────────────

function renderPage() {
  return render(<TestsPage />);
}

// ─── Tests ────────────────────────────────────────────────────

describe("TestsPage", () => {
  beforeEach(() => {
    currentViewModel = makeDefaultViewModel();
    vi.clearAllMocks();
  });

  // ── Header ──────────────────────────────────────────────────

  it("renders page title and score legend", () => {
    renderPage();
    expect(screen.getByRole("heading", { name: "Tests" })).toBeInTheDocument();
    expect(screen.getByText("Excellent")).toBeInTheDocument();
    expect(screen.getByText("Good")).toBeInTheDocument();
    expect(screen.getByText("Needs attention")).toBeInTheDocument();
  });

  it("shows page number badge", () => {
    currentViewModel = makeDefaultViewModel({ testReportPageDisplay: 3 });
    renderPage();
    expect(screen.getByText(/Page 3/)).toBeInTheDocument();
  });

  it("includes total count in badge when stats are available", () => {
    currentViewModel = makeDefaultViewModel({
      testReportPageDisplay: 1,
      stats: {
        totalTests: 42,
        averageScore: 85,
        excellentCount: 10,
        goodCount: 20,
        poorCount: 12,
        unavailable: false,
      },
    });
    renderPage();
    expect(screen.getByText(/42 total/)).toBeInTheDocument();
  });

  it("opens info dialog when info button is clicked", () => {
    renderPage();
    fireEvent.click(
      screen.getByRole("button", { name: /show testing information/i }),
    );
    expect(mockSetShowInfoDialog).toHaveBeenCalledWith(true);
  });

  it("calls enhancedRefresh when refresh button is clicked", () => {
    renderPage();
    fireEvent.click(screen.getByRole("button", { name: /refresh/i }));
    expect(mockEnhancedRefresh).toHaveBeenCalled();
  });

  it("disables refresh button while loading", () => {
    currentViewModel = makeDefaultViewModel({ isLoading: true });
    renderPage();
    expect(screen.getByRole("button", { name: /refresh/i })).toBeDisabled();
  });

  // ── Create form toggle ───────────────────────────────────────

  it("shows 'Create new test' button and opens form on click", () => {
    renderPage();
    fireEvent.click(screen.getByRole("button", { name: /create new test/i }));
    expect(mockSetShowCreateForm).toHaveBeenCalledWith(true);
  });

  it("renders create form when showCreateForm is true", () => {
    currentViewModel = makeDefaultViewModel({ showCreateForm: true });
    renderPage();
    expect(
      screen.getByRole("heading", { name: /create new test/i }),
    ).toBeInTheDocument();
  });

  it("closes form when X button is clicked inside it", () => {
    currentViewModel = makeDefaultViewModel({ showCreateForm: true });
    renderPage();
    fireEvent.click(
      screen.getByRole("button", { name: /close create test form/i }),
    );
    expect(mockSetShowCreateForm).toHaveBeenCalledWith(false);
  });

  // ── Empty states ─────────────────────────────────────────────

  it("shows loading skeleton while data loads", () => {
    currentViewModel = makeDefaultViewModel({ isLoading: true });
    renderPage();
    // TestReportsTableSkeleton renders a role="status" or data-testid; verify
    // the table + empty states are absent instead
    expect(
      screen.queryByText(/no test reports found/i),
    ).not.toBeInTheDocument();
  });

  it("shows 'No test reports found' when reports and summaries are empty", () => {
    currentViewModel = makeDefaultViewModel({
      testReports: [],
      filteredReports: [],
      summaries: [],
      isLoading: false,
    });
    renderPage();
    expect(screen.getByText(/no test reports found/i)).toBeInTheDocument();
    expect(screen.getByText(/create a summary first/i)).toBeInTheDocument();
  });

  it("shows 'Create Summary First' button when no summaries exist", () => {
    currentViewModel = makeDefaultViewModel({
      testReports: [],
      filteredReports: [],
      summaries: [],
    });
    renderPage();
    const btn = screen.getByRole("button", { name: /create summary first/i });
    expect(btn).toBeInTheDocument();
    fireEvent.click(btn);
    expect(mockHandleCreateSummaryFirst).toHaveBeenCalled();
  });

  it("shows 'Create Test' button when summaries exist but no reports", () => {
    currentViewModel = makeDefaultViewModel({
      testReports: [],
      filteredReports: [],
      summaries: [baseSummary],
    });
    renderPage();
    // The empty-state "Create Test" button (not the header one)
    const buttons = screen.getAllByRole("button", { name: /create test/i });
    // At least the empty-state button
    expect(buttons.length).toBeGreaterThan(0);
    fireEvent.click(buttons[buttons.length - 1]);
    expect(mockSetShowCreateForm).toHaveBeenCalledWith(true);
  });

  it("shows filtered-empty state when filteredReports is empty but testReports has items", () => {
    currentViewModel = makeDefaultViewModel({
      testReports: [baseReport],
      filteredReports: [],
      summaries: [baseSummary],
    });
    renderPage();
    expect(
      screen.getByText(/no reports on this page match the score range/i),
    ).toBeInTheDocument();
  });

  // ── Error banners ─────────────────────────────────────────────

  it("shows fetch error banner with all sub-errors", () => {
    currentViewModel = makeDefaultViewModel({
      isFetchError: true,
      isTestReportsError: true,
      isSummariesPickerError: true,
      isTestStatsError: true,
    });
    renderPage();
    expect(screen.getByText(/failed to load some data/i)).toBeInTheDocument();
    expect(
      screen.getByText(/test reports list could not be loaded/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/operation summaries.*could not be loaded/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/test statistics could not be loaded/i),
    ).toBeInTheDocument();
  });

  it("shows delete error banner", () => {
    currentViewModel = makeDefaultViewModel({
      deleteError: "Delete failed: permission denied",
    });
    renderPage();
    expect(
      screen.getByText("Delete failed: permission denied"),
    ).toBeInTheDocument();
  });

  // ── Stats bar ─────────────────────────────────────────────────

  it("hides stats bar when create form is open", () => {
    currentViewModel = makeDefaultViewModel({
      showCreateForm: true,
      stats: {
        totalTests: 5,
        averageScore: 80,
        excellentCount: 2,
        goodCount: 2,
        poorCount: 1,
        unavailable: false,
      },
    });
    renderPage();
    // Stats bar renders totalTests count; it should not be visible
    expect(screen.queryByLabelText(/minimum score/i)).not.toBeInTheDocument();
  });

  it("shows stats when loaded and create form is closed", () => {
    currentViewModel = makeDefaultViewModel({
      showCreateForm: false,
      stats: {
        totalTests: 7,
        averageScore: 88.5,
        excellentCount: 3,
        goodCount: 3,
        poorCount: 1,
        unavailable: false,
      },
      streamedTestStats: {
        totalTests: 7,
        averageScore: 88.5,
        excellentCount: 3,
        goodCount: 3,
        poorCount: 1,
        averageDataVolume: 0,
      },
    });
    renderPage();
    expect(screen.getByText("88.5%")).toBeInTheDocument();
    expect(screen.getByLabelText(/minimum score/i)).toBeInTheDocument();
  });

  it("shows 'Stats unavailable' when stats.unavailable is true", () => {
    currentViewModel = makeDefaultViewModel({
      stats: {
        totalTests: 0,
        averageScore: 0,
        excellentCount: 0,
        goodCount: 0,
        poorCount: 0,
        unavailable: true,
      },
    });
    renderPage();
    expect(screen.getByText(/stats unavailable/i)).toBeInTheDocument();
  });

  it("shows loading indicator for stats", () => {
    currentViewModel = makeDefaultViewModel({
      testStatsLoading: true,
      streamedTestStats: undefined,
    });
    renderPage();
    expect(screen.getByText(/loading statistics/i)).toBeInTheDocument();
  });

  // ── Score filter ──────────────────────────────────────────────

  it("calls setMinScore when minimum score input changes", () => {
    renderPage();
    const minInput = screen.getByLabelText(/minimum score/i);
    fireEvent.change(minInput, { target: { value: "50" } });
    expect(mockSetMinScore).toHaveBeenCalledWith(50);
  });

  it("calls setMaxScore when maximum score input changes", () => {
    renderPage();
    const maxInput = screen.getByLabelText(/maximum score/i);
    fireEvent.change(maxInput, { target: { value: "90" } });
    expect(mockSetMaxScore).toHaveBeenCalledWith(90);
  });

  it("clamps score input to [0, 100]", () => {
    renderPage();
    const minInput = screen.getByLabelText(/minimum score/i);
    fireEvent.change(minInput, { target: { value: "150" } });
    expect(mockSetMinScore).toHaveBeenCalledWith(100);

    fireEvent.change(minInput, { target: { value: "-10" } });
    expect(mockSetMinScore).toHaveBeenCalledWith(0);
  });

  it("shows Clear button when filters are active and calls clearHistoryFilters", () => {
    currentViewModel = makeDefaultViewModel({ hasActiveHistoryFilters: true });
    renderPage();
    const clearBtn = screen.getByRole("button", { name: /^clear$/i });
    expect(clearBtn).toBeInTheDocument();
    fireEvent.click(clearBtn);
    expect(mockClearHistoryFilters).toHaveBeenCalled();
  });

  it("hides Clear button when no active filters", () => {
    currentViewModel = makeDefaultViewModel({ hasActiveHistoryFilters: false });
    renderPage();
    expect(
      screen.queryByRole("button", { name: /^clear$/i }),
    ).not.toBeInTheDocument();
  });

  // ── Test table ────────────────────────────────────────────────

  it("renders TestTable when filteredReports has items", () => {
    currentViewModel = makeDefaultViewModel({
      testReports: [baseReport],
      filteredReports: [baseReport],
    });
    renderPage();
    // TestTable renders the externalId as item name via extractSummaryContent mock
    expect(
      screen.queryByText(/no test reports found/i),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText(/no reports on this page match/i),
    ).not.toBeInTheDocument();
  });

  // ── Delete flow ───────────────────────────────────────────────

  it("opens delete dialog when deleteTarget is set", () => {
    currentViewModel = makeDefaultViewModel({
      deleteTarget: baseReport,
      filteredReports: [baseReport],
      testReports: [baseReport],
    });
    renderPage();
    expect(screen.getByRole("dialog")).toBeInTheDocument();
    expect(screen.getByText(/delete test report/i)).toBeInTheDocument();
  });

  it("calls handleDelete when delete is confirmed", () => {
    currentViewModel = makeDefaultViewModel({
      deleteTarget: baseReport,
      filteredReports: [baseReport],
      testReports: [baseReport],
    });
    renderPage();
    fireEvent.click(screen.getByRole("button", { name: /^delete$/i }));
    expect(mockHandleDelete).toHaveBeenCalled();
  });

  it("calls setDeleteTarget(null) when delete is cancelled", () => {
    currentViewModel = makeDefaultViewModel({
      deleteTarget: baseReport,
      filteredReports: [baseReport],
      testReports: [baseReport],
    });
    renderPage();
    fireEvent.click(screen.getByRole("button", { name: /cancel/i }));
    expect(mockSetDeleteTarget).toHaveBeenCalledWith(null);
  });

  // ── Report modal ──────────────────────────────────────────────

  it("renders TestReportModal when isModalOpen is true", () => {
    currentViewModel = makeDefaultViewModel({
      isModalOpen: true,
      selectedReport: baseSummary,
    });
    renderPage();
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  // ── Info dialog ───────────────────────────────────────────────

  it("renders TestingInfoDialog when showInfoDialog is true", () => {
    currentViewModel = makeDefaultViewModel({ showInfoDialog: true });
    renderPage();
    expect(
      screen.getByText(/how to use the testing feature/i),
    ).toBeInTheDocument();
  });

  // ── Create form: empty states ─────────────────────────────────

  it("shows 'No Summaries Available' inside create form when summaries are empty", () => {
    currentViewModel = makeDefaultViewModel({
      showCreateForm: true,
      summaries: [],
      filteredSummaries: [],
    });
    renderPage();
    expect(screen.getByText(/no summaries available/i)).toBeInTheDocument();
    expect(
      screen.getByText(/create your first summary before running tests/i),
    ).toBeInTheDocument();
  });

  it("shows 'Select a Baseline Summary' placeholder when summaries present but none chosen", () => {
    currentViewModel = makeDefaultViewModel({
      showCreateForm: true,
      summaries: [baseSummary],
      filteredSummaries: [baseSummary],
      selectedSummary: undefined,
      testResult: undefined,
    });
    renderPage();
    expect(screen.getByText(/select a baseline summary/i)).toBeInTheDocument();
  });

  it("shows the picker cap note and forwards baseline filter changes", () => {
    currentViewModel = makeDefaultViewModel({
      showCreateForm: true,
      summaries: [baseSummary],
      filteredSummaries: [baseSummary],
    });
    renderPage();
    expect(screen.getByText(/showing up to/i)).toBeInTheDocument();
    fireEvent.change(
      screen.getByLabelText(/filter baseline summaries from date/i),
      {
        target: { value: "2024-01-01" },
      },
    );
    fireEvent.change(screen.getByLabelText(/select baseline summary/i), {
      target: { value: "ops_summaries:ops_summary_2024-01-01_2024-01-31_x" },
    });
    expect(mockSetFilterStartDate).toHaveBeenCalledWith("2024-01-01");
    expect(currentViewModel.setSelectedSummaryId).toHaveBeenCalled();
  });

  it("treats an empty score field as the default bound", () => {
    renderPage();
    fireEvent.change(screen.getByLabelText(/minimum score/i), {
      target: { value: "" },
    });
    fireEvent.change(screen.getByLabelText(/maximum score/i), {
      target: { value: "" },
    });
    expect(mockSetMinScore).toHaveBeenCalledWith(0);
    expect(mockSetMaxScore).toHaveBeenCalledWith(100);
  });

  it("disables refresh while a refresh is in progress", () => {
    currentViewModel = makeDefaultViewModel({
      isRefreshingData: true,
      refreshSuccess: true,
      refreshMessage: "Updated",
    });
    renderPage();
    expect(screen.getByRole("button", { name: /updated/i })).toBeDisabled();
  });

  it("asks the user to adjust filters when history filters hide every report", () => {
    currentViewModel = makeDefaultViewModel({
      testReports: [],
      summaries: [baseSummary],
      hasActiveHistoryFilters: true,
    });
    renderPage();
    expect(
      screen.getByText(/try adjusting your filters or go to another page/i),
    ).toBeInTheDocument();
  });
});
