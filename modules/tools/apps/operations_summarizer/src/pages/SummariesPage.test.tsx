/**
 * Component tests for SummariesPage.
 * useSummariesPageModel is mocked so tests drive UI state directly.
 */

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SummariesPageViewModel } from "@/hooks/useSummariesPageModel";
import type { DataTypeName, SummaryInstance } from "@/types/operations";
import { SummariesPage } from "./SummariesPage";

// Deps context is a passthrough in page tests. The view model mock owns the behavior.
vi.mock("@/contexts/SummariesPageModelDepsContext", () => ({
  SummariesPageModelDepsProvider: ({ children }: { children: ReactNode }) =>
    children,
  useSummariesPageModelDeps: vi.fn(),
}));

// react-pdf loads a PDF.js worker and canvas that jsdom does not provide.
vi.mock("react-pdf", () => ({
  pdfjs: { GlobalWorkerOptions: { workerSrc: "" } },
  Document: ({ children }: { children?: ReactNode }) => (
    <div data-testid="mock-pdf-doc">{children}</div>
  ),
  Page: () => <div data-testid="mock-pdf-page" />,
}));

// The page test covers the list. The modal is replaced so PDF.js is not loaded.
vi.mock("@/pages/SummaryDetailModal", () => ({
  SummaryDetailModal: () => null,
}));

const VIEW = "cdf_cdm/CogniteFile/v1";
const today = new Date("2024-06-15T12:00:00.000Z");

const summary: SummaryInstance = {
  externalId: "ops_summary_2024-01-01_2024-01-31_x",
  space: "ops_summaries",
  createdTime: 1_700_000_000_000,
  properties: {
    [VIEW]: { name: "Jan Summary", content: "# Hello" },
  },
};

const mockSetShowCreateForm = vi.fn();
const mockEnhancedRefresh = vi.fn();
const mockClearFilters = vi.fn();
const mockHandleDateFilterApply = vi.fn();
const mockSetStartDate = vi.fn();
const mockSetEndDate = vi.fn();
const mockHandleGenerateSummary = vi.fn();
const mockHandleCancelGeneration = vi.fn();
const mockToggleSummaryContent = vi.fn();

function makeDefaultViewModel(
  overrides: Partial<SummariesPageViewModel> = {},
): SummariesPageViewModel {
  return {
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
      generatedSummary: null,
    },
    dispatch: vi.fn(),
    toggleSummaryContent: mockToggleSummaryContent,
    navigate: vi.fn(),
    generate: vi.fn(),
    progress: {
      status: "idle",
      progress: 0,
      message: "",
      currentStep: undefined,
    },
    isGenerating: false,
    elapsedSeconds: 0,
    generationError: null,
    recoverFromGenerationError: vi.fn(),
    retryGeneration: vi.fn(),
    canRetryGeneration: false,
    saveMutation: { mutateAsync: vi.fn(), isPending: false },
    dateFilterEnabled: false,
    dateRange: null,
    contentTypeFilterEnabled: false,
    selectedContentTypes: [] as DataTypeName[],
    setDateRange: vi.fn(),
    toggleContentType: vi.fn(),
    setSelectedContentTypes: vi.fn(),
    hasActiveFilters: false,
    toggleDateFilter: vi.fn(),
    toggleContentTypeFilter: vi.fn(),
    summaries: [],
    isLoading: false,
    isFetchError: false,
    refetch: vi.fn(),
    pageDisplay: 1,
    serverHasNext: false,
    serverHasPrev: false,
    goToNextPage: vi.fn(),
    goToPreviousPage: vi.fn(),
    deleteMutation: { isPending: false, mutateAsync: vi.fn() },
    clearFilters: mockClearFilters,
    showCreateForm: false,
    setShowCreateForm: mockSetShowCreateForm,
    deleteError: null,
    selectedSummary: null,
    setSelectedSummary: vi.fn(),
    isModalOpen: false,
    setIsModalOpen: vi.fn(),
    deleteCandidate: null,
    setDeleteCandidate: vi.fn(),
    savedPdfInfo: null,
    isSaving: false,
    saveError: null,
    startDate: "",
    setStartDate: mockSetStartDate,
    endDate: "",
    setEndDate: mockSetEndDate,
    enabledDataTypes: [
      "assets",
      "files",
      "work_orders",
      "time_series",
      "equipment",
    ] as DataTypeName[],
    enhancedRefresh: mockEnhancedRefresh,
    isRefreshingData: false,
    refreshSuccess: false,
    refreshError: false,
    refreshMessage: "Refresh",
    robustDownloadMarkdown: vi.fn(),
    isDownloadingFile: false,
    downloadError: null,
    lastFailedSummary: null,
    retryLastDownload: vi.fn(),
    clearError: vi.fn(),
    canRetry: false,
    selectedCount: 1,
    daysSelected: 1,
    handleStartDateChange: vi.fn(),
    handleEndDateChange: vi.fn(),
    handleGenerateSummary: mockHandleGenerateSummary,
    handleCancelGeneration: mockHandleCancelGeneration,
    handleSave: vi.fn(),
    handleDownloadMarkdown: vi.fn(),
    handleDateFilterApply: mockHandleDateFilterApply,
    handleViewSummary: vi.fn(),
    handleCloseModal: vi.fn(),
    handleDeleteConfirm: vi.fn(),
    handleDeleteCancel: vi.fn(),
    handleDownloadMd: vi.fn(),
    handleDownloadPdf: vi.fn(),
    formatDateForInput: (d: Date) => d.toISOString().split("T")[0],
    handleCreateTest: vi.fn(),
    preservedConfig: undefined,
    handleErrorRetry: vi.fn(),
    handleResetToIdle: vi.fn(),
    ...overrides,
  } as SummariesPageViewModel;
}

let currentViewModel = makeDefaultViewModel();

// Page tests render layout. The view model is stubbed so CDF is not called.
vi.mock("@/hooks/useSummariesPageModel", () => ({
  useSummariesPageModel: () => currentViewModel,
}));

function renderPage() {
  return render(<SummariesPage />);
}

describe("SummariesPage", () => {
  beforeEach(() => {
    currentViewModel = makeDefaultViewModel();
    vi.clearAllMocks();
  });

  it("renders heading and page badge", () => {
    renderPage();
    expect(
      screen.getByRole("heading", { name: /summaries/i }),
    ).toBeInTheDocument();
    expect(screen.getByText(/Page 1/)).toBeInTheDocument();
  });

  it("opens create form when Create new summary is clicked", () => {
    renderPage();
    fireEvent.click(
      screen.getByRole("button", { name: /create new summary/i }),
    );
    expect(mockSetShowCreateForm).toHaveBeenCalledWith(true);
  });

  it("calls enhancedRefresh from the refresh button", () => {
    renderPage();
    fireEvent.click(screen.getByRole("button", { name: /refresh/i }));
    expect(mockEnhancedRefresh).toHaveBeenCalled();
  });

  it("shows fetch and delete error banners", () => {
    currentViewModel = makeDefaultViewModel({
      isFetchError: true,
      deleteError: "Delete failed",
    });
    renderPage();
    expect(screen.getByText(/Failed to load summaries/i)).toBeInTheDocument();
    expect(screen.getByText("Delete failed")).toBeInTheDocument();
  });

  it("shows empty state with Get Started when no filters", () => {
    renderPage();
    expect(screen.getByText("No summaries found")).toBeInTheDocument();
    expect(screen.getByText(/Create your first summary/i)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /get started/i }));
    expect(mockSetShowCreateForm).toHaveBeenCalledWith(true);
  });

  it("shows filtered empty messaging when filters are active", () => {
    currentViewModel = makeDefaultViewModel({ hasActiveFilters: true });
    renderPage();
    expect(screen.getByText(/Try adjusting your filters/i)).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /get started/i }),
    ).not.toBeInTheDocument();
  });

  it("renders create form empty prompt and Generate button", () => {
    currentViewModel = makeDefaultViewModel({ showCreateForm: true });
    renderPage();
    expect(
      screen.getByText(/Configure options above and click Generate/i),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /generate \(1\)/i }),
    ).toBeInTheDocument();
  });

  it("disables Generate when no agent is selected", () => {
    currentViewModel = makeDefaultViewModel({
      showCreateForm: true,
      state: {
        ...makeDefaultViewModel().state,
        selectedAgent: null,
      },
      selectedCount: 1,
      daysSelected: 1,
    });
    renderPage();
    expect(screen.getByText(/No agent selected/i)).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /generate \(1\)/i }),
    ).toBeDisabled();
  });

  it("shows Stop waiting while generating and wires the cancel handler", () => {
    currentViewModel = makeDefaultViewModel({
      showCreateForm: true,
      isGenerating: true,
      elapsedSeconds: 3,
      progress: {
        status: "generating",
        progress: 40,
        message: "Generating in CDF Function…",
        currentStep: "Running",
      },
    });
    renderPage();
    expect(screen.getByText(/Generating in CDF Function/)).toBeInTheDocument();
    expect(screen.queryByText("40%")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /stop waiting/i }));
    expect(mockHandleCancelGeneration).toHaveBeenCalled();
  });

  it("applies list date filters and clears them", () => {
    currentViewModel = makeDefaultViewModel({
      startDate: "2024-01-01",
      endDate: "2024-01-31",
      hasActiveFilters: true,
    });
    renderPage();
    fireEvent.click(screen.getByRole("button", { name: /apply/i }));
    expect(mockHandleDateFilterApply).toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: /clear/i }));
    expect(mockClearFilters).toHaveBeenCalled();
    expect(mockSetStartDate).toHaveBeenCalledWith("");
    expect(mockSetEndDate).toHaveBeenCalledWith("");
  });

  it("renders SummaryTable when summaries exist", () => {
    currentViewModel = makeDefaultViewModel({ summaries: [summary] });
    renderPage();
    expect(screen.getAllByText("Jan Summary").length).toBeGreaterThan(0);
    expect(screen.queryByText("No summaries found")).not.toBeInTheDocument();
  });

  it("shows a loading table and refresh success styling", () => {
    currentViewModel = makeDefaultViewModel({
      isLoading: true,
      refreshSuccess: true,
      refreshMessage: "Updated",
      serverHasNext: true,
    });
    renderPage();
    expect(screen.getByLabelText("Loading summaries")).toBeInTheDocument();
    expect(screen.getByText(/paged/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /updated/i })).toBeDisabled();
  });

  it("shows the generated summary and a generation error", () => {
    const handleGenerateSummary = vi.fn();
    const handleStartDateChange = vi.fn();
    currentViewModel = makeDefaultViewModel({
      showCreateForm: true,
      generationError: null,
      handleGenerateSummary,
      handleStartDateChange,
      state: {
        ...makeDefaultViewModel().state,
        showSummary: true,
        generatedSummary: {
          workOrders: [],
          instances: [],
          fullText: "# Done",
          generatedAt: new Date("2024-06-01T00:00:00Z"),
        },
      },
    });
    renderPage();
    expect(screen.getByText("Done")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /generate \(1\)/i }));
    expect(handleGenerateSummary).toHaveBeenCalled();
    fireEvent.click(
      screen.getByRole("button", { name: /close create summary form/i }),
    );
    expect(mockSetShowCreateForm).toHaveBeenCalledWith(false);
  });

  it("opens download and recovery dialogs and confirms delete", () => {
    const clearError = vi.fn();
    const recoverFromGenerationError = vi.fn();
    const setDeleteCandidate = vi.fn();
    const toggleContentType = vi.fn();
    const toggleContentTypeFilter = vi.fn();
    currentViewModel = makeDefaultViewModel({
      downloadError: "PDF failed",
      lastFailedSummary: summary,
      canRetry: true,
      clearError,
      generationError: "Generation failed",
      canRetryGeneration: true,
      recoverFromGenerationError,
      preservedConfig: {
        dateRange: "Jan 1 – Jan 7",
        dataTypes: ["assets"],
        agent: "Helper",
      },
      deleteCandidate: summary,
      setDeleteCandidate,
      toggleContentType,
      toggleContentTypeFilter,
      refreshError: true,
      refreshMessage: "Refresh failed",
    });
    renderPage();
    expect(screen.getAllByText("PDF failed").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Generation failed").length).toBeGreaterThan(0);
    expect(screen.getByText(/Refresh failed/)).toBeInTheDocument();
    expect(screen.getByText("Download Failed")).toBeInTheDocument();
    expect(clearError).not.toHaveBeenCalled();
    expect(recoverFromGenerationError).not.toHaveBeenCalled();
    expect(setDeleteCandidate).not.toHaveBeenCalled();
  });

  it("disables refresh while a refresh is in flight and marks a previous page", () => {
    currentViewModel = makeDefaultViewModel({
      isRefreshingData: true,
      refreshMessage: "Refreshing",
      serverHasPrev: true,
      summaries: [summary],
    });
    renderPage();
    expect(screen.getByRole("button", { name: /refreshing/i })).toBeDisabled();
    expect(screen.getAllByText(/paged/).length).toBeGreaterThan(0);
  });

  it("updates list date inputs", () => {
    renderPage();
    fireEvent.change(screen.getByLabelText("Filter summaries from date"), {
      target: { value: "2024-02-01" },
    });
    fireEvent.change(screen.getByLabelText("Filter summaries to date"), {
      target: { value: "2024-02-28" },
    });
    expect(mockSetStartDate).toHaveBeenCalledWith("2024-02-01");
    expect(mockSetEndDate).toHaveBeenCalledWith("2024-02-28");
  });

  it("wires table view, test, and paging actions", () => {
    const handleViewSummary = vi.fn();
    const handleCreateTest = vi.fn();
    const goToNextPage = vi.fn();
    const goToPreviousPage = vi.fn();
    currentViewModel = makeDefaultViewModel({
      summaries: [summary],
      serverHasNext: true,
      serverHasPrev: true,
      handleViewSummary,
      handleCreateTest,
      goToNextPage,
      goToPreviousPage,
    });
    renderPage();
    fireEvent.click(screen.getAllByRole("button", { name: /^view$/i })[0]);
    fireEvent.click(screen.getAllByRole("button", { name: /^test$/i })[0]);
    fireEvent.click(screen.getAllByRole("button", { name: /^next$/i })[0]);
    fireEvent.click(screen.getAllByRole("button", { name: /^previous$/i })[0]);
    expect(handleViewSummary).toHaveBeenCalledWith(summary);
    expect(handleCreateTest).toHaveBeenCalledWith(summary);
    expect(goToNextPage).toHaveBeenCalled();
    expect(goToPreviousPage).toHaveBeenCalled();
  });

  it("confirms delete from the summary table", async () => {
    const handleDeleteConfirm = vi.fn().mockResolvedValue(undefined);
    currentViewModel = makeDefaultViewModel({
      summaries: [summary],
      handleDeleteConfirm,
    });
    renderPage();
    fireEvent.click(
      screen.getAllByRole("button", { name: /delete summary jan summary/i })[0],
    );
    fireEvent.click(screen.getByRole("button", { name: /^delete$/i }));
    await waitFor(() =>
      expect(handleDeleteConfirm).toHaveBeenCalledWith(summary),
    );
  });

  it("shows a pending page delete dialog", () => {
    currentViewModel = makeDefaultViewModel({
      deleteCandidate: summary,
      deleteMutation: { isPending: true, mutateAsync: vi.fn() },
    });
    renderPage();
    expect(screen.getByRole("dialog")).toHaveTextContent(/delet/i);
    expect(screen.getByRole("button", { name: /delet/i })).toBeDisabled();
  });

  it("cancels the page delete dialog", () => {
    const handleDeleteCancel = vi.fn();
    const setDeleteCandidate = vi.fn();
    currentViewModel = makeDefaultViewModel({
      deleteCandidate: summary,
      handleDeleteCancel,
      setDeleteCandidate,
    });
    renderPage();
    expect(screen.getByText("Jan Summary")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /^cancel$/i }));
    expect(handleDeleteCancel).toHaveBeenCalled();
    expect(setDeleteCandidate).toHaveBeenCalledWith(null);
  });

  it("retries, falls back to markdown, and dismisses a download error", async () => {
    const retryLastDownload = vi.fn().mockResolvedValue(undefined);
    const robustDownloadMarkdown = vi.fn().mockResolvedValue(undefined);
    const clearError = vi.fn();
    currentViewModel = makeDefaultViewModel({
      downloadError: "PDF failed",
      lastFailedSummary: summary,
      canRetry: true,
      retryLastDownload,
      robustDownloadMarkdown,
      clearError,
    });
    renderPage();
    fireEvent.click(
      screen.getByRole("button", { name: /try pdf download again/i }),
    );
    await waitFor(() => expect(retryLastDownload).toHaveBeenCalled());
    expect(clearError).toHaveBeenCalled();

    clearError.mockClear();
    fireEvent.click(
      screen.getByRole("button", { name: /download as markdown instead/i }),
    );
    await waitFor(() =>
      expect(robustDownloadMarkdown).toHaveBeenCalledWith(summary),
    );

    clearError.mockClear();
    fireEvent.click(
      screen.getByRole("button", { name: /cancel and continue browsing/i }),
    );
    expect(clearError).toHaveBeenCalled();
  });

  it("skips the markdown fallback when no summary is attached to the error", async () => {
    const robustDownloadMarkdown = vi.fn();
    const clearError = vi.fn();
    currentViewModel = makeDefaultViewModel({
      downloadError: "PDF failed",
      lastFailedSummary: null,
      canRetry: false,
      robustDownloadMarkdown,
      clearError,
    });
    renderPage();
    fireEvent.click(
      screen.getByRole("button", { name: /download as markdown instead/i }),
    );
    await waitFor(() => expect(clearError).toHaveBeenCalled());
    expect(robustDownloadMarkdown).not.toHaveBeenCalled();
  });

  it("retries, resets, and closes summary generation recovery", async () => {
    const handleErrorRetry = vi.fn().mockResolvedValue(undefined);
    const handleResetToIdle = vi.fn();
    const recoverFromGenerationError = vi.fn();
    currentViewModel = makeDefaultViewModel({
      generationError: "Generation failed",
      canRetryGeneration: true,
      handleErrorRetry,
      handleResetToIdle,
      recoverFromGenerationError,
      showCreateForm: true,
    });
    renderPage();
    fireEvent.click(
      screen.getByRole("button", { name: /try again with same settings/i }),
    );
    await waitFor(() => expect(handleErrorRetry).toHaveBeenCalled());
    expect(recoverFromGenerationError).toHaveBeenCalled();

    recoverFromGenerationError.mockClear();
    fireEvent.click(
      screen.getByRole("button", { name: /reset and modify settings/i }),
    );
    expect(handleResetToIdle).toHaveBeenCalled();
    expect(recoverFromGenerationError).toHaveBeenCalled();

    recoverFromGenerationError.mockClear();
    fireEvent.click(
      screen.getByRole("button", { name: /cancel and continue$/i }),
    );
    expect(recoverFromGenerationError).toHaveBeenCalled();
  });

  it("keeps the generation error on the form while a run is still in progress", () => {
    currentViewModel = makeDefaultViewModel({
      showCreateForm: true,
      isGenerating: true,
      generationError: "Still running",
      elapsedSeconds: 2,
      progress: {
        status: "generating",
        progress: 10,
        message: "",
        currentStep: undefined,
      },
    });
    renderPage();
    expect(screen.getByText("Still running")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /try again with same settings/i }),
    ).not.toBeInTheDocument();
    expect(screen.getByText(/Generating in CDF Function/)).toBeInTheDocument();
  });

  it("closes the create form from the header toggle and downloads the result", () => {
    const handleDownloadMarkdown = vi.fn();
    currentViewModel = makeDefaultViewModel({
      showCreateForm: true,
      handleDownloadMarkdown,
      savedPdfInfo: {
        fileName: "summary.pdf",
        fileId: "file-1",
      },
      isSaving: false,
      saveError: "Could not save",
      state: {
        ...makeDefaultViewModel().state,
        showSummary: true,
        generatedSummary: {
          workOrders: [],
          instances: [],
          fullText: "# Done",
          generatedAt: new Date("2024-06-01T00:00:00Z"),
        },
      },
    });
    renderPage();
    expect(screen.getByText("Could not save")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /download md/i }));
    expect(handleDownloadMarkdown).toHaveBeenCalled();
    fireEvent.click(
      screen.getByRole("button", { name: /create new summary/i }),
    );
    expect(mockSetShowCreateForm).toHaveBeenCalledWith(false);
  });
});
