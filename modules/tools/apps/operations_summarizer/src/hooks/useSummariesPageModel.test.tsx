import { QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { useEffect } from "react";
import { describe, expect, it, vi } from "vitest";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import { NavigationProvider } from "@/contexts/NavigationContext";
import {
  OperationsProvider,
  useOperations,
} from "@/contexts/OperationsSummarizerContext";
import {
  type SummariesPageModelDeps,
  SummariesPageModelDepsProvider,
  type SummariesRobustDownloadValue,
  type SummaryGenerationVmValue,
} from "@/contexts/SummariesPageModelDepsContext";
import type {
  SummariesListDeleteMutationRef,
  SummariesListPipelineValue,
  SummariesListSaveMutationRef,
} from "@/hooks/useDefaultSummariesListPipeline";
import { createMockSdk } from "@/test-utils/mockSdk";
import { createTestQueryClient } from "@/test-utils/wrappers";
import type { SummaryInstance } from "@/types/operations";

const hoisted = vi.hoisted(() => {
  const summaryFixtures: SummaryInstance[] = [
    {
      externalId: "ops_summary_2024-01-01_2024-01-31_x",
      space: "ops_summaries",
      createdTime: 1,
      properties: {},
    },
  ];
  const refetchList = vi.fn();
  const mutateAsyncDelete = vi.fn().mockResolvedValue(true);
  const mutateAsyncSave = vi.fn().mockResolvedValue({
    fileName: "a.pdf",
    externalId: "new-id",
  });
  const generate = vi.fn().mockResolvedValue({
    markdown: "# Generated",
    generatedAt: new Date(),
    dataTypeTruncation: {},
  });
  const robustDownloadPdf = vi.fn().mockResolvedValue(undefined);
  const robustDownloadMarkdown = vi.fn().mockResolvedValue(undefined);
  const signedUrlFirst = vi.fn().mockResolvedValue(undefined);
  const toggleDateFilter = vi.fn();
  const setDateRange = vi.fn();
  const downloadAsMarkdown = vi.fn();
  return {
    summaryFixtures,
    refetchList,
    mutateAsyncDelete,
    mutateAsyncSave,
    generate,
    robustDownloadPdf,
    robustDownloadMarkdown,
    signedUrlFirst,
    toggleDateFilter,
    setDateRange,
    downloadAsMarkdown,
  };
});

function useTestSummariesListPipeline(): SummariesListPipelineValue {
  return {
    dateFilterEnabled: false,
    dateRange: null,
    contentTypeFilterEnabled: false,
    selectedContentTypes: [],
    setDateRange: hoisted.setDateRange,
    toggleContentType: vi.fn(),
    setSelectedContentTypes: vi.fn(),
    hasActiveFilters: false,
    toggleDateFilter: hoisted.toggleDateFilter,
    toggleContentTypeFilter: vi.fn(),
    items: hoisted.summaryFixtures,
    isLoading: false,
    isError: false,
    refetch: hoisted.refetchList,
    pageDisplay: 1,
    hasNextPage: false,
    hasPreviousPage: false,
    goToNextPage: vi.fn(),
    goToPreviousPage: vi.fn(),
    deleteMutation: {
      mutateAsync: hoisted.mutateAsyncDelete,
      isPending: false,
    } satisfies SummariesListDeleteMutationRef,
    saveMutation: {
      mutateAsync: hoisted.mutateAsyncSave,
      isPending: false,
    } satisfies SummariesListSaveMutationRef,
    clearFilters: vi.fn(),
  };
}

function useTestSummaryGeneration(): SummaryGenerationVmValue {
  return {
    generate: hoisted.generate,
    cancel: vi.fn(),
    progress: { status: "idle", message: "", progress: 0 },
    isGenerating: false,
    elapsedSeconds: 0,
    result: null,
    error: null,
    reset: vi.fn(),
    recoverFromError: vi.fn(),
    retryGeneration: vi.fn(),
    canRetry: false,
  };
}

function useTestSummariesRobustDownload(): SummariesRobustDownloadValue {
  return {
    downloadPdf: hoisted.robustDownloadPdf,
    downloadMarkdown: hoisted.robustDownloadMarkdown,
    isDownloading: false,
    downloadError: null,
    lastFailedSummary: null,
    retryLastDownload: vi.fn(),
    clearError: vi.fn(),
    canRetry: false,
  };
}

const testSummariesPageModelDeps: SummariesPageModelDeps = {
  useSummariesListPipeline: useTestSummariesListPipeline,
  useSummaryGeneration: useTestSummaryGeneration,
  useSummariesRobustDownload: useTestSummariesRobustDownload,
  downloadAsMarkdown: hoisted.downloadAsMarkdown,
  downloadOperationSummaryPdfWithSignedUrlFirst: hoisted.signedUrlFirst,
};

// OperationsProvider still reads the Fusion shell client through CdfClientProvider.
// Stub the Fusion host so unit tests do not boot the real App SDK.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: vi.fn(),
}));

import { useCogniteSdk } from "@cognite/app-sdk/react";
import type { CogniteClient } from "@cognite/sdk";
import {
  UseSummariesPageClientContext,
  useSummariesPageModel,
} from "./useSummariesPageModel";

function AutoAgent({ id }: { id: string }) {
  const { setSelectedAgent } = useOperations();
  useEffect(() => {
    setSelectedAgent(id);
  }, [id, setSelectedAgent]);
  return null;
}

describe("useSummariesPageModel", () => {
  function setup(options?: {
    skipAutoAgent?: boolean;
    sdk?: ReturnType<typeof useCogniteSdk> | null;
    deps?: SummariesPageModelDeps;
  }) {
    const sdk =
      options && "sdk" in options
        ? options.sdk
        : createMockSdk({ project: "p" });
    vi.mocked(useCogniteSdk).mockReturnValue(
      sdk as ReturnType<typeof useCogniteSdk>,
    );
    const queryClient = createTestQueryClient();
    const deps = options?.deps ?? testSummariesPageModelDeps;
    function Wrapper({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={queryClient}>
          <UseSummariesPageClientContext.Provider
            value={{
              useCdfClient: () => ({
                sdk: (sdk ?? null) as CogniteClient | null,
                isAuthLoading: !sdk,
              }),
            }}
          >
            <CdfClientProvider>
              <NavigationProvider>
                <OperationsProvider>
                  <SummariesPageModelDepsProvider deps={deps}>
                    {options?.skipAutoAgent ? null : <AutoAgent id="agent-1" />}
                    {children}
                  </SummariesPageModelDepsProvider>
                </OperationsProvider>
              </NavigationProvider>
            </CdfClientProvider>
          </UseSummariesPageClientContext.Provider>
        </QueryClientProvider>
      );
    }
    return { Wrapper };
  }

  function buildDepsWithGeneration(
    gen: Partial<SummaryGenerationVmValue>,
  ): SummariesPageModelDeps {
    function useGen(): SummaryGenerationVmValue {
      return {
        generate: hoisted.generate,
        cancel: vi.fn(),
        progress: { status: "idle", message: "", progress: 0 },
        isGenerating: false,
        elapsedSeconds: 0,
        result: null,
        error: null,
        reset: vi.fn(),
        recoverFromError: vi.fn(),
        retryGeneration: vi.fn(),
        canRetry: false,
        ...gen,
      };
    }
    return {
      ...testSummariesPageModelDeps,
      useSummaryGeneration: useGen,
    };
  }

  it("exposes summaries and opens modal on view", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.summaries).toHaveLength(1);
    });

    act(() => {
      result.current.handleViewSummary(hoisted.summaryFixtures[0]);
    });
    expect(result.current.isModalOpen).toBe(true);
    expect(result.current.selectedSummary?.externalId).toBe(
      hoisted.summaryFixtures[0].externalId,
    );

    act(() => {
      result.current.handleCloseModal();
    });
    expect(result.current.isModalOpen).toBe(false);
  });

  it("handleDeleteConfirm calls delete mutation", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setDeleteCandidate(hoisted.summaryFixtures[0]);
    });

    await act(async () => {
      await result.current.handleDeleteConfirm();
    });

    expect(hoisted.mutateAsyncDelete).toHaveBeenCalledWith(
      hoisted.summaryFixtures[0],
    );
  });

  it("handleGenerateSummary dispatches when agent selected", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    await waitFor(() =>
      expect(result.current.state.selectedAgent).toBe("agent-1"),
    );

    act(() => {
      result.current.setShowCreateForm(true);
    });
    hoisted.generate.mockResolvedValueOnce({
      markdown: "# Generated",
      generatedAt: new Date(),
      dataTypeTruncation: {},
      cdfResult: { external_id: "ops_summary_1", file_name: "summary.pdf" },
    });

    await act(async () => {
      await result.current.handleGenerateSummary();
    });

    expect(hoisted.generate).toHaveBeenCalled();
    await waitFor(() => {
      expect(result.current.state.showSummary).toBe(true);
    });
    expect(result.current.showCreateForm).toBe(true);
  });

  it("handleDownloadMarkdown calls downloadAsMarkdown with cleaned text", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    await act(async () => {
      result.current.dispatch({
        type: "SET_GENERATED_SUMMARY",
        payload: {
          workOrders: [],
          instances: [],
          fullText: "<p># Doc</p>",
          generatedAt: new Date(),
          dataTypeTruncation: {},
        },
      });
    });

    act(() => {
      result.current.handleDownloadMarkdown();
    });

    expect(hoisted.downloadAsMarkdown).toHaveBeenCalled();
    const [text] = hoisted.downloadAsMarkdown.mock.calls[0] ?? [];
    expect(String(text)).toContain("# Doc");
  });

  it("handleDownloadMd delegates to robust markdown download", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    await act(async () => {
      await result.current.handleDownloadMd(hoisted.summaryFixtures[0]);
    });
    expect(hoisted.robustDownloadMarkdown).toHaveBeenCalledWith(
      hoisted.summaryFixtures[0],
    );
  });

  it("handleDownloadPdf uses robust path when sdk is missing", async () => {
    hoisted.robustDownloadPdf.mockClear();
    hoisted.signedUrlFirst.mockClear();
    const { Wrapper } = setup({ sdk: null });
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    await act(async () => {
      await result.current.handleDownloadPdf(hoisted.summaryFixtures[0]);
    });

    expect(hoisted.robustDownloadPdf).toHaveBeenCalledWith(
      hoisted.summaryFixtures[0],
    );
    expect(hoisted.signedUrlFirst).not.toHaveBeenCalled();
  });

  it("handleDateFilterApply enables date filter and sets range", () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setStartDate("2024-01-01");
      result.current.setEndDate("2024-01-31");
    });

    act(() => {
      result.current.handleDateFilterApply();
    });

    expect(hoisted.toggleDateFilter).toHaveBeenCalled();
    expect(hoisted.setDateRange).toHaveBeenCalledWith({
      start: new Date("2024-01-01"),
      end: new Date("2024-01-31"),
    });
  });

  it("handleDeleteConfirm uses summary argument and clears deleteCandidate", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    await act(async () => {
      await result.current.handleDeleteConfirm(hoisted.summaryFixtures[0]);
    });

    expect(hoisted.mutateAsyncDelete).toHaveBeenCalledWith(
      hoisted.summaryFixtures[0],
    );
    expect(result.current.deleteCandidate).toBeNull();
  });

  it("handleDeleteConfirm closes modal when deleting viewed summary", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.handleViewSummary(hoisted.summaryFixtures[0]);
    });
    act(() => {
      result.current.setDeleteCandidate(hoisted.summaryFixtures[0]);
    });

    await act(async () => {
      await result.current.handleDeleteConfirm();
    });

    expect(result.current.isModalOpen).toBe(false);
  });

  it("handleDeleteConfirm sets deleteError when mutation fails", async () => {
    hoisted.mutateAsyncDelete.mockRejectedValueOnce(new Error("network"));
    const { Wrapper } = setup();
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setDeleteCandidate(hoisted.summaryFixtures[0]);
    });

    await act(async () => {
      await result.current.handleDeleteConfirm();
    });

    expect(result.current.deleteError).toBeTruthy();
  });

  it("handleDeleteCancel clears deleteCandidate", () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setDeleteCandidate(hoisted.summaryFixtures[0]);
    });
    act(() => {
      result.current.handleDeleteCancel();
    });
    expect(result.current.deleteCandidate).toBeNull();
  });

  it("preservedConfig and recovery handlers use generation deps", async () => {
    const retryGeneration = vi.fn();
    const recoverFromError = vi.fn();
    const deps = buildDepsWithGeneration({
      error: "generation failed",
      retryGeneration,
      recoverFromError,
    });
    const { Wrapper } = setup({ deps });
    const { result } = renderHook(() => useSummariesPageModel(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.preservedConfig?.agent).toBeTruthy();
    });

    await act(async () => {
      await result.current.handleErrorRetry();
    });
    expect(retryGeneration).toHaveBeenCalled();

    act(() => {
      result.current.handleResetToIdle();
    });
    expect(recoverFromError).toHaveBeenCalled();
    expect(result.current.state.showSummary).toBe(false);
  });
});
