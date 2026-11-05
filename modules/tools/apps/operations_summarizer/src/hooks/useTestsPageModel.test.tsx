import { QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import {
  NavigationProvider,
  useNavigation,
} from "@/contexts/NavigationContext";
import { OperationsProvider } from "@/contexts/OperationsSummarizerContext";
import {
  type ComparisonTestVmValue,
  type TestsPageModelDeps,
  TestsPageModelDepsProvider,
} from "@/contexts/TestsPageModelDepsContext";
import type {
  TestsPageCdfBundleValue,
  TestsPageDeleteMutationRef,
} from "@/hooks/useDefaultTestsPageCdfBundle";
import { createMockSdk } from "@/test-utils/mockSdk";
import { createTestQueryClient } from "@/test-utils/wrappers";
import type {
  DataTypeName,
  SummaryInstance,
  TestResultData,
} from "@/types/operations";
import { encodeTestBaselineSelectionKey } from "@/utils/summaryInstanceMapping";

const hoisted = vi.hoisted(() => {
  const viewKey = "cdf_cdm/CogniteFile/v1";
  const opSummary: SummaryInstance = {
    externalId: "ops_summary_2024-01-01_2024-01-31_z",
    space: "ops_summaries",
    createdTime: 1_700_000_000_000,
    properties: {
      [viewKey]: {
        content: "# R\nSimilarity Score: 85/100",
        name: "r",
      },
    },
  };
  const testReport: SummaryInstance = {
    externalId: "test_report_a",
    space: "ops_summaries",
    createdTime: 1,
    properties: {
      [viewKey]: {
        content: "# T\nSimilarity Score: 80/100",
        name: "t",
      },
    },
  };
  const refetchList = vi.fn();
  const refetchPicker = vi.fn();
  const refetchStats = vi.fn();
  const mutateAsyncDelete = vi.fn().mockResolvedValue(true);
  const runComparisonTest = vi.fn().mockResolvedValue(undefined);
  const saveTestReportToCdf = vi.fn().mockResolvedValue(undefined);
  const opSummaryFeb: SummaryInstance = {
    externalId: "ops_summary_feb",
    space: "ops_summaries",
    createdTime: new Date("2024-02-15T12:00:00Z").getTime(),
    properties: {
      [viewKey]: { content: "# x", name: "feb" },
    },
  };
  const opSummaryJun: SummaryInstance = {
    externalId: "ops_summary_jun",
    space: "ops_summaries",
    createdTime: new Date("2024-06-01T12:00:00Z").getTime(),
    properties: {
      [viewKey]: { content: "# x", name: "jun" },
    },
  };
  const opSummaryContentDates: SummaryInstance = {
    externalId: "custom_no_ops_prefix",
    space: "ops_summaries",
    createdTime: 1,
    properties: {
      [viewKey]: {
        name: "n",
        content:
          "Summary for 2024-05-10 to 2024-05-20\nSimilarity Score: 90/100",
      },
    },
  };
  return {
    opSummary,
    opSummaryFeb,
    opSummaryJun,
    opSummaryContentDates,
    testReport,
    refetchList,
    refetchPicker,
    refetchStats,
    mutateAsyncDelete,
    runComparisonTest,
    saveTestReportToCdf,
  };
});

function getDefaultTestsPageCdfBundle(): TestsPageCdfBundleValue {
  return {
    testReports: [hoisted.testReport],
    testReportsLoading: false,
    isTestReportsError: false,
    refetchTestReports: hoisted.refetchList,
    testReportPageDisplay: 1,
    testReportsHasNext: false,
    testReportsHasPrev: false,
    testReportsGoNext: vi.fn(),
    testReportsGoPrev: vi.fn(),
    summaries: [hoisted.opSummary],
    summariesPickerLoading: false,
    isSummariesPickerError: false,
    refetchSummariesPicker: hoisted.refetchPicker,
    streamedTestStats: {
      totalTests: 1,
      averageScore: 80,
      excellentCount: 0,
      goodCount: 1,
      poorCount: 0,
      averageDataVolume: 0,
    },
    testStatsLoading: false,
    isTestStatsError: false,
    refetchTestStats: hoisted.refetchStats,
    deleteMutation: {
      mutateAsync: hoisted.mutateAsyncDelete,
      isPending: false,
    } satisfies TestsPageDeleteMutationRef,
  };
}

function useTestTestsPageCdfBundle(): TestsPageCdfBundleValue {
  return getDefaultTestsPageCdfBundle();
}

function buildTestsPageDeps(
  bundlePatch: Partial<TestsPageCdfBundleValue> = {},
): TestsPageModelDeps {
  function useBundle(): TestsPageCdfBundleValue {
    return { ...getDefaultTestsPageCdfBundle(), ...bundlePatch };
  }
  return {
    useTestsPageCdfBundle: useBundle,
    useComparisonTest: useTestComparisonTest,
    saveTestReportToCdf: hoisted.saveTestReportToCdf,
  };
}

function useTestComparisonTest(): ComparisonTestVmValue {
  return {
    runComparisonTest: hoisted.runComparisonTest,
    isRunning: false,
  };
}

const testTestsPageModelDeps: TestsPageModelDeps = {
  useTestsPageCdfBundle: useTestTestsPageCdfBundle,
  useComparisonTest: useTestComparisonTest,
  saveTestReportToCdf: hoisted.saveTestReportToCdf,
};

// OperationsProvider still reads the Fusion shell client through CdfClientProvider.
// Stub the Fusion host so unit tests do not boot the real App SDK.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: vi.fn(),
}));

import { useCogniteSdk } from "@cognite/app-sdk/react";
import type { CogniteClient } from "@cognite/sdk";
import {
  UseTestsPageClientContext,
  useTestsPageModel,
} from "./useTestsPageModel";

const emptyCounts: TestResultData["originalDataCounts"] = {
  assets: 0,
  files: 0,
  work_orders: 0,
  time_series: 0,
  equipment: 0,
};

function makeTestResultData(
  overrides: Partial<TestResultData> = {},
): TestResultData {
  return {
    originalSummaryId: hoisted.opSummary.externalId,
    originalSummarySpace: hoisted.opSummary.space,
    originalContent: "orig",
    newSummary: "new",
    similarityScore: 80,
    dataAccuracyScore: 80,
    originalDataCounts: emptyCounts,
    newDataCounts: emptyCounts,
    comparisonAnalysis: "analysis",
    timestamp: new Date().toISOString(),
    ...overrides,
  };
}

describe("useTestsPageModel", () => {
  function setup(options?: {
    deps?: TestsPageModelDeps;
    sdk?: ReturnType<typeof useCogniteSdk> | null;
  }) {
    const sdk =
      options && "sdk" in options
        ? options.sdk
        : createMockSdk({ project: "p" });
    vi.mocked(useCogniteSdk).mockReturnValue(
      sdk as ReturnType<typeof useCogniteSdk>,
    );

    const deps = options?.deps ?? testTestsPageModelDeps;
    const queryClient = createTestQueryClient();
    function Wrapper({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={queryClient}>
          <UseTestsPageClientContext.Provider
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
                  <TestsPageModelDepsProvider deps={deps}>
                    {children}
                  </TestsPageModelDepsProvider>
                </OperationsProvider>
              </NavigationProvider>
            </CdfClientProvider>
          </UseTestsPageClientContext.Provider>
        </QueryClientProvider>
      );
    }
    return { Wrapper };
  }

  it("exposes filtered test reports and summaries", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.testReports).toHaveLength(1);
    });
    expect(result.current.summaries[0]?.externalId).toBe(
      hoisted.opSummary.externalId,
    );
    expect(result.current.stats.totalTests).toBe(1);
  });

  it("handleRunTest invokes comparison when summary selected", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    await waitFor(() =>
      expect(result.current.summaries.length).toBeGreaterThan(0),
    );

    act(() => {
      result.current.setSelectedSummaryId(
        encodeTestBaselineSelectionKey(hoisted.opSummary),
      );
    });

    await waitFor(() =>
      expect(result.current.selectedSummary?.externalId).toBe(
        hoisted.opSummary.externalId,
      ),
    );

    act(() => {
      result.current.handleRunTest();
    });

    expect(hoisted.runComparisonTest).toHaveBeenCalledWith(hoisted.opSummary);
  });

  it("handleRunTest resolves baseline when state stores legacy externalId only", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    await waitFor(() =>
      expect(result.current.summaries.length).toBeGreaterThan(0),
    );

    act(() => {
      result.current.setSelectedSummaryId(hoisted.opSummary.externalId);
    });

    await waitFor(() =>
      expect(result.current.selectedSummary?.externalId).toBe(
        hoisted.opSummary.externalId,
      ),
    );

    act(() => {
      result.current.handleRunTest();
    });

    expect(hoisted.runComparisonTest).toHaveBeenCalledWith(hoisted.opSummary);
  });

  it("handleDelete calls delete mutation", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setDeleteTarget(hoisted.testReport);
    });

    await act(async () => {
      await result.current.handleDelete();
    });

    expect(hoisted.mutateAsyncDelete).toHaveBeenCalledWith(hoisted.testReport);
  });

  it("handleCreateSummaryFirst navigates to summaries", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(
      () => {
        const nav = useNavigation();
        const vm = useTestsPageModel();
        return { nav, vm };
      },
      { wrapper: Wrapper },
    );

    act(() => {
      result.current.vm.handleCreateSummaryFirst();
    });

    expect(result.current.nav.currentPage).toBe("summaries");
  });

  it("clearHistoryFilters resets score range", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setMinScore(10);
      result.current.setMaxScore(90);
    });
    expect(result.current.hasActiveHistoryFilters).toBe(true);

    act(() => {
      result.current.clearHistoryFilters();
    });
    expect(result.current.minScore).toBe(0);
    expect(result.current.maxScore).toBe(100);
  });

  it("filteredReports excludes reports outside score range", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    await waitFor(() =>
      expect(result.current.filteredReports.length).toBeGreaterThan(0),
    );

    act(() => {
      result.current.setMinScore(81);
      result.current.setMaxScore(100);
    });
    expect(result.current.filteredReports).toHaveLength(0);

    act(() => {
      result.current.setMinScore(0);
      result.current.setMaxScore(100);
    });
    expect(result.current.filteredReports.length).toBeGreaterThan(0);
  });

  it("filteredSummaries applies date window to picker list", async () => {
    const { Wrapper } = setup({
      deps: buildTestsPageDeps({
        summaries: [hoisted.opSummaryFeb, hoisted.opSummaryJun],
      }),
    });
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    await waitFor(() => expect(result.current.summaries).toHaveLength(2));

    act(() => {
      result.current.setFilterStartDate("2024-01-01");
      result.current.setFilterEndDate("2024-03-31");
    });
    expect(result.current.filteredSummaries).toHaveLength(1);
    expect(result.current.filteredSummaries[0]?.externalId).toBe(
      hoisted.opSummaryFeb.externalId,
    );
  });

  it("stats reflects test stats error state", async () => {
    const { Wrapper } = setup({
      deps: buildTestsPageDeps({
        isTestStatsError: true,
        streamedTestStats: undefined,
      }),
    });
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.stats.unavailable).toBe(true);
      expect(result.current.stats.totalTests).toBe(0);
    });
  });

  it("stats returns zeros when streamed stats not yet loaded", async () => {
    const { Wrapper } = setup({
      deps: buildTestsPageDeps({
        streamedTestStats: undefined,
        isTestStatsError: false,
      }),
    });
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    await waitFor(() => {
      expect(result.current.stats.unavailable).toBe(false);
      expect(result.current.stats.totalTests).toBe(0);
    });
  });

  it("testResult is null when baseline summary is missing from picker", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setTestResultData(
        makeTestResultData({
          originalSummaryId: "missing-id",
          originalSummarySpace: hoisted.opSummary.space,
        }),
      );
    });

    expect(result.current.testResult).toBeNull();
  });

  it("toggleDataType adds and removes types", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setSelectedDataTypes(["assets"]);
    });
    const first = "assets" as DataTypeName;
    act(() => {
      result.current.toggleDataType(first);
    });
    expect(result.current.selectedDataTypes).not.toContain(first);

    act(() => {
      result.current.toggleDataType(first);
    });
    expect(result.current.selectedDataTypes).toContain(first);
  });

  it("saveTestResults returns early without sdk or testResult", async () => {
    hoisted.saveTestReportToCdf.mockClear();
    const { Wrapper } = setup({ sdk: null });
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    await act(async () => {
      await result.current.saveTestResults();
    });
    expect(hoisted.saveTestReportToCdf).not.toHaveBeenCalled();

    const { Wrapper: w2 } = setup();
    const { result: r2 } = renderHook(() => useTestsPageModel(), {
      wrapper: w2,
    });
    hoisted.saveTestReportToCdf.mockClear();
    await act(async () => {
      await r2.current.saveTestResults();
    });
    expect(hoisted.saveTestReportToCdf).not.toHaveBeenCalled();
  });

  it("saveTestResults saves, refetches, and clears results", async () => {
    hoisted.saveTestReportToCdf.mockClear();
    hoisted.refetchList.mockClear();
    hoisted.refetchPicker.mockClear();
    hoisted.refetchStats.mockClear();

    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setTestStartDate("2024-01-01");
      result.current.setTestEndDate("2024-01-31");
      result.current.setTestResultData(makeTestResultData());
      result.current.setShowCreateForm(true);
    });

    await act(async () => {
      await result.current.saveTestResults();
    });

    expect(hoisted.saveTestReportToCdf).toHaveBeenCalled();
    expect(hoisted.refetchList).toHaveBeenCalled();
    expect(hoisted.refetchPicker).toHaveBeenCalled();
    expect(hoisted.refetchStats).toHaveBeenCalled();
    expect(result.current.testResultData).toBeNull();
    expect(result.current.showCreateForm).toBe(false);
  });

  it("saveTestResults sets savingError when save fails", async () => {
    hoisted.saveTestReportToCdf.mockRejectedValueOnce(new Error("cdf"));
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setTestResultData(makeTestResultData());
    });

    await act(async () => {
      await result.current.saveTestResults();
    });

    expect(result.current.savingError).toBeTruthy();
  });

  it("exportToYaml downloadYaml and copyYamlToClipboard", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setTestResultData(makeTestResultData());
      result.current.setTestStartDate("2024-01-01");
      result.current.setTestEndDate("2024-01-31");
    });

    await waitFor(() => expect(result.current.testResult).not.toBeNull());

    act(() => {
      result.current.exportToYaml();
    });
    expect(result.current.yamlExported).toBe(true);
    expect(result.current.yamlContent.length).toBeGreaterThan(0);

    const click = vi.fn();
    const link = document.createElement("a");
    const clickSpy = vi.spyOn(link, "click").mockImplementation(click);
    const origCreateEl = document.createElement.bind(document);
    const createElSpy = vi
      .spyOn(document, "createElement")
      .mockImplementation((tag: string, options?: ElementCreationOptions) => {
        if (tag === "a") return link;
        return origCreateEl(tag, options);
      });
    const urlSpy = vi
      .spyOn(URL, "createObjectURL")
      .mockReturnValue("blob:test");
    const revokeSpy = vi
      .spyOn(URL, "revokeObjectURL")
      .mockImplementation(() => {});

    try {
      act(() => {
        result.current.downloadYaml();
      });
      expect(click).toHaveBeenCalled();
    } finally {
      createElSpy.mockRestore();
      clickSpy.mockRestore();
      urlSpy.mockRestore();
      revokeSpy.mockRestore();
    }

    const writeText = vi.fn().mockResolvedValue(undefined);
    const clipboardDescriptor = Object.getOwnPropertyDescriptor(
      globalThis.navigator,
      "clipboard",
    );
    Object.defineProperty(globalThis.navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    try {
      await act(async () => {
        await result.current.copyYamlToClipboard();
      });
      expect(writeText).toHaveBeenCalledWith(result.current.yamlContent);
    } finally {
      if (clipboardDescriptor) {
        Object.defineProperty(
          globalThis.navigator,
          "clipboard",
          clipboardDescriptor,
        );
      } else {
        Reflect.deleteProperty(globalThis.navigator, "clipboard");
      }
    }
  });

  it("clearResults resets test run state", async () => {
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setTestResultData(makeTestResultData());
      result.current.setYamlContent("x");
      result.current.setYamlExported(true);
      result.current.setTestError("e");
      result.current.setGenerationProgress({
        status: "generating",
        message: "m",
        progress: 50,
      });
    });

    act(() => {
      result.current.clearResults();
    });

    expect(result.current.testResultData).toBeNull();
    expect(result.current.yamlContent).toBe("");
    expect(result.current.yamlExported).toBe(false);
    expect(result.current.testError).toBeNull();
    expect(result.current.generationProgress.status).toBe("idle");
  });

  it("handleDelete sets deleteError when mutation fails", async () => {
    hoisted.mutateAsyncDelete.mockRejectedValueOnce(new Error("del"));
    const { Wrapper } = setup();
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setDeleteTarget(hoisted.testReport);
    });

    await act(async () => {
      await result.current.handleDelete();
    });

    expect(result.current.deleteError).toBeTruthy();
  });

  it("populates test dates from summary content when externalId has no date pattern", async () => {
    const { Wrapper } = setup({
      deps: buildTestsPageDeps({
        summaries: [hoisted.opSummaryContentDates],
      }),
    });
    const { result } = renderHook(() => useTestsPageModel(), {
      wrapper: Wrapper,
    });

    act(() => {
      result.current.setSelectedSummaryId(
        encodeTestBaselineSelectionKey(hoisted.opSummaryContentDates),
      );
    });

    await waitFor(() => {
      expect(result.current.testStartDate).toBe("2024-05-10");
      expect(result.current.testEndDate).toBe("2024-05-20");
    });
  });
});
