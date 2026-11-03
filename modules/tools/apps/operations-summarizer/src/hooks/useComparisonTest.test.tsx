import type { CogniteClient } from "@cognite/sdk";
import { act, renderHook, waitFor } from "@testing-library/react";
import { type ReactNode, useEffect } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getAppConfig } from "@/config/appConfig";
import { getEnabledDataTypes } from "@/config/dataTypes";
import { useOperations } from "@/contexts/OperationsSummarizerContext";
import { CDF_QUERY_KEYS } from "@/hooks/cdfQueryKeys";
import { useTestingState } from "@/hooks/useTestingState";
import { CDFHelper } from "@/services/cdfHelper";
import { CogniteSummaryFunctionService } from "@/services/summaryFunctionService";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import { createOperationsWithQueryWrapper } from "@/test-utils/wrappers";
import type {
  ChatCompletionResponse,
  SummaryInstance,
} from "@/types/operations";
import { VIEW_IDS } from "@/types/operations";
import {
  UseComparisonTestContext,
  useComparisonTest,
} from "./useComparisonTest";

// OperationsProvider still reads the Fusion shell client through CdfClientProvider.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: vi.fn(),
}));

import { useCogniteSdk } from "@cognite/app-sdk/react";

const { Wrapper: AppWrapper, queryClient } = createOperationsWithQueryWrapper();

const viewKey = `${VIEW_IDS.SUMMARY_OUTPUT_FILE.space}/${VIEW_IDS.SUMMARY_OUTPUT_FILE.externalId}/${VIEW_IDS.SUMMARY_OUTPUT_FILE.version}`;

type PeriodSeed = {
  dataTypes?: string[];
  running?: boolean;
};

let periodSeed: PeriodSeed | null = null;

function Wrapper({ children }: { children: ReactNode }) {
  return (
    <UseComparisonTestContext.Provider
      value={{
        useCdfClient: () => ({
          sdk: (vi.mocked(useCogniteSdk)() ?? null) as CogniteClient | null,
          isAuthLoading: !vi.mocked(useCogniteSdk)(),
        }),
      }}
    >
      <AppWrapper>
        <SeedPeriod />
        {children}
      </AppWrapper>
    </UseComparisonTestContext.Provider>
  );
}

describe("useComparisonTest", () => {
  beforeEach(() => {
    periodSeed = null;
    queryClient.clear();
    vi.mocked(useCogniteSdk).mockReturnValue(
      // @ts-expect-error Exercises null SDK path (CdfClientContext allows null before auth completes)
      null,
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("runComparisonTest returns early when sdk unavailable", async () => {
    const { result } = renderHook(() => useComparisonTest(), {
      wrapper: Wrapper,
    });

    const summary: SummaryInstance = {
      externalId: "x",
      space: "s",
      createdTime: 1,
      properties: {},
    };

    await act(async () => {
      await result.current.runComparisonTest(summary);
    });

    expect(result.current.isRunning).toBe(false);
  });

  it("runComparisonTest returns early when test period dates are not set", async () => {
    vi.mocked(useCogniteSdk).mockReturnValue(
      cogniteClientMock({
        project: "p",
        get: vi
          .fn()
          .mockResolvedValue({ data: { items: [], invalidAgents: [] } }),
        post: vi.fn().mockResolvedValue({ data: {} }),
        instances: { list: vi.fn().mockResolvedValue({ items: [] }) },
      }) as ReturnType<typeof useCogniteSdk>,
    );

    const { result } = renderHook(() => useComparisonTest(), {
      wrapper: Wrapper,
    });

    const summary: SummaryInstance = {
      externalId: "x",
      space: "s",
      createdTime: 1,
      properties: {},
    };

    await act(async () => {
      await result.current.runComparisonTest(summary);
    });

    expect(result.current.isRunning).toBe(false);
  });

  it("returns early while a comparison is already running", async () => {
    const generate = stubComparisonApis();
    periodSeed = { running: true };
    const { result } = renderHook(() => useComparisonHarness(), {
      wrapper: Wrapper,
    });
    await waitFor(() => expect(result.current.testing.isRunning).toBe(true));

    await act(async () => {
      await result.current.comparison.runComparisonTest(
        summaryWithContent("10 assets"),
      );
    });

    expect(generate).not.toHaveBeenCalled();
    expect(result.current.testing.isRunning).toBe(true);
  });

  it("stores scores when the helper agent already exists", async () => {
    const generate = stubComparisonApis({
      agents: [
        {
          externalId: "helper-agent",
          name: getAppConfig().agent.name,
        },
      ],
    });
    periodSeed = { dataTypes: ["files"] };
    const { result } = renderHook(() => useComparisonHarness(), {
      wrapper: Wrapper,
    });
    await waitFor(() =>
      expect(result.current.testing.testEndDate).toBe("2024-01-31"),
    );
    await waitForAgentCount(1);

    await act(async () => {
      await result.current.comparison.runComparisonTest(
        summaryWithContent("10 assets"),
      );
    });

    expect(generate).toHaveBeenCalledWith(
      expect.objectContaining({
        selectedAgent: "helper-agent",
        saveToCdf: false,
        summaryContents: expect.objectContaining({
          files: true,
          assets: false,
        }),
      }),
    );
    expect(result.current.testing.testResultData?.similarityScore).toBe(77);
    expect(
      result.current.testing.testResultData?.dataAccuracyScore,
    ).toBeLessThan(100);
    expect(result.current.testing.generationProgress.status).toBe("complete");
    expect(result.current.comparison.isRunning).toBe(false);
  });

  it("uses every enabled data type when none are selected", async () => {
    const generate = stubComparisonApis({
      agents: [{ externalId: "helper-agent", name: getAppConfig().agent.name }],
      newMarkdown: "10 assets",
      comparisonText: "SIMILARITY_SCORE: 100",
    });
    periodSeed = {};
    const { result } = renderHook(() => useComparisonHarness(), {
      wrapper: Wrapper,
    });
    await waitFor(() =>
      expect(result.current.testing.testStartDate).toBe("2024-01-01"),
    );
    await waitForAgentCount(1);

    await act(async () => {
      await result.current.comparison.runComparisonTest(
        summaryWithContent("10 assets"),
      );
    });

    const request = generate.mock.calls[0]?.[0];
    for (const dataType of getEnabledDataTypes()) {
      expect(request?.summaryContents[dataType]).toBe(true);
    }
  });

  it("warns and continues when the default agent cannot be created", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const generate = stubComparisonApis({ agents: [] });
    periodSeed = {};
    const { result } = renderHook(() => useComparisonHarness(), {
      wrapper: Wrapper,
    });
    await waitFor(() =>
      expect(result.current.testing.testStartDate).toBe("2024-01-01"),
    );

    await act(async () => {
      await result.current.comparison.runComparisonTest(
        summaryWithContent("plain summary"),
      );
    });

    expect(warn).toHaveBeenCalled();
    expect(generate).toHaveBeenCalledWith(
      expect.objectContaining({
        selectedAgent: "p_operation_summarizer_helper_agent",
      }),
    );
    expect(result.current.testing.testResultData?.newSummary).toBe("8 assets");
  });

  it("records an error when the original summary has no content", async () => {
    stubComparisonApis({
      agents: [{ externalId: "helper-agent", name: getAppConfig().agent.name }],
    });
    periodSeed = {};
    const { result } = renderHook(() => useComparisonHarness(), {
      wrapper: Wrapper,
    });
    await waitFor(() =>
      expect(result.current.testing.testStartDate).toBe("2024-01-01"),
    );

    await act(async () => {
      await result.current.comparison.runComparisonTest(emptySummary());
    });

    expect(result.current.testing.testError).toMatch(/unable to connect/i);
    expect(result.current.testing.generationProgress.status).toBe("error");
    expect(result.current.testing.testResultData).toBeNull();
  });

  it("records an error when summary generation fails", async () => {
    const generate = stubComparisonApis({
      agents: [{ externalId: "helper-agent", name: getAppConfig().agent.name }],
    });
    generate.mockRejectedValue(new Error("function unavailable"));
    periodSeed = {};
    const { result } = renderHook(() => useComparisonHarness(), {
      wrapper: Wrapper,
    });
    await waitFor(() =>
      expect(result.current.testing.testStartDate).toBe("2024-01-01"),
    );

    await act(async () => {
      await result.current.comparison.runComparisonTest(
        summaryWithContent("10 assets"),
      );
    });

    expect(result.current.testing.testError).toMatch(/unexpected error/i);
    expect(result.current.testing.generationProgress.status).toBe("error");
    expect(result.current.comparison.isRunning).toBe(false);
  });

  it("scores identical summaries at 100 when the model omits a similarity score", async () => {
    stubComparisonApis({
      agents: [{ externalId: "helper-agent", name: getAppConfig().agent.name }],
      newMarkdown: "10 assets",
      comparisonText: "",
    });
    periodSeed = {};
    const { result } = renderHook(() => useComparisonHarness(), {
      wrapper: Wrapper,
    });
    await waitFor(() =>
      expect(result.current.testing.testStartDate).toBe("2024-01-01"),
    );

    await act(async () => {
      await result.current.comparison.runComparisonTest(
        summaryWithContent("10 assets"),
      );
    });

    expect(result.current.testing.testResultData?.comparisonAnalysis).toBe(
      "Comparison analysis not available.",
    );
    expect(result.current.testing.testResultData?.similarityScore).toBe(100);
  });

  it("sets accuracy to zero when only the new summary has data counts", async () => {
    stubComparisonApis({
      agents: [{ externalId: "helper-agent", name: getAppConfig().agent.name }],
      newMarkdown: "5 assets",
      comparisonText: "no score listed",
    });
    periodSeed = {};
    const { result } = renderHook(() => useComparisonHarness(), {
      wrapper: Wrapper,
    });
    await waitFor(() =>
      expect(result.current.testing.testStartDate).toBe("2024-01-01"),
    );

    await act(async () => {
      await result.current.comparison.runComparisonTest(
        summaryWithContent("narrative only"),
      );
    });

    expect(result.current.testing.testResultData?.dataAccuracyScore).toBe(0);
    expect(result.current.testing.testResultData?.similarityScore).toBe(0);
  });
});

function waitForAgentCount(count: number) {
  return waitFor(() => {
    const agents = queryClient.getQueryData<unknown[]>(CDF_QUERY_KEYS.agents);
    expect(agents).toHaveLength(count);
  });
}

function useComparisonHarness() {
  return { comparison: useComparisonTest(), testing: useTestingState() };
}

function SeedPeriod() {
  const { dispatch } = useOperations();
  useEffect(() => {
    if (!periodSeed) return;
    dispatch({ type: "SET_TEST_START_DATE", payload: "2024-01-01" });
    dispatch({ type: "SET_TEST_END_DATE", payload: "2024-01-31" });
    if (periodSeed.dataTypes) {
      dispatch({
        type: "SET_TEST_SELECTED_DATA_TYPES",
        payload: periodSeed.dataTypes,
      });
    }
    if (periodSeed.running) {
      dispatch({ type: "SET_IS_TESTING", payload: true });
    }
  }, [dispatch]);
  return null;
}

function summaryWithContent(content: string): SummaryInstance {
  return {
    externalId: "ops_summary_2024-01-01_2024-01-31_x",
    space: "ops_summaries",
    createdTime: 1,
    properties: { [viewKey]: { content } },
  };
}

function emptySummary(): SummaryInstance {
  return {
    externalId: "ops_summary_empty",
    space: "ops_summaries",
    createdTime: 1,
    properties: {},
  };
}

function stubComparisonApis(
  options: {
    agents?: Array<{ externalId: string; name: string }>;
    newMarkdown?: string;
    comparisonText?: string;
  } = {},
) {
  vi.mocked(useCogniteSdk).mockReturnValue(
    cogniteClientMock({
      project: "p",
      get: vi.fn().mockRejectedValue(new Error("agents acl unavailable")),
      post: vi.fn().mockResolvedValue({ data: {} }),
    }) as ReturnType<typeof useCogniteSdk>,
  );
  vi.spyOn(CDFHelper.prototype, "getAgents").mockResolvedValue({
    agents: options.agents ?? [],
    invalidAgents: [],
  });
  vi.spyOn(CDFHelper.prototype, "compareSummaries").mockResolvedValue(
    comparisonResponse(options.comparisonText ?? "SIMILARITY_SCORE: 77"),
  );
  return vi
    .spyOn(CogniteSummaryFunctionService.prototype, "generate")
    .mockResolvedValue({
      markdown: options.newMarkdown ?? "8 assets",
      generatedAt: new Date("2026-01-01T00:00:00Z"),
      instanceCounts: {
        assets: 0,
        files: 0,
        work_orders: 0,
        time_series: 0,
        equipment: 0,
      },
      dataTypeTruncation: {},
    });
}

function comparisonResponse(content: string): ChatCompletionResponse {
  if (!content) return { choices: [] };
  return { choices: [{ message: { content } }] };
}
