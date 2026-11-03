import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { type ReactNode, useEffect } from "react";
import { describe, expect, it, vi } from "vitest";
import { getDefaultPromptTemplates } from "@/config/dataTypes";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import {
  OperationsProvider,
  useOperations,
} from "@/contexts/OperationsSummarizerContext";
import type { SummaryFunctionService } from "@/services/summaryFunctionService";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import {
  UseSummaryGenerationContext,
  useSummaryGeneration,
} from "./useSummaryGeneration";

const { shellSdk } = vi.hoisted(() => ({
  shellSdk: {
    project: "proj",
    get: vi.fn().mockResolvedValue({ data: { items: [], invalidAgents: [] } }),
    post: vi.fn().mockResolvedValue({ data: {} }),
    instances: { list: vi.fn().mockResolvedValue({ items: [] }) },
  },
}));

// OperationsProvider still reads the Fusion shell client through CdfClientProvider.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => shellSdk,
}));

const generate = vi.fn<SummaryFunctionService["generate"]>();
const customPrompts = { assets: "custom assets prompt" };

function AutoSelectAgent({ id }: { id: string }) {
  const { setSelectedAgent } = useOperations();
  useEffect(() => {
    setSelectedAgent(id);
  }, [id, setSelectedAgent]);
  return null;
}

function ConfigureRun({
  agentId,
  prompts,
  clearContents,
}: {
  agentId?: string;
  prompts?: Record<string, string>;
  clearContents?: boolean;
}) {
  const { dispatch, setSelectedAgent } = useOperations();
  useEffect(() => {
    if (agentId) setSelectedAgent(agentId);
    if (prompts) dispatch({ type: "SET_SUMMARY_PROMPTS", payload: prompts });
    if (clearContents) {
      dispatch({
        type: "SET_SUMMARY_CONTENTS",
        payload: {
          assets: false,
          files: false,
          work_orders: false,
          time_series: false,
          equipment: false,
        },
      });
    }
  }, [agentId, clearContents, dispatch, prompts, setSelectedAgent]);
  return null;
}

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient();
  return (
    <QueryClientProvider client={client}>
      <CdfClientProvider>
        <OperationsProvider>
          <UseSummaryGenerationContext.Provider
            value={{
              useCdfClient: () => ({
                sdk: cogniteClientMock(shellSdk),
                isAuthLoading: false,
              }),
              createService: () => ({ generate }),
            }}
          >
            <AutoSelectAgent id="agent-1" />
            {children}
          </UseSummaryGenerationContext.Provider>
        </OperationsProvider>
      </CdfClientProvider>
    </QueryClientProvider>
  );
}

describe(useSummaryGeneration.name, () => {
  it("returns the function result and marks the run complete", async () => {
    generate.mockResolvedValue({
      markdown: "# Generated",
      generatedAt: new Date("2026-10-28T00:00:00Z"),
      instanceCounts: {
        assets: 1,
        files: 0,
        work_orders: 4,
        time_series: 0,
        equipment: 0,
      },
      dataTypeTruncation: {},
      cdfResult: {
        external_id: "ops_summary_1",
        fusion_url: "https://fusion.example.test/file",
      },
    });

    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    await waitFor(() => expect(result.current).toBeDefined());

    await act(async () => {
      await result.current.generate();
    });

    expect(result.current.result?.markdown).toBe("# Generated");
    expect(result.current.result?.workOrderCount).toBe(4);
    expect(result.current.result?.cdfResult?.external_id).toBe("ops_summary_1");
    expect(result.current.progress.status).toBe("complete");
  });

  it("moves progress forward from function logs while the call is running", async () => {
    let release: (
      value: Awaited<ReturnType<SummaryFunctionService["generate"]>>,
    ) => void = () => {};
    generate.mockImplementation(async (_request, opts) => {
      opts?.onLog?.("Processing work_orders...");
      return new Promise((resolve) => {
        release = resolve;
      });
    });

    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    await waitFor(() => expect(result.current).toBeDefined());

    let pending: Promise<unknown> = Promise.resolve();
    act(() => {
      pending = result.current.generate();
    });

    await waitFor(() => {
      expect(result.current.progress.message).toBe("Summarizing work orders");
      expect(result.current.progress.progress).toBe(56);
    });

    await act(async () => {
      release({
        markdown: "# Generated",
        generatedAt: new Date("2026-10-28T00:00:00Z"),
        instanceCounts: {
          assets: 0,
          files: 0,
          work_orders: 1,
          time_series: 0,
          equipment: 0,
        },
        dataTypeTruncation: {},
      });
      await pending;
    });
    expect(result.current.progress.status).toBe("complete");
  });

  it("surfaces a failed save as an error", async () => {
    generate.mockRejectedValue(new Error("Failed to save summary: denied"));
    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    await act(async () => {
      await result.current.generate();
    });
    expect(result.current.progress.status).toBe("error");
    expect(result.current.error).toBeTruthy();
    expect(result.current.result).toBeNull();
  });

  it("reports when the agent is not selected", async () => {
    const client = new QueryClient();
    function bare({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={client}>
          <CdfClientProvider>
            <OperationsProvider>
              <UseSummaryGenerationContext.Provider
                value={{
                  useCdfClient: () => ({
                    sdk: cogniteClientMock(shellSdk),
                    isAuthLoading: false,
                  }),
                  createService: () => ({ generate }),
                }}
              >
                {children}
              </UseSummaryGenerationContext.Provider>
            </OperationsProvider>
          </CdfClientProvider>
        </QueryClientProvider>
      );
    }
    const { result } = renderHook(() => useSummaryGeneration(), {
      wrapper: bare,
    });
    await act(async () => {
      expect(await result.current.generate()).toBeNull();
    });
    expect(result.current.error).toMatch(/agent not available/i);
  });

  it("reports when no data types are selected", async () => {
    const client = new QueryClient();
    function bare({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={client}>
          <CdfClientProvider>
            <OperationsProvider>
              <UseSummaryGenerationContext.Provider
                value={{
                  useCdfClient: () => ({
                    sdk: cogniteClientMock(shellSdk),
                    isAuthLoading: false,
                  }),
                  createService: () => ({ generate }),
                }}
              >
                <ConfigureRun agentId="agent-1" clearContents />
                {children}
              </UseSummaryGenerationContext.Provider>
            </OperationsProvider>
          </CdfClientProvider>
        </QueryClientProvider>
      );
    }
    const { result } = renderHook(() => useSummaryGeneration(), {
      wrapper: bare,
    });
    await waitFor(() => expect(result.current).toBeDefined());
    await act(async () => {
      expect(await result.current.generate()).toBeNull();
    });
    expect(result.current.error).toMatch(/date range and data types/i);
  });

  it("sends edited prompt templates and says Generated when nothing was saved", async () => {
    generate.mockImplementation(async (request) => {
      expect(request.promptTemplates?.assets).toBe("custom assets prompt");
      return {
        markdown: "# Local",
        generatedAt: new Date(),
        instanceCounts: {
          assets: 0,
          files: 0,
          work_orders: 0,
          time_series: 0,
          equipment: 0,
        },
        dataTypeTruncation: {},
      };
    });
    const client = new QueryClient();
    function bare({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={client}>
          <CdfClientProvider>
            <OperationsProvider>
              <UseSummaryGenerationContext.Provider
                value={{
                  useCdfClient: () => ({
                    sdk: cogniteClientMock(shellSdk),
                    isAuthLoading: false,
                  }),
                  createService: () => ({ generate }),
                }}
              >
                <ConfigureRun agentId="agent-1" prompts={customPrompts} />
                {children}
              </UseSummaryGenerationContext.Provider>
            </OperationsProvider>
          </CdfClientProvider>
        </QueryClientProvider>
      );
    }
    const { result } = renderHook(() => useSummaryGeneration(), {
      wrapper: bare,
    });
    await act(async () => {
      await result.current.generate();
    });
    expect(result.current.progress.message).toBe("Generated");
  });

  it("explains a missing function and a timeout", async () => {
    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    generate.mockRejectedValueOnce(new Error("Function not found"));
    await act(async () => {
      await result.current.generate();
    });
    expect(result.current.error).toMatch(/was not found/i);

    generate.mockRejectedValueOnce(new Error("Call timed out"));
    await act(async () => {
      await result.current.generate();
    });
    expect(result.current.error).toMatch(/shorter date range/i);
  });

  it("resets when the call is aborted and stops waiting on cancel", async () => {
    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    generate.mockRejectedValueOnce(
      new DOMException("The operation was aborted", "AbortError"),
    );
    await act(async () => {
      await result.current.generate();
    });
    expect(result.current.progress.status).toBe("idle");

    let release: () => void = () => {};
    generate.mockImplementation(
      () =>
        new Promise(() => {
          release = () => {};
        }),
    );
    act(() => {
      void result.current.generate();
    });
    act(() => {
      result.current.cancel();
    });
    expect(result.current.progress.message).toMatch(/Stopped waiting/);
    release();
  });

  it("retries after a failure and reset clears the result", async () => {
    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    generate.mockRejectedValueOnce(new Error("temporary"));
    await act(async () => {
      await result.current.generate();
    });
    generate.mockResolvedValue({
      markdown: "# Again",
      generatedAt: new Date(),
      instanceCounts: {
        assets: 0,
        files: 0,
        work_orders: 0,
        time_series: 0,
        equipment: 0,
      },
      dataTypeTruncation: {},
      cdfResult: { external_id: "ops_summary_2" },
    });
    await act(async () => {
      await result.current.retryGeneration();
    });
    expect(result.current.result?.markdown).toBe("# Again");
    act(() => {
      result.current.reset();
    });
    expect(result.current.result).toBeNull();
  });

  it("reports when the SDK is not available", async () => {
    const client = new QueryClient();
    function bare({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={client}>
          <CdfClientProvider>
            <OperationsProvider>
              <UseSummaryGenerationContext.Provider
                value={{
                  useCdfClient: () => ({ sdk: null, isAuthLoading: true }),
                  createService: () => ({ generate }),
                }}
              >
                <AutoSelectAgent id="agent-1" />
                {children}
              </UseSummaryGenerationContext.Provider>
            </OperationsProvider>
          </CdfClientProvider>
        </QueryClientProvider>
      );
    }
    const { result } = renderHook(() => useSummaryGeneration(), {
      wrapper: bare,
    });
    await waitFor(() => expect(result.current).toBeDefined());
    await act(async () => {
      expect(await result.current.generate()).toBeNull();
    });
    expect(result.current.error).toMatch(/SDK or agent not available/i);
  });

  it("leaves prompt templates unset when they match the defaults", async () => {
    generate.mockImplementation(async (request) => {
      expect(request.promptTemplates).toBeUndefined();
      return quietResult("# Defaults");
    });
    const client = new QueryClient();
    function bare({ children }: { children: ReactNode }) {
      return (
        <QueryClientProvider client={client}>
          <CdfClientProvider>
            <OperationsProvider>
              <UseSummaryGenerationContext.Provider
                value={{
                  useCdfClient: () => ({
                    sdk: cogniteClientMock(shellSdk),
                    isAuthLoading: false,
                  }),
                  createService: () => ({ generate }),
                }}
              >
                <ConfigureRun
                  agentId="agent-1"
                  prompts={getDefaultPromptTemplates()}
                />
                {children}
              </UseSummaryGenerationContext.Provider>
            </OperationsProvider>
          </CdfClientProvider>
        </QueryClientProvider>
      );
    }
    const { result } = renderHook(() => useSummaryGeneration(), {
      wrapper: bare,
    });
    await act(async () => {
      await result.current.generate();
    });
    expect(result.current.result?.markdown).toBe("# Defaults");
  });

  it("ignores log lines that do not move progress forward", async () => {
    let release: (
      value: Awaited<ReturnType<SummaryFunctionService["generate"]>>,
    ) => void = () => {};
    let log: ((line: string) => void) | undefined;
    generate.mockImplementation(async (_request, opts) => {
      log = opts?.onLog;
      opts?.onLog?.("Processing work_orders...");
      return new Promise((resolve) => {
        release = resolve;
      });
    });
    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    let pending: Promise<unknown> = Promise.resolve();
    act(() => {
      pending = result.current.generate();
    });
    await waitFor(() => {
      expect(result.current.progress.progress).toBe(56);
    });
    act(() => {
      log?.("Processing assets...");
      log?.("not a progress line");
    });
    expect(result.current.progress.message).toBe("Summarizing work orders");
    expect(result.current.progress.progress).toBe(56);
    await act(async () => {
      release(quietResult("# Generated"));
      await pending;
    });
  });

  it("discards a superseded run and ignores its later logs", async () => {
    let firstLog: ((line: string) => void) | undefined;
    let releaseFirst: (
      value: Awaited<ReturnType<SummaryFunctionService["generate"]>>,
    ) => void = () => {};
    generate.mockImplementationOnce(async (_request, opts) => {
      firstLog = opts?.onLog;
      return new Promise((resolve) => {
        releaseFirst = resolve;
      });
    });
    generate.mockResolvedValueOnce(quietResult("# Second"));
    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    let firstRun: Promise<unknown> = Promise.resolve();
    act(() => {
      firstRun = result.current.generate();
    });
    await waitFor(() => expect(firstLog).toBeTypeOf("function"));

    await act(async () => {
      await result.current.generate();
    });
    expect(result.current.result?.markdown).toBe("# Second");

    firstLog?.("Processing equipment...");
    expect(result.current.progress.status).toBe("complete");
    expect(result.current.progress.progress).toBe(100);

    await act(async () => {
      releaseFirst(quietResult("# First"));
      expect(await firstRun).toBeNull();
    });
    expect(result.current.result?.markdown).toBe("# Second");
  });

  it("turns a non-error rejection into a failed run", async () => {
    generate.mockRejectedValue("upstream failed");
    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    await act(async () => {
      expect(await result.current.generate()).toBeNull();
    });
    expect(result.current.progress.status).toBe("error");
    expect(result.current.progress.message).toBe("Failed");
    expect(result.current.error).toMatch(/Failed to generate summary/i);
  });

  it("stops waiting even when no call is in flight", async () => {
    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    act(() => {
      result.current.cancel();
    });
    expect(result.current.progress.message).toMatch(/Stopped waiting/);
    expect(result.current.isGenerating).toBe(false);
  });

  it("clears a failed run when the user recovers", async () => {
    generate.mockRejectedValue(new Error("temporary"));
    const { result } = renderHook(() => useSummaryGeneration(), { wrapper });
    await act(async () => {
      await result.current.generate();
    });
    expect(result.current.canRetry).toBe(true);
    act(() => {
      result.current.recoverFromError();
    });
    expect(result.current.error).toBeNull();
    expect(result.current.progress.status).toBe("idle");
  });

  it("advances elapsed time while the function stays quiet", async () => {
    generate.mockImplementation(() => new Promise(() => {}));
    const { result, unmount } = renderHook(() => useSummaryGeneration(), {
      wrapper,
    });
    act(() => {
      void result.current.generate();
    });
    await waitFor(
      () => {
        expect(result.current.elapsedSeconds).toBeGreaterThanOrEqual(1);
        expect(result.current.progress.progress).toBeGreaterThan(10);
      },
      { timeout: 3000 },
    );
    unmount();
  });
});

function quietResult(markdown: string) {
  return {
    markdown,
    generatedAt: new Date("2026-10-28T00:00:00Z"),
    instanceCounts: {
      assets: 0,
      files: 0,
      work_orders: 0,
      time_series: 0,
      equipment: 0,
    },
    dataTypeTruncation: {},
  };
}
