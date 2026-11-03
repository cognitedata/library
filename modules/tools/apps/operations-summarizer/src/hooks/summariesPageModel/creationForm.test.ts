import { renderHook } from "@testing-library/react";
import type { ChangeEvent } from "react";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { DEFAULT_NODE_PROPERTIES } from "@/__mocks__/defaultNodeProperties";
import { parseAppConfig, setAppConfig } from "@/config/appConfig";
import type { OperationsState } from "@/contexts/operations-summarizer/types";
import type { GeneratedSummary, SummaryContents } from "@/types/operations";
import {
  createSummariesCreationHandlers,
  derivePreservedConfig,
  formatDateForInput,
  useSummariesCreationComputed,
} from "./creationForm";

beforeAll(() => {
  setAppConfig(parseAppConfig(DEFAULT_NODE_PROPERTIES));
});

describe("formatDateForInput", () => {
  it("returns a calendar YYYY-MM-DD", () => {
    expect(formatDateForInput(new Date(2024, 5, 2))).toBe("2024-06-02");
  });
});

describe("derivePreservedConfig", () => {
  it("returns undefined when there is no generation error", () => {
    expect(derivePreservedConfig(null, state())).toBeUndefined();
  });

  it("keeps the date range, selected types, and agent", () => {
    const preserved = derivePreservedConfig("failed", state());
    expect(preserved).toEqual({
      dateRange: "2024-01-01 to 2024-01-03",
      dataTypes: ["Assets"],
      agent: "agent-1",
    });
  });

  it("labels a missing agent", () => {
    const preserved = derivePreservedConfig(
      "failed",
      state({
        selectedAgent: null,
        summaryContents: { assets: false, files: false } as SummaryContents,
      }),
    );
    expect(preserved?.agent).toBe("No agent selected");
    expect(preserved?.dataTypes).toEqual([]);
  });
});

describe("useSummariesCreationComputed", () => {
  it("counts selected types and ceils the day span", () => {
    const start = new Date(2024, 0, 1, 0, 0, 0);
    const end = new Date(start.getTime() + 36 * 60 * 60 * 1000);
    const { result } = renderHook(() =>
      useSummariesCreationComputed(
        state({
          summaryPeriodStart: start,
          summaryPeriodEnd: end,
          summaryContents: {
            assets: true,
            files: false,
            work_orders: true,
          } as SummaryContents,
        }),
      ),
    );
    expect(result.current.selectedCount).toBe(2);
    expect(result.current.daysSelected).toBe(2);
  });
});

describe("createSummariesCreationHandlers", () => {
  it("ignores an empty or invalid start date", () => {
    const { dispatch, handlers } = handlersFor();
    handlers.handleStartDateChange(input(""));
    handlers.handleStartDateChange(input("not-a-date"));
    expect(dispatch).not.toHaveBeenCalled();
  });

  it("moves the end date when the new start is later", () => {
    const { dispatch, handlers } = handlersFor();
    handlers.handleStartDateChange(input("2024-01-10"));
    expect(dispatch).toHaveBeenNthCalledWith(1, {
      type: "SET_SUMMARY_PERIOD_START",
      payload: new Date("2024-01-10T00:00:00"),
    });
    expect(dispatch).toHaveBeenNthCalledWith(2, {
      type: "SET_SUMMARY_PERIOD_END",
      payload: new Date("2024-01-10T00:00:00"),
    });
  });

  it("keeps the end date when the new start is earlier", () => {
    const { dispatch, handlers } = handlersFor();
    handlers.handleStartDateChange(input("2023-12-01"));
    expect(dispatch).toHaveBeenCalledTimes(1);
    expect(dispatch).toHaveBeenCalledWith({
      type: "SET_SUMMARY_PERIOD_START",
      payload: new Date("2023-12-01T00:00:00"),
    });
  });

  it("ignores an empty, invalid, or inverted end date", () => {
    const { dispatch, handlers } = handlersFor();
    handlers.handleEndDateChange(input(""));
    handlers.handleEndDateChange(input("not-a-date"));
    handlers.handleEndDateChange(input("2023-12-01"));
    expect(dispatch).not.toHaveBeenCalled();
  });

  it("sets a later end date", () => {
    const { dispatch, handlers } = handlersFor();
    handlers.handleEndDateChange(input("2024-02-01"));
    expect(dispatch).toHaveBeenCalledWith({
      type: "SET_SUMMARY_PERIOD_END",
      payload: new Date("2024-02-01T00:00:00"),
    });
  });

  it("does nothing when no agent is selected", async () => {
    const setSavedPdfInfo = vi.fn();
    const generate = vi.fn();
    const { handlers } = handlersFor({
      state: state({ selectedAgent: null }),
      setSavedPdfInfo,
      generate,
    });
    await handlers.handleGenerateSummary();
    expect(setSavedPdfInfo).not.toHaveBeenCalled();
    expect(generate).not.toHaveBeenCalled();
  });

  it("clears saved pdf info when generation returns nothing", async () => {
    const setSavedPdfInfo = vi.fn();
    const dispatch = vi.fn();
    const { handlers } = handlersFor({
      dispatch,
      setSavedPdfInfo,
      generate: vi.fn().mockResolvedValue(null),
    });
    await handlers.handleGenerateSummary();
    expect(setSavedPdfInfo).toHaveBeenCalledWith(null);
    expect(dispatch).not.toHaveBeenCalled();
  });

  it("stores the summary without a file when CDF omits an external id", async () => {
    const setSavedPdfInfo = vi.fn();
    const dispatch = vi.fn();
    const generatedAt = new Date("2024-01-02T00:00:00Z");
    const { handlers } = handlersFor({
      dispatch,
      setSavedPdfInfo,
      generate: vi.fn().mockResolvedValue({
        markdown: "# Hi",
        generatedAt,
        dataTypeTruncation: {},
        cdfResult: {},
      }),
    });
    await handlers.handleGenerateSummary();
    expect(dispatch).toHaveBeenCalledWith({
      type: "SET_SHOW_SUMMARY",
      payload: true,
    });
    expect(dispatch).toHaveBeenCalledWith({
      type: "SET_GENERATED_SUMMARY",
      payload: {
        workOrders: [],
        instances: [],
        fullText: "# Hi",
        generatedAt,
        dataTypeTruncation: {},
      },
    });
    expect(setSavedPdfInfo).toHaveBeenCalledTimes(1);
  });

  it("resolves a Fusion URL and keeps the CDF file name", async () => {
    const setSavedPdfInfo = vi.fn();
    const resolveFusionFileUrl = vi
      .fn()
      .mockResolvedValue("https://fusion.test/file");
    const { handlers } = handlersFor({
      setSavedPdfInfo,
      resolveFusionFileUrl,
      generate: vi.fn().mockResolvedValue({
        markdown: "body",
        generatedAt: new Date(),
        dataTypeTruncation: {},
        cdfResult: { external_id: "ext-1", file_name: "report.pdf" },
      }),
    });
    await handlers.handleGenerateSummary();
    expect(setSavedPdfInfo).toHaveBeenLastCalledWith({
      fileName: "report.pdf",
      fileId: "ext-1",
      fusionUrl: "https://fusion.test/file",
    });
  });

  it("still saves the file when Fusion URL lookup fails", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const setSavedPdfInfo = vi.fn();
    const { handlers } = handlersFor({
      setSavedPdfInfo,
      resolveFusionFileUrl: vi.fn().mockRejectedValue(new Error("url")),
      generate: vi.fn().mockResolvedValue({
        markdown: "body",
        generatedAt: new Date(),
        dataTypeTruncation: {},
        cdfResult: { external_id: "ext-2" },
      }),
    });
    await handlers.handleGenerateSummary();
    expect(setSavedPdfInfo).toHaveBeenLastCalledWith({
      fileName: "Operations summary",
      fileId: "ext-2",
      fusionUrl: undefined,
    });
    errorSpy.mockRestore();
  });

  it("logs a generation failure", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const { handlers } = handlersFor({
      generate: vi.fn().mockRejectedValue(new Error("boom")),
    });
    await handlers.handleGenerateSummary();
    expect(errorSpy).toHaveBeenCalled();
    errorSpy.mockRestore();
  });

  it("skips download when there is no summary", () => {
    const downloadAsMarkdown = vi.fn();
    const { handlers } = handlersFor({ downloadAsMarkdown });
    handlers.handleDownloadMarkdown();
    expect(downloadAsMarkdown).not.toHaveBeenCalled();
  });

  it("downloads stripped markdown", () => {
    const downloadAsMarkdown = vi.fn();
    const summary: GeneratedSummary = {
      workOrders: [],
      instances: [],
      fullText: "<p>Hello</p>",
      generatedAt: new Date(),
    };
    const { handlers } = handlersFor({
      downloadAsMarkdown,
      state: state({ generatedSummary: summary }),
    });
    handlers.handleDownloadMarkdown();
    expect(downloadAsMarkdown).toHaveBeenCalledWith(
      "Hello",
      expect.stringMatching(
        /^operation_summary_2024-01-01_to_2024-01-03_.*\.md$/,
      ),
    );
  });
});

function input(value: string): ChangeEvent<HTMLInputElement> {
  return { target: { value } } as ChangeEvent<HTMLInputElement>;
}

function state(partial: Partial<OperationsState> = {}): OperationsState {
  return {
    selectedAgent: "agent-1",
    summaryPeriodStart: new Date(2024, 0, 1),
    summaryPeriodEnd: new Date(2024, 0, 3),
    summaryContents: { assets: true, files: false } as SummaryContents,
    generatedSummary: null,
    ...partial,
  } as OperationsState;
}

function handlersFor(
  overrides: Partial<
    Parameters<typeof createSummariesCreationHandlers>[0]
  > = {},
) {
  const dispatch = overrides.dispatch ?? vi.fn();
  const deps = {
    state: overrides.state ?? state(),
    dispatch,
    generate: overrides.generate ?? vi.fn(),
    downloadAsMarkdown: overrides.downloadAsMarkdown ?? vi.fn(),
    setSavedPdfInfo: overrides.setSavedPdfInfo ?? vi.fn(),
    resolveFusionFileUrl: overrides.resolveFusionFileUrl,
  };
  return {
    dispatch,
    handlers: createSummariesCreationHandlers(deps),
  };
}
