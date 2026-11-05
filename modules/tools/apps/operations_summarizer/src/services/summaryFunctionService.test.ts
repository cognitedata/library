import { describe, expect, it, vi } from "vitest";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import {
  buildSummaryFunctionData,
  CogniteSummaryFunctionService,
  formatLocalDate,
  parseSummaryFunctionResult,
} from "./summaryFunctionService";

// withCdfConcurrency waits on a shared queue. Tests run the task immediately.
vi.mock("@/utils/cdfRequest", () => ({
  withCdfConcurrency: <T>(fn: () => Promise<T>) => fn(),
}));

const contents = {
  assets: true,
  files: false,
  work_orders: true,
  time_series: false,
  equipment: false,
};

describe(formatLocalDate.name, () => {
  it("formats a local calendar date without shifting the day", () => {
    expect(formatLocalDate(new Date(2026, 0, 7))).toBe("2026-01-07");
  });
});

describe(buildSummaryFunctionData.name, () => {
  it("builds the summary task payload", () => {
    expect(
      buildSummaryFunctionData({
        periodStart: new Date(2026, 0, 1),
        periodEnd: new Date(2026, 0, 7),
        summaryContents: contents,
        saveToCdf: true,
        generatePdf: false,
      }),
    ).toEqual({
      task: "summary",
      summary_period_start: "2026-01-01",
      summary_period_end: "2026-01-07",
      summary_contents: {
        assets: true,
        files: false,
        work_orders: true,
        time_series: false,
        equipment: false,
      },
      save_to_cdf: true,
      generate_pdf: false,
    });
  });

  it("includes agent, prompt templates, and fusion domain when provided", () => {
    expect(
      buildSummaryFunctionData({
        periodStart: new Date(2026, 0, 1),
        periodEnd: new Date(2026, 0, 7),
        summaryContents: contents,
        selectedAgent: "agent-1",
        promptTemplates: { assets: "Focus on pumps" },
        saveToCdf: false,
        generatePdf: true,
        fusionDomain: "fusion.example.test",
      }),
    ).toMatchObject({
      selected_agent: "agent-1",
      prompt_templates: { assets: "Focus on pumps" },
      fusion_domain: "fusion.example.test",
    });
  });

  it("omits empty prompt templates", () => {
    const data = buildSummaryFunctionData({
      periodStart: new Date(2026, 0, 1),
      periodEnd: new Date(2026, 0, 2),
      summaryContents: contents,
      promptTemplates: {},
      saveToCdf: false,
      generatePdf: false,
    });
    expect(data).not.toHaveProperty("prompt_templates");
    expect(data).not.toHaveProperty("selected_agent");
  });
});

describe(parseSummaryFunctionResult.name, () => {
  it("parses markdown, counts, truncation, and cdf result", () => {
    const result = parseSummaryFunctionResult({
      success: true,
      markdown: "# Summary",
      instance_counts: { assets: 3, files: "nope", work_orders: 2 },
      data_type_truncation: {
        assets: { limit: 100, had_more_in_cdf: true },
        files: { limit: 10, had_more_in_cdf: false },
        work_orders: "bad",
      },
      cdf_result: {
        external_id: "ops_summary_1",
        file_name: "summary.pdf",
        file_id: 42,
        fusion_url: "https://fusion.example.test/file",
      },
    });

    expect(result.markdown).toBe("# Summary");
    expect(result.instanceCounts.assets).toBe(3);
    expect(result.instanceCounts.work_orders).toBe(2);
    expect(result.instanceCounts.files).toBe(0);
    expect(result.dataTypeTruncation.assets).toEqual({
      limit: 100,
      hadMoreInCdf: true,
    });
    expect(result.dataTypeTruncation.files).toBeUndefined();
    expect(result.cdfResult).toEqual({
      external_id: "ops_summary_1",
      file_name: "summary.pdf",
      file_id: 42,
      fusion_url: "https://fusion.example.test/file",
    });
  });

  it("accepts a success response without optional fields", () => {
    const result = parseSummaryFunctionResult({
      success: true,
      markdown: "plain",
      instance_counts: null,
      data_type_truncation: null,
      cdf_result: null,
    });
    expect(result.instanceCounts.assets).toBe(0);
    expect(result.cdfResult).toBeUndefined();
  });

  it("throws when success is false", () => {
    expect(() =>
      parseSummaryFunctionResult({ success: false, error: "No data in range" }),
    ).toThrow("No data in range");
    expect(() => parseSummaryFunctionResult({ success: false })).toThrow(
      "Summary generation failed",
    );
  });

  it("throws on a non-OK response shape", () => {
    expect(() => parseSummaryFunctionResult(null)).toThrow(
      "invalid response shape",
    );
    expect(() => parseSummaryFunctionResult([])).toThrow(
      "invalid response shape",
    );
    expect(() => parseSummaryFunctionResult({ markdown: "x" })).toThrow(
      "missing a success flag",
    );
    expect(() =>
      parseSummaryFunctionResult({ success: true, markdown: 1 }),
    ).toThrow("missing markdown");
  });
});

describe(CogniteSummaryFunctionService.name, () => {
  it("posts the summary request and returns the parsed response", async () => {
    const retrieve = vi.fn().mockResolvedValue([{ id: 42 }]);
    const createSession = vi
      .fn()
      .mockResolvedValue([{ nonce: "nonce-1", id: 1, status: "READY" }]);
    const post = vi.fn().mockResolvedValue({
      data: { id: 99, status: "Running", functionId: 42 },
    });
    const get = vi
      .fn()
      .mockResolvedValueOnce({
        data: { id: 99, status: "Completed", functionId: 42 },
      })
      .mockResolvedValueOnce({
        data: {
          functionId: 42,
          callId: 99,
          response: { success: true, markdown: "# From function" },
        },
      });

    const client = cogniteClientMock({
      project: "demo",
      functions: { retrieve },
      sessions: { create: createSession },
      post,
      get,
    });

    const service = new CogniteSummaryFunctionService(client);
    const onStatus = vi.fn();
    const result = await service.generate(
      {
        periodStart: new Date(2026, 0, 1),
        periodEnd: new Date(2026, 0, 7),
        summaryContents: contents,
        saveToCdf: true,
        generatePdf: true,
      },
      { onStatus },
    );

    expect(result.markdown).toBe("# From function");
    expect(post).toHaveBeenCalled();
    const posted = post.mock.calls[0]?.[1] as {
      data: { data: Record<string, unknown> };
    };
    expect(posted.data.data.task).toBe("summary");
    expect(posted.data.data.summary_period_start).toBe("2026-01-01");
    expect(onStatus).toHaveBeenCalledWith("Running");
    expect(onStatus).toHaveBeenCalledWith("Completed");
  });

  it("throws when the function response is not successful", async () => {
    const client = cogniteClientMock({
      project: "demo",
      functions: { retrieve: vi.fn().mockResolvedValue([{ id: 7 }]) },
      sessions: {
        create: vi
          .fn()
          .mockResolvedValue([{ nonce: "n", id: 1, status: "READY" }]),
      },
      post: vi.fn().mockResolvedValue({
        data: { id: 1, status: "Running", functionId: 7 },
      }),
      get: vi
        .fn()
        .mockResolvedValueOnce({
          data: { id: 1, status: "Completed", functionId: 7 },
        })
        .mockResolvedValueOnce({
          data: {
            functionId: 7,
            callId: 1,
            response: { success: false, error: "upstream failed" },
          },
        }),
    });

    const service = new CogniteSummaryFunctionService(client);
    await expect(
      service.generate({
        periodStart: new Date(2026, 0, 1),
        periodEnd: new Date(2026, 0, 2),
        summaryContents: contents,
        saveToCdf: false,
        generatePdf: false,
      }),
    ).rejects.toThrow("upstream failed");
  });
});
