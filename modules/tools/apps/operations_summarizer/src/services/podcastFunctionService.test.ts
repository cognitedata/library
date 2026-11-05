import { describe, expect, it, vi } from "vitest";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import {
  buildPodcastFunctionData,
  CognitePodcastFunctionService,
  parsePodcastFunctionResult,
} from "./podcastFunctionService";

// withCdfConcurrency waits on a shared queue. Tests run the task immediately.
vi.mock("@/utils/cdfRequest", () => ({
  withCdfConcurrency: <T>(fn: () => Promise<T>) => fn(),
}));

describe(buildPodcastFunctionData.name, () => {
  it("builds the expected function data payload", () => {
    expect(
      buildPodcastFunctionData({
        summaryExternalId: "ops_summary_2026-01-01_2026-01-07_abc",
        startDate: "2026-01-01",
        endDate: "2026-01-07",
      }),
    ).toEqual({
      task: "podcast",
      summary_external_id: "ops_summary_2026-01-01_2026-01-07_abc",
      summary_space: "ops_summaries",
      source_summary_id: "ops_summary_2026-01-01_2026-01-07_abc",
      generate_audio: true,
      save_to_cdf: true,
      start_date: "2026-01-01",
      end_date: "2026-01-07",
    });
  });

  it("omits empty dates and respects overrides", () => {
    expect(
      buildPodcastFunctionData({
        summaryExternalId: "sum-1",
        summarySpace: "custom_space",
        sourceSummaryId: "src-1",
        generateAudio: false,
        saveToCdf: false,
      }),
    ).toEqual({
      task: "podcast",
      summary_external_id: "sum-1",
      summary_space: "custom_space",
      source_summary_id: "src-1",
      generate_audio: false,
      save_to_cdf: false,
    });
  });
});

describe(parsePodcastFunctionResult.name, () => {
  it("parses a successful response with audio and cdf_result", () => {
    const result = parsePodcastFunctionResult({
      success: true,
      script: "Hello listeners",
      source_summary_id: "sum-1",
      audio_generated: true,
      audio_size_bytes: 1234,
      cdf_result: {
        script_external_id: "sum-1_podcast_script",
        audio_external_id: "sum-1_podcast_audio",
      },
    });

    expect(result.success).toBe(true);
    expect(result.script).toBe("Hello listeners");
    expect(result.cdf_result?.audio_external_id).toBe("sum-1_podcast_audio");
  });

  it("parses audio-skipped success", () => {
    const result = parsePodcastFunctionResult({
      success: true,
      script: "Script only",
      audio_generated: false,
      audio_error: "TTS engine returned no audio",
    });

    expect(result.audio_generated).toBe(false);
    expect(result.audio_error).toBe("TTS engine returned no audio");
  });

  it("throws when success is false", () => {
    expect(() =>
      parsePodcastFunctionResult({
        success: false,
        error: "No summary text available",
      }),
    ).toThrow("No summary text available");
  });

  it("throws on invalid shape", () => {
    expect(() => parsePodcastFunctionResult(null)).toThrow(
      "invalid response shape",
    );
    expect(() => parsePodcastFunctionResult({ script: "x" })).toThrow(
      "missing a success flag",
    );
  });
});

describe(CognitePodcastFunctionService.name, () => {
  it("calls the function, polls until completed, and returns parsed result", async () => {
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
        data: { id: 99, status: "Running", functionId: 42 },
      })
      .mockResolvedValueOnce({
        data: { id: 99, status: "Completed", functionId: 42 },
      })
      .mockResolvedValueOnce({
        data: {
          functionId: 42,
          callId: 99,
          response: {
            success: true,
            script: "Generated script",
            audio_generated: true,
            cdf_result: { script_external_id: "sum_podcast_script" },
          },
        },
      });

    const client = cogniteClientMock({
      project: "demo",
      functions: { retrieve },
      sessions: { create: createSession },
      post,
      get,
    });

    const sleep = vi.fn().mockResolvedValue(undefined);
    const service = new CognitePodcastFunctionService(client, {
      pollIntervalMs: 1,
      sleep,
    });

    const result = await service.generate({
      summaryExternalId: "ops_summary_a",
      startDate: "2026-01-01",
      endDate: "2026-01-02",
    });

    expect(retrieve).toHaveBeenCalledWith([
      { externalId: "Operations_Summarizer_Function_v2" },
    ]);
    expect(createSession).toHaveBeenCalledWith([{ tokenExchange: true }]);
    expect(post).toHaveBeenCalledWith(
      "/api/v1/projects/demo/functions/42/call",
      {
        data: {
          nonce: "nonce-1",
          data: {
            task: "podcast",
            summary_external_id: "ops_summary_a",
            summary_space: "ops_summaries",
            source_summary_id: "ops_summary_a",
            generate_audio: true,
            save_to_cdf: true,
            start_date: "2026-01-01",
            end_date: "2026-01-02",
          },
        },
      },
    );
    expect(result.script).toBe("Generated script");
    expect(sleep).toHaveBeenCalled();
  });

  it("falls back to oneshot token exchange when token exchange fails", async () => {
    const createSession = vi
      .fn()
      .mockRejectedValueOnce(new Error("token exchange unsupported"))
      .mockResolvedValueOnce([
        { nonce: "oneshot-nonce", id: 2, status: "READY" },
      ]);
    const client = cogniteClientMock({
      project: "demo",
      functions: { retrieve: vi.fn().mockResolvedValue([{ id: 7 }]) },
      sessions: { create: createSession },
      post: vi.fn().mockResolvedValue({
        data: { id: 1, status: "Completed", functionId: 7 },
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
            response: { success: true, script: "ok" },
          },
        }),
    });

    const service = new CognitePodcastFunctionService(client, {
      pollIntervalMs: 1,
      sleep: async () => undefined,
    });

    await service.generate({ summaryExternalId: "sum" });

    expect(createSession).toHaveBeenNthCalledWith(1, [{ tokenExchange: true }]);
    expect(createSession).toHaveBeenNthCalledWith(2, [
      { oneshotTokenExchange: true },
    ]);
  });

  it("throws when the function call fails", async () => {
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
      get: vi.fn().mockResolvedValue({
        data: {
          id: 1,
          status: "Failed",
          functionId: 7,
          error: "boom",
        },
      }),
    });

    const service = new CognitePodcastFunctionService(client, {
      pollIntervalMs: 1,
      sleep: async () => undefined,
    });

    await expect(
      service.generate({ summaryExternalId: "sum" }),
    ).rejects.toThrow("boom");
  });

  it("retries polling when retrieveCall fails with transient errors", async () => {
    const get = vi
      .fn()
      .mockRejectedValueOnce(new Error("503 Service Unavailable"))
      .mockRejectedValueOnce(new Error("502 Bad Gateway"))
      .mockResolvedValueOnce({
        data: { id: 1, status: "Running", functionId: 7 },
      })
      .mockResolvedValueOnce({
        data: { id: 1, status: "Completed", functionId: 7 },
      })
      .mockResolvedValueOnce({
        data: {
          functionId: 7,
          callId: 1,
          response: { success: true, script: "recovered" },
        },
      });

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
      get,
    });

    const service = new CognitePodcastFunctionService(client, {
      pollIntervalMs: 1,
      sleep: async () => undefined,
    });

    const result = await service.generate({ summaryExternalId: "sum" });

    expect(result.script).toBe("recovered");
    expect(get).toHaveBeenCalledTimes(5);
  });

  it("throws after too many consecutive retrieveCall failures", async () => {
    const get = vi.fn().mockRejectedValue(new Error("503 Service Unavailable"));

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
      get,
    });

    const service = new CognitePodcastFunctionService(client, {
      pollIntervalMs: 1,
      sleep: async () => undefined,
    });

    await expect(
      service.generate({ summaryExternalId: "sum" }),
    ).rejects.toThrow(
      "Failed to retrieve podcast function status after 5 consecutive attempts",
    );
    expect(get).toHaveBeenCalledTimes(5);
  });
});
