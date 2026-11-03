/**
 * Calls the deployed Podcast Generation Cognite Function and waits for its result.
 * The JS SDK exposes function CRUD but not call/poll/response — those use REST via client.get/post.
 */

import type { CogniteClient } from "@cognite/sdk";
import { getAppConfig } from "@/config/appConfig";
import {
  getSummarizerFunctionExternalId,
  getSummarySpace,
} from "@/config/dataTypes";
import {
  CogniteFunctionCallClient,
  type FunctionCallStatus,
} from "@/services/functionCallClient";

export type { FunctionCallStatus };

export interface PodcastCdfResult {
  script_external_id?: string;
  audio_external_id?: string;
  audio_file_id?: number;
  fusion_url?: string;
}

export interface PodcastFunctionResult {
  success: boolean;
  script?: string;
  source_summary_id?: string;
  audio_generated?: boolean;
  audio_size_bytes?: number;
  audio_error?: string;
  cdf_result?: PodcastCdfResult;
  cdf_save_error?: string;
  error?: string;
}

export interface PodcastGenerateRequest {
  summaryExternalId: string;
  summarySpace?: string;
  sourceSummaryId?: string;
  startDate?: string | null;
  endDate?: string | null;
  generateAudio?: boolean;
  saveToCdf?: boolean;
}

export interface PodcastFunctionService {
  generate(request: PodcastGenerateRequest): Promise<PodcastFunctionResult>;
}

export interface PodcastFunctionServiceOptions {
  functionExternalId?: string;
  pollIntervalMs?: number;
  timeoutMs?: number;
  sleep?: (ms: number) => Promise<void>;
}

function podcastSettings() {
  return getAppConfig().functions.summarizer;
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export function buildPodcastFunctionData(
  request: PodcastGenerateRequest,
): Record<string, unknown> {
  const data: Record<string, unknown> = {
    task: "podcast",
    summary_external_id: request.summaryExternalId,
    summary_space: request.summarySpace ?? getSummarySpace(),
    source_summary_id: request.sourceSummaryId ?? request.summaryExternalId,
    generate_audio: request.generateAudio ?? true,
    save_to_cdf: request.saveToCdf ?? true,
  };
  if (request.startDate) data.start_date = request.startDate;
  if (request.endDate) data.end_date = request.endDate;
  return data;
}

export function parsePodcastFunctionResult(
  raw: unknown,
): PodcastFunctionResult {
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("Podcast function returned an invalid response shape");
  }
  const record = raw as Record<string, unknown>;
  if (typeof record.success !== "boolean") {
    throw new Error("Podcast function response is missing a success flag");
  }

  const result: PodcastFunctionResult = { success: record.success };

  if (typeof record.script === "string") result.script = record.script;
  if (typeof record.source_summary_id === "string") {
    result.source_summary_id = record.source_summary_id;
  }
  if (typeof record.audio_generated === "boolean") {
    result.audio_generated = record.audio_generated;
  }
  if (typeof record.audio_size_bytes === "number") {
    result.audio_size_bytes = record.audio_size_bytes;
  }
  if (typeof record.audio_error === "string") {
    result.audio_error = record.audio_error;
  }
  if (typeof record.cdf_save_error === "string") {
    result.cdf_save_error = record.cdf_save_error;
  }
  if (typeof record.error === "string") result.error = record.error;

  if (record.cdf_result !== null && typeof record.cdf_result === "object") {
    const cdf = record.cdf_result as Record<string, unknown>;
    const cdfResult: PodcastCdfResult = {};
    if (typeof cdf.script_external_id === "string") {
      cdfResult.script_external_id = cdf.script_external_id;
    }
    if (typeof cdf.audio_external_id === "string") {
      cdfResult.audio_external_id = cdf.audio_external_id;
    }
    if (typeof cdf.audio_file_id === "number") {
      cdfResult.audio_file_id = cdf.audio_file_id;
    }
    if (typeof cdf.fusion_url === "string") {
      cdfResult.fusion_url = cdf.fusion_url;
    }
    result.cdf_result = cdfResult;
  }

  if (!result.success) {
    throw new Error(result.error || "Podcast generation failed");
  }

  return result;
}

export class CognitePodcastFunctionService implements PodcastFunctionService {
  private readonly calls: CogniteFunctionCallClient;

  constructor(
    client: CogniteClient,
    options: PodcastFunctionServiceOptions = {},
  ) {
    const settings = podcastSettings();
    this.calls = new CogniteFunctionCallClient(client, {
      functionExternalId:
        options.functionExternalId ?? getSummarizerFunctionExternalId(),
      pollIntervalMs: options.pollIntervalMs ?? settings.pollIntervalMs,
      timeoutMs: options.timeoutMs ?? settings.timeoutMs,
      maxConsecutivePollErrors: settings.maxConsecutivePollErrors,
      label: "Podcast function",
      sleep: options.sleep ?? defaultSleep,
    });
  }

  async generate(
    request: PodcastGenerateRequest,
  ): Promise<PodcastFunctionResult> {
    const response = await this.calls.call(buildPodcastFunctionData(request));
    return parsePodcastFunctionResult(response);
  }
}
