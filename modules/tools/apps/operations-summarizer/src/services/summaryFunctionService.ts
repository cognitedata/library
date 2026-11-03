/**
 * Calls Operations_Summarizer_Function_v2 with task "summary".
 */

import type { CogniteClient } from "@cognite/sdk";
import { getAppConfig } from "@/config/appConfig";
import { getSummarizerFunctionExternalId } from "@/config/dataTypes";
import {
  CogniteFunctionCallClient,
  type FunctionCallClient,
} from "@/services/functionCallClient";
import type { DataTypeName, SummaryContents } from "@/types/operations";

const DATA_TYPE_NAMES: DataTypeName[] = [
  "assets",
  "files",
  "work_orders",
  "time_series",
  "equipment",
];

export interface SummaryCdfResult {
  external_id?: string;
  file_id?: number;
  file_name?: string;
  fusion_url?: string;
}

export interface SummaryFunctionResult {
  markdown: string;
  generatedAt: Date;
  instanceCounts: Record<DataTypeName, number>;
  dataTypeTruncation: Partial<
    Record<DataTypeName, { limit: number; hadMoreInCdf: boolean }>
  >;
  cdfResult?: SummaryCdfResult;
}

export interface SummaryGenerateRequest {
  periodStart: Date;
  periodEnd: Date;
  summaryContents: SummaryContents;
  selectedAgent?: string;
  promptTemplates?: Record<string, string>;
  saveToCdf: boolean;
  generatePdf: boolean;
  fusionDomain?: string;
}

export interface SummaryFunctionCallOptions {
  signal?: AbortSignal;
  onStatus?: (status: "Running" | "Completed" | "Failed" | "Timeout") => void;
  onLog?: (message: string) => void;
}

export interface SummaryFunctionService {
  generate(
    request: SummaryGenerateRequest,
    opts?: SummaryFunctionCallOptions,
  ): Promise<SummaryFunctionResult>;
}

export function formatLocalDate(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

export function buildSummaryFunctionData(
  request: SummaryGenerateRequest,
): Record<string, unknown> {
  const summaryContents: Record<string, boolean> = {};
  for (const name of DATA_TYPE_NAMES) {
    summaryContents[name] = Boolean(request.summaryContents[name]);
  }
  const data: Record<string, unknown> = {
    task: "summary",
    summary_period_start: formatLocalDate(request.periodStart),
    summary_period_end: formatLocalDate(request.periodEnd),
    summary_contents: summaryContents,
    save_to_cdf: request.saveToCdf,
    generate_pdf: request.generatePdf,
  };
  if (request.selectedAgent) data.selected_agent = request.selectedAgent;
  if (
    request.promptTemplates &&
    Object.keys(request.promptTemplates).length > 0
  ) {
    data.prompt_templates = request.promptTemplates;
  }
  if (request.fusionDomain) data.fusion_domain = request.fusionDomain;
  return data;
}

function emptyCounts(): Record<DataTypeName, number> {
  return {
    assets: 0,
    files: 0,
    work_orders: 0,
    time_series: 0,
    equipment: 0,
  };
}

export function parseSummaryFunctionResult(
  raw: unknown,
): SummaryFunctionResult {
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("Summary function returned an invalid response shape");
  }
  const record = raw as Record<string, unknown>;
  if (typeof record.success !== "boolean") {
    throw new Error("Summary function response is missing a success flag");
  }
  if (!record.success) {
    throw new Error(
      typeof record.error === "string"
        ? record.error
        : "Summary generation failed",
    );
  }
  if (typeof record.markdown !== "string") {
    throw new Error("Summary function response is missing markdown");
  }

  const instanceCounts = emptyCounts();
  if (
    record.instance_counts !== null &&
    typeof record.instance_counts === "object"
  ) {
    const counts = record.instance_counts as Record<string, unknown>;
    for (const name of DATA_TYPE_NAMES) {
      const value = counts[name];
      if (typeof value === "number") instanceCounts[name] = value;
    }
  }

  const dataTypeTruncation: SummaryFunctionResult["dataTypeTruncation"] = {};
  if (
    record.data_type_truncation !== null &&
    typeof record.data_type_truncation === "object"
  ) {
    const truncation = record.data_type_truncation as Record<string, unknown>;
    for (const name of DATA_TYPE_NAMES) {
      const entry = truncation[name];
      if (entry === null || typeof entry !== "object") continue;
      const item = entry as Record<string, unknown>;
      if (item.had_more_in_cdf === true && typeof item.limit === "number") {
        dataTypeTruncation[name] = {
          limit: item.limit,
          hadMoreInCdf: true,
        };
      }
    }
  }

  const result: SummaryFunctionResult = {
    markdown: record.markdown,
    generatedAt: new Date(),
    instanceCounts,
    dataTypeTruncation,
  };

  if (record.cdf_result !== null && typeof record.cdf_result === "object") {
    const cdf = record.cdf_result as Record<string, unknown>;
    const cdfResult: SummaryCdfResult = {};
    if (typeof cdf.external_id === "string")
      cdfResult.external_id = cdf.external_id;
    if (typeof cdf.file_name === "string") cdfResult.file_name = cdf.file_name;
    if (typeof cdf.file_id === "number") cdfResult.file_id = cdf.file_id;
    if (typeof cdf.fusion_url === "string")
      cdfResult.fusion_url = cdf.fusion_url;
    result.cdfResult = cdfResult;
  }

  return result;
}

export class CogniteSummaryFunctionService implements SummaryFunctionService {
  private readonly calls: FunctionCallClient;

  constructor(client: CogniteClient) {
    const settings = getAppConfig().functions.summarizer;
    this.calls = new CogniteFunctionCallClient(client, {
      functionExternalId: getSummarizerFunctionExternalId(),
      pollIntervalMs: settings.pollIntervalMs,
      timeoutMs: settings.timeoutMs,
      maxConsecutivePollErrors: settings.maxConsecutivePollErrors,
      label: "Summarizer function",
    });
  }

  generate(
    request: SummaryGenerateRequest,
    opts?: SummaryFunctionCallOptions,
  ): Promise<SummaryFunctionResult> {
    return this.calls
      .call(buildSummaryFunctionData(request), opts)
      .then((response) => parseSummaryFunctionResult(response));
  }
}
