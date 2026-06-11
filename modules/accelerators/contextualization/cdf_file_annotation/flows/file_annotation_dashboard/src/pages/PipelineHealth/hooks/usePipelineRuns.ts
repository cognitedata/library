import { useQuery } from "@tanstack/react-query";
import type { CogniteClient } from "@cognite/sdk";
import type { PipelineRun } from "@/shared/utils/types";
import { CallerType } from "@/shared/utils/constants";
import { isLocalMockMode } from "@/runtime/authMode";
import { getLocalFunctionLogs, localRunsByPipeline } from "@/mocks/mockData";

/**
 * Response structure from extraction pipeline runs API
 */
interface ExtractionPipelineRunsResponse {
  items: Array<{
    id: number;
    status: string;
    message?: string;
    createdTime: number;
    externalId?: string;
  }>;
  nextCursor?: string;
}

/**
 * Fetch extraction pipeline runs from CDF SDK
 */
async function fetchPipelineRuns(
  sdk: CogniteClient,
  pipelineExternalId: string
): Promise<PipelineRun[]> {
  try {
    const extractionPipelinesApi = (sdk as unknown as { extractionPipelines?: { runs?: { list?: (scope: { externalId: string; limit?: number }) => Promise<ExtractionPipelineRunsResponse> } } }).extractionPipelines;
    if (extractionPipelinesApi?.runs?.list) {
      const response = await extractionPipelinesApi.runs.list({
        externalId: pipelineExternalId,
        limit: 1000,
      });
      const items = response?.items ?? [];
      return items
        .map((run) => {
          const parsedMessage = parseRunMessage(run.message);
          return {
            id: String(run.id),
            status: run.status,
            message: run.message,
            createdTime: run.createdTime,
            caller: parsedMessage.caller,
            functionId: parsedMessage.functionId,
            callId: parsedMessage.callId,
            total: parsedMessage.total,
            success: parsedMessage.success,
            failed: parsedMessage.failed,
          };
        })
        .sort((a, b) => b.createdTime - a.createdTime);
    }

    // Fallback: extraction pipeline runs are not exposed in the SDK for this client.
    const allRuns: PipelineRun[] = [];
    let cursor: string | undefined;

    do {
      const response = await sdk.post<ExtractionPipelineRunsResponse>(
        `/api/v1/projects/${sdk.project}/extpipes/runs/list`,
        {
          data: {
            filter: { externalId: pipelineExternalId },
            limit: 1000,
            cursor,
          },
        }
      );

      if (response.data?.items) {
        for (const run of response.data.items) {
          const parsedMessage = parseRunMessage(run.message);

          allRuns.push({
            id: String(run.id),
            status: run.status,
            message: run.message,
            createdTime: run.createdTime,
            caller: parsedMessage.caller,
            functionId: parsedMessage.functionId,
            callId: parsedMessage.callId,
            total: parsedMessage.total,
            success: parsedMessage.success,
            failed: parsedMessage.failed,
          });
        }
      }

      cursor = response.data?.nextCursor;
    } while (cursor);

    return allRuns.sort((a, b) => b.createdTime - a.createdTime);
  } catch (error) {
    console.error("Failed to fetch pipeline runs:", error);
    return [];
  }
}

/**
 * Parse structured data from run message
 * Message format: (caller:Finalize, function_id:3035717719542834, call_id:1411406210929260) - total files processed: 11 - successful files: 11 - failed files: 0
 */
function parseRunMessage(message: string | undefined): {
  caller?: string;
  functionId?: string;
  callId?: string;
  total?: number;
  success?: number;
  failed?: number;
} {
  if (!message) return {};

  // Full regex pattern matching the expected format
  const fullPattern =
    /\(caller:(\w+),\s*function_id:([\w.-]+),\s*call_id:([\w.-]+)\)\s*-\s*total files processed:\s*(\d+)\s*-\s*successful files:\s*(\d+)\s*-\s*failed files:\s*(\d+)/i;

  const fullMatch = message.match(fullPattern);
  if (fullMatch) {
    return {
      caller: fullMatch[1],
      functionId: fullMatch[2],
      callId: fullMatch[3],
      total: parseInt(fullMatch[4], 10),
      success: parseInt(fullMatch[5], 10),
      failed: parseInt(fullMatch[6], 10),
    };
  }

  // Try a simpler pattern for partial matches
  const callerMatch = message.match(/caller:(\w+)/i);
  const functionIdMatch = message.match(/function_id:([\w.-]+)/i);
  const callIdMatch = message.match(/call_id:([\w.-]+)/i);
  const totalMatch = message.match(/total files processed:\s*(\d+)/i);
  const successMatch = message.match(/successful files:\s*(\d+)/i);
  const failedMatch = message.match(/failed files:\s*(\d+)/i);

  return {
    caller: callerMatch?.[1],
    functionId: functionIdMatch?.[1],
    callId: callIdMatch?.[1],
    total: totalMatch ? parseInt(totalMatch[1], 10) : undefined,
    success: successMatch ? parseInt(successMatch[1], 10) : undefined,
    failed: failedMatch ? parseInt(failedMatch[1], 10) : undefined,
  };
}

/**
 * Fetch function logs from CDF SDK
 */
async function fetchFunctionLogs(
  sdk: CogniteClient,
  functionId: number | string,
  callId: number | string
): Promise<string> {
  try {
    const functionsApi = (sdk as unknown as { functions?: { calls?: { getLogs?: (functionId: number | string, callId: number | string) => Promise<{ items?: Array<{ message: string; timestamp: number }> }> } } }).functions;
    if (functionsApi?.calls?.getLogs) {
      const response = await functionsApi.calls.getLogs(functionId, callId);
      const items = response?.items ?? [];
      return items
        .sort((a, b) => a.timestamp - b.timestamp)
        .map((item) => item.message)
        .join("\n");
    }

    // Fallback: function logs are not exposed in the SDK for this client.
    const response = await sdk.get<{ items?: Array<{ message: string; timestamp: number }> }>(
      `/api/v1/projects/${sdk.project}/functions/${functionId}/calls/${callId}/logs`
    );

    const items = response.data?.items ?? [];
    return items
      .sort((a, b) => a.timestamp - b.timestamp)
      .map((item) => item.message)
      .join("\n");
  } catch (error) {
    console.error("Failed to fetch function logs:", error);
    return "";
  }
}

/**
 * Filter log lines to show only those relevant to a specific file
 * Returns lines containing the file external ID with context lines before/after
 */
export function filterLogLines(
  logs: string,
  fileExternalId: string,
  contextLines = 2
): string {
  if (!logs || !fileExternalId) return "";

  const lines = logs.split("\n");
  const matchingIndices: number[] = [];

  // Find all lines containing the file external ID
  for (let i = 0; i < lines.length; i++) {
    if (lines[i].includes(fileExternalId)) {
      matchingIndices.push(i);
    }
  }

  if (matchingIndices.length === 0) return "";

  // Build result with context lines
  const resultLines: string[] = [];
  const includedLines = new Set<number>();

  for (const index of matchingIndices) {
    const start = Math.max(0, index - contextLines);
    const end = Math.min(lines.length - 1, index + contextLines);

    for (let i = start; i <= end; i++) {
      if (!includedLines.has(i)) {
        includedLines.add(i);
        resultLines.push(lines[i]);
      }
    }
  }

  return resultLines.join("\n");
}

function mapRunType(caller: string | undefined): string {
  if (!caller) return "Unknown";
  if (caller === CallerType.LAUNCH) return "Launch";
  if (caller === CallerType.FINALIZE) return "Finalize";
  if (caller === CallerType.PROMOTE) return "Promote";
  if (caller === CallerType.PREPARE) return "Prepare";
  return caller;
}

export function calculateRunMetrics(runs: PipelineRun[]) {
  const metrics: Record<string, { processed: number; success: number; failed: number }> = {};

  runs.forEach((run) => {
    const type = mapRunType(run.caller);
    if (!metrics[type]) {
      metrics[type] = { processed: 0, success: 0, failed: 0 };
    }

    metrics[type].processed += run.total ?? 0;
    metrics[type].success += run.success ?? 0;
    metrics[type].failed += run.failed ?? 0;
  });

  return metrics;
}

export function usePipelineRuns(sdk: CogniteClient | null, pipelineExternalId: string | null) {
  return useQuery({
    queryKey: ["pipeline-runs", isLocalMockMode ? "local" : sdk?.project, pipelineExternalId],
    queryFn: () => {
      if (isLocalMockMode) {
        return Promise.resolve(pipelineExternalId ? localRunsByPipeline[pipelineExternalId] || [] : []);
      }
      if (!sdk || !pipelineExternalId) return [] as PipelineRun[];
      return fetchPipelineRuns(sdk, pipelineExternalId);
    },
    enabled: (isLocalMockMode && !!pipelineExternalId) || (!!sdk && !!pipelineExternalId),
    staleTime: 5 * 60 * 1000,
  });
}

export function useFunctionLogs(
  sdk: CogniteClient | null,
  functionId: number | string | null,
  callId: number | string | null
) {
  return useQuery({
    queryKey: ["function-logs", isLocalMockMode ? "local" : sdk?.project, functionId, callId],
    queryFn: () => {
      if (isLocalMockMode) {
        return Promise.resolve(functionId && callId ? getLocalFunctionLogs(functionId, callId) : "");
      }
      if (!sdk || !functionId || !callId) return "";
      return fetchFunctionLogs(sdk, functionId, callId);
    },
    enabled: (isLocalMockMode && !!functionId && !!callId) || (!!sdk && !!functionId && !!callId),
    staleTime: 10 * 60 * 1000,
  });
}
