import { useCallback, useEffect, useMemo, useState } from "react";
import { normalizeStatus } from "@/shared/time-utils";
import {
  DEFAULT_PROCESSING_EXECUTION_CAP,
  type LoadState,
  type ProcessingDataLoadProgress,
  type ProcessingRequestStats,
  type WorkflowExecutionSummary,
} from "./types";
import { useI18n } from "@/shared/i18n";
import { withTransientRetries } from "@/shared/transient-http-retry";
import {
  isStaleProcessingFetch,
  noteForbiddenFailure,
  processingRequestStats,
  useProcessingWindowSessionReset,
} from "./processing-request-stats";

type WorkflowExecutionsListApiResponse = {
  data?: {
    items?: WorkflowExecutionSummary[];
    nextCursor?: string | null;
  };
};

type WorkflowExecutionDetailApiResponse = {
  data?: Record<string, unknown>;
};

type UseWorkflowDataArgs = {
  isSdkLoading: boolean;
  sdk: { project: string; post: Function; get: Function };
  windowRange: { start: number; end: number } | null;
  fetchEnabled?: boolean;
  windowSessionKey?: string;
  fetchGeneration?: number;
  refetchExecutionsOnly?: boolean;
  executionLimit?: number | null;
};

export function useWorkflowData({
  isSdkLoading,
  sdk,
  windowRange,
  fetchEnabled = true,
  windowSessionKey = "",
  fetchGeneration = 0,
  refetchExecutionsOnly = false,
  executionLimit = DEFAULT_PROCESSING_EXECUTION_CAP,
}: UseWorkflowDataArgs) {
  const { t } = useI18n();
  const [workflowsStatus, setWorkflowsStatus] = useState<LoadState>("idle");
  const [executionsTruncated, setExecutionsTruncated] = useState(false);
  const [workflowsError, setWorkflowsError] = useState<string | null>(null);
  const [workflowExecutionsAll, setWorkflowExecutionsAll] = useState<WorkflowExecutionSummary[]>([]);
  const [workflowDetails, setWorkflowDetails] = useState<Record<string, unknown> | null>(null);
  const [workflowDetailsStatus, setWorkflowDetailsStatus] = useState<LoadState>("idle");
  const [workflowDetailsError, setWorkflowDetailsError] = useState<string | null>(null);
  const [loadProgress, setLoadProgress] = useState<ProcessingDataLoadProgress | null>(null);
  const [requestStats, setRequestStats] = useState<ProcessingRequestStats | null>(null);

  const resetForNewWindow = useCallback(() => {
    setWorkflowsStatus("idle");
    setLoadProgress(null);
    setExecutionsTruncated(false);
    setWorkflowsError(null);
    setRequestStats(null);
    setWorkflowExecutionsAll([]);
  }, []);

  useProcessingWindowSessionReset(windowSessionKey, resetForNewWindow);

  useEffect(() => {
    if (!fetchEnabled) {
      setLoadProgress(null);
      return;
    }
    if (isSdkLoading) return;
    if (!windowRange) return;

    const generation = fetchGeneration;
    if (!refetchExecutionsOnly) {
      setWorkflowsStatus("loading");
    }
    let cancelled = false;
    const loadWorkflows = async () => {
      setExecutionsTruncated(false);
      setWorkflowsError(null);
      setRequestStats(null);
      if (!refetchExecutionsOnly) {
        setWorkflowExecutionsAll([]);
      }
      setLoadProgress({
        kind: "workflows_executions",
        loaded: refetchExecutionsOnly ? workflowExecutionsAll.length : 0,
      });
      const startMs = windowRange.start;
      const endMs = windowRange.end;
      try {
        const executions: WorkflowExecutionSummary[] = [];
        let cursor: string | undefined;
        let failedRequests = 0;
        let totalRequests = 0;
        const permissionsDenied = { current: false };
        const cap = executionLimit;
        let hitExecutionCap = false;
        do {
          totalRequests++;
          try {
            const response = (await withTransientRetries(() =>
              sdk.post(`/api/v1/projects/${sdk.project}/workflows/executions/list`, {
                data: {
                  filter: {
                    createdTimeStart: startMs,
                    createdTimeEnd: endMs,
                  },
                  limit: 1000,
                  cursor,
                },
              })
            )) as WorkflowExecutionsListApiResponse;
            for (const item of response.data?.items ?? []) {
              if (cap != null && executions.length >= cap) {
                hitExecutionCap = true;
                break;
              }
              executions.push(item);
            }
            if (hitExecutionCap) {
              cursor = undefined;
            } else {
              cursor = response.data?.nextCursor ?? undefined;
            }
          } catch (e) {
            failedRequests++;
            noteForbiddenFailure(permissionsDenied, e);
            break;
          }
          if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
            setLoadProgress({ kind: "workflows_executions", loaded: executions.length });
          }
        } while (cursor);

        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setWorkflowExecutionsAll(executions);
          setExecutionsTruncated(hitExecutionCap);
          setLoadProgress(null);
          setRequestStats(
            processingRequestStats(failedRequests, totalRequests, permissionsDenied.current)
          );
          setWorkflowsStatus("success");
        }
      } catch (error) {
        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setLoadProgress(null);
          setRequestStats(null);
          setWorkflowsError(error instanceof Error ? error.message : t("processing.error.workflows"));
          setWorkflowsStatus("error");
        }
      }
    };

    loadWorkflows();
    return () => {
      cancelled = true;
    };
  }, [
    executionLimit,
    fetchEnabled,
    fetchGeneration,
    isSdkLoading,
    refetchExecutionsOnly,
    sdk,
    windowRange?.start,
    windowRange?.end,
    t,
  ]);

  const filteredWorkflowExecutions = useMemo(() => {
    if (!windowRange) return [];
    const startWindow = windowRange.start;
    const endWindow = windowRange.end;
    return workflowExecutionsAll.filter((ex) => {
      const start = ex.startTime ?? ex.createdTime;
      const end = ex.endTime ?? ex.startTime ?? ex.createdTime;
      if (!start || !end) return false;
      return start <= endWindow && end >= startWindow;
    });
  }, [workflowExecutionsAll, windowRange]);

  const getWorkflowDuration = (execution: WorkflowExecutionSummary) => {
    const start = execution.startTime ?? execution.createdTime;
    const end = execution.endTime ?? execution.startTime ?? execution.createdTime;
    if (!start || !end || end < start) return null;
    return end - start;
  };

  const getWorkflowRadius = (execution: WorkflowExecutionSummary) => {
    const duration = getWorkflowDuration(execution);
    if (duration == null) return 6;
    const minutes = duration / 60000;
    const scaled = 4 + Math.sqrt(minutes) * 6;
    return Math.min(18, Math.max(4, scaled));
  };

  const getWorkflowColor = (execution: WorkflowExecutionSummary) => {
    const status = normalizeStatus(execution.status);
    if (status.includes("completed")) return "#16a34a";
    if (status.includes("timed_out")) return "#7c3aed";
    if (status.includes("failed")) return "#f97316";
    if (status.includes("running")) return "#2563eb";
    return "#a855f7";
  };

  const fetchWorkflowDetails = async (executionId: string) => {
    setWorkflowDetailsStatus("loading");
    setWorkflowDetailsError(null);
    setWorkflowDetails(null);
    try {
      const response = (await withTransientRetries(() =>
        sdk.get(`/api/v1/projects/${sdk.project}/workflows/executions/${executionId}`)
      )) as WorkflowExecutionDetailApiResponse;
      setWorkflowDetails(response.data ?? null);
      setWorkflowDetailsStatus("success");
    } catch (error) {
      setWorkflowDetailsError(
        error instanceof Error ? error.message : t("processing.modal.workflow.error")
      );
      setWorkflowDetailsStatus("error");
    }
  };

  const resetWorkflowDetails = () => {
    setWorkflowDetails(null);
    setWorkflowDetailsStatus("idle");
    setWorkflowDetailsError(null);
  };

  return {
    workflowsStatus,
    executionsTruncated,
    loadProgress,
    requestStats,
    workflowsError,
    workflowExecutionsAll,
    filteredWorkflowExecutions,
    workflowDetails,
    workflowDetailsStatus,
    workflowDetailsError,
    getWorkflowDuration,
    getWorkflowRadius,
    getWorkflowColor,
    fetchWorkflowDetails,
    resetWorkflowDetails,
  };
}
