import { useEffect, useLayoutEffect, useMemo, useState } from "react";
import { normalizeStatus } from "@/shared/time-utils";
import type {
  LoadState,
  ProcessingDataLoadProgress,
  ProcessingRequestStats,
  WorkflowExecutionSummary,
} from "./types";
import { useI18n } from "@/shared/i18n";
import { withTransientRetries } from "@/shared/transient-http-retry";

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
};

export function useWorkflowData({
  isSdkLoading,
  sdk,
  windowRange,
  fetchEnabled = true,
}: UseWorkflowDataArgs) {
  const { t } = useI18n();
  const [workflowsStatus, setWorkflowsStatus] = useState<LoadState>("idle");
  const [workflowsError, setWorkflowsError] = useState<string | null>(null);
  const [workflowExecutionsAll, setWorkflowExecutionsAll] = useState<WorkflowExecutionSummary[]>([]);
  const [workflowDetails, setWorkflowDetails] = useState<Record<string, unknown> | null>(null);
  const [workflowDetailsStatus, setWorkflowDetailsStatus] = useState<LoadState>("idle");
  const [workflowDetailsError, setWorkflowDetailsError] = useState<string | null>(null);
  const [loadProgress, setLoadProgress] = useState<ProcessingDataLoadProgress | null>(null);
  const [requestStats, setRequestStats] = useState<ProcessingRequestStats | null>(null);

  useLayoutEffect(() => {
    if (!fetchEnabled || isSdkLoading) return;
    setWorkflowsStatus("loading");
  }, [fetchEnabled, isSdkLoading]);

  useEffect(() => {
    if (!fetchEnabled) return;
    if (isSdkLoading) return;
    if (!windowRange) return;
    let cancelled = false;
    const loadWorkflows = async () => {
      setWorkflowsStatus("loading");
      setWorkflowsError(null);
      setRequestStats(null);
      setWorkflowExecutionsAll([]);
      setLoadProgress({ kind: "workflows_executions", loaded: 0 });
      const startMs = windowRange.start;
      const endMs = windowRange.end;
      try {
        const executions: WorkflowExecutionSummary[] = [];
        let cursor: string | undefined;
        let failedRequests = 0;
        let totalRequests = 0;
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
            executions.push(...(response.data?.items ?? []));
            cursor = response.data?.nextCursor ?? undefined;
          } catch {
            failedRequests++;
            break;
          }
          if (!cancelled) {
            setLoadProgress({ kind: "workflows_executions", loaded: executions.length });
          }
        } while (cursor);

        if (!cancelled) {
          setWorkflowExecutionsAll(executions);
          setLoadProgress(null);
          if (failedRequests > 0) {
            setRequestStats({ failed: failedRequests, total: totalRequests });
          }
          setWorkflowsStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
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
  }, [fetchEnabled, isSdkLoading, sdk, windowRange?.start, windowRange?.end, t]);

  const filteredWorkflowExecutions = useMemo(() => {
    if (!windowRange) return [];
    const startWindow = windowRange.start;
    const endWindow = windowRange.end;
    return workflowExecutionsAll.filter((execution) => {
      const created = execution.createdTime;
      if (created < startWindow || created > endWindow) return false;
      const start = execution.startTime ?? execution.createdTime;
      const end = execution.endTime ?? execution.startTime ?? execution.createdTime;
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
