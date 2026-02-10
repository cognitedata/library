import { useEffect, useMemo, useState } from "react";
import { normalizeStatus } from "@/shared/time-utils";
import type { LoadState, WorkflowExecutionSummary } from "./types";
import { useI18n } from "@/shared/i18n";

type UseWorkflowDataArgs = {
  isSdkLoading: boolean;
  sdk: { project: string; post: Function; get: Function };
  windowRange: { start: number; end: number } | null;
};

export function useWorkflowData({ isSdkLoading, sdk, windowRange }: UseWorkflowDataArgs) {
  const { t } = useI18n();
  const [workflowsStatus, setWorkflowsStatus] = useState<LoadState>("idle");
  const [workflowsError, setWorkflowsError] = useState<string | null>(null);
  const [workflowExecutionsAll, setWorkflowExecutionsAll] = useState<WorkflowExecutionSummary[]>([]);
  const [workflowDetails, setWorkflowDetails] = useState<Record<string, unknown> | null>(null);
  const [workflowDetailsStatus, setWorkflowDetailsStatus] = useState<LoadState>("idle");
  const [workflowDetailsError, setWorkflowDetailsError] = useState<string | null>(null);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const loadWorkflows = async () => {
      setWorkflowsStatus("loading");
      setWorkflowsError(null);
      setWorkflowExecutionsAll([]);
      try {
        const executions: WorkflowExecutionSummary[] = [];
        let cursor: string | undefined;
        do {
          const response = await sdk.post<{
            items?: WorkflowExecutionSummary[];
            nextCursor?: string | null;
          }>(`/api/v1/projects/${sdk.project}/workflows/executions/list`, {
            data: { limit: 1000, cursor },
          });
          executions.push(...(response.data?.items ?? []));
          cursor = response.data?.nextCursor ?? undefined;
        } while (cursor);

        if (!cancelled) {
          setWorkflowExecutionsAll(executions);
          setWorkflowsStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setWorkflowsError(error instanceof Error ? error.message : t("processing.error.workflows"));
          setWorkflowsStatus("error");
        }
      }
    };

    loadWorkflows();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, t]);

  const filteredWorkflowExecutions = useMemo(() => {
    if (!windowRange) return [];
    const startWindow = windowRange.start;
    const endWindow = windowRange.end;
    return workflowExecutionsAll.filter((execution) => {
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
      const response = await sdk.get<Record<string, unknown>>(
        `/api/v1/projects/${sdk.project}/workflows/executions/${executionId}`
      );
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
