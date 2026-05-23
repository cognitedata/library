import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";
import { PIPELINE_STEP_ORDER, type RunStepId, type StepRunStatus } from "../utils/runStream";

export type RunWorkflowNode = {
  id: RunStepId;
  labelKey: MessageKey;
  functionId: string;
};

export const RUN_WORKFLOW_NODES: RunWorkflowNode[] = [
  {
    id: "extract",
    labelKey: "run.workflowNode.extract",
    functionId: "fn_dm_extract_assets_by_pattern",
  },
  {
    id: "create",
    labelKey: "run.workflowNode.create",
    functionId: "fn_dm_create_asset_hierarchy",
  },
  {
    id: "write",
    labelKey: "run.workflowNode.write",
    functionId: "fn_dm_write_asset_hierarchy",
  },
];

type NodeDisplayStatus = StepRunStatus | "skipped";

type Props = {
  plannedSteps: RunStepId[];
  stepStatuses: Record<RunStepId, StepRunStatus>;
  runBusy: boolean;
};

function nodeStatus(
  stepId: RunStepId,
  planned: Set<RunStepId>,
  stepStatuses: Record<RunStepId, StepRunStatus>,
  runBusy: boolean
): NodeDisplayStatus {
  if (!planned.has(stepId)) {
    return runBusy ? "skipped" : "idle";
  }
  return stepStatuses[stepId] ?? "idle";
}

export function RunWorkflowStrip({ plannedSteps, stepStatuses, runBusy }: Props) {
  const { t } = useAppSettings();
  const planned = new Set(plannedSteps);

  return (
    <div className="fas-run-workflow" role="list" aria-label={t("run.workflowAria")}>
      {RUN_WORKFLOW_NODES.map((node, index) => {
        const status = nodeStatus(node.id, planned, stepStatuses, runBusy);
        const executing = status === "running";
        return (
          <div key={node.id} className="fas-run-workflow__segment" role="listitem">
            {index > 0 ? (
              <span
                className={`fas-run-workflow__arrow${
                  stepStatuses[RUN_WORKFLOW_NODES[index - 1].id] === "succeeded"
                    ? " fas-run-workflow__arrow--done"
                    : ""
                }`}
                aria-hidden
              >
                →
              </span>
            ) : null}
            <div
              className={`fas-run-wf-node fas-run-wf-node--${status}${executing ? " fas-run-wf-node--executing" : ""}`}
              data-status={status}
              aria-current={executing ? "step" : undefined}
            >
              <span className="fas-run-wf-node__title">{t(node.labelKey)}</span>
              <code className="fas-run-wf-node__fn">{node.functionId}</code>
              {executing ? (
                <span className="fas-run-wf-node__badge">{t("run.workflowExecuting")}</span>
              ) : status === "succeeded" ? (
                <span className="fas-run-wf-node__badge fas-run-wf-node__badge--ok">
                  {t("run.stepStatus.succeeded")}
                </span>
              ) : status === "failed" ? (
                <span className="fas-run-wf-node__badge fas-run-wf-node__badge--fail">
                  {t("run.stepStatus.failed")}
                </span>
              ) : status === "skipped" ? (
                <span className="fas-run-wf-node__badge fas-run-wf-node__badge--skip">
                  {t("run.workflowSkipped")}
                </span>
              ) : runBusy && planned.has(node.id) ? (
                <span className="fas-run-wf-node__badge">{t("run.stepStatus.idle")}</span>
              ) : null}
            </div>
          </div>
        );
      })}
    </div>
  );
}
