import { Handle, Position, type NodeProps } from "@xyflow/react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { WorkflowGraphTask } from "../../types/discoveryNodes";
import { workflowTaskKindLabel } from "../../utils/workflowTaskKind";

export type WfTaskNodeData = {
  task: WorkflowGraphTask;
  selected?: boolean;
  dimmed?: boolean;
};

export function WfTaskFlowNode({ data }: NodeProps) {
  const { t } = useAppSettings();
  const d = data as WfTaskNodeData;
  const task = d.task;
  const title = task.label?.trim() || task.name?.trim() || task.external_id;
  const kind = workflowTaskKindLabel(task, t);
  const className = [
    "disc-dm-flow-node",
    "disc-wf-flow-node",
    d.selected ? "disc-dm-flow-node--selected" : "",
    d.dimmed ? "disc-dm-flow-node--dimmed" : "",
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <div className={className}>
      <div className="disc-dm-flow-node__title">{title}</div>
      <div className="disc-dm-flow-node__kind">{kind}</div>
      <div className="disc-wf-flow-node__connector-bar">
        <span className="disc-wf-flow-node__connector-label">{t("wfViewer.inputConnector")}</span>
        <span className="disc-wf-flow-node__connector-label">{t("wfViewer.outputConnector")}</span>
      </div>
      <Handle type="target" position={Position.Left} className="disc-wf-flow-node__handle disc-wf-flow-node__handle--input" />
      <Handle type="source" position={Position.Right} className="disc-wf-flow-node__handle disc-wf-flow-node__handle--output" />
    </div>
  );
}
