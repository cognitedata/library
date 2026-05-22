import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { WorkflowGraphTask } from "../../types/explorerNodes";
import { workflowTaskKindLabel } from "../../utils/workflowTaskKind";

export type WfTaskNodeData = {
  task: WorkflowGraphTask;
  selected?: boolean;
  dimmed?: boolean;
};

export function WfTaskFlowNode({ data }: NodeProps) {
  const d = data as WfTaskNodeData;
  const task = d.task;
  const title = task.label?.trim() || task.name?.trim() || task.external_id;
  const kind = workflowTaskKindLabel(task);
  const className = [
    "exp-dm-flow-node",
    "exp-wf-flow-node",
    d.selected ? "exp-dm-flow-node--selected" : "",
    d.dimmed ? "exp-dm-flow-node--dimmed" : "",
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <div className={className}>
      <Handle type="target" position={Position.Left} />
      <div className="exp-dm-flow-node__title">{title}</div>
      <div className="exp-dm-flow-node__kind">{kind}</div>
      <div className="exp-dm-flow-node__props">{task.external_id}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
