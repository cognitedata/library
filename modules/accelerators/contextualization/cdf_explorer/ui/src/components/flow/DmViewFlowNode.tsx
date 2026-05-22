import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { DataModelGraphView } from "../../types/explorerNodes";

export type DmViewNodeData = {
  view: DataModelGraphView;
  selected?: boolean;
  dimmed?: boolean;
};

export function DmViewFlowNode({ data }: NodeProps) {
  const d = data as DmViewNodeData;
  const view = d.view;
  const title = view.name?.trim() || view.external_id;
  const className = [
    "exp-dm-flow-node",
    d.selected ? "exp-dm-flow-node--selected" : "",
    d.dimmed ? "exp-dm-flow-node--dimmed" : "",
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <div className={className}>
      <Handle type="target" position={Position.Left} />
      <div className="exp-dm-flow-node__title">{title}</div>
      <div className="exp-dm-flow-node__meta">
        {view.space} · {view.version}
      </div>
      <div className="exp-dm-flow-node__props">
        {view.property_count} {view.property_count === 1 ? "property" : "properties"}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
