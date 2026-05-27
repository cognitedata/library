import { Handle, Position, type NodeProps } from "@xyflow/react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useFlowHandleOrientation } from "../transform/FlowHandleOrientationContext";
import type { FlowHandleOrientationNodeData } from "../transform/flowHandleOrientation";
import type { DataModelGraphView } from "../../types/discoveryNodes";

export type DmViewNodeData = {
  view: DataModelGraphView;
  selected?: boolean;
  dimmed?: boolean;
};

function useDmViewHandles(data: FlowHandleOrientationNodeData): { target: Position; source: Position } {
  const orientation = data.flowHandleOrientation ?? useFlowHandleOrientation();
  return orientation === "tb"
    ? { target: Position.Top, source: Position.Bottom }
    : { target: Position.Left, source: Position.Right };
}

export function DmViewFlowNode({ data }: NodeProps) {
  const { t } = useAppSettings();
  const d = data as DmViewNodeData & FlowHandleOrientationNodeData;
  const handles = useDmViewHandles(d);
  const view = d.view;
  const title = view.name?.trim() || view.external_id;
  const propertyLabel =
    view.property_count === 1
      ? t("dmViewer.propertyCountOne")
      : t("dmViewer.propertyCountMany", { count: String(view.property_count) });
  const className = [
    "disc-dm-flow-node",
    d.selected ? "disc-dm-flow-node--selected" : "",
    d.dimmed ? "disc-dm-flow-node--dimmed" : "",
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <div className={className}>
      <Handle type="target" position={handles.target} />
      <div className="disc-dm-flow-node__title">{title}</div>
      <div className="disc-dm-flow-node__meta">
        {view.space} · {view.version}
      </div>
      <div className="disc-dm-flow-node__props">{propertyLabel}</div>
      <Handle type="source" position={handles.source} />
    </div>
  );
}
