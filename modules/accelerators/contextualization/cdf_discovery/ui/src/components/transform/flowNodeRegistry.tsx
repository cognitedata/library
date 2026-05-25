import type { CSSProperties } from "react";
import type { NodeProps } from "@xyflow/react";
import { Handle, NodeResizer, Position } from "@xyflow/react";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import { kindToRfType } from "../../types/transformCanvas";
import { useFlowHandleOrientation } from "./FlowHandleOrientationContext";
import type { FlowHandleOrientationNodeData } from "./flowHandleOrientation";
import { mergeEtlNodeCardStyle } from "./flowNodeAccent";
import {
  ETL_NODE_MAX_HEIGHT,
  ETL_NODE_MAX_WIDTH,
  ETL_NODE_MIN_HEIGHT,
  ETL_NODE_MIN_WIDTH,
} from "./etlFlowNodeSizing";
import { etlFlowNodeCanvasDescription } from "./etlFlowNodeDescription";

function useDataHandles(data: FlowHandleOrientationNodeData): { in: Position; out: Position; key: string } {
  const o = data.flowHandleOrientation ?? useFlowHandleOrientation();
  return o === "tb"
    ? { in: Position.Top, out: Position.Bottom, key: "tb" }
    : { in: Position.Left, out: Position.Right, key: "lr" };
}

type EtlNodeProps = NodeProps & {
  data: {
    label?: string;
    kind?: TransformCanvasNodeKind;
    notes?: string;
    config?: unknown;
    canvas_node_enabled?: boolean;
    canvas_resize_enabled?: boolean;
    node_color?: string;
    node_bg_color?: string;
  };
};

function EtlNodeBodyContent({
  label,
  kindLabel,
  description,
}: {
  label: string;
  kindLabel: string;
  description: string;
}) {
  return (
    <>
      <span className="etl-flow-node__label">{label}</span>
      <span className="etl-flow-node__kind">{kindLabel}</span>
      {description ? <span className="etl-flow-node__description">{description}</span> : null}
    </>
  );
}

function EtlNodeResizer({ selected, enabled }: { selected: boolean; enabled: boolean }) {
  if (!enabled) return null;
  return (
    <NodeResizer
      isVisible={selected}
      minWidth={ETL_NODE_MIN_WIDTH}
      minHeight={ETL_NODE_MIN_HEIGHT}
      maxWidth={ETL_NODE_MAX_WIDTH}
      maxHeight={ETL_NODE_MAX_HEIGHT}
      lineClassName="etl-flow-node__resize-line"
      handleClassName="etl-flow-node__resize-handle"
    />
  );
}

function joinHandleStyle(inPos: Position, index: number, total: number): CSSProperties {
  const pct = total <= 1 ? 50 : ((index + 1) / (total + 1)) * 100;
  if (inPos === Position.Top || inPos === Position.Bottom) {
    return { left: `${pct}%`, transform: "translateX(-50%)" };
  }
  return { top: `${pct}%`, transform: "translateY(-50%)" };
}

function JoinFlowNode({ data, selected }: EtlNodeProps) {
  const handles = useDataHandles(data);
  const label = data.label ?? "join";
  const description = etlFlowNodeCanvasDescription("join", data as Record<string, unknown>);
  const disabled = data.canvas_node_enabled === false;
  const resizeEnabled = data.canvas_resize_enabled !== false;
  const accent = "var(--etl-node-default, #6366f1)";
  const customStyle = mergeEtlNodeCardStyle(data as Record<string, unknown>);
  const bodyStyle = {
    borderLeftColor: customStyle?.borderLeftColor ?? accent,
    ...(customStyle?.backgroundColor ? { backgroundColor: customStyle.backgroundColor } : {}),
    ...(customStyle?.borderLeftWidth ? { borderLeftWidth: customStyle.borderLeftWidth } : {}),
    ...(customStyle?.borderLeftStyle ? { borderLeftStyle: customStyle.borderLeftStyle } : {}),
  };

  return (
    <div
      className={`etl-flow-node etl-flow-node--join etl-flow-node--resizable${selected ? " etl-flow-node--selected" : ""}${disabled ? " etl-flow-node--disabled" : ""}`}
    >
      <EtlNodeResizer selected={Boolean(selected)} enabled={resizeEnabled} />
      <Handle
        key={`in-left-${handles.key}`}
        type="target"
        id="in__left"
        position={handles.in}
        className="etl-flow-handle etl-flow-handle--join-left"
        style={joinHandleStyle(handles.in, 0, 2)}
      />
      <Handle
        key={`in-right-${handles.key}`}
        type="target"
        id="in__right"
        position={handles.in}
        className="etl-flow-handle etl-flow-handle--join-right"
        style={joinHandleStyle(handles.in, 1, 2)}
      />
      <div className="etl-flow-node__body" style={bodyStyle}>
        <EtlNodeBodyContent label={label} kindLabel="join" description={description} />
      </div>
      <Handle
        key={`out-${handles.key}`}
        type="source"
        id="out"
        position={handles.out}
        className="etl-flow-handle"
      />
    </div>
  );
}

function EtlFlowNode({ data, selected }: EtlNodeProps) {
  const handles = useDataHandles(data);
  const kind = data.kind ?? "transform";
  const label =
    data.label ?? (kind === "start" ? "Workflow trigger" : kind === "end" ? "End" : kind.replace(/_/g, " "));
  const kindLabel = kind === "start" ? "workflow trigger" : kind === "end" ? "end" : kind.replace(/_/g, " ");
  const description = etlFlowNodeCanvasDescription(kind, data as Record<string, unknown>);
  const disabled = data.canvas_node_enabled === false;
  const resizeEnabled = data.canvas_resize_enabled !== false;
  const accent =
    kind === "start"
      ? "var(--etl-node-start, #22c55e)"
      : kind === "end"
        ? "var(--etl-node-end, #ef4444)"
        : kind.startsWith("query_") || kind.startsWith("save_")
          ? "var(--etl-node-io, #3b82f6)"
          : kind === "spark_transform" || kind === "transformation_ref"
            ? "var(--etl-node-spark, #f59e0b)"
            : "var(--etl-node-default, #6366f1)";
  const customStyle = mergeEtlNodeCardStyle(data as Record<string, unknown>);
  const bodyStyle = {
    borderLeftColor: customStyle?.borderLeftColor ?? accent,
    ...(customStyle?.backgroundColor ? { backgroundColor: customStyle.backgroundColor } : {}),
    ...(customStyle?.borderLeftWidth ? { borderLeftWidth: customStyle.borderLeftWidth } : {}),
    ...(customStyle?.borderLeftStyle ? { borderLeftStyle: customStyle.borderLeftStyle } : {}),
  };

  return (
    <div
      className={`etl-flow-node etl-flow-node--${kind} etl-flow-node--resizable${selected ? " etl-flow-node--selected" : ""}${disabled ? " etl-flow-node--disabled" : ""}`}
    >
      <EtlNodeResizer selected={Boolean(selected)} enabled={resizeEnabled} />
      {kind !== "start" && (
        <Handle
          key={`in-${handles.key}`}
          type="target"
          id="in"
          position={handles.in}
          className="etl-flow-handle"
        />
      )}
      <div className="etl-flow-node__body" style={bodyStyle}>
        <EtlNodeBodyContent label={label} kindLabel={kindLabel} description={description} />
      </div>
      {kind !== "end" && (
        <Handle
          key={`out-${handles.key}`}
          type="source"
          id="out"
          position={handles.out}
          className="etl-flow-handle"
        />
      )}
    </div>
  );
}

function makeNodeComponent(defaultKind: TransformCanvasNodeKind) {
  return function BoundEtlNode(props: NodeProps) {
    const data = props.data as EtlNodeProps["data"];
    return <EtlFlowNode {...props} data={{ ...data, kind: data.kind ?? defaultKind }} />;
  };
}

export const ETL_FLOW_NODE_TYPES = {
  etlStart: makeNodeComponent("start"),
  etlEnd: makeNodeComponent("end"),
  etlQueryView: makeNodeComponent("query_view"),
  etlQueryRaw: makeNodeComponent("query_raw"),
  etlQueryClassic: makeNodeComponent("query_classic"),
  etlQuerySql: makeNodeComponent("query_sql"),
  etlScore: makeNodeComponent("score"),
  etlTransform: makeNodeComponent("transform"),
  etlFilter: makeNodeComponent("filter"),
  etlFieldMap: makeNodeComponent("field_map"),
  etlJoin: JoinFlowNode,
  etlMerge: makeNodeComponent("merge"),
  etlBuildIndex: makeNodeComponent("build_index"),
  etlSaveView: makeNodeComponent("save_view"),
  etlSaveRaw: makeNodeComponent("save_raw"),
  etlSaveClassic: makeNodeComponent("save_classic"),
  etlRawCleanup: makeNodeComponent("raw_cleanup"),
  etlSparkTransform: makeNodeComponent("spark_transform"),
  etlTransformationRef: makeNodeComponent("transformation_ref"),
  etlFunctionRef: makeNodeComponent("function_ref"),
  etlDynamicFanout: makeNodeComponent("dynamic_fanout"),
  etlSubworkflow: makeNodeComponent("subworkflow"),
  etlSimulation: makeNodeComponent("simulation"),
  etlCdfTask: makeNodeComponent("cdf_task"),
  etlSubgraph: makeNodeComponent("subgraph"),
};

export function defaultLabelForKind(kind: TransformCanvasNodeKind): string {
  return kind.replace(/_/g, " ");
}

export function nextEtlNodeId(kind: TransformCanvasNodeKind, existingIds: Set<string>): string {
  const base = kind === "start" || kind === "end" ? kind : `${kind}_${Date.now().toString(36)}`;
  let id = base;
  let n = 1;
  while (existingIds.has(id)) {
    id = `${base}_${n++}`;
  }
  return id;
}

export function rfTypeForKind(kind: TransformCanvasNodeKind): string {
  return kindToRfType(kind);
}
