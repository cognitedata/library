import type { CSSProperties } from "react";
import { Handle, NodeResizer, Position, type NodeProps, useReactFlow } from "@xyflow/react";
import {
  type SubflowPortEntry,
  type WorkflowCanvasNodeData,
  subflowSourceHandleForPort,
  subflowTargetHandleForPort,
} from "../../types/workflowCanvas";
import { useFlowHandleOrientation } from "./FlowHandleOrientationContext";
import { mergeNodeCardStyle } from "./flowNodeAccent";

function useDataHandles(): { in: Position; out: Position } {
  const o = useFlowHandleOrientation();
  return o === "tb" ? { in: Position.Top, out: Position.Bottom } : { in: Position.Left, out: Position.Right };
}

/** Data in/out + validation branch for extraction / aliasing (lr: left|right + bottom; tb: top|bottom + right). */
function useExtractionAliasingHandleLayout(): {
  dataIn: Position;
  dataOut: Position;
  validationOut: Position;
} {
  const o = useFlowHandleOrientation();
  if (o === "tb") {
    return { dataIn: Position.Top, dataOut: Position.Bottom, validationOut: Position.Right };
  }
  return { dataIn: Position.Left, dataOut: Position.Right, validationOut: Position.Bottom };
}

function nodeClass(selected: boolean, variant: string): string {
  return `kea-flow-node kea-flow-node--${variant}${selected ? " kea-flow-node--selected" : ""}`;
}

function portHandleStyle(
  position: Position,
  index: number,
  total: number
): CSSProperties {
  const frac = total === 1 ? 50 : (100 * (index + 1)) / (total + 1);
  if (position === Position.Left || position === Position.Right) {
    return { top: `${frac}%`, transform: "translateY(-50%)" };
  }
  return { left: `${frac}%`, transform: "translateX(-50%)" };
}

/** Subflow frame — organizational grouping only (no boundary I/O; use ``keaSubgraph`` for named ports). */
export function KeaSubflowNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;

  return (
    <div
      className={nodeClass(!!selected, "subflow")}
      style={mergeNodeCardStyle(d, {
        width: "100%",
        height: "100%",
        minWidth: 200,
        minHeight: 140,
        position: "relative",
      })}
    >
      <NodeResizer
        minWidth={200}
        minHeight={140}
        isVisible={selected}
        autoScale={false}
        keepAspectRatio={false}
        lineStyle={{ borderWidth: 2, opacity: selected ? 1 : 0 }}
      />
      <div className="kea-flow-node__badge">subflow</div>
      <div className="kea-flow-node__title">{d.label || "Subflow"}</div>
    </div>
  );
}

/** Collapsed composite: card-sized node; boundary ports match ``subflow_*`` handle ids; inner graph is drill-in only. */
export function KeaSubgraphNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const o = useFlowHandleOrientation();
  const ports = d.subflow_ports;
  const inputs = ports?.inputs ?? [];
  const outputs = ports?.outputs ?? [];
  const inPos = o === "tb" ? Position.Top : Position.Left;
  const outPos = o === "tb" ? Position.Bottom : Position.Right;
  const innerBody =
    d.inner_canvas?.nodes?.filter(
      (n) => n.kind !== "subflow_graph_in" && n.kind !== "subflow_graph_out"
    ).length ?? 0;

  return (
    <div className={nodeClass(!!selected, "subgraph")} style={mergeNodeCardStyle(d, { position: "relative" })}>
      {inputs.map((p: SubflowPortEntry, i: number) => (
        <Handle
          key={`sg-in-${p.id}`}
          type="target"
          position={inPos}
          id={subflowTargetHandleForPort(p.id)}
          style={{ ...portHandleStyle(inPos, i, inputs.length), zIndex: 6 }}
        />
      ))}
      {outputs.map((p: SubflowPortEntry, i: number) => (
        <Handle
          key={`sg-out-${p.id}`}
          type="source"
          position={outPos}
          id={subflowSourceHandleForPort(p.id)}
          style={{ ...portHandleStyle(outPos, i, outputs.length), zIndex: 6 }}
        />
      ))}
      <div className="kea-flow-node__badge">subgraph</div>
      <div className="kea-flow-node__title">{d.label || "Subgraph"}</div>
      <div className="kea-flow-node__meta">
        {innerBody === 0 ? "empty" : `${innerBody} step${innerBody === 1 ? "" : "s"}`}
      </div>
    </div>
  );
}

/** Internal subgraph input hub — sources mirror subflow input port ids. */
export function KeaSubflowGraphInNode({ data, selected, parentId }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const { getNode } = useReactFlow();
  const parent = parentId ? getNode(parentId) : undefined;
  const portsFromOwn = d.subflow_ports?.inputs?.length ? d.subflow_ports : undefined;
  const ports =
    portsFromOwn ?? ((parent?.data ?? {}) as WorkflowCanvasNodeData).subflow_ports;
  const inputs = ports?.inputs ?? [];
  const o = useFlowHandleOrientation();
  const outPos = o === "tb" ? Position.Bottom : Position.Right;

  return (
    <div
      className={nodeClass(!!selected, "subflow-graph")}
      style={mergeNodeCardStyle(d, { position: "relative", minWidth: 120, minHeight: 56 })}
    >
      <div className="kea-flow-node__badge">in</div>
      <div className="kea-flow-node__title">{d.label || "Graph inputs"}</div>
      {inputs.map((p: SubflowPortEntry, i: number) => (
        <Handle
          key={`hub-in-${p.id}`}
          type="source"
          position={outPos}
          id={subflowSourceHandleForPort(p.id)}
          style={{ ...portHandleStyle(outPos, i, inputs.length), zIndex: 4 }}
        />
      ))}
    </div>
  );
}

/** Internal subgraph output hub — targets mirror subflow output port ids. */
export function KeaSubflowGraphOutNode({ data, selected, parentId }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const { getNode } = useReactFlow();
  const parent = parentId ? getNode(parentId) : undefined;
  const portsFromOwn = d.subflow_ports?.outputs?.length ? d.subflow_ports : undefined;
  const ports =
    portsFromOwn ?? ((parent?.data ?? {}) as WorkflowCanvasNodeData).subflow_ports;
  const outputs = ports?.outputs ?? [];
  const o = useFlowHandleOrientation();
  const inPos = o === "tb" ? Position.Top : Position.Left;

  return (
    <div
      className={nodeClass(!!selected, "subflow-graph")}
      style={mergeNodeCardStyle(d, { position: "relative", minWidth: 120, minHeight: 56 })}
    >
      {outputs.map((p: SubflowPortEntry, i: number) => (
        <Handle
          key={`hub-out-${p.id}`}
          type="target"
          position={inPos}
          id={subflowTargetHandleForPort(p.id)}
          style={{ ...portHandleStyle(inPos, i, outputs.length), zIndex: 4 }}
        />
      ))}
      <div className="kea-flow-node__badge">out</div>
      <div className="kea-flow-node__title">{d.label || "Graph outputs"}</div>
    </div>
  );
}

export function KeaStartNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "start")} style={mergeNodeCardStyle(d)}>
      <div className="kea-flow-node__badge">start</div>
      <div className="kea-flow-node__title">{d.label || "Start"}</div>
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}

export function KeaEndNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "end")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" />
      <div className="kea-flow-node__badge">end</div>
      <div className="kea-flow-node__title">{d.label || "End"}</div>
    </div>
  );
}

export function KeaSourceViewNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "source")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" />
      <div className="kea-flow-node__badge">source</div>
      <div className="kea-flow-node__title">{d.label || "Source view"}</div>
      {d.ref?.view_external_id && (
        <div className="kea-flow-node__meta">{String(d.ref.view_external_id)}</div>
      )}
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}

export function KeaExtractionNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const handler = d.handler_id ? String(d.handler_id) : "—";
  const h = useExtractionAliasingHandleLayout();
  const valStyle: CSSProperties = {
    ...portHandleStyle(h.validationOut, 0, 1),
    zIndex: 5,
  };
  return (
    <div className={nodeClass(!!selected, "extract")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.dataIn} id="in" style={{ zIndex: 5 }} />
      <div className="kea-flow-node__badge">
        extract{d.preset_from_palette ? " ●" : ""}
      </div>
      <div className="kea-flow-node__title">{d.label || "Extraction"}</div>
      <div className="kea-flow-node__meta">{handler}</div>
      <Handle type="source" position={h.dataOut} id="out" style={{ zIndex: 5 }} />
      <Handle type="source" position={h.validationOut} id="validation" style={valStyle} />
    </div>
  );
}

export function KeaAliasingNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const handler = d.handler_id ? String(d.handler_id) : "—";
  const h = useExtractionAliasingHandleLayout();
  const valStyle: CSSProperties = {
    ...portHandleStyle(h.validationOut, 0, 1),
    zIndex: 5,
  };
  return (
    <div className={nodeClass(!!selected, "alias")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.dataIn} id="in" style={{ zIndex: 5 }} />
      <div className="kea-flow-node__badge">
        alias{d.preset_from_palette ? " ●" : ""}
      </div>
      <div className="kea-flow-node__title">{d.label || "Aliasing"}</div>
      <div className="kea-flow-node__meta">{handler}</div>
      <Handle type="source" position={h.dataOut} id="out" style={{ zIndex: 5 }} />
      <Handle type="source" position={h.validationOut} id="validation" style={valStyle} />
    </div>
  );
}

const validationNodeTargetStyle: CSSProperties = {
  left: "50%",
  transform: "translateX(-50%)",
  zIndex: 5,
};

export function KeaValidationNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const ak = d.annotation_kind ? String(d.annotation_kind) : "—";
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "validation")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={Position.Top} id="in" style={validationNodeTargetStyle} />
      <div className="kea-flow-node__badge">annotation</div>
      <div className="kea-flow-node__title">{d.label || "Validation"}</div>
      <div className="kea-flow-node__meta">{ak}</div>
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}

/** Scope `validation_rules[]` — evaluated during listing / extraction / aliasing per parent config. */
export function KeaValidationRuleNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const ctx = d.validation_rule_context ? String(d.validation_rule_context) : "—";
  const ruleName = d.validation_rule_name ? String(d.validation_rule_name) : "—";
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "validation-rule")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={Position.Top} id="in" style={validationNodeTargetStyle} />
      <div className="kea-flow-node__badge">match</div>
      <div className="kea-flow-node__title">{d.label || "Match validation"}</div>
      <div className="kea-flow-node__meta">
        {ctx} · {ruleName}
      </div>
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}

/** fn_dm_alias_persistence — writes aliases (and optional FK strings) back to describable instances. */
export function KeaAliasPersistenceNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "persist-alias")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" />
      <div className="kea-flow-node__badge">persist</div>
      <div className="kea-flow-node__title">{d.label || "Alias write-back"}</div>
      <div className="kea-flow-node__meta">fn_dm_alias_persistence</div>
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}

/** fn_dm_reference_index — inverted RAW index from FK / document reference JSON in extraction store. */
export function KeaReferenceIndexNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "ref-index")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" />
      <div className="kea-flow-node__badge">index</div>
      <div className="kea-flow-node__title">{d.label || "Reference index"}</div>
      <div className="kea-flow-node__meta">fn_dm_reference_index</div>
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}
