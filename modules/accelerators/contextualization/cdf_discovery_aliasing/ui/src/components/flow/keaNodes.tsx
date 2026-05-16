import type { CSSProperties } from "react";
import { Handle, Position, type NodeProps, useReactFlow } from "@xyflow/react";
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

/** Collapsed composite: card-sized node; boundary ports match ``subflow_*`` handle ids; inner graph is drill-in only. */
export function KeaSubgraphNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const o = useFlowHandleOrientation();
  const ports = d.subflow_ports;
  const inputs = ports?.inputs ?? [];
  const outputs = ports?.outputs ?? [];
  const inPos = o === "tb" ? Position.Top : Position.Left;
  const outPos = o === "tb" ? Position.Bottom : Position.Right;
  const innerNodes =
    d.inner_canvas?.nodes?.filter(
      (n) => n.kind !== "subflow_graph_in" && n.kind !== "subflow_graph_out"
    ) ?? [];
  const innerBody = innerNodes.length;
  const innerValidate = innerNodes.filter((n) => n.kind === "validation").length;

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
        {innerBody === 0
          ? "empty · double-click to open"
          : innerValidate > 0
            ? `${innerValidate} validate · ${innerBody} step${innerBody === 1 ? "" : "s"} · double-click`
            : `${innerBody} step${innerBody === 1 ? "" : "s"} · double-click`}
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

function DiscoveryDataStageNode({
  data,
  selected,
  variant,
  badge,
  title,
}: NodeProps & {
  variant: string;
  badge: string;
  title: string;
}) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useExtractionAliasingHandleLayout();
  const inlineRow = d.config;
  let summary: string | null = null;
  if (inlineRow && typeof inlineRow === "object" && !Array.isArray(inlineRow)) {
    const o = inlineRow as Record<string, unknown>;
    const ve = o.view_external_id != null ? String(o.view_external_id).trim() : "";
    const dsc = o.description != null ? String(o.description).trim() : "";
    if (ve) summary = ve;
    else if (dsc) summary = dsc;
  }
  const meta = summary ?? "—";
  return (
    <div className={nodeClass(!!selected, variant)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.dataIn} id="in" style={{ zIndex: 5 }} />
      <div className="kea-flow-node__badge">{badge}</div>
      <div className="kea-flow-node__title">{d.label || title}</div>
      <div className="kea-flow-node__meta">{meta}</div>
      <Handle type="source" position={h.dataOut} id="out" style={{ zIndex: 5 }} />
    </div>
  );
}

export function KeaViewQueryNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="extract"
      badge="view_q"
      title="View query"
    />
  );
}

export function KeaRawQueryNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="ref-index"
      badge="raw_q"
      title="RAW query"
    />
  );
}

export function KeaClassicQueryNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="source"
      badge="classic_q"
      title="Classic query"
    />
  );
}

export function KeaTransformNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="alias"
      badge="transform"
      title="Transform"
    />
  );
}

export function KeaJoinNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useExtractionAliasingHandleLayout();
  const inPos = h.dataIn;
  const leftId = subflowTargetHandleForPort("left");
  const rightId = subflowTargetHandleForPort("right");
  const inlineRow = d.config;
  let summary: string | null = null;
  if (inlineRow && typeof inlineRow === "object" && !Array.isArray(inlineRow)) {
    const o = inlineRow as Record<string, unknown>;
    const dsc = o.description != null ? String(o.description).trim() : "";
    if (dsc) summary = dsc;
  }
  const meta = summary ?? "—";
  return (
    <div className={nodeClass(!!selected, "alias")} style={mergeNodeCardStyle(d)}>
      <Handle
        type="target"
        position={inPos}
        id={leftId}
        style={{ ...portHandleStyle(inPos, 0, 2), zIndex: 5 }}
      />
      <Handle
        type="target"
        position={inPos}
        id={rightId}
        style={{ ...portHandleStyle(inPos, 1, 2), zIndex: 5 }}
      />
      <div className="kea-flow-node__badge">join</div>
      <div className="kea-flow-node__title">{d.label || "Join"}</div>
      <div className="kea-flow-node__meta">{meta}</div>
      <Handle type="source" position={h.dataOut} id="out" style={{ zIndex: 5 }} />
    </div>
  );
}

/** Executable ``fn_dm_validate`` — data in / out (match-definition layout uses the same ``out`` as other stages). */
export function KeaDiscoveryValidateNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  const inlineRow = d.config;
  let summary: string | null = null;
  if (inlineRow && typeof inlineRow === "object" && !Array.isArray(inlineRow)) {
    const row = inlineRow as Record<string, unknown>;
    const dsc = row.description != null ? String(row.description).trim() : "";
    if (dsc) summary = dsc;
  }
  const meta = summary ?? "—";
  return (
    <div className={nodeClass(!!selected, "validation")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" style={{ zIndex: 5 }} />
      <div className="kea-flow-node__badge">validate</div>
      <div className="kea-flow-node__title">{d.label || "Validation"}</div>
      <div className="kea-flow-node__meta">{meta}</div>
      <Handle type="source" position={h.out} id="out" style={{ zIndex: 5 }} />
    </div>
  );
}

export function KeaViewSaveNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="persist-alias"
      badge="view_sv"
      title="View save"
    />
  );
}

export function KeaRawSaveNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="persist-alias"
      badge="raw_sv"
      title="RAW save"
    />
  );
}

export function KeaClassicSaveNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="persist-alias"
      badge="classic_sv"
      title="Classic save"
    />
  );
}

const validationNodeTargetStyle: CSSProperties = {
  left: "50%",
  transform: "translateX(-50%)",
  zIndex: 5,
};

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

/** Legacy canvas kind ``alias_persistence`` — displays the DM apply function used today (`fn_dm_view_save`). */
export function KeaAliasPersistenceNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "persist-alias")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" />
      <div className="kea-flow-node__badge">persist</div>
      <div className="kea-flow-node__title">{d.label || "Alias write-back"}</div>
      <div className="kea-flow-node__meta">fn_dm_view_save</div>
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}

/** fn_dm_inverted_index — inverted RAW index; discovery path uses predecessor task snapshots (IR payload). */
export function KeaInvertedIndexNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "ref-index")} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" />
      <div className="kea-flow-node__badge">index</div>
      <div className="kea-flow-node__title">{d.label || "Inverted index"}</div>
      <div className="kea-flow-node__meta">fn_dm_inverted_index</div>
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}
