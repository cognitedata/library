import type { CSSProperties } from "react";
import { Handle, Position, type NodeProps, useReactFlow } from "@xyflow/react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";
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

function isFlowNodeCanvasDisabled(
  data?: WorkflowCanvasNodeData | Record<string, unknown>
): boolean {
  return data != null && (data as WorkflowCanvasNodeData).canvas_node_enabled === false;
}

function nodeClass(
  selected: boolean,
  variant: string,
  data?: WorkflowCanvasNodeData | Record<string, unknown>
): string {
  const disabled = isFlowNodeCanvasDisabled(data);
  return `discovery-flow-node discovery-flow-node--${variant}${selected ? " discovery-flow-node--selected" : ""}${disabled ? " discovery-flow-node--disabled" : ""}`;
}

/** Shown when the node is omitted from compile (manual or cascade disable). */
function FlowNodeDisabledBadge({
  data,
}: {
  data?: WorkflowCanvasNodeData | Record<string, unknown>;
}) {
  const { t } = useAppSettings();
  if (!isFlowNodeCanvasDisabled(data)) return null;
  return (
    <div className="discovery-flow-node__badge discovery-flow-node__badge--off">{t("flow.nodeBadgeOff")}</div>
  );
}

/** Operator label on top, stage kind below (Transform, View query, …). */
function FlowNodeLabelKind({ label, kind }: { label: string; kind: string }) {
  return (
    <>
      <div className="discovery-flow-node__title">{label}</div>
      <div className="discovery-flow-node__kind">{kind}</div>
    </>
  );
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
export function DiscoverySubgraphNode({ data, selected }: NodeProps) {
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
    <div className={nodeClass(!!selected, "subgraph", d)} style={mergeNodeCardStyle(d, { position: "relative" })}>
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
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || "Subgraph"} kind="Subgraph" />
      <div className="discovery-flow-node__meta">
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
export function DiscoverySubflowGraphInNode({ data, selected, parentId }: NodeProps) {
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
      className={nodeClass(!!selected, "subflow-graph", d)}
      style={mergeNodeCardStyle(d, { position: "relative", minWidth: 120, minHeight: 56 })}
    >
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || "Graph inputs"} kind="Graph inputs" />
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
export function DiscoverySubflowGraphOutNode({ data, selected, parentId }: NodeProps) {
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
      className={nodeClass(!!selected, "subflow-graph", d)}
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
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || "Graph outputs"} kind="Graph outputs" />
    </div>
  );
}

export function DiscoveryStartNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "start", d)} style={mergeNodeCardStyle(d)}>
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || "Start"} kind="Start" />
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}

export function DiscoveryEndNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "end", d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || "End"} kind="End" />
    </div>
  );
}

export function DiscoverySourceViewNode({ data, selected }: NodeProps) {
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "source", d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || "Source view"} kind="Source view" />
      {d.ref?.view_external_id && (
        <div className="discovery-flow-node__meta">{String(d.ref.view_external_id)}</div>
      )}
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}

export function DiscoveryExtractionNode({ data, selected }: NodeProps) {
  const { t } = useAppSettings();
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const handler = d.handler_id ? String(d.handler_id) : "—";
  const kind = t("flow.structuralExtraction");
  const h = useExtractionAliasingHandleLayout();
  const valStyle: CSSProperties = {
    ...portHandleStyle(h.validationOut, 0, 1),
    zIndex: 5,
  };
  return (
    <div className={nodeClass(!!selected, "extract", d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.dataIn} id="in" style={{ zIndex: 5 }} />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || kind} kind={kind} />
      <div className="discovery-flow-node__meta">{handler}</div>
      <Handle type="source" position={h.dataOut} id="out" style={{ zIndex: 5 }} />
      <Handle type="source" position={h.validationOut} id="validation" style={valStyle} />
    </div>
  );
}

export function DiscoveryAliasingNode({ data, selected }: NodeProps) {
  const { t } = useAppSettings();
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const handler = d.handler_id ? String(d.handler_id) : "—";
  const kind = t("flow.structuralAliasing");
  const h = useExtractionAliasingHandleLayout();
  const valStyle: CSSProperties = {
    ...portHandleStyle(h.validationOut, 0, 1),
    zIndex: 5,
  };
  return (
    <div className={nodeClass(!!selected, "alias", d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.dataIn} id="in" style={{ zIndex: 5 }} />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || kind} kind={kind} />
      <div className="discovery-flow-node__meta">{handler}</div>
      <Handle type="source" position={h.dataOut} id="out" style={{ zIndex: 5 }} />
      <Handle type="source" position={h.validationOut} id="validation" style={valStyle} />
    </div>
  );
}

function DiscoveryDataStageNode({
  data,
  selected,
  variant,
  badge: _badge,
  titleKey,
}: NodeProps & {
  variant: string;
  badge: string;
  titleKey: MessageKey;
}) {
  const { t } = useAppSettings();
  const title = t(titleKey);
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
    <div className={nodeClass(!!selected, variant, d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.dataIn} id="in" style={{ zIndex: 5 }} />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || title} kind={title} />
      <div className="discovery-flow-node__meta">{meta}</div>
      <Handle type="source" position={h.dataOut} id="out" style={{ zIndex: 5 }} />
    </div>
  );
}

export function DiscoveryViewQueryNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="extract"
      badge="view_q"
      titleKey="flow.discoveryViewQuery"
    />
  );
}

export function DiscoveryRawQueryNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="ref-index"
      badge="raw_q"
      titleKey="flow.discoveryRawQuery"
    />
  );
}

export function DiscoveryClassicQueryNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="source"
      badge="classic_q"
      titleKey="flow.discoveryClassicQuery"
    />
  );
}

export function DiscoverySqlQueryNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="extract"
      badge="sql_q"
      titleKey="flow.discoverySqlQuery"
    />
  );
}

export function DiscoveryTransformNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="alias"
      badge="transform"
      titleKey="flow.discoveryTransform"
    />
  );
}

export function DiscoveryMergeNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="alias"
      badge="merge"
      titleKey="flow.discoveryMerge"
    />
  );
}

export function DiscoveryJoinNode({ data, selected }: NodeProps) {
  const { t } = useAppSettings();
  const kind = t("flow.discoveryJoin");
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
    <div className={nodeClass(!!selected, "alias", d)} style={mergeNodeCardStyle(d)}>
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
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || kind} kind={kind} />
      <div className="discovery-flow-node__meta">{meta}</div>
      <Handle type="source" position={h.dataOut} id="out" style={{ zIndex: 5 }} />
    </div>
  );
}

/** Executable ``fn_dm_filter`` — row-level cohort filter DSL. */
export function DiscoveryInstanceFilterNode({ data, selected }: NodeProps) {
  const { t } = useAppSettings();
  const kind = t("flow.discoveryInstanceFilter");
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
    <div className={nodeClass(!!selected, "validation", d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" style={{ zIndex: 5 }} />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || kind} kind={kind} />
      <div className="discovery-flow-node__meta">{meta}</div>
      <Handle type="source" position={h.out} id="out" style={{ zIndex: 5 }} />
    </div>
  );
}

/** Executable ``fn_dm_confidence_filter`` — prune values by ``{value_field}_confidence``. */
export function DiscoveryConfidenceFilterNode({ data, selected }: NodeProps) {
  const { t } = useAppSettings();
  const kind = t("flow.discoveryConfidenceFilter");
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
    <div className={nodeClass(!!selected, "validation", d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" style={{ zIndex: 5 }} />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || kind} kind={kind} />
      <div className="discovery-flow-node__meta">{meta}</div>
      <Handle type="source" position={h.out} id="out" style={{ zIndex: 5 }} />
    </div>
  );
}

/** Executable ``fn_dm_validate`` — data in / out (match-definition layout uses the same ``out`` as other stages). */
export function DiscoveryValidateNode({ data, selected }: NodeProps) {
  const { t } = useAppSettings();
  const kind = t("flow.discoveryValidate");
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
    <div className={nodeClass(!!selected, "validation", d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" style={{ zIndex: 5 }} />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || kind} kind={kind} />
      <div className="discovery-flow-node__meta">{meta}</div>
      <Handle type="source" position={h.out} id="out" style={{ zIndex: 5 }} />
    </div>
  );
}

export function DiscoveryViewSaveNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="persist-alias"
      badge="view_sv"
      titleKey="flow.discoveryViewSave"
    />
  );
}

export function DiscoveryRawSaveNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="persist-alias"
      badge="raw_sv"
      titleKey="flow.discoveryRawSave"
    />
  );
}

export function DiscoveryClassicSaveNode(props: NodeProps) {
  return (
    <DiscoveryDataStageNode
      {...props}
      variant="persist-alias"
      badge="classic_sv"
      titleKey="flow.discoveryClassicSave"
    />
  );
}

const validationNodeTargetStyle: CSSProperties = {
  left: "50%",
  transform: "translateX(-50%)",
  zIndex: 5,
};

export function DiscoveryValidationRuleNode({ data, selected }: NodeProps) {
  const { t } = useAppSettings();
  const kind = t("flow.discoveryValidationRule");
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const ctx = d.validation_rule_context ? String(d.validation_rule_context) : "—";
  const ruleName = d.validation_rule_name ? String(d.validation_rule_name) : "—";
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "validation-rule", d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={Position.Top} id="in" style={validationNodeTargetStyle} />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || kind} kind={kind} />
      <div className="discovery-flow-node__meta">
        {ctx} · {ruleName}
      </div>
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}

/** Legacy canvas kind ``alias_persistence`` — displays the DM apply function used today (`fn_dm_view_save`). */
export function DiscoveryAliasPersistenceNode({ data, selected }: NodeProps) {
  const { t } = useAppSettings();
  const label = t("flow.structuralAliasPersistence");
  const kind = t("flow.discoveryViewSave");
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "persist-alias", d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || label} kind={kind} />
      <div className="discovery-flow-node__meta">fn_dm_view_save</div>
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}

/** fn_dm_inverted_index — inverted RAW index; discovery path uses predecessor task snapshots (IR payload). */
export function DiscoveryInvertedIndexNode({ data, selected }: NodeProps) {
  const { t } = useAppSettings();
  const kind = t("flow.discoveryInvertedIndex");
  const d = (data ?? {}) as WorkflowCanvasNodeData;
  const h = useDataHandles();
  return (
    <div className={nodeClass(!!selected, "ref-index", d)} style={mergeNodeCardStyle(d)}>
      <Handle type="target" position={h.in} id="in" />
      <FlowNodeDisabledBadge data={d} />
      <FlowNodeLabelKind label={d.label || kind} kind={kind} />
      <div className="discovery-flow-node__meta">fn_dm_inverted_index</div>
      <Handle type="source" position={h.out} id="out" />
    </div>
  );
}
