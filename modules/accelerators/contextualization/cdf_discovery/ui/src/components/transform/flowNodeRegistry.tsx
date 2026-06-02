import type { CSSProperties, ReactNode } from "react";
import { useEffect, useState } from "react";
import type { NodeProps } from "@xyflow/react";
import { Handle, NodeResizer, Position } from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import type { TransformCanvasHandleOrientation, TransformCanvasNodeKind } from "../../types/transformCanvas";
import { kindToRfType } from "../../types/transformCanvas";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useFlowHandleOrientation } from "./FlowHandleOrientationContext";
import { mergeEtlNodeCardStyle } from "./flowNodeAccent";
import { resolveEtlNodeAccentColor } from "../../utils/etlPaletteGroupColors";
import {
  canvasNodeProgressPercent,
  canvasNodeProgressVisible,
  formatNodeRunElapsedMs,
  resolveNodeRunElapsedMs,
  type CanvasNodeRunProgress,
} from "./canvasNodeRunProgress";
import {
  ETL_NODE_MAX_HEIGHT,
  ETL_NODE_MIN_HEIGHT,
  ETL_NODE_MIN_WIDTH,
  etlDualInputMinSize,
  maxEtlNodeWidth,
} from "./etlFlowNodeSizing";
import { etlFlowNodeCanvasDescription } from "./etlFlowNodeDescription";
import {
  INPUT_A_LABEL_CONFIG_KEY,
  INPUT_B_LABEL_CONFIG_KEY,
  INPUT_LABEL_CONFIG_KEY,
  OUTPUT_LABEL_CONFIG_KEY,
  resolveConnectorLabel,
  resolveDualInputConnectorLabel,
} from "../../utils/dualInputConnectorLabels";
import { canvasNodeDisplayLabel, canvasNodeKindLabel } from "../../utils/canvasNodeKindLabel";

function useDataHandles(data: {
  flowHandleOrientation?: TransformCanvasHandleOrientation | undefined;
  [key: string]: unknown;
}): { in: Position; out: Position; key: TransformCanvasHandleOrientation } {
  const contextOrientation = useFlowHandleOrientation();
  const o = data.flowHandleOrientation ?? contextOrientation;
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
    nodeRunProgress?: CanvasNodeRunProgress;
    nodeRunExecuting?: boolean;
  };
};

function EtlNodeRunProgressBar({
  progress,
  isExecuting = false,
}: {
  progress: CanvasNodeRunProgress;
  isExecuting?: boolean;
}) {
  const { t } = useAppSettings();
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    if (!isExecuting || progress.elapsedMs != null) return;
    const id = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [isExecuting, progress.elapsedMs, progress.startedAtMs]);

  const pct = canvasNodeProgressPercent(progress);
  const indeterminate = pct == null && progress.current > 0;
  const countLabel =
    progress.total != null && progress.total > 0
      ? t("run.nodeProgressCount", { current: progress.current, total: progress.total })
      : progress.current > 0
        ? t("run.nodeProgressCountIndeterminate", { current: progress.current })
        : "";
  const elapsedMs = resolveNodeRunElapsedMs(progress, nowMs);
  const elapsedLabel = elapsedMs != null ? formatNodeRunElapsedMs(elapsedMs) : "";
  const ariaDetail = [elapsedLabel, countLabel].filter(Boolean).join(" · ");
  const ariaLabel = ariaDetail
    ? t("run.nodeProgressAria", { detail: ariaDetail })
    : t("run.nodeProgressAriaIndeterminate");

  return (
    <div className="etl-flow-node__progress" aria-label={ariaLabel}>
      <div
        className={`etl-flow-node__progress-track${indeterminate ? " etl-flow-node__progress-track--indeterminate" : ""}`}
      >
        <div
          className="etl-flow-node__progress-fill"
          style={pct != null ? { width: `${pct}%` } : undefined}
        />
      </div>
      {elapsedLabel || countLabel ? (
        <div className="etl-flow-node__progress-footer">
          {elapsedLabel ? (
            <span
              className="etl-flow-node__progress-elapsed"
              aria-label={t("run.nodeProgressElapsedAria", { elapsed: elapsedLabel })}
            >
              {elapsedLabel}
            </span>
          ) : (
            <span className="etl-flow-node__progress-elapsed" aria-hidden="true" />
          )}
          {countLabel ? <span className="etl-flow-node__progress-label">{countLabel}</span> : null}
        </div>
      ) : null}
    </div>
  );
}

function EtlNodeBodyContent({
  label,
}: {
  label: string;
}) {
  return (
    <>
      <span className="etl-flow-node__label">{label}</span>
    </>
  );
}

function EtlNodeConnectorBar({
  inputLabel,
  outputLabel,
  showInput = true,
  showOutput = true,
  inputHandle = null,
  outputHandle = null,
}: {
  inputLabel: string;
  outputLabel: string;
  showInput?: boolean;
  showOutput?: boolean;
  inputHandle?: ReactNode;
  outputHandle?: ReactNode;
}) {
  return (
    <div className="etl-flow-node__connector-bar" aria-hidden="true">
      <span className="etl-flow-node__connector-slot etl-flow-node__connector-slot--input">
        <span className="etl-flow-node__connector-label">
          {showInput ? inputLabel : ""}
        </span>
        {showInput ? inputHandle : null}
      </span>
      <span className="etl-flow-node__connector-slot etl-flow-node__connector-slot--output">
        <span className="etl-flow-node__connector-label">
          {showOutput ? outputLabel : ""}
        </span>
        {showOutput ? outputHandle : null}
      </span>
    </div>
  );
}

function EtlNodeResizer({
  selected,
  enabled,
  minWidth = ETL_NODE_MIN_WIDTH,
  minHeight = ETL_NODE_MIN_HEIGHT,
  maxWidth,
}: {
  selected: boolean;
  enabled: boolean;
  minWidth?: number;
  minHeight?: number;
  maxWidth?: number;
}) {
  if (!enabled) return null;
  return (
    <NodeResizer
      isVisible={selected}
      minWidth={minWidth}
      minHeight={minHeight}
      maxWidth={maxWidth}
      maxHeight={ETL_NODE_MAX_HEIGHT}
      lineClassName="etl-flow-node__resize-line"
      handleClassName="etl-flow-node__resize-handle"
    />
  );
}

const DUAL_INPUT_HANDLE_SLOTS = 2;

function dualInputHandlePercent(index: number, total = DUAL_INPUT_HANDLE_SLOTS): number {
  return total <= 1 ? 50 : ((index + 1) / (total + 1)) * 100;
}

function joinHandleStyle(inPos: Position, index: number, total: number): CSSProperties {
  const pct = dualInputHandlePercent(index, total);
  if (inPos === Position.Top || inPos === Position.Bottom) {
    return { left: `${pct}%`, transform: "translateX(-50%)" };
  }
  return { top: `${pct}%`, transform: "translateY(-50%)" };
}

function dualInputHandleLabelOffsetStyle(index: number): CSSProperties {
  return { "--etl-dual-handle-pct": `${dualInputHandlePercent(index)}%` } as CSSProperties;
}

function DualInputConnectorLabels({
  orientation,
  leftLabel,
  rightLabel,
}: {
  orientation: TransformCanvasHandleOrientation;
  leftLabel: string;
  rightLabel: string;
}) {
  return (
    <div
      className={`etl-flow-node__connector-labels etl-flow-node__connector-labels--in-${orientation}`}
      aria-hidden="true"
    >
      <span
        className="etl-flow-handle-label etl-flow-handle-label--input-a"
        style={dualInputHandleLabelOffsetStyle(0)}
      >
        {leftLabel}
      </span>
      <span
        className="etl-flow-handle-label etl-flow-handle-label--input-b"
        style={dualInputHandleLabelOffsetStyle(1)}
      >
        {rightLabel}
      </span>
    </div>
  );
}

function DualInputFlowNode({
  data,
  selected,
  kind,
  kindLabelKey,
  leftHandleId,
  rightHandleId,
  leftLabelKey,
  rightLabelKey,
  showGenericInputLabel = true,
}: EtlNodeProps & {
  kind: TransformCanvasNodeKind;
  kindLabelKey: MessageKey;
  leftHandleId: string;
  rightHandleId: string;
  leftLabelKey: MessageKey;
  rightLabelKey: MessageKey;
  showGenericInputLabel?: boolean;
}) {
  const { t } = useAppSettings();
  const handles = useDataHandles(data);
  const kindLabel = t(kindLabelKey);
  const label = data.label?.trim() || canvasNodeDisplayLabel(data, kind, t);
  const description = etlFlowNodeCanvasDescription(kind, data as Record<string, unknown>);
  const disabled = data.canvas_node_enabled === false;
  const resizeEnabled = data.canvas_resize_enabled !== false;
  const accent = resolveEtlNodeAccentColor(kind, data as Record<string, unknown>);
  const customStyle = mergeEtlNodeCardStyle(data as Record<string, unknown>);
  const bodyStyle = {
    borderLeftColor: customStyle?.borderLeftColor ?? accent,
    ...(customStyle?.backgroundColor ? { backgroundColor: customStyle.backgroundColor } : {}),
    ...(customStyle?.borderLeftWidth ? { borderLeftWidth: customStyle.borderLeftWidth } : {}),
    ...(customStyle?.borderLeftStyle ? { borderLeftStyle: customStyle.borderLeftStyle } : {}),
  };
  const showProgress = canvasNodeProgressVisible(data.nodeRunProgress);
  const config = (data.config ?? {}) as Record<string, unknown>;
  const leftLabel = resolveDualInputConnectorLabel(config, INPUT_A_LABEL_CONFIG_KEY, leftLabelKey, t);
  const rightLabel = resolveDualInputConnectorLabel(config, INPUT_B_LABEL_CONFIG_KEY, rightLabelKey, t);
  const inputLabel = resolveConnectorLabel(config, INPUT_LABEL_CONFIG_KEY, "wfViewer.inputConnector", t);
  const outputLabel = resolveConnectorLabel(config, OUTPUT_LABEL_CONFIG_KEY, "wfViewer.outputConnector", t);
  const dualMin = etlDualInputMinSize();
  const isFanoutPlanner = kind === "workflow_fanout_plan";
  const inlineConnectorHandles = handles.key === "lr";

  return (
    <div
      className={`etl-flow-node etl-flow-node--dual-input etl-flow-node--in-${handles.key} etl-flow-node--${kind} etl-flow-node--resizable${selected ? " etl-flow-node--selected" : ""}${disabled ? " etl-flow-node--disabled" : ""}${showProgress ? " etl-flow-node--has-progress" : ""}`}
    >
      <EtlNodeResizer
        selected={Boolean(selected)}
        enabled={resizeEnabled}
        minWidth={dualMin.width}
        minHeight={dualMin.height}
        maxWidth={maxEtlNodeWidth(kind)}
      />
      {!inlineConnectorHandles ? (
        <>
          <Handle
            key={`in-left-${handles.key}`}
            type="target"
            id={leftHandleId}
            position={handles.in}
            className="etl-flow-handle etl-flow-handle--input-a"
            style={joinHandleStyle(handles.in, 0, DUAL_INPUT_HANDLE_SLOTS)}
            aria-label={leftLabel}
            title={leftLabel}
          />
          <Handle
            key={`in-right-${handles.key}`}
            type="target"
            id={rightHandleId}
            position={handles.in}
            className="etl-flow-handle etl-flow-handle--input-b"
            style={joinHandleStyle(handles.in, 1, DUAL_INPUT_HANDLE_SLOTS)}
            aria-label={rightLabel}
            title={rightLabel}
          />
        </>
      ) : null}
      <div className="etl-flow-node__body" style={bodyStyle}>
        {!isFanoutPlanner && !inlineConnectorHandles ? (
          <DualInputConnectorLabels orientation={handles.key} leftLabel={leftLabel} rightLabel={rightLabel} />
        ) : null}
        <div className="etl-flow-node__body-main">
          <EtlNodeBodyContent label={label} />
          <span className="etl-flow-node__kind">{kindLabel}</span>
          {inlineConnectorHandles ? (
            <div className="etl-flow-node__fanout-connector-row" aria-hidden="true">
              <div className="etl-flow-node__fanout-input-stack">
                <span className="etl-flow-node__connector-label etl-flow-node__fanout-input-label">
                  {leftLabel}
                  <Handle
                    key={`in-inline-left-${handles.key}`}
                    type="target"
                    id={leftHandleId}
                    position={handles.in}
                    className="etl-flow-handle etl-flow-handle--input-inline etl-flow-handle--input-inline-a"
                  />
                </span>
                <span className="etl-flow-node__connector-label etl-flow-node__fanout-input-label">
                  {rightLabel}
                  <Handle
                    key={`in-inline-right-${handles.key}`}
                    type="target"
                    id={rightHandleId}
                    position={handles.in}
                    className="etl-flow-handle etl-flow-handle--input-inline etl-flow-handle--input-inline-b"
                  />
                </span>
              </div>
              <span className="etl-flow-node__connector-label etl-flow-node__fanout-output-label">
                {outputLabel}
                <Handle
                  key={`out-inline-${handles.key}`}
                  type="source"
                  id="out"
                  position={handles.out}
                  className="etl-flow-handle etl-flow-handle--output-inline"
                />
              </span>
            </div>
          ) : isFanoutPlanner ? (
            <div className="etl-flow-node__fanout-connector-row" aria-hidden="true">
              <div className="etl-flow-node__fanout-input-stack">
                <span className="etl-flow-node__connector-label etl-flow-node__fanout-input-label">
                  {leftLabel}
                </span>
                <span className="etl-flow-node__connector-label etl-flow-node__fanout-input-label">
                  {rightLabel}
                </span>
              </div>
              <span className="etl-flow-node__connector-label etl-flow-node__fanout-output-label">
                {outputLabel}
              </span>
            </div>
          ) : (
            <EtlNodeConnectorBar
              inputLabel={inputLabel}
              outputLabel={outputLabel}
              showInput={showGenericInputLabel}
            />
          )}
          {description ? <span className="etl-flow-node__description">{description}</span> : null}
        </div>
        {showProgress && data.nodeRunProgress ? (
          <EtlNodeRunProgressBar progress={data.nodeRunProgress} isExecuting={data.nodeRunExecuting} />
        ) : null}
      </div>
      {!inlineConnectorHandles ? (
        <Handle
          key={`out-${handles.key}`}
          type="source"
          id="out"
          position={handles.out}
          className="etl-flow-handle"
        />
      ) : null}
    </div>
  );
}

function FanoutPlanFlowNode(props: EtlNodeProps) {
  const cfg = (props.data.config ?? {}) as Record<string, unknown>;
  const profile = String(cfg.fanout_profile ?? "file_annotation");
  const labels =
    profile === "file_annotation"
      ? {
          left: "transform.fanoutPlan.handle.inputA.context" as MessageKey,
          right: "transform.fanoutPlan.handle.inputB.files" as MessageKey,
        }
      : {
          left: "transform.fanoutPlan.handle.inputA" as MessageKey,
          right: "transform.fanoutPlan.handle.inputB" as MessageKey,
        };
  return (
    <DualInputFlowNode
      {...props}
      kind="workflow_fanout_plan"
      kindLabelKey="transform.palette.workflow_fanout_plan"
      leftHandleId="in__input_a"
      rightHandleId="in__input_b"
      leftLabelKey={labels.left}
      rightLabelKey={labels.right}
      showGenericInputLabel={false}
    />
  );
}

function FileAnnotationFlowNode(props: EtlNodeProps) {
  return (
    <DualInputFlowNode
      {...props}
      kind="file_annotation"
      kindLabelKey="transform.palette.file_annotation"
      leftHandleId="in__entities"
      rightHandleId="in__files"
      leftLabelKey="transform.fileAnnotation.handle.entities"
      rightLabelKey="transform.fileAnnotation.handle.files"
    />
  );
}

function JoinFlowNode({ data, selected }: EtlNodeProps) {
  const { t } = useAppSettings();
  const handles = useDataHandles(data);
  const kind = "join" as const;
  const kindLabel = canvasNodeKindLabel(kind, t);
  const label = data.label?.trim() || canvasNodeDisplayLabel(data, kind, t);
  const description = etlFlowNodeCanvasDescription(kind, data as Record<string, unknown>);
  const disabled = data.canvas_node_enabled === false;
  const resizeEnabled = data.canvas_resize_enabled !== false;
  const accent = resolveEtlNodeAccentColor(kind, data as Record<string, unknown>);
  const customStyle = mergeEtlNodeCardStyle(data as Record<string, unknown>);
  const bodyStyle = {
    borderLeftColor: customStyle?.borderLeftColor ?? accent,
    ...(customStyle?.backgroundColor ? { backgroundColor: customStyle.backgroundColor } : {}),
    ...(customStyle?.borderLeftWidth ? { borderLeftWidth: customStyle.borderLeftWidth } : {}),
    ...(customStyle?.borderLeftStyle ? { borderLeftStyle: customStyle.borderLeftStyle } : {}),
  };
  const showProgress = canvasNodeProgressVisible(data.nodeRunProgress);
  const config = (data.config ?? {}) as Record<string, unknown>;
  const inputLabel = resolveConnectorLabel(config, INPUT_LABEL_CONFIG_KEY, "wfViewer.inputConnector", t);
  const outputLabel = resolveConnectorLabel(config, OUTPUT_LABEL_CONFIG_KEY, "wfViewer.outputConnector", t);
  const inlineConnectorHandles = handles.key === "lr";

  return (
    <div
      className={`etl-flow-node etl-flow-node--join etl-flow-node--resizable${selected ? " etl-flow-node--selected" : ""}${disabled ? " etl-flow-node--disabled" : ""}`}
    >
      <EtlNodeResizer selected={Boolean(selected)} enabled={resizeEnabled} maxWidth={maxEtlNodeWidth(kind)} />
      {!inlineConnectorHandles ? (
        <>
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
        </>
      ) : null}
      <div className="etl-flow-node__body" style={bodyStyle}>
        <EtlNodeBodyContent label={label} />
        <span className="etl-flow-node__kind">{kindLabel}</span>
        {inlineConnectorHandles ? (
          <div className="etl-flow-node__fanout-connector-row" aria-hidden="true">
            <div className="etl-flow-node__fanout-input-stack">
              <span className="etl-flow-node__connector-label etl-flow-node__fanout-input-label">
                L
                <Handle
                  key={`in-inline-left-${handles.key}`}
                  type="target"
                  id="in__left"
                  position={handles.in}
                  className="etl-flow-handle etl-flow-handle--input-inline etl-flow-handle--input-inline-a"
                />
              </span>
              <span className="etl-flow-node__connector-label etl-flow-node__fanout-input-label">
                R
                <Handle
                  key={`in-inline-right-${handles.key}`}
                  type="target"
                  id="in__right"
                  position={handles.in}
                  className="etl-flow-handle etl-flow-handle--input-inline etl-flow-handle--input-inline-b"
                />
              </span>
            </div>
            <span className="etl-flow-node__connector-label etl-flow-node__fanout-output-label">
              {outputLabel}
              <Handle
                key={`out-inline-${handles.key}`}
                type="source"
                id="out"
                position={handles.out}
                className="etl-flow-handle etl-flow-handle--output-inline"
              />
            </span>
          </div>
        ) : (
          <EtlNodeConnectorBar inputLabel={inputLabel} outputLabel={outputLabel} />
        )}
        {description ? <span className="etl-flow-node__description">{description}</span> : null}
        {showProgress && data.nodeRunProgress ? (
          <EtlNodeRunProgressBar progress={data.nodeRunProgress} isExecuting={data.nodeRunExecuting} />
        ) : null}
      </div>
      {!inlineConnectorHandles ? (
        <Handle
          key={`out-${handles.key}`}
          type="source"
          id="out"
          position={handles.out}
          className="etl-flow-handle"
        />
      ) : null}
    </div>
  );
}

function NodePreviewFlowNode(props: EtlNodeProps) {
  return <EtlFlowNode {...props} data={{ ...props.data, kind: "node_preview" }} />;
}

function EtlFlowNode({ data, selected }: EtlNodeProps) {
  const { t } = useAppSettings();
  const handles = useDataHandles(data);
  const kind = data.kind ?? "transform";
  const kindLabel = canvasNodeKindLabel(kind, t);
  const label = data.label?.trim() || canvasNodeDisplayLabel(data, kind, t);
  const description =
    kind === "dynamic_fanout"
      ? ""
      : etlFlowNodeCanvasDescription(kind, data as Record<string, unknown>);
  const disabled = data.canvas_node_enabled === false;
  const resizeEnabled = data.canvas_resize_enabled !== false;
  const accent = resolveEtlNodeAccentColor(kind, data as Record<string, unknown>);
  const customStyle = mergeEtlNodeCardStyle(data as Record<string, unknown>);
  const bodyStyle = {
    borderLeftColor: customStyle?.borderLeftColor ?? accent,
    ...(customStyle?.backgroundColor ? { backgroundColor: customStyle.backgroundColor } : {}),
    ...(customStyle?.borderLeftWidth ? { borderLeftWidth: customStyle.borderLeftWidth } : {}),
    ...(customStyle?.borderLeftStyle ? { borderLeftStyle: customStyle.borderLeftStyle } : {}),
  };
  const showProgress = canvasNodeProgressVisible(data.nodeRunProgress);
  const config = (data.config ?? {}) as Record<string, unknown>;
  const inputLabel =
    kind === "dynamic_fanout"
      ? resolveConnectorLabel(config, INPUT_LABEL_CONFIG_KEY, "transform.dynamicFanout.inputConnector", t)
      : resolveConnectorLabel(config, INPUT_LABEL_CONFIG_KEY, "wfViewer.inputConnector", t);
  const outputLabel = resolveConnectorLabel(config, OUTPUT_LABEL_CONFIG_KEY, "wfViewer.outputConnector", t);
  const showInput = kind !== "start";
  const showOutput = kind !== "end";
  const inlineConnectorHandles = handles.key === "lr";

  return (
    <div
      className={`etl-flow-node etl-flow-node--${kind} etl-flow-node--resizable${kind === "node_preview" ? " etl-flow-node--preview" : ""}${selected ? " etl-flow-node--selected" : ""}${disabled ? " etl-flow-node--disabled" : ""}`}
    >
      <EtlNodeResizer selected={Boolean(selected)} enabled={resizeEnabled} maxWidth={maxEtlNodeWidth(kind)} />
      {!inlineConnectorHandles && kind !== "start" && (
        <Handle
          key={`in-${handles.key}`}
          type="target"
          id="in"
          position={handles.in}
          className="etl-flow-handle etl-flow-handle--input"
        />
      )}
      <div className="etl-flow-node__body" style={bodyStyle}>
        <EtlNodeBodyContent label={label} />
        <span className="etl-flow-node__kind">{kindLabel}</span>
        <EtlNodeConnectorBar
          inputLabel={inputLabel}
          outputLabel={outputLabel}
          showInput={showInput}
          showOutput={showOutput}
          inputHandle={
            inlineConnectorHandles && showInput ? (
              <Handle
                key={`in-inline-${handles.key}`}
                type="target"
                id="in"
                position={handles.in}
                className="etl-flow-handle etl-flow-handle--input-inline"
              />
            ) : null
          }
          outputHandle={
            inlineConnectorHandles && showOutput ? (
              <Handle
                key={`out-inline-${handles.key}`}
                type="source"
                id="out"
                position={handles.out}
                className="etl-flow-handle etl-flow-handle--output-inline"
              />
            ) : null
          }
        />
        {description ? <span className="etl-flow-node__description">{description}</span> : null}
        {showProgress && data.nodeRunProgress ? (
          <EtlNodeRunProgressBar progress={data.nodeRunProgress} isExecuting={data.nodeRunExecuting} />
        ) : null}
      </div>
      {!inlineConnectorHandles && kind !== "end" && (
        <Handle
          key={`out-${handles.key}`}
          type="source"
          id="out"
          position={handles.out}
          className="etl-flow-handle etl-flow-handle--output"
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
  etlQueryRecords: makeNodeComponent("query_records"),
  etlScore: makeNodeComponent("score"),
  etlTransform: makeNodeComponent("transform"),
  etlFilter: makeNodeComponent("filter"),
  etlJsonMapping: makeNodeComponent("json_mapping"),
  etlJoin: JoinFlowNode,
  etlMerge: makeNodeComponent("merge"),
  etlBuildIndex: makeNodeComponent("build_index"),
  etlSaveView: makeNodeComponent("save_view"),
  etlSaveRaw: makeNodeComponent("save_raw"),
  etlSaveClassic: makeNodeComponent("save_classic"),
  etlSaveRecords: makeNodeComponent("save_records"),
  etlSaveStream: makeNodeComponent("save_stream"),
  etlRawCleanup: makeNodeComponent("raw_cleanup"),
  etlSparkTransform: makeNodeComponent("spark_transform"),
  etlTransformationRef: makeNodeComponent("transformation_ref"),
  etlFunctionRef: makeNodeComponent("function_ref"),
  etlDynamicFanout: makeNodeComponent("dynamic_fanout"),
  etlWorkflowFanoutPlan: FanoutPlanFlowNode,
  etlFileAnnotation: FileAnnotationFlowNode,
  etlSubworkflow: makeNodeComponent("subworkflow"),
  etlSimulation: makeNodeComponent("simulation"),
  etlCdfTask: makeNodeComponent("cdf_task"),
  etlSubgraph: makeNodeComponent("subgraph"),
  etlNodePreview: NodePreviewFlowNode,
};

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
