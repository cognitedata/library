import { useCallback, useEffect, useRef, useState, type Dispatch, type SetStateAction } from "react";
import { createPortal } from "react-dom";
import { useDebouncedNodePatch } from "../../hooks/useDebouncedNodePatch";
import { useModalDialog } from "../../hooks/useModalDialog";
import { DeferredCommitInput, DeferredCommitTextarea } from "../query/DeferredCommitTextField";
import type { Node, Edge } from "@xyflow/react";
import { fetchTransformWorkflowBuildPairing, type TransformWorkflowBuildPairing } from "../../api";
import type { MessageKey } from "../../i18n";
import { rfTypeToKind, type TransformCanvasNodeKind } from "../../types/transformCanvas";
import { EtlNodeConfigFields } from "./EtlNodeConfigFields";
import { EtlJoinNodeConfigFields } from "./EtlJoinNodeConfigFields";
import { EtlMergeNodeConfigFields } from "./EtlMergeNodeConfigFields";
import { EtlBuildIndexNodeConfigFields } from "./EtlBuildIndexNodeConfigFields";
import { EtlTransformNodeConfigFields } from "./EtlTransformNodeConfigFields";
import { EtlFileAnnotationNodeConfigFields } from "./EtlFileAnnotationNodeConfigFields";
import { EtlWorkflowFanoutPlanNodeConfigFields } from "./EtlWorkflowFanoutPlanNodeConfigFields";
import { EtlDynamicFanoutNodeConfigFields } from "./EtlDynamicFanoutNodeConfigFields";
import { EtlScoreNodeConfigFields } from "./EtlScoreNodeConfigFields";
import { EtlSaveNodeConfigFields } from "./EtlSaveNodeConfigFields";
import { EtlOrchestrationNodeConfigFields } from "./EtlOrchestrationNodeConfigFields";
import { EtlStartNodeConfigFields } from "./EtlStartNodeConfigFields";
import { EtlRawCleanupNodeConfigFields } from "./EtlRawCleanupNodeConfigFields";
import { EtlSubgraphNodeConfigFields } from "./EtlSubgraphNodeConfigFields";
import { SparkTransformConfigFields } from "./SparkTransformConfigFields";
import { TransformationRefConfigFields } from "./TransformationRefConfigFields";
import { ViewQueryConfigFields } from "../query/ViewQueryConfigFields";
import { RawQueryConfigFields } from "../query/RawQueryConfigFields";
import { ClassicQueryConfigFields } from "../query/ClassicQueryConfigFields";
import { SqlQueryConfigFields } from "../query/SqlQueryConfigFields";
import { FilterNodeConfigFields } from "../query/FilterNodeConfigFields";
import { RecordsQueryConfigFields } from "../query/RecordsQueryConfigFields";
import { RecordsSaveConfigFields } from "./RecordsSaveConfigFields";
import { StreamSaveConfigFields } from "./StreamSaveConfigFields";
import { JsonMappingNodeConfigFields } from "./JsonMappingNodeConfigFields";
import { ConnectorLabelFields } from "./ConnectorLabelFields";
import type { JsonObject } from "../../types/jsonConfig";
import { canvasNodeKindMessageKey } from "../../utils/canvasNodeKindLabel";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  node: Node | null;
  onClose: () => void;
  onPatchNode: (nodeId: string, data: Record<string, unknown>) => void;
  t: TFn;
  pipelineId?: string;
  schemaSpace?: string;
  flowNodes?: readonly Node[];
  flowEdges?: readonly Edge[];
  readOnly?: boolean;
};

function titleKey(kind: TransformCanvasNodeKind): MessageKey {
  switch (kind) {
    case "query_view":
    case "query_raw":
    case "query_classic":
    case "query_sql":
    case "query_records":
      return "transform.nodeEditor.titleQuery";
    case "transform":
      return "transform.nodeEditor.titleTransform";
    case "filter":
      return "transform.nodeEditor.titleFilter";
    case "score":
      return "transform.nodeEditor.titleScore";
    case "join":
      return "transform.nodeEditor.titleJoin";
    case "merge":
      return "transform.nodeEditor.titleMerge";
    case "build_index":
      return "transform.nodeEditor.titleBuildIndex";
    case "json_mapping":
    case "spark_transform":
    case "transformation_ref":
    case "function_ref":
    case "subworkflow":
    case "simulation":
    case "cdf_task":
    case "dynamic_fanout":
      return canvasNodeKindMessageKey(kind);
    case "save_view":
    case "save_raw":
    case "save_classic":
    case "save_records":
    case "save_stream":
      return "transform.nodeEditor.titleSave";
    case "file_annotation":
      return "transform.nodeEditor.titleFileAnnotation";
    case "workflow_fanout_plan":
      return "transform.nodeEditor.titleFanoutPlan";
    case "raw_cleanup":
      return "transform.nodeEditor.titleRawCleanup";
    case "subgraph":
      return "transform.nodeEditor.titleSubgraph";
    case "start":
    case "end":
      return "transform.nodeEditor.titleBoundary";
    default:
      return "transform.nodeEditor.title";
  }
}

function QueryEditorBody({
  kind,
  config,
  fieldKey,
  schemaSpace,
  onChange,
}: {
  kind: TransformCanvasNodeKind;
  config: JsonObject;
  fieldKey: string;
  schemaSpace?: string;
  onChange: (next: JsonObject) => void;
}) {
  switch (kind) {
    case "query_view":
      return (
        <ViewQueryConfigFields
          value={config}
          onChange={onChange}
          fieldKey={fieldKey}
          schema_space={schemaSpace}
        />
      );
    case "query_raw":
      return <RawQueryConfigFields value={config} onChange={onChange} fieldKey={fieldKey} />;
    case "query_classic":
      return <ClassicQueryConfigFields value={config} onChange={onChange} fieldKey={fieldKey} />;
    case "query_sql":
      return <SqlQueryConfigFields value={config} onChange={onChange} fieldKey={fieldKey} />;
    case "filter":
      return <FilterNodeConfigFields value={config} onChange={onChange} fieldKey={fieldKey} />;
    case "query_records":
      return <RecordsQueryConfigFields value={config} onChange={onChange} fieldKey={fieldKey} />;
    default:
      return null;
  }
}

function ConfigEditorBody({
  kind,
  config,
  nodeId,
  schemaSpace,
  flowNodes,
  flowEdges,
  buildPairing,
  paletteHandlerLocked,
  onChange,
  t,
}: {
  kind: TransformCanvasNodeKind;
  config: JsonObject;
  nodeId: string;
  schemaSpace?: string;
  flowNodes: readonly Node[];
  flowEdges: readonly Edge[];
  buildPairing: TransformWorkflowBuildPairing | null;
  paletteHandlerLocked?: boolean;
  onChange: (next: JsonObject) => void;
  t: TFn;
}) {
  if (["query_view", "query_raw", "query_classic", "query_sql", "query_records", "filter"].includes(kind)) {
    return (
      <QueryEditorBody
        kind={kind}
        config={config}
        fieldKey={nodeId}
        schemaSpace={schemaSpace}
        onChange={onChange}
      />
    );
  }
  if (kind === "transform") {
    return (
      <EtlTransformNodeConfigFields
        value={config}
        onChange={onChange}
        fieldKey={nodeId}
        handlerLocked={paletteHandlerLocked}
      />
    );
  }
  if (kind === "merge") return <EtlMergeNodeConfigFields value={config} onChange={onChange} />;
  if (kind === "build_index") return <EtlBuildIndexNodeConfigFields value={config} onChange={onChange} />;
  if (kind === "join") return <EtlJoinNodeConfigFields value={config} onChange={onChange} fieldKey={nodeId} />;
  if (kind === "json_mapping") {
    return (
      <JsonMappingNodeConfigFields
        value={config}
        onChange={onChange}
        nodeId={nodeId}
        flowNodes={flowNodes}
        flowEdges={flowEdges}
      />
    );
  }
  if (kind === "spark_transform") {
    return (
      <SparkTransformConfigFields value={config} onChange={onChange} fieldKey={nodeId} nodeId={nodeId} />
    );
  }
  if (kind === "transformation_ref") {
    return <TransformationRefConfigFields value={config} onChange={onChange} />;
  }
  if (kind === "file_annotation") {
    return <EtlFileAnnotationNodeConfigFields value={config} onChange={onChange} />;
  }
  if (kind === "workflow_fanout_plan") {
    return (
      <EtlWorkflowFanoutPlanNodeConfigFields value={config} onChange={onChange} flowNodes={flowNodes} />
    );
  }
  if (kind === "dynamic_fanout") {
    return <EtlDynamicFanoutNodeConfigFields value={config} onChange={onChange} flowNodes={flowNodes} />;
  }
  if (kind === "score") return <EtlScoreNodeConfigFields value={config} onChange={onChange} />;
  if (kind === "save_records") {
    return <RecordsSaveConfigFields value={config} onChange={onChange} fieldKey={nodeId} />;
  }
  if (kind === "save_stream") {
    return <StreamSaveConfigFields value={config} onChange={onChange} fieldKey={nodeId} />;
  }
  if (kind === "save_view" || kind === "save_raw" || kind === "save_classic") {
    return (
      <EtlSaveNodeConfigFields
        kind={kind}
        value={config}
        onChange={onChange}
        fieldKey={nodeId}
        schemaSpace={schemaSpace}
      />
    );
  }
  if (kind === "function_ref" || kind === "subworkflow" || kind === "simulation" || kind === "cdf_task") {
    return (
      <EtlOrchestrationNodeConfigFields kind={kind} value={config} onChange={onChange} fieldKey={nodeId} />
    );
  }
  if (kind === "start") {
    return <EtlStartNodeConfigFields value={config} onChange={onChange} buildPairing={buildPairing} />;
  }
  if (kind === "raw_cleanup" || kind === "end") {
    return <EtlRawCleanupNodeConfigFields value={config} onChange={onChange} kind={kind} />;
  }
  if (kind === "subgraph") return <EtlSubgraphNodeConfigFields value={config} onChange={onChange} />;
  return <EtlNodeConfigFields t={t} kind={kind} config={config} onChange={onChange} />;
}

export function FlowNodeEditorModal({
  node,
  onClose,
  onPatchNode,
  t,
  pipelineId,
  schemaSpace,
  flowNodes = [],
  flowEdges = [],
  readOnly = false,
}: Props) {
  const kind = node ? rfTypeToKind(node.type) : null;
  const [buildPairing, setBuildPairing] = useState<TransformWorkflowBuildPairing | null>(null);
  const dialogRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (kind !== "start" || !pipelineId || !node) {
      setBuildPairing(null);
      return;
    }
    let cancelled = false;
    void fetchTransformWorkflowBuildPairing(pipelineId)
      .then((p) => {
        if (!cancelled) setBuildPairing(p);
      })
      .catch(() => {
        if (!cancelled) setBuildPairing(null);
      });
    return () => {
      cancelled = true;
    };
  }, [kind, pipelineId, node?.id]);

  const data = (node?.data ?? {}) as Record<string, unknown>;
  const committedConfig =
    data.config && typeof data.config === "object" && !Array.isArray(data.config)
      ? (data.config as JsonObject)
      : {};

  const dataRef = useRef(data);
  dataRef.current = data;

  const [configDraft, setConfigDraft] = useState<JsonObject>(committedConfig);
  const configDraftRef = useRef(configDraft);
  configDraftRef.current = configDraft;

  useEffect(() => {
    if (!node?.id) return;
    const d = (node.data ?? {}) as Record<string, unknown>;
    const cfg =
      d.config && typeof d.config === "object" && !Array.isArray(d.config)
        ? (d.config as JsonObject)
        : {};
    setConfigDraft(cfg);
    configDraftRef.current = cfg;
  }, [node?.id]);

  const { schedulePatch: scheduleNodePatch, flushNow: flushNodePatch } = useDebouncedNodePatch(
    node?.id ?? "",
    onPatchNode
  );

  const onConfigChange: Dispatch<SetStateAction<JsonObject>> = useCallback(
    (next) => {
      setConfigDraft((prev) => {
        const resolved = typeof next === "function" ? next(prev) : next;
        configDraftRef.current = resolved;
        scheduleNodePatch({ ...dataRef.current, config: resolved });
        return resolved;
      });
    },
    [scheduleNodePatch]
  );

  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  const handleClose = useCallback(() => {
    const active = document.activeElement;
    if (active instanceof HTMLElement) active.blur();
    // Let deferred input blur handlers commit into configDraftRef before final flush.
    queueMicrotask(() => {
      scheduleNodePatch({ ...dataRef.current, config: configDraftRef.current });
      flushNodePatch();
      onCloseRef.current();
    });
  }, [flushNodePatch, scheduleNodePatch]);

  useModalDialog({ open: Boolean(node), onClose: handleClose, dialogRef });

  if (!node || !kind) return null;

  return createPortal(
    <div className="gov-modal-backdrop transform-node-editor-backdrop">
      <button
        type="button"
        className="gov-modal-backdrop__dismiss"
        aria-label={t("btn.cancel")}
        onClick={handleClose}
      />
      <div
        ref={dialogRef}
        className="gov-modal transform-node-editor-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="transform-node-editor-title"
        aria-describedby="transform-node-editor-hint"
      >
        <h2 id="transform-node-editor-title" className="gov-modal__title">
          {t(titleKey(kind))}
        </h2>
        <p className="transform-node-editor-modal__subtitle">
          {node.id} · {kind.replace(/_/g, " ")}
        </p>
        <p id="transform-node-editor-hint" className="transform-node-editor-modal__hint">
          {t("transform.nodeEditor.hint")}
        </p>
        <div className="gov-modal__body transform-node-editor-modal__body">
          <fieldset
            className="transform-node-editor-modal__fieldset"
            disabled={readOnly}
          >
            <label className="gov-label">
              {t("transform.inspector.label")}
              <DeferredCommitInput
                className="gov-input"
                committedValue={String(data.label ?? "")}
                syncKey={node.id}
                onCommit={(label) => scheduleNodePatch({ ...dataRef.current, label })}
                spellCheck={false}
                autoComplete="off"
              />
            </label>
            <label className="gov-label">
              {t("transform.inspector.notes")}
              <DeferredCommitTextarea
                className="gov-input"
                rows={3}
                committedValue={String(data.notes ?? "")}
                syncKey={node.id}
                onCommit={(notes) => scheduleNodePatch({ ...dataRef.current, notes })}
                spellCheck={false}
              />
            </label>
            <ConfigEditorBody
              kind={kind}
              config={configDraft}
              nodeId={node.id}
              schemaSpace={schemaSpace}
              flowNodes={flowNodes}
              flowEdges={flowEdges}
              buildPairing={buildPairing}
              paletteHandlerLocked={data.palette_handler_locked === true}
              onChange={onConfigChange}
              t={t}
            />
            {kind !== "start" && kind !== "end" ? (
              <ConnectorLabelFields
                value={configDraft}
                onChange={onConfigChange}
                showInput={true}
                showOutput={true}
              />
            ) : null}
          </fieldset>
        </div>
        <div className="gov-modal__actions">
          <button type="button" className="disc-btn disc-btn--primary" onClick={handleClose}>
            {t("transform.nodeEditor.done")}
          </button>
        </div>
      </div>
    </div>,
    document.body
  );
}
