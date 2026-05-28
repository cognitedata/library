import type { Edge, Node } from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import {
  rfTypeToKind,
  type TransformCanvasEdgeKind,
  type TransformCanvasNodeKind,
} from "../../types/transformCanvas";
import {
  isNodePreviewKind,
  isOrchestrationNodeKind,
  MODAL_EDITOR_NODE_KINDS,
} from "../../utils/transformNodeEditorKinds";
import { EtlNodePreviewConfigFields } from "./EtlNodePreviewConfigFields";
import { DeferredCommitInput, DeferredCommitTextarea } from "../query/DeferredCommitTextField";
import type { FlowEdgeData } from "./flowDocumentBridge";
import { configSummaryForKind } from "./EtlNodeConfigFields";
import { EtlNodeAccentFields } from "./flowNodeAccent";
import { canvasNodeKindLabel } from "../../utils/canvasNodeKindLabel";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

function isNodeEnabled(data: Record<string, unknown>): boolean {
  return data.canvas_node_enabled !== false;
}

type Props = {
  t: TFn;
  pipelineId?: string;
  selectedNode: Node | null;
  selectedEdge: Edge | null;
  flowNodes?: Node[];
  readOnly?: boolean;
  onPatchNode: (nodeId: string, data: Record<string, unknown>) => void;
  onPatchEdge?: (edgeId: string, kind: TransformCanvasEdgeKind) => void;
  onOpenEditor?: (node: Node) => void;
  onOpenPreviewQuery?: (node: Node) => void;
  onDeleteNode?: (nodeId: string) => void;
  onDeleteEdge?: (edgeId: string) => void;
};

export function FlowNodeInspector({
  t,
  pipelineId: _pipelineId,
  selectedNode,
  selectedEdge,
  flowNodes,
  readOnly = false,
  onPatchNode,
  onPatchEdge,
  onOpenEditor,
  onOpenPreviewQuery,
  onDeleteNode,
  onDeleteEdge,
}: Props) {
  const liveNode = selectedNode
    ? flowNodes?.length
      ? flowNodes.find((n) => n.id === selectedNode.id) ?? selectedNode
      : selectedNode
    : null;
  const kind = liveNode ? rfTypeToKind(liveNode.type) : null;

  if (selectedEdge) {
    const fd = (selectedEdge.data ?? {}) as FlowEdgeData;
    const kind: TransformCanvasEdgeKind =
      fd.kind === "sequence" || fd.kind === "parallel_group" ? fd.kind : "data";
    return (
      <div className="transform-flow-inspector">
        <h3 className="transform-flow-inspector__title">{t("transform.inspector.edgeTitle")}</h3>
        <dl className="transform-flow-inspector__meta">
          <div>
            <dt>{t("transform.inspector.id")}</dt>
            <dd>{selectedEdge.id}</dd>
          </div>
          <div>
            <dt>{t("transform.inspector.edgeSource")}</dt>
            <dd>{selectedEdge.source}</dd>
          </div>
          <div>
            <dt>{t("transform.inspector.edgeTarget")}</dt>
            <dd>{selectedEdge.target}</dd>
          </div>
        </dl>
        {!readOnly && onPatchEdge ? (
          <label className="transform-flow-inspector__field">
            <span>{t("transform.inspector.edgeKind")}</span>
            <select
              value={kind}
              onChange={(e) => onPatchEdge(selectedEdge.id, e.target.value as TransformCanvasEdgeKind)}
            >
              <option value="data">{t("transform.inspector.edgeKindData")}</option>
              <option value="sequence">{t("transform.inspector.edgeKindSequence")}</option>
              <option value="parallel_group">{t("transform.inspector.edgeKindParallel")}</option>
            </select>
          </label>
        ) : null}
        {!readOnly && onDeleteEdge ? (
          <div className="transform-flow-inspector__actions">
            <button
              type="button"
              className="disc-btn disc-btn--sm disc-btn--danger"
              onClick={() => onDeleteEdge(selectedEdge.id)}
            >
              {t("transform.inspector.deleteEdge")}
            </button>
          </div>
        ) : null}
      </div>
    );
  }

  if (!liveNode) {
    return (
      <div className="transform-flow-inspector transform-flow-inspector--empty">
        <p>{t("transform.inspector.empty")}</p>
      </div>
    );
  }

  const data = (liveNode.data ?? {}) as Record<string, unknown>;
  const nodeKind = kind ?? "transform";
  const isBoundary = nodeKind === "start" || nodeKind === "end";
  const config =
    data.config && typeof data.config === "object" && !Array.isArray(data.config)
      ? (data.config as Record<string, unknown>)
      : {};
  const summary = configSummaryForKind(nodeKind, config);
  const showConfig = Boolean(summary);

  return (
    <div className="transform-flow-inspector">
      <h3 className="transform-flow-inspector__title">{t("transform.inspector.title")}</h3>
      <dl className="transform-flow-inspector__meta">
        <div>
          <dt>{t("transform.inspector.kind")}</dt>
          <dd>{canvasNodeKindLabel(nodeKind, t)}</dd>
        </div>
        <div>
          <dt>{t("transform.inspector.id")}</dt>
          <dd>{liveNode.id}</dd>
        </div>
      </dl>

      {!isBoundary && !readOnly ? (
        <div className="transform-flow-inspector__enabled">
          <label className="transform-flow-inspector__enabled-label">
            <input
              type="checkbox"
              checked={isNodeEnabled(data)}
              onChange={(e) =>
                onPatchNode(liveNode.id, { ...data, canvas_node_enabled: e.target.checked })
              }
            />
            {t("transform.inspector.enabled")}
          </label>
          <p className="transform-flow-inspector__hint">{t("transform.inspector.enabledHint")}</p>
        </div>
      ) : null}

      {isNodePreviewKind(nodeKind) && onOpenPreviewQuery ? (
        <div className="transform-flow-inspector__actions">
          <button type="button" className="disc-btn disc-btn--sm" onClick={() => onOpenPreviewQuery(liveNode)}>
            {t("transform.inspector.openPreviewQuery")}
          </button>
          {!isBoundary && onDeleteNode ? (
            <button
              type="button"
              className="disc-btn disc-btn--sm disc-btn--danger"
              onClick={() => onDeleteNode(liveNode.id)}
            >
              {t("transform.inspector.deleteNode")}
            </button>
          ) : null}
        </div>
      ) : null}
      {onOpenEditor && (!readOnly || isOrchestrationNodeKind(nodeKind)) && !isNodePreviewKind(nodeKind) ? (
        <div className="transform-flow-inspector__actions">
          <button type="button" className="disc-btn disc-btn--sm" onClick={() => onOpenEditor(liveNode)}>
            {readOnly ? t("transform.inspector.viewEditor") : t("transform.inspector.openEditor")}
          </button>
          {!isBoundary && onDeleteNode ? (
            <button
              type="button"
              className="disc-btn disc-btn--sm disc-btn--danger"
              onClick={() => onDeleteNode(liveNode.id)}
            >
              {t("transform.inspector.deleteNode")}
            </button>
          ) : null}
        </div>
      ) : null}

      <label className="transform-flow-inspector__field">
        <span>{t("transform.inspector.label")}</span>
        {readOnly ? (
          <input type="text" value={String(data.label ?? "")} disabled readOnly />
        ) : (
          <DeferredCommitInput
            type="text"
            committedValue={String(data.label ?? "")}
            syncKey={liveNode.id}
            onCommit={(label) => onPatchNode(liveNode.id, { ...data, label })}
            spellCheck={false}
            autoComplete="off"
          />
        )}
      </label>

      <EtlNodeAccentFields t={t} nodeId={liveNode.id} data={data} onPatchNode={onPatchNode} />

      {isNodePreviewKind(nodeKind) && !readOnly ? (
        <EtlNodePreviewConfigFields
          value={config}
          onChange={(next) => onPatchNode(liveNode.id, { ...data, config: next })}
        />
      ) : null}

      {showConfig && summary ? (
        <p className="transform-flow-inspector__summary">
          <span className="transform-flow-inspector__summary-label">{t("transform.inspector.configSummary")}</span>
          {summary}
        </p>
      ) : null}

      {(MODAL_EDITOR_NODE_KINDS.has(nodeKind) && !readOnly) ||
      (readOnly && isOrchestrationNodeKind(nodeKind)) ? (
        <p className="transform-flow-inspector__hint">
          {readOnly
            ? t("transform.inspector.orchestrationViewEditor")
            : t("transform.inspector.openFiltersEditor")}
        </p>
      ) : null}

      <label className="transform-flow-inspector__field">
        <span>{t("transform.inspector.notes")}</span>
        {readOnly ? (
          <textarea rows={4} value={String(data.notes ?? "")} disabled readOnly />
        ) : (
          <DeferredCommitTextarea
            rows={4}
            committedValue={String(data.notes ?? "")}
            syncKey={liveNode.id}
            onCommit={(notes) => onPatchNode(liveNode.id, { ...data, notes })}
            spellCheck={false}
          />
        )}
      </label>

      {isNodePreviewKind(nodeKind) ? (
        <p className="transform-flow-inspector__hint transform-flow-inspector__hint--footer">
          {t("transform.nodePreview.canvasHint")}
        </p>
      ) : !readOnly || isOrchestrationNodeKind(nodeKind) ? (
        <p className="transform-flow-inspector__hint transform-flow-inspector__hint--footer">
          {t("transform.inspector.doubleClickHint")}
        </p>
      ) : null}
    </div>
  );
}
