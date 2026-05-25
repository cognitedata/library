import { useEffect, useState } from "react";
import type { Edge, Node } from "@xyflow/react";
import { fetchTransformBuildPairing, type TransformBuildPairing } from "../../api";
import type { MessageKey } from "../../i18n";
import {
  rfTypeToKind,
  type TransformCanvasEdgeKind,
  type TransformCanvasNodeKind,
} from "../../types/transformCanvas";
import type { FlowEdgeData } from "./flowDocumentBridge";
import { configSummaryForKind, EtlNodeConfigFields } from "./EtlNodeConfigFields";
import { EtlNodeAccentFields } from "./flowNodeAccent";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

const MODAL_EDITOR_KINDS = new Set<TransformCanvasNodeKind>([
  "query_view",
  "query_raw",
  "query_classic",
  "query_sql",
  "filter",
  "field_map",
  "join",
  "merge",
  "build_index",
  "spark_transform",
  "transformation_ref",
]);

const CONFIG_INLINE_KINDS = new Set<TransformCanvasNodeKind>([
  "transform",
  "score",
  "save_view",
  "save_raw",
  "save_classic",
  "spark_transform",
  "transformation_ref",
  "function_ref",
  "subworkflow",
  "dynamic_fanout",
  "simulation",
  "cdf_task",
  "raw_cleanup",
]);

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
  onDeleteNode?: (nodeId: string) => void;
  onDeleteEdge?: (edgeId: string) => void;
};

export function FlowNodeInspector({
  t,
  pipelineId,
  selectedNode,
  selectedEdge,
  flowNodes,
  readOnly = false,
  onPatchNode,
  onPatchEdge,
  onOpenEditor,
  onDeleteNode,
  onDeleteEdge,
}: Props) {
  const liveNode = selectedNode
    ? flowNodes?.length
      ? flowNodes.find((n) => n.id === selectedNode.id) ?? selectedNode
      : selectedNode
    : null;
  const kind = liveNode ? rfTypeToKind(liveNode.type) : null;

  const [buildPairing, setBuildPairing] = useState<TransformBuildPairing | null>(null);

  useEffect(() => {
    if (kind !== "start" || !pipelineId || !liveNode) {
      setBuildPairing(null);
      return;
    }
    let cancelled = false;
    void fetchTransformBuildPairing(pipelineId, true)
      .then((p) => {
        if (!cancelled) setBuildPairing(p);
      })
      .catch(() => {
        if (!cancelled) setBuildPairing(null);
      });
    return () => {
      cancelled = true;
    };
  }, [kind, pipelineId, liveNode?.id]);

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
  const showConfig = CONFIG_INLINE_KINDS.has(nodeKind) || nodeKind === "start";

  const patchConfig = (nextCfg: Record<string, unknown>) => {
    onPatchNode(liveNode.id, { ...data, config: nextCfg });
  };

  return (
    <div className="transform-flow-inspector">
      <h3 className="transform-flow-inspector__title">{t("transform.inspector.title")}</h3>
      <dl className="transform-flow-inspector__meta">
        <div>
          <dt>{t("transform.inspector.kind")}</dt>
          <dd>{nodeKind.replace(/_/g, " ")}</dd>
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

      {!readOnly && onOpenEditor ? (
        <div className="transform-flow-inspector__actions">
          <button type="button" className="disc-btn disc-btn--sm" onClick={() => onOpenEditor(liveNode)}>
            {t("transform.inspector.openEditor")}
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
        <input
          type="text"
          value={String(data.label ?? "")}
          disabled={readOnly}
          onChange={(e) => onPatchNode(liveNode.id, { ...data, label: e.target.value })}
        />
      </label>

      <EtlNodeAccentFields t={t} nodeId={liveNode.id} data={data} onPatchNode={onPatchNode} />

      {showConfig && summary ? (
        <p className="transform-flow-inspector__summary">
          <span className="transform-flow-inspector__summary-label">{t("transform.inspector.configSummary")}</span>
          {summary}
        </p>
      ) : null}

      {MODAL_EDITOR_KINDS.has(nodeKind) && !readOnly ? (
        <p className="transform-flow-inspector__hint">{t("transform.inspector.openFiltersEditor")}</p>
      ) : null}

      {showConfig && !readOnly && !MODAL_EDITOR_KINDS.has(nodeKind) ? (
        <>
          <p className="transform-flow-inspector__hint">{t("transform.inspector.configHint")}</p>
          <EtlNodeConfigFields
            t={t}
            kind={nodeKind}
            config={config}
            onChange={patchConfig}
            compact
            buildPairing={nodeKind === "start" ? buildPairing : null}
          />
        </>
      ) : null}

      <label className="transform-flow-inspector__field">
        <span>{t("transform.inspector.notes")}</span>
        <textarea
          rows={4}
          value={String(data.notes ?? "")}
          disabled={readOnly}
          onChange={(e) => onPatchNode(liveNode.id, { ...data, notes: e.target.value })}
        />
      </label>

      {!readOnly ? (
        <p className="transform-flow-inspector__hint transform-flow-inspector__hint--footer">
          {t("transform.inspector.doubleClickHint")}
        </p>
      ) : null}
    </div>
  );
}
