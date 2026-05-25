import { useEffect } from "react";
import { createPortal } from "react-dom";
import type { Node, Edge } from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import { rfTypeToKind, type TransformCanvasNodeKind } from "../../types/transformCanvas";
import { EtlNodeConfigFields } from "./EtlNodeConfigFields";
import { EtlJoinNodeConfigFields } from "./EtlJoinNodeConfigFields";
import { EtlMergeNodeConfigFields } from "./EtlMergeNodeConfigFields";
import { EtlBuildIndexNodeConfigFields } from "./EtlBuildIndexNodeConfigFields";
import { EtlTransformNodeConfigFields } from "./EtlTransformNodeConfigFields";
import { SparkTransformConfigFields } from "./SparkTransformConfigFields";
import { TransformationRefConfigFields } from "./TransformationRefConfigFields";
import { ViewQueryConfigFields } from "../query/ViewQueryConfigFields";
import { RawQueryConfigFields } from "../query/RawQueryConfigFields";
import { ClassicQueryConfigFields } from "../query/ClassicQueryConfigFields";
import { SqlQueryConfigFields } from "../query/SqlQueryConfigFields";
import { FilterNodeConfigFields } from "../query/FilterNodeConfigFields";
import { FieldMapConfigFields } from "./FieldMapConfigFields";
import type { JsonObject } from "../../types/jsonConfig";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  node: Node | null;
  onClose: () => void;
  onPatchNode: (nodeId: string, data: Record<string, unknown>) => void;
  t: TFn;
  schemaSpace?: string;
  flowNodes?: readonly Node[];
  flowEdges?: readonly Edge[];
};

function titleKey(kind: TransformCanvasNodeKind): MessageKey {
  switch (kind) {
    case "query_view":
    case "query_raw":
    case "query_classic":
    case "query_sql":
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
    case "field_map":
      return "transform.nodeEditor.titleFieldMap";
    case "save_view":
    case "save_raw":
    case "save_classic":
      return "transform.nodeEditor.titleSave";
    case "spark_transform":
    case "transformation_ref":
      return "transform.nodeEditor.titleSpark";
    case "subworkflow":
    case "dynamic_fanout":
    case "function_ref":
    case "simulation":
    case "cdf_task":
      return "transform.nodeEditor.titleOrchestration";
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
    default:
      return null;
  }
}

export function FlowNodeEditorModal({
  node,
  onClose,
  onPatchNode,
  t,
  schemaSpace,
  flowNodes = [],
  flowEdges = [],
}: Props) {
  useEffect(() => {
    if (!node) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [node, onClose]);

  if (!node) return null;

  const data = (node.data ?? {}) as Record<string, unknown>;
  const kind = rfTypeToKind(node.type);
  const config =
    data.config && typeof data.config === "object" && !Array.isArray(data.config)
      ? (data.config as JsonObject)
      : {};

  const patchConfig = (nextCfg: JsonObject) => {
    onPatchNode(node.id, { ...data, config: nextCfg });
  };

  const usesQueryEditor = ["query_view", "query_raw", "query_classic", "query_sql", "filter"].includes(kind);
  const usesTransformEditor = kind === "transform";
  const usesMergeEditor = kind === "merge";
  const usesBuildIndexEditor = kind === "build_index";
  const usesJoinEditor = kind === "join";
  const usesFieldMapEditor = kind === "field_map";
  const usesSparkEditor = kind === "spark_transform";
  const usesTransformationRefEditor = kind === "transformation_ref";

  return createPortal(
    <div
      className="gov-modal-backdrop transform-node-editor-backdrop"
      role="presentation"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        className="gov-modal transform-node-editor-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="transform-node-editor-title"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <h2 id="transform-node-editor-title" className="gov-modal__title">
          {t(titleKey(kind))}
        </h2>
        <p className="transform-node-editor-modal__subtitle">
          {node.id} · {kind.replace(/_/g, " ")}
        </p>
        <p className="transform-node-editor-modal__hint">{t("transform.nodeEditor.hint")}</p>
        <div className="gov-modal__body transform-node-editor-modal__body">
          <label className="gov-label">
            {t("transform.inspector.label")}
            <input
              className="gov-input"
              value={String(data.label ?? "")}
              onChange={(e) => onPatchNode(node.id, { ...data, label: e.target.value })}
            />
          </label>
          <label className="gov-label">
            {t("transform.inspector.notes")}
            <textarea
              className="gov-input"
              rows={3}
              value={String(data.notes ?? "")}
              onChange={(e) => onPatchNode(node.id, { ...data, notes: e.target.value })}
            />
          </label>
          {usesQueryEditor ? (
            <QueryEditorBody
              kind={kind}
              config={config}
              fieldKey={node.id}
              schemaSpace={schemaSpace}
              onChange={patchConfig}
            />
          ) : usesTransformEditor ? (
            <EtlTransformNodeConfigFields value={config} onChange={patchConfig} />
          ) : usesMergeEditor ? (
            <EtlMergeNodeConfigFields value={config} onChange={patchConfig} />
          ) : usesBuildIndexEditor ? (
            <EtlBuildIndexNodeConfigFields value={config} onChange={patchConfig} />
          ) : usesJoinEditor ? (
            <EtlJoinNodeConfigFields value={config} onChange={patchConfig} />
          ) : usesFieldMapEditor ? (
            <FieldMapConfigFields
              value={config}
              onChange={patchConfig}
              nodeId={node.id}
              flowNodes={flowNodes}
              flowEdges={flowEdges}
            />
          ) : usesSparkEditor ? (
            <SparkTransformConfigFields
              value={config}
              onChange={patchConfig}
              fieldKey={node.id}
              nodeId={node.id}
            />
          ) : usesTransformationRefEditor ? (
            <TransformationRefConfigFields value={config} onChange={patchConfig} />
          ) : (
            <EtlNodeConfigFields t={t} kind={kind} config={config} onChange={patchConfig} />
          )}
        </div>
        <div className="gov-modal__actions">
          <button type="button" className="disc-btn disc-btn--primary" onClick={onClose}>
            {t("transform.nodeEditor.done")}
          </button>
        </div>
      </div>
    </div>,
    document.body
  );
}
