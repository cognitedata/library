import { useEffect, type ReactNode } from "react";
import { createPortal } from "react-dom";
import type { Node } from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import { AliasingControls } from "../AliasingControls";
import { MatchDefinitionsScopePanel } from "../MatchDefinitionsScopePanel";
import { QueriesControls } from "../QueriesControls";
import { TransformsControls } from "../TransformsControls";
import { JoinsControls } from "../JoinsControls";
import { MergesControls } from "../MergesControls";
import { FilterNodeModalEditor } from "./FilterNodeModalEditor";
import { ConfidenceFilterNodeModalEditor } from "./ConfidenceFilterNodeModalEditor";
import { ValidationsControls } from "../ValidationsControls";
import { SaveNodeConfigFields } from "./SaveNodeConfigFields";
import { InvertedIndexNodeConfigFields } from "./InvertedIndexNodeConfigFields";
import { SourceViewsControls } from "../SourceViewsControls";
import type { WorkflowCanvasDocument, WorkflowCanvasNodeData } from "../../types/workflowCanvas";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  node: Node | null;
  workflowDoc: Record<string, unknown>;
  onPatchWorkflowScope: (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => void;
  onClose: () => void;
  t: TFn;
  schemaSpace?: string;
  workflowCanvas?: WorkflowCanvasDocument;
  onPatchWorkflowCanvas?: (
    patch:
      | WorkflowCanvasDocument
      | ((prev: WorkflowCanvasDocument) => WorkflowCanvasDocument)
  ) => void;
};

function readRef(data: Record<string, unknown>): Record<string, unknown> {
  const r = data.ref;
  if (r && typeof r === "object" && !Array.isArray(r)) return { ...r };
  return {};
}

function strOpt(v: unknown): string | undefined {
  if (v == null) return undefined;
  const s = String(v).trim();
  return s || undefined;
}

function firstStrInArray(a: unknown): string | undefined {
  if (!Array.isArray(a) || a.length === 0) return undefined;
  return strOpt(a[0]);
}

function modalTitleKey(kind: string | undefined): MessageKey {
  switch (kind) {
    case "discoverySourceView":
    case "discoveryMatchValidationRuleSourceView":
      return "flow.nodeEditorTitleSourceViews";
    case "discoveryInvertedIndex":
      return "flow.discoveryInvertedIndex";
    case "discoveryMatchValidationRuleExtraction":
      return "flow.nodeEditorTitleMatchDefinitions";
    case "discoveryViewQuery":
    case "discoveryRawQuery":
    case "discoveryClassicQuery":
    case "discoverySqlQuery":
      return "flow.nodeEditorTitleQueries";
    case "discoveryTransform":
      return "flow.nodeEditorTitleTransforms";
    case "discoveryMerge":
      return "flow.nodeEditorTitleMerges";
    case "discoveryJoin":
      return "flow.nodeEditorTitleJoins";
    case "discoveryValidate":
      return "flow.nodeEditorTitleValidations";
    case "discoveryInstanceFilter":
      return "flow.nodeEditorTitleInstanceFilters";
    case "discoveryConfidenceFilter":
      return "flow.nodeEditorTitleConfidenceFilters";
    case "discoveryViewSave":
    case "discoveryRawSave":
    case "discoveryClassicSave":
      return "flow.nodeEditorTitleSave";
    case "discoveryAliasPersistence":
      return "flow.nodeEditorTitleAliasing";
    case "discoveryMatchValidationRuleAliasing":
      return "flow.nodeEditorTitleMatchDefinitions";
    case "discoveryStart":
    case "discoveryEnd":
    case "discoverySubgraph":
    case "discoverySubflowGraphIn":
    case "discoverySubflowGraphOut":
      return "flow.nodeEditorTitlePipelineStub";
    default:
      return "flow.nodeEditorTitle";
  }
}

export function FlowNodeEditorModal({
  node,
  workflowDoc,
  onPatchWorkflowScope,
  onClose,
  t,
  schemaSpace,
  workflowCanvas,
  onPatchWorkflowCanvas,
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

  const kind = node.type;
  const nodeData = (node.data ?? {}) as Record<string, unknown>;
  const ref = readRef(nodeData);
  const matchRuleFocus = strOpt(nodeData.validation_rule_name);

  const patch = (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => {
    onPatchWorkflowScope(recipe);
  };

  let svIndex: number | undefined;
  if (typeof ref.source_view_index === "number") {
    svIndex = ref.source_view_index;
  } else if (ref.source_view_index != null) {
    const p = parseInt(String(ref.source_view_index), 10);
    svIndex = Number.isFinite(p) ? p : undefined;
  } else {
    const chain = ref.source_view_indices;
    if (Array.isArray(chain) && chain.length > 0) {
      const x = chain[0];
      const n = typeof x === "number" ? x : parseInt(String(x), 10);
      svIndex = Number.isFinite(n) ? n : undefined;
    }
  }
  const initialSv =
    svIndex !== undefined && Number.isFinite(svIndex) ? Math.max(0, Math.floor(svIndex)) : undefined;

  let body: ReactNode = null;

  switch (kind) {
    case "discoverySourceView":
    case "discoveryMatchValidationRuleSourceView":
      body = (
        <SourceViewsControls
          key={node.id}
          value={workflowDoc.source_views}
          initialViewIndex={initialSv}
          singleView
          onChange={(v) => patch((d) => ({ ...d, source_views: v }))}
          schemaSpace={schemaSpace}
        />
      );
      break;
    case "discoveryInvertedIndex":
      body =
        workflowCanvas && onPatchWorkflowCanvas && node?.id ? (
          <InvertedIndexNodeConfigFields
            key={node.id}
            canvas={workflowCanvas}
            onChange={onPatchWorkflowCanvas}
            nodeId={node.id}
            t={t}
          />
        ) : (
          <p className="discovery-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorInvertedIndexCanvasMissing")}
          </p>
        );
      break;
    case "discoveryMatchValidationRuleExtraction":
      body = (
        <MatchDefinitionsScopePanel
          key={node.id}
          scopeDocument={workflowDoc}
          onPatch={patch}
          initialSelectedDefId={matchRuleFocus}
        />
      );
      break;
    case "discoveryViewQuery":
    case "discoveryRawQuery":
    case "discoveryClassicQuery":
    case "discoverySqlQuery":
      body =
        workflowCanvas && onPatchWorkflowCanvas ? (
          <QueriesControls
            key={node.id}
            canvas={workflowCanvas}
            onChange={onPatchWorkflowCanvas}
            initialNodeId={node.id}
            schemaSpace={schemaSpace}
            singleNode
          />
        ) : (
          <p className="discovery-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorQueriesCanvasMissing")}
          </p>
        );
      break;
    case "discoveryTransform":
      body =
        workflowCanvas && onPatchWorkflowCanvas ? (
          <TransformsControls
            key={node.id}
            canvas={workflowCanvas}
            onChange={onPatchWorkflowCanvas}
            initialNodeId={node.id}
            singleNode
          />
        ) : (
          <p className="discovery-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorTransformsCanvasMissing")}
          </p>
        );
      break;
    case "discoveryMerge":
      body =
        workflowCanvas && onPatchWorkflowCanvas ? (
          <MergesControls
            key={node.id}
            canvas={workflowCanvas}
            onChange={onPatchWorkflowCanvas}
            initialNodeId={node.id}
            t={t}
            singleNode
          />
        ) : (
          <p className="discovery-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorTransformsCanvasMissing")}
          </p>
        );
      break;
    case "discoveryJoin":
      body =
        workflowCanvas && onPatchWorkflowCanvas ? (
          <JoinsControls
            key={node.id}
            canvas={workflowCanvas}
            onChange={onPatchWorkflowCanvas}
            initialNodeId={node.id}
            t={t}
            singleNode
          />
        ) : (
          <p className="discovery-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorTransformsCanvasMissing")}
          </p>
        );
      break;
    case "discoveryValidate":
      body =
        workflowCanvas && onPatchWorkflowCanvas ? (
          <ValidationsControls
            key={node.id}
            canvas={workflowCanvas}
            onChange={onPatchWorkflowCanvas}
            initialNodeId={node.id}
            singleNode
          />
        ) : (
          <p className="discovery-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorValidationsCanvasMissing")}
          </p>
        );
      break;
    case "discoveryInstanceFilter":
      body =
        workflowCanvas && onPatchWorkflowCanvas ? (
          <FilterNodeModalEditor
            key={node.id}
            nodeId={node.id}
            nodeData={(node.data ?? {}) as WorkflowCanvasNodeData}
            onChange={onPatchWorkflowCanvas}
            t={t}
          />
        ) : (
          <p className="discovery-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorInstanceFiltersCanvasMissing")}
          </p>
        );
      break;
    case "discoveryConfidenceFilter":
      body =
        workflowCanvas && onPatchWorkflowCanvas ? (
          <ConfidenceFilterNodeModalEditor
            key={node.id}
            nodeId={node.id}
            nodeData={(node.data ?? {}) as WorkflowCanvasNodeData}
            onChange={onPatchWorkflowCanvas}
            t={t}
          />
        ) : (
          <p className="discovery-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorConfidenceFiltersCanvasMissing")}
          </p>
        );
      break;
    case "discoveryViewSave":
    case "discoveryRawSave":
    case "discoveryClassicSave":
      body =
        workflowCanvas && onPatchWorkflowCanvas && node?.id ? (
          <SaveNodeConfigFields
            key={node.id}
            canvas={workflowCanvas}
            onChange={onPatchWorkflowCanvas}
            nodeId={node.id}
            t={t}
            schemaSpace={schemaSpace}
          />
        ) : (
          <p className="discovery-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorTransformsCanvasMissing")}
          </p>
        );
      break;
    case "discoveryAliasPersistence":
      body = (
        <AliasingControls
          key={node.id}
          value={workflowDoc.aliasing}
          onChange={(v) => patch((d) => ({ ...d, aliasing: v }))}
          scopeDocument={workflowDoc}
          initialEditorSub="settings"
        />
      );
      break;
    case "discoveryMatchValidationRuleAliasing":
      body = (
        <MatchDefinitionsScopePanel
          key={node.id}
          scopeDocument={workflowDoc}
          onPatch={patch}
          initialSelectedDefId={matchRuleFocus}
        />
      );
      break;
    case "discoveryStart":
    case "discoveryEnd":
      body = <p className="discovery-hint" style={{ marginTop: 0 }}>{t("flow.nodeEditorPipelineStubBody")}</p>;
      break;
    case "discoverySubgraph":
      body = <p className="discovery-hint" style={{ marginTop: 0 }}>{t("flow.nodeEditorSubgraphBody")}</p>;
      break;
    case "discoverySubflowGraphIn":
    case "discoverySubflowGraphOut":
      body = <p className="discovery-hint" style={{ marginTop: 0 }}>{t("flow.nodeEditorGraphHubBody")}</p>;
      break;
    default:
      body = <p className="discovery-hint discovery-hint--warn">{t("flow.nodeEditorUnsupported")}</p>;
  }

  const title = t(modalTitleKey(kind));

  return createPortal(
    <div
      className="discovery-modal-backdrop discovery-modal-backdrop--flow-node-editor"
      role="presentation"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        className="discovery-modal discovery-modal--flow-node-editor"
        role="dialog"
        aria-modal="true"
        aria-labelledby="discovery-flow-node-editor-title"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <h2 id="discovery-flow-node-editor-title" className="discovery-modal__title">
          {title}
        </h2>
        <p className="discovery-hint" style={{ marginTop: "-0.35rem", marginBottom: "0.75rem" }}>
          {t("flow.nodeEditorHint")}
        </p>
        <div className="discovery-modal__body discovery-modal__body--scroll">{body}</div>
        <div className="discovery-modal__actions">
          <button type="button" className="discovery-btn discovery-btn--primary" onClick={onClose}>
            {t("flow.nodeEditorDone")}
          </button>
        </div>
      </div>
    </div>,
    document.body
  );
}
