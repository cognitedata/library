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
    case "keaSourceView":
    case "keaMatchValidationRuleSourceView":
      return "flow.nodeEditorTitleSourceViews";
    case "keaInvertedIndex":
      return "flow.discoveryInvertedIndex";
    case "keaMatchValidationRuleExtraction":
      return "flow.nodeEditorTitleMatchDefinitions";
    case "keaViewQuery":
    case "keaRawQuery":
    case "keaClassicQuery":
      return "flow.nodeEditorTitleQueries";
    case "keaTransform":
      return "flow.nodeEditorTitleTransforms";
    case "keaMerge":
      return "flow.nodeEditorTitleMerges";
    case "keaJoin":
      return "flow.nodeEditorTitleJoins";
    case "keaDiscoveryValidate":
      return "flow.nodeEditorTitleValidations";
    case "keaDiscoveryInstanceFilter":
      return "flow.nodeEditorTitleInstanceFilters";
    case "keaDiscoveryConfidenceFilter":
      return "flow.nodeEditorTitleConfidenceFilters";
    case "keaViewSave":
    case "keaRawSave":
    case "keaClassicSave":
      return "flow.nodeEditorTitleSave";
    case "keaAliasPersistence":
      return "flow.nodeEditorTitleAliasing";
    case "keaMatchValidationRuleAliasing":
      return "flow.nodeEditorTitleMatchDefinitions";
    case "keaStart":
    case "keaEnd":
    case "keaSubgraph":
    case "keaSubflowGraphIn":
    case "keaSubflowGraphOut":
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
    case "keaSourceView":
    case "keaMatchValidationRuleSourceView":
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
    case "keaInvertedIndex":
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
          <p className="kea-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorInvertedIndexCanvasMissing")}
          </p>
        );
      break;
    case "keaMatchValidationRuleExtraction":
      body = (
        <MatchDefinitionsScopePanel
          key={node.id}
          scopeDocument={workflowDoc}
          onPatch={patch}
          initialSelectedDefId={matchRuleFocus}
        />
      );
      break;
    case "keaViewQuery":
    case "keaRawQuery":
    case "keaClassicQuery":
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
          <p className="kea-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorQueriesCanvasMissing")}
          </p>
        );
      break;
    case "keaTransform":
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
          <p className="kea-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorTransformsCanvasMissing")}
          </p>
        );
      break;
    case "keaMerge":
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
          <p className="kea-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorTransformsCanvasMissing")}
          </p>
        );
      break;
    case "keaJoin":
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
          <p className="kea-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorTransformsCanvasMissing")}
          </p>
        );
      break;
    case "keaDiscoveryValidate":
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
          <p className="kea-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorValidationsCanvasMissing")}
          </p>
        );
      break;
    case "keaDiscoveryInstanceFilter":
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
          <p className="kea-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorInstanceFiltersCanvasMissing")}
          </p>
        );
      break;
    case "keaDiscoveryConfidenceFilter":
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
          <p className="kea-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorConfidenceFiltersCanvasMissing")}
          </p>
        );
      break;
    case "keaViewSave":
    case "keaRawSave":
    case "keaClassicSave":
      body =
        workflowCanvas && onPatchWorkflowCanvas && node?.id ? (
          <SaveNodeConfigFields
            key={node.id}
            canvas={workflowCanvas}
            onChange={onPatchWorkflowCanvas}
            nodeId={node.id}
            t={t}
          />
        ) : (
          <p className="kea-hint" style={{ marginTop: 0 }}>
            {t("flow.nodeEditorTransformsCanvasMissing")}
          </p>
        );
      break;
    case "keaAliasPersistence":
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
    case "keaMatchValidationRuleAliasing":
      body = (
        <MatchDefinitionsScopePanel
          key={node.id}
          scopeDocument={workflowDoc}
          onPatch={patch}
          initialSelectedDefId={matchRuleFocus}
        />
      );
      break;
    case "keaStart":
    case "keaEnd":
      body = <p className="kea-hint" style={{ marginTop: 0 }}>{t("flow.nodeEditorPipelineStubBody")}</p>;
      break;
    case "keaSubgraph":
      body = <p className="kea-hint" style={{ marginTop: 0 }}>{t("flow.nodeEditorSubgraphBody")}</p>;
      break;
    case "keaSubflowGraphIn":
    case "keaSubflowGraphOut":
      body = <p className="kea-hint" style={{ marginTop: 0 }}>{t("flow.nodeEditorGraphHubBody")}</p>;
      break;
    default:
      body = <p className="kea-hint kea-hint--warn">{t("flow.nodeEditorUnsupported")}</p>;
  }

  const title = t(modalTitleKey(kind));

  return createPortal(
    <div
      className="kea-modal-backdrop kea-modal-backdrop--flow-node-editor"
      role="presentation"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        className="kea-modal kea-modal--flow-node-editor"
        role="dialog"
        aria-modal="true"
        aria-labelledby="kea-flow-node-editor-title"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <h2 id="kea-flow-node-editor-title" className="kea-modal__title">
          {title}
        </h2>
        <p className="kea-hint" style={{ marginTop: "-0.35rem", marginBottom: "0.75rem" }}>
          {t("flow.nodeEditorHint")}
        </p>
        <div className="kea-modal__body kea-modal__body--scroll">{body}</div>
        <div className="kea-modal__actions">
          <button type="button" className="kea-btn kea-btn--primary" onClick={onClose}>
            {t("flow.nodeEditorDone")}
          </button>
        </div>
      </div>
    </div>,
    document.body
  );
}
