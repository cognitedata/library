import { useCallback, useEffect, useRef, useState } from "react";
import { fetchTransformWorkflowByWorkflow } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { WorkflowDocumentTab } from "../../types/discoveryNodes";
import {
  emptyTransformCanvasDocument,
  type TransformCanvasDocument,
} from "../../types/transformCanvas";
import { WorkflowFlowPane } from "../WorkflowFlowPane";
import { TransformFlowPanel } from "./TransformFlowPanel";

type Props = {
  tab: WorkflowDocumentTab;
  onTabUpdate: (tab: WorkflowDocumentTab) => void;
  onOpenInTransform?: () => void;
  openInTransformBusy?: boolean;
  openInTransformError?: string | null;
};

function FusionWorkflowOpenInTransformBar({
  onOpenInTransform,
  openInTransformBusy,
  openInTransformError,
}: Pick<Props, "onOpenInTransform" | "openInTransformBusy" | "openInTransformError">) {
  const { t } = useAppSettings();
  if (!onOpenInTransform) return null;
  return (
    <div className="transform-fusion-workflow-pane__actions">
      {openInTransformError ? (
        <div className="disc-banner--error" role="alert">
          {t("status.error", { detail: openInTransformError })}
        </div>
      ) : null}
      <button
        type="button"
        className="disc-btn disc-btn--primary"
        disabled={openInTransformBusy}
        onClick={onOpenInTransform}
        title={t("wfViewer.openInTransformHint")}
      >
        {openInTransformBusy ? t("wfViewer.openInTransformBusy") : t("wfViewer.openInTransform")}
      </button>
    </div>
  );
}

export function TransformFusionWorkflowPane({
  tab,
  onTabUpdate,
  onOpenInTransform,
  openInTransformBusy = false,
  openInTransformError = null,
}: Props) {
  const { t } = useAppSettings();
  const [canvas, setCanvas] = useState<TransformCanvasDocument>(emptyTransformCanvasDocument());
  const [workflowId, setWorkflowId] = useState<string | null>(null);
  const [reloadNonce, setReloadNonce] = useState(0);
  const [resolved, setResolved] = useState(false);
  const [useCdfGraphFallback, setUseCdfGraphFallback] = useState(false);
  const loadGen = useRef(0);
  const tabRef = useRef(tab);
  const onTabUpdateRef = useRef(onTabUpdate);
  tabRef.current = tab;
  onTabUpdateRef.current = onTabUpdate;

  useEffect(() => {
    const expectedTabId = tab.id;
    const workflowExternalId = tab.workflow.external_id;
    const gen = ++loadGen.current;
    let cancelled = false;

    const load = async () => {
      onTabUpdateRef.current({ ...tabRef.current, loading: true, error: null });
      setResolved(false);
      setUseCdfGraphFallback(false);
      setCanvas(emptyTransformCanvasDocument());
      setWorkflowId(null);
      try {
        const { workflow_id, workflow } = await fetchTransformWorkflowByWorkflow(workflowExternalId);
        if (cancelled || gen !== loadGen.current || tabRef.current.id !== expectedTabId) return;
        const nextCanvas = workflow.canvas ?? emptyTransformCanvasDocument();
        setWorkflowId(workflow_id);
        setCanvas(nextCanvas);
        setReloadNonce((n) => n + 1);
        setResolved(true);
        onTabUpdateRef.current({ ...tabRef.current, loading: false, error: null });
      } catch {
        if (cancelled || gen !== loadGen.current || tabRef.current.id !== expectedTabId) return;
        setWorkflowId(null);
        setUseCdfGraphFallback(true);
        setResolved(true);
        // Keep loading true so WorkflowFlowPane fetches the deployed CDF task graph.
        onTabUpdateRef.current({
          ...tabRef.current,
          graph: null,
          loading: true,
          error: null,
        });
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [tab.id, tab.workflow.external_id]);

  const onCanvasChange = useCallback((doc: TransformCanvasDocument) => {
    setCanvas(doc);
  }, []);

  if (!resolved || (tab.loading && !useCdfGraphFallback)) {
    return <div className="transform-flow-loading">{t("transform.fusionWorkflow.loading")}</div>;
  }

  if (useCdfGraphFallback) {
    return (
      <div className="transform-fusion-workflow-pane">
        <p className="transform-fusion-workflow-pane__hint" role="status">
          {t("transform.fusionWorkflow.noLocalPipeline", { externalId: tab.workflow.external_id })}
        </p>
        <WorkflowFlowPane
          tab={tab}
          onTabUpdate={onTabUpdate}
          readOnly
          onOpenInTransform={onOpenInTransform}
          openInTransformBusy={openInTransformBusy}
          openInTransformError={openInTransformError}
        />
      </div>
    );
  }

  return (
    <div className="transform-fusion-workflow-pane">
      <p className="transform-fusion-workflow-pane__hint" role="status">
        {t("transform.fusionWorkflow.readOnlyHint", { externalId: tab.workflow.external_id })}
      </p>
      <FusionWorkflowOpenInTransformBar
        onOpenInTransform={onOpenInTransform}
        openInTransformBusy={openInTransformBusy}
        openInTransformError={openInTransformError}
      />
      <TransformFlowPanel
        t={t}
        pipelineId={workflowId ?? undefined}
        initialDocument={canvas}
        reloadNonce={reloadNonce}
        readOnly
        onChange={onCanvasChange}
      />
    </div>
  );
}
