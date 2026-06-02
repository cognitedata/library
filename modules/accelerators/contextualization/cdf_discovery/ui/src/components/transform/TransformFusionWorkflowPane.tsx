import { useCallback, useEffect, useRef, useState } from "react";
import { deleteTransformWorkflow, fetchTransformWorkflowByWorkflow } from "../../api";
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
  onDeleteInTransform?: () => void;
  openInTransformBusy?: boolean;
  openInTransformError?: string | null;
};

function FusionWorkflowOpenInTransformBar({
  onOpenInTransform,
  onDeleteInTransform,
  canDeleteInTransform,
  deleteInTransformBusy,
  deleteInTransformError,
  openInTransformBusy,
  openInTransformError,
}: Pick<Props, "onOpenInTransform" | "onDeleteInTransform" | "openInTransformBusy" | "openInTransformError"> & {
  canDeleteInTransform: boolean;
  deleteInTransformBusy: boolean;
  deleteInTransformError: string | null;
}) {
  const { t } = useAppSettings();
  if (!onOpenInTransform && !canDeleteInTransform) return null;
  return (
    <div className="transform-fusion-workflow-pane__actions">
      {openInTransformError ? (
        <div className="disc-banner--error" role="alert">
          {t("status.error", { detail: openInTransformError })}
        </div>
      ) : null}
      {deleteInTransformError ? (
        <div className="disc-banner--error" role="alert">
          {t("status.error", { detail: deleteInTransformError })}
        </div>
      ) : null}
      {onOpenInTransform ? (
        <button
          type="button"
          className="disc-btn disc-btn--primary"
          disabled={openInTransformBusy || deleteInTransformBusy}
          onClick={onOpenInTransform}
          title={t("wfViewer.openInTransformHint")}
        >
          {openInTransformBusy ? t("wfViewer.openInTransformBusy") : t("wfViewer.openInTransform")}
        </button>
      ) : null}
      {canDeleteInTransform ? (
        <button
          type="button"
          className="disc-btn disc-btn--ghost"
          disabled={deleteInTransformBusy || openInTransformBusy}
          onClick={onDeleteInTransform}
          title={t("transform.pipelines.delete")}
        >
          {deleteInTransformBusy ? t("transform.toolbar.reloading") : t("transform.pipelines.delete")}
        </button>
      ) : null}
    </div>
  );
}

export function TransformFusionWorkflowPane({
  tab,
  onTabUpdate,
  onOpenInTransform,
  onDeleteInTransform,
  openInTransformBusy = false,
  openInTransformError = null,
}: Props) {
  const { t } = useAppSettings();
  const [canvas, setCanvas] = useState<TransformCanvasDocument>(emptyTransformCanvasDocument());
  const [workflowId, setWorkflowId] = useState<string | null>(null);
  const [deleteInTransformBusy, setDeleteInTransformBusy] = useState(false);
  const [deleteInTransformError, setDeleteInTransformError] = useState<string | null>(null);
  const [deleteRefreshNonce, setDeleteRefreshNonce] = useState(0);
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
      setDeleteInTransformError(null);
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
  }, [tab.id, tab.workflow.external_id, deleteRefreshNonce]);

  const onCanvasChange = useCallback((doc: TransformCanvasDocument) => {
    setCanvas(doc);
  }, []);

  const deleteLocalWorkflow = useCallback(async () => {
    if (!workflowId) return;
    if (!window.confirm(t("transform.pipelines.deleteConfirm", { name: tab.label }))) return;
    setDeleteInTransformError(null);
    setDeleteInTransformBusy(true);
    try {
      await deleteTransformWorkflow(workflowId);
      onDeleteInTransform?.();
      setDeleteRefreshNonce((n) => n + 1);
    } catch (e) {
      setDeleteInTransformError(`${t("transform.pipelines.deleteFailed")}: ${String(e)}`);
    } finally {
      setDeleteInTransformBusy(false);
    }
  }, [onDeleteInTransform, t, tab.label, workflowId]);

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
        onDeleteInTransform={() => void deleteLocalWorkflow()}
        canDeleteInTransform={Boolean(workflowId)}
        deleteInTransformBusy={deleteInTransformBusy}
        deleteInTransformError={deleteInTransformError}
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
