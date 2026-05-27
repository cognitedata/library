import { useCallback, useEffect, useRef, useState } from "react";
import { fetchTransformPipelineByWorkflow } from "../../api";
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
};

export function TransformFusionWorkflowPane({ tab, onTabUpdate }: Props) {
  const { t } = useAppSettings();
  const [canvas, setCanvas] = useState<TransformCanvasDocument>(emptyTransformCanvasDocument());
  const [pipelineId, setPipelineId] = useState<string | null>(null);
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
      setPipelineId(null);
      try {
        const { pipeline_id, pipeline } = await fetchTransformPipelineByWorkflow(workflowExternalId);
        if (cancelled || gen !== loadGen.current || tabRef.current.id !== expectedTabId) return;
        const nextCanvas = pipeline.canvas ?? emptyTransformCanvasDocument();
        setPipelineId(pipeline_id);
        setCanvas(nextCanvas);
        setReloadNonce((n) => n + 1);
        setResolved(true);
        onTabUpdateRef.current({ ...tabRef.current, loading: false, error: null });
      } catch {
        if (cancelled || gen !== loadGen.current || tabRef.current.id !== expectedTabId) return;
        setPipelineId(null);
        setUseCdfGraphFallback(true);
        setResolved(true);
        onTabUpdateRef.current({ ...tabRef.current, loading: false, error: null });
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
        <WorkflowFlowPane tab={tab} onTabUpdate={onTabUpdate} readOnly />
      </div>
    );
  }

  return (
    <div className="transform-fusion-workflow-pane">
      <p className="transform-fusion-workflow-pane__hint" role="status">
        {t("transform.fusionWorkflow.readOnlyHint", { externalId: tab.workflow.external_id })}
      </p>
      <TransformFlowPanel
        t={t}
        pipelineId={pipelineId ?? undefined}
        initialDocument={canvas}
        reloadNonce={reloadNonce}
        readOnly
        onChange={onCanvasChange}
      />
    </div>
  );
}
