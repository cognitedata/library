import { useCallback, useEffect, useMemo, useState } from "react";
import {
  applyEdgeChanges,
  applyNodeChanges,
  Background,
  BackgroundVariant,
  Controls,
  type EdgeChange,
  MiniMap,
  type NodeChange,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
} from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import { normalizeWorkflowCanvasHandleOrientation, type WorkflowCanvasDocument } from "../../types/workflowCanvas";
import { canvasToFlowEdges, canvasToFlowNodes } from "./flowDocumentBridge";
import { FlowHandleOrientationProvider } from "./FlowHandleOrientationContext";
import { KEA_FLOW_NODE_TYPES } from "./flowNodeRegistry";
import { runProgressAnimatedEdgeIds } from "./flowRunProgressEdges";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

/** Live local-run progress for canvas preview (controls live in Configure main toolbar). */
export type WorkflowPreviewRunProgress = {
  busy: boolean;
  /** Canvas node ids for tasks currently running (outline on graph). */
  executingCanvasNodeIds: readonly string[];
  /** Canvas node ids for tasks that have started but not yet ended (run progress). */
  runActiveCanvasNodeIds: readonly string[];
  /** Canvas node ids for tasks that have completed this run (run progress). */
  runCompletedCanvasNodeIds: readonly string[];
  /** Canvas node ids whose last streamed local run task ended with ``status: failed`` (red outline). */
  failedCanvasNodeIds: readonly string[];
};

type Props = {
  t: TFn;
  document: WorkflowCanvasDocument;
  reloadNonce: number;
  onEdit: () => void;
  runProgress?: WorkflowPreviewRunProgress;
};

/** v12 ``fitView`` prop runs mainly on mount; refit after nodes/edges sync so the graph is not off-screen. */
function PreviewFitView({ graphRevision }: { graphRevision: number }) {
  const { fitView } = useReactFlow();
  useEffect(() => {
    const id = window.requestAnimationFrame(() => {
      fitView({ padding: 0.12, duration: 0 });
    });
    return () => window.cancelAnimationFrame(id);
  }, [graphRevision, fitView]);
  return null;
}

function PreviewInner({
  doc,
  reloadNonce,
  executingCanvasNodeIds,
  runActiveCanvasNodeIds,
  runCompletedCanvasNodeIds,
  failedCanvasNodeIds,
}: {
  doc: WorkflowCanvasDocument;
  reloadNonce: number;
  executingCanvasNodeIds: readonly string[];
  runActiveCanvasNodeIds: readonly string[];
  runCompletedCanvasNodeIds: readonly string[];
  failedCanvasNodeIds: readonly string[];
}) {
  /**
   * Keep RF-internal node/edge state separate from execution styling. React Flow applies
   * ``onNodesChange`` updates (e.g. dimensions) that would strip a naïve ``useEffect`` className.
   */
  const [rfNodes, setRfNodes] = useState(() => canvasToFlowNodes(doc.nodes));
  const [rfEdges, setRfEdges] = useState(() => canvasToFlowEdges(doc.edges));

  useEffect(() => {
    setRfNodes(canvasToFlowNodes(doc.nodes));
    setRfEdges(canvasToFlowEdges(doc.edges));
  }, [doc, reloadNonce]);

  const progressAnimatedEdgeIds = useMemo(
    () =>
      runProgressAnimatedEdgeIds(doc.edges, runActiveCanvasNodeIds, runCompletedCanvasNodeIds),
    [doc.edges, runActiveCanvasNodeIds, runCompletedCanvasNodeIds]
  );

  const executingSet = useMemo(() => new Set(executingCanvasNodeIds), [executingCanvasNodeIds]);

  const completedSet = useMemo(() => new Set(runCompletedCanvasNodeIds), [runCompletedCanvasNodeIds]);

  const failedSet = useMemo(() => new Set(failedCanvasNodeIds), [failedCanvasNodeIds]);

  const nodes = useMemo(
    () =>
      rfNodes.map((n) => {
        const failed = failedSet.has(n.id);
        const executing = executingSet.has(n.id);
        const completed = completedSet.has(n.id);
        let className: string | undefined;
        if (failed) className = "kea-flow-node--run-failed";
        else if (executing) className = "kea-flow-node--executing";
        else if (completed) className = "kea-flow-node--run-completed";
        return className ? { ...n, className } : { ...n, className: undefined };
      }),
    [rfNodes, failedSet, executingSet, completedSet]
  );

  const edges = useMemo(
    () =>
      rfEdges.map((e) => ({
        ...e,
        animated: progressAnimatedEdgeIds.has(e.id),
      })),
    [rfEdges, progressAnimatedEdgeIds]
  );

  const onNodesChange = useCallback((changes: NodeChange[]) => {
    setRfNodes((prev) => applyNodeChanges(changes, prev));
  }, []);

  const onEdgesChange = useCallback((changes: EdgeChange[]) => {
    setRfEdges((prev) => applyEdgeChanges(changes, prev));
  }, []);

  const graphRevision = useMemo(
    () =>
      reloadNonce +
      doc.nodes.length +
      doc.edges.length +
      executingCanvasNodeIds.length +
      runActiveCanvasNodeIds.length +
      runCompletedCanvasNodeIds.length +
      failedCanvasNodeIds.length,
    [
      reloadNonce,
      doc.nodes.length,
      doc.edges.length,
      executingCanvasNodeIds.length,
      runActiveCanvasNodeIds.length,
      runCompletedCanvasNodeIds.length,
      failedCanvasNodeIds.length,
    ]
  );

  const orient = normalizeWorkflowCanvasHandleOrientation(doc.handle_orientation);
  return (
    <FlowHandleOrientationProvider value={orient}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={KEA_FLOW_NODE_TYPES}
        defaultEdgeOptions={{ animated: false }}
        nodesDraggable={false}
        nodesConnectable={false}
        edgesReconnectable={false}
        elementsSelectable={false}
        panOnScroll
        zoomOnScroll
        zoomOnPinch
        minZoom={0.2}
        maxZoom={1.5}
        deleteKeyCode={null}
        fitView
        onInit={(inst) => {
          window.requestAnimationFrame(() => inst.fitView({ padding: 0.12, duration: 0 }));
        }}
        proOptions={{ hideAttribution: true }}
      >
        <PreviewFitView graphRevision={graphRevision} />
        <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
        <Controls />
        <MiniMap zoomable pannable className="kea-flow-minimap" />
      </ReactFlow>
    </FlowHandleOrientationProvider>
  );
}

export function WorkflowFlowCanvasPreview({ t, document: doc, reloadNonce, onEdit, runProgress }: Props) {
  return (
    <div className="kea-flow-preview">
      <div className="kea-flow-preview__toolbar">
        <p className="kea-hint kea-flow-preview__toolbar-hint">{t("flow.canvasPreviewHint")}</p>
        <button
          type="button"
          className="kea-btn kea-btn--primary"
          disabled={Boolean(runProgress?.busy)}
          onClick={onEdit}
        >
          {t("flow.editWorkflow")}
        </button>
      </div>
      <div className="kea-flow-preview__canvas">
        <ReactFlowProvider>
          <PreviewInner
            key={reloadNonce}
            doc={doc}
            reloadNonce={reloadNonce}
            executingCanvasNodeIds={runProgress?.executingCanvasNodeIds ?? []}
            runActiveCanvasNodeIds={runProgress?.runActiveCanvasNodeIds ?? []}
            runCompletedCanvasNodeIds={runProgress?.runCompletedCanvasNodeIds ?? []}
            failedCanvasNodeIds={runProgress?.failedCanvasNodeIds ?? []}
          />
        </ReactFlowProvider>
      </div>
    </div>
  );
}
