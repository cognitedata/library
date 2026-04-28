import { useEffect, useMemo } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import { normalizeWorkflowCanvasHandleOrientation, type WorkflowCanvasDocument } from "../../types/workflowCanvas";
import { canvasToFlowEdges, canvasToFlowNodes } from "./flowDocumentBridge";
import { FlowHandleOrientationProvider } from "./FlowHandleOrientationContext";
import { KEA_FLOW_NODE_TYPES } from "./flowNodeRegistry";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

export type WorkflowPreviewLocalRun = {
  runAll: boolean;
  onRunAllChange: (next: boolean) => void;
  busy: boolean;
  executingTaskIds: readonly string[];
  onRun: () => void;
};

type Props = {
  t: TFn;
  document: WorkflowCanvasDocument;
  reloadNonce: number;
  onEdit: () => void;
  localRun?: WorkflowPreviewLocalRun;
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
  executingTaskIds,
}: {
  doc: WorkflowCanvasDocument;
  reloadNonce: number;
  executingTaskIds: readonly string[];
}) {
  const [nodes, setNodes, onNodesChange] = useNodesState(canvasToFlowNodes(doc.nodes));
  const [edges, setEdges, onEdgesChange] = useEdgesState(canvasToFlowEdges(doc.edges));

  useEffect(() => {
    const active = new Set(executingTaskIds);
    const nextNodes = canvasToFlowNodes(doc.nodes).map((n) => {
      if (!active.has(n.id)) return n;
      return {
        ...n,
        className: "kea-flow-node--executing",
      };
    });
    setNodes(nextNodes);
    setEdges(canvasToFlowEdges(doc.edges));
  }, [doc, reloadNonce, executingTaskIds, setNodes, setEdges]);

  const graphRevision = useMemo(
    () => reloadNonce + doc.nodes.length + doc.edges.length + executingTaskIds.length,
    [reloadNonce, doc.nodes.length, doc.edges.length, executingTaskIds.length]
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
        defaultEdgeOptions={{ animated: true }}
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

export function WorkflowFlowCanvasPreview({ t, document: doc, reloadNonce, onEdit, localRun }: Props) {
  return (
    <div className="kea-flow-preview">
      <div className="kea-flow-preview__toolbar">
        <p className="kea-hint kea-flow-preview__toolbar-hint">{t("flow.canvasPreviewHint")}</p>
        <button
          type="button"
          className="kea-btn kea-btn--primary"
          disabled={Boolean(localRun?.busy)}
          onClick={onEdit}
        >
          {t("flow.editWorkflow")}
        </button>
      </div>
      {localRun ? (
        <div className="kea-flow-preview__runbar" role="group" aria-label={t("flow.previewRunLocal")}>
          <button
            type="button"
            className="kea-btn kea-btn--primary"
            disabled={localRun.busy}
            onClick={() => localRun.onRun()}
          >
            {localRun.busy ? t("status.running") : t("flow.previewRunLocal")}
          </button>
          <label className="kea-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.35rem" }}>
            <input
              type="checkbox"
              checked={localRun.runAll}
              disabled={localRun.busy}
              onChange={(e) => localRun.onRunAllChange(e.target.checked)}
            />
            <span>{t("run.runAll")}</span>
          </label>
          <p className="kea-hint kea-flow-preview__runbar-hint">{t("flow.previewRunLocalHint")}</p>
        </div>
      ) : null}
      <div className="kea-flow-preview__canvas">
        <ReactFlowProvider>
          <PreviewInner
            key={reloadNonce}
            doc={doc}
            reloadNonce={reloadNonce}
            executingTaskIds={localRun?.executingTaskIds ?? []}
          />
        </ReactFlowProvider>
      </div>
    </div>
  );
}
