import { useEffect } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
} from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import { normalizeWorkflowCanvasHandleOrientation, type WorkflowCanvasDocument } from "../../types/workflowCanvas";
import { canvasToFlowEdges, canvasToFlowNodes } from "./flowDocumentBridge";
import { FlowHandleOrientationProvider } from "./FlowHandleOrientationContext";
import { KEA_FLOW_NODE_TYPES } from "./flowNodeRegistry";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  document: WorkflowCanvasDocument;
  reloadNonce: number;
  onEdit: () => void;
};

function PreviewInner({ doc, reloadNonce }: { doc: WorkflowCanvasDocument; reloadNonce: number }) {
  const [nodes, setNodes, onNodesChange] = useNodesState(canvasToFlowNodes(doc.nodes));
  const [edges, setEdges, onEdgesChange] = useEdgesState(canvasToFlowEdges(doc.edges));

  useEffect(() => {
    setNodes(canvasToFlowNodes(doc.nodes));
    setEdges(canvasToFlowEdges(doc.edges));
  }, [doc, reloadNonce, setNodes, setEdges]);

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
        proOptions={{ hideAttribution: true }}
      >
        <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
        <Controls />
        <MiniMap zoomable pannable className="kea-flow-minimap" />
      </ReactFlow>
    </FlowHandleOrientationProvider>
  );
}

export function WorkflowFlowCanvasPreview({ t, document: doc, reloadNonce, onEdit }: Props) {
  return (
    <div className="kea-flow-preview">
      <div className="kea-flow-preview__toolbar">
        <p className="kea-hint" style={{ margin: 0, flex: "1 1 auto" }}>
          {t("flow.canvasPreviewHint")}
        </p>
        <button type="button" className="kea-btn kea-btn--primary" onClick={onEdit}>
          {t("flow.editCanvas")}
        </button>
      </div>
      <div className="kea-flow-preview__canvas">
        <ReactFlowProvider>
          <PreviewInner key={reloadNonce} doc={doc} reloadNonce={reloadNonce} />
        </ReactFlowProvider>
      </div>
    </div>
  );
}
