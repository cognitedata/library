import { useCallback, useRef } from "react";
import {
  GraphCanvas as ReagraphCanvas,
  type GraphCanvasRef,
  type LayoutTypes,
} from "reagraph";
import type { Theme } from "reagraph";
import type { GraphData, GraphEdge, GraphNode, LayoutType } from "./types";
import { ZoomControls } from "./ZoomControls";
import { GraphViewerLegend } from "./GraphViewerLegend";
import { useCanvasResize } from "./useCanvasResize";
import type { LiteFeatureFlags } from "./types";

const LAYOUT_MAP: Record<LayoutType, LayoutTypes> = {
  forceDirected2d: "forceDirected2d",
  forceDirected3d: "forceDirected3d",
  treeTd2d: "treeTd2d",
  treeLr2d: "treeLr2d",
  radialOut2d: "radialOut2d",
  circular2d: "circular2d",
};

const DOUBLE_CLICK_MS = 300;

export interface GraphViewerCanvasProps {
  reagraphNodes: Array<{
    id: string;
    label: string;
    fill: string;
    icon: string;
    data: GraphNode;
  }>;
  reagraphEdges: Array<{
    id: string;
    source: string;
    target: string;
    label?: string;
    fill?: string;
    size?: number;
    data: GraphEdge;
  }>;
  displayedGraphData: GraphData;
  layout: LayoutType;
  theme: Theme;
  selections: string[];
  selectedNode: GraphNode | null;
  selectedEdge: GraphEdge | null;
  features: LiteFeatureFlags;
  selectedNodeType: string | null;
  graphRef: React.RefObject<GraphCanvasRef | null>;
  onNodeClick: (node: GraphNode) => void;
  onEdgeClick: (edge: GraphEdge) => void;
  onCanvasClick: () => void;
  onExpandNode: (nodeId: string) => void;
  onNodeTypeClick: (typeKey: string) => void;
  onClearNodeTypeSelection: () => void;
  className?: string;
}

export function GraphViewerCanvas({
  reagraphNodes,
  reagraphEdges,
  displayedGraphData,
  layout,
  theme,
  selections,
  features,
  selectedNodeType,
  graphRef,
  onNodeClick,
  onEdgeClick,
  onCanvasClick,
  onExpandNode,
  onNodeTypeClick,
  onClearNodeTypeSelection,
  className,
}: GraphViewerCanvasProps) {
  const canvasContainerRef = useRef<HTMLDivElement>(null);
  const lastClickedIdRef = useRef<string | null>(null);
  const lastClickTimeRef = useRef(0);
  const pendingClickRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useCanvasResize(canvasContainerRef, graphRef as React.RefObject<GraphCanvasRef>);

  const handleNodeClick = useCallback(
    (node: { id: string }) => {
      const graphNode = displayedGraphData.nodes.find((n) => n.id === node.id);
      if (!graphNode) return;

      const now = Date.now();
      const isDoubleClick =
        lastClickedIdRef.current === node.id &&
        now - lastClickTimeRef.current < DOUBLE_CLICK_MS;

      lastClickedIdRef.current = node.id;
      lastClickTimeRef.current = now;

      if (isDoubleClick && features.enableNodeExpansion) {
        if (pendingClickRef.current) {
          clearTimeout(pendingClickRef.current);
          pendingClickRef.current = null;
        }
        onExpandNode(node.id);
        return;
      }

      pendingClickRef.current = setTimeout(() => {
        pendingClickRef.current = null;
        onNodeClick(graphNode);
      }, DOUBLE_CLICK_MS);
    },
    [displayedGraphData.nodes, onNodeClick, onExpandNode, features.enableNodeExpansion]
  );

  const handleEdgeClick = useCallback(
    (edge: { id: string }) => {
      const graphEdge = displayedGraphData.connections.find((c) => c.id === edge.id);
      if (graphEdge) {
        onEdgeClick(graphEdge);
      }
    },
    [displayedGraphData.connections, onEdgeClick]
  );

  const hasNodes = reagraphNodes.length > 0;

  return (
    <div
      ref={canvasContainerRef}
      className={`relative w-full h-full min-h-0 min-w-0 ${className ?? ""}`}
    >
      {hasNodes && (
        <ReagraphCanvas
          ref={graphRef}
          nodes={reagraphNodes}
          edges={reagraphEdges}
          layoutType={LAYOUT_MAP[layout]}
          theme={theme}
          labelType="nodes"
          edgeLabelPosition="natural"
          edgeArrowPosition="end"
          draggable
          animated
          onNodeClick={handleNodeClick}
          onEdgeClick={handleEdgeClick}
          onCanvasClick={onCanvasClick}
          selections={selections}
          cameraMode={layout.includes("3d") ? "rotate" : "pan"}
        />
      )}

      {!hasNodes && (
        <div className="absolute inset-0 flex items-center justify-center text-muted-foreground text-sm">
          No nodes to display
        </div>
      )}

      {features.enableZoomControls && hasNodes && (
        <ZoomControls
          onZoomIn={() => graphRef.current?.zoomIn()}
          onZoomOut={() => graphRef.current?.zoomOut()}
          onFitView={() => graphRef.current?.fitNodesInView()}
        />
      )}

      {features.enableLegend &&
        displayedGraphData.nodeTypes.length > 0 && (
          <GraphViewerLegend
            nodeTypes={displayedGraphData.nodeTypes}
            selectedNodeType={selectedNodeType}
            onNodeTypeClick={onNodeTypeClick}
            onClearSelection={onClearNodeTypeSelection}
          />
        )}
    </div>
  );
}
