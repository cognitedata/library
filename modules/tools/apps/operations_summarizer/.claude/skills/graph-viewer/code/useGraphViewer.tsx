import { useDune } from "@cognite/dune";
import { useCallback, useMemo, useRef, useState } from "react";
import type { GraphCanvasRef } from "reagraph";

import { useGraphSelection } from "./useGraphSelection";
import { useNodeBuffer } from "./useNodeBuffer";
import { useGraphDataPipeline } from "./useGraphDataPipeline";
import { buildReagraphTheme, mergeVisualConfig } from "./graph-config";
import { fetchConnectedNodes } from "./graph-service";
import type { GraphNode, GraphEdge, LayoutType } from "./types";
import { createInstanceId, parseInstanceId } from "./types";

import { useDataModelLoader } from "./useDataModelLoader";
import { useSeedNode } from "./useSeedNode";
import { GraphViewerCanvas } from "./GraphViewerCanvas";
import {
  DEFAULT_LITE_FEATURES,
  type LiteFeatureFlags,
  type UseGraphViewerConfig,
  type UseGraphViewerReturn,
} from "./types";

/**
 * `useGraphViewer` -- the single entry point for embedding a CDF graph viewer.
 *
 * Returns a self-contained `<GraphCanvas>` component plus state and controls.
 *
 * @example
 * ```tsx
 * const { GraphCanvas, isLoading, error } = useGraphViewer({
 *   dataModel: { space: "my-space", externalId: "my-dm", version: "1" },
 *   instance: { space: "my-inst-space", externalId: "pump-001" },
 * });
 *
 * return <GraphCanvas className="h-full w-full" />;
 * ```
 */
export function useGraphViewer(config: UseGraphViewerConfig): UseGraphViewerReturn {
  const { sdk } = useDune();

  const opts = config.options ?? {};
  const maxNodes = opts.maxNodes ?? 1000;
  const initialConnectionLimit = opts.initialConnectionLimit ?? 100;
  const features: LiteFeatureFlags = { ...DEFAULT_LITE_FEATURES, ...opts.features };

  const whitelistedRelationProps = useMemo(
    () =>
      opts.whitelistedRelationProps
        ? new Set(opts.whitelistedRelationProps)
        : undefined,
    [JSON.stringify(opts.whitelistedRelationProps)]
  );

  const visualConfig = useMemo(
    () => mergeVisualConfig(opts.visualConfig),
    [opts.visualConfig]
  );

  const themeConfig = useMemo(
    () => buildReagraphTheme(opts.themeConfig),
    [opts.themeConfig]
  );

  const [layout, setLayout] = useState<LayoutType>(opts.layout ?? "forceDirected2d");

  const [selections, setSelections] = useState<string[]>([]);
  const [selectedNodeType, setSelectedNodeType] = useState<string | null>(null);
  const {
    selectedNode,
    selectedEdge,
    selectNode,
    selectEdge,
    clearSelection,
  } = useGraphSelection();

  const {
    nodes: bufferNodes,
    edges: bufferEdges,
    addNodes,
    addEdges,
    touchNode,
    clear: clearBuffer,
  } = useNodeBuffer(maxNodes);

  const {
    dataModel,
    isLoading: isDataModelLoading,
    error: dataModelError,
  } = useDataModelLoader(config.dataModel);

  const {
    isLoading: isSeedLoading,
    error: seedError,
    loadInstance,
  } = useSeedNode({
    dataModel,
    initialInstance: config.instance,
    addNodes,
    addEdges,
    clearBuffer,
    whitelistedRelationProps,
    coreReverseQueries: opts.coreReverseQueries,
    viewPriorityConfig: opts.viewPriorityConfig,
    initialConnectionLimit,
  });

  const graphRef = useRef<GraphCanvasRef>(null);

  const {
    graphData,
    displayedGraphData,
    reagraphNodes,
    reagraphEdges,
    displayedStats,
  } = useGraphDataPipeline({
    bufferNodes,
    bufferConnections: bufferEdges,
    visualConfig,
  });

  const [isExpanding, setIsExpanding] = useState(false);

  const expandNode = useCallback(
    async (nodeId: string) => {
      if (!sdk || !dataModel) return;

      try {
        setIsExpanding(true);
        const { space, externalId } = parseInstanceId(nodeId);
        const existingIds = new Set(
          bufferNodes.map((n) => createInstanceId(n.space, n.externalId))
        );

        const result = await fetchConnectedNodes(
          sdk,
          space,
          externalId,
          existingIds,
          dataModel,
          initialConnectionLimit,
          whitelistedRelationProps,
          opts.coreReverseQueries,
          opts.viewPriorityConfig
        );

        addNodes(result.newNodes);
        addEdges(result.newEdges);
        touchNode(nodeId);
      } finally {
        setIsExpanding(false);
      }
    },
    [
      sdk,
      dataModel,
      bufferNodes,
      addNodes,
      addEdges,
      touchNode,
      initialConnectionLimit,
      whitelistedRelationProps,
      opts.coreReverseQueries,
      opts.viewPriorityConfig,
    ]
  );

  const handleNodeClick = useCallback(
    (node: GraphNode) => {
      selectNode(node);
      setSelections([node.id]);
      setSelectedNodeType(null);
      touchNode(node.id);
    },
    [selectNode, setSelections, touchNode]
  );

  const handleEdgeClick = useCallback(
    (edge: GraphEdge) => {
      selectEdge(edge);
      setSelections([edge.id]);
      setSelectedNodeType(null);
    },
    [selectEdge, setSelections]
  );

  const handleCanvasClick = useCallback(() => {
    clearSelection();
    setSelections([]);
    setSelectedNodeType(null);
  }, [clearSelection]);

  const handleNodeTypeClick = useCallback(
    (typeKey: string) => {
      if (selectedNodeType === typeKey) {
        setSelectedNodeType(null);
        setSelections([]);
        clearSelection();
      } else {
        const nodeIds = displayedGraphData.nodes
          .filter((n) => {
            const key = n.data?.type
              ? `${n.data.type.space}:${n.data.type.externalId}`
              : "unknown";
            return key === typeKey;
          })
          .map((n) => n.id);
        setSelectedNodeType(typeKey);
        setSelections(nodeIds);
        clearSelection();
      }
    },
    [selectedNodeType, displayedGraphData.nodes, clearSelection]
  );

  const handleClearNodeTypeSelection = useCallback(() => {
    setSelectedNodeType(null);
    setSelections([]);
  }, []);

  const GraphCanvasComponent = useMemo(() => {
    const Component: React.FC<{ className?: string }> = ({ className }) => (
      <GraphViewerCanvas
        reagraphNodes={reagraphNodes}
        reagraphEdges={reagraphEdges}
        displayedGraphData={displayedGraphData}
        layout={layout}
        theme={themeConfig}
        selections={selections}
        selectedNode={selectedNode}
        selectedEdge={selectedEdge}
        features={features}
        selectedNodeType={selectedNodeType}
        graphRef={graphRef}
        onNodeClick={handleNodeClick}
        onEdgeClick={handleEdgeClick}
        onCanvasClick={handleCanvasClick}
        onExpandNode={expandNode}
        onNodeTypeClick={handleNodeTypeClick}
        onClearNodeTypeSelection={handleClearNodeTypeSelection}
        className={className}
      />
    );
    Component.displayName = "GraphViewerCanvas";
    return Component;
  }, [
    reagraphNodes,
    reagraphEdges,
    displayedGraphData,
    layout,
    themeConfig,
    selections,
    selectedNode,
    selectedEdge,
    features,
    selectedNodeType,
    graphRef,
    handleNodeClick,
    handleEdgeClick,
    handleCanvasClick,
    expandNode,
    handleNodeTypeClick,
    handleClearNodeTypeSelection,
  ]);

  const isLoading = isDataModelLoading || isSeedLoading || isExpanding;
  const error = dataModelError || seedError;

  return {
    GraphCanvas: GraphCanvasComponent,
    isLoading,
    error,
    graphData,
    stats: displayedStats,
    layout,
    setLayout,
    selections,
    setSelections,
    selectedNode,
    selectedEdge,
    expandNode,
    loadInstance,
    fitView: () => graphRef.current?.fitNodesInView(),
    zoomIn: () => graphRef.current?.zoomIn(),
    zoomOut: () => graphRef.current?.zoomOut(),
    clear: clearBuffer,
    graphRef,
  };
}
