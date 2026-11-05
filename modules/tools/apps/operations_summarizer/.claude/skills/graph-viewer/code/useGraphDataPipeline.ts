import { useMemo } from "react";
import type {
  CDFEdge,
  CDFNode,
  GraphData,
  GraphEdge,
  GraphNode,
  GraphVisualConfig,
  NodeTypeInfo,
} from "./types";
import { createInstanceId, getNodeLabel } from "./types";
import { getGraphStats } from "./graph-service";
import { transformEdges, transformNodes } from "./graph-config";

interface UseGraphDataPipelineParams {
  bufferNodes: CDFNode[];
  bufferConnections: CDFEdge[];
  visualConfig: GraphVisualConfig;
}

export function useGraphDataPipeline({
  bufferNodes,
  bufferConnections,
  visualConfig,
}: UseGraphDataPipelineParams) {
  const graphData: GraphData = useMemo(() => {
    const typeMap = new Map<string, NodeTypeInfo>();
    const colorMap = new Map<string, string>();
    let colorIndex = 0;

    const graphNodes: GraphNode[] = bufferNodes.map((node) => {
      const typeKey = node.type ? `${node.type.space}:${node.type.externalId}` : "unknown";

      if (node.type) {
        if (!typeMap.has(typeKey)) {
          const color =
            visualConfig.nodeTypePalette[colorIndex % visualConfig.nodeTypePalette.length] ||
            visualConfig.defaultNodeColor;
          typeMap.set(typeKey, {
            externalId: node.type.externalId,
            space: node.type.space,
            color,
            count: 1,
          });
          colorMap.set(typeKey, color);
          colorIndex += 1;
        } else {
          const existing = typeMap.get(typeKey);
          if (existing) existing.count += 1;
        }
      }

      const fill = node.type
        ? (colorMap.get(typeKey) ?? visualConfig.defaultNodeColor)
        : visualConfig.defaultNodeColor;

      return {
        id: createInstanceId(node.space, node.externalId),
        label: getNodeLabel(node),
        fill,
        data: node,
      };
    });

    const graphConnections: GraphEdge[] = bufferConnections.map((edge) => ({
      id: createInstanceId(edge.space, edge.externalId),
      source: createInstanceId(edge.startNode.space, edge.startNode.externalId),
      target: createInstanceId(edge.endNode.space, edge.endNode.externalId),
      label: edge.type?.externalId || "",
      data: edge,
    }));

    return {
      nodes: graphNodes,
      connections: graphConnections,
      nodeTypes: Array.from(typeMap.values()),
    };
  }, [bufferNodes, bufferConnections, visualConfig]);

  const reagraphNodes = useMemo(
    () => transformNodes(graphData.nodes, visualConfig),
    [graphData.nodes, visualConfig]
  );

  const emptyHighlights = useMemo(() => new Set<string>(), []);

  const reagraphEdges = useMemo(
    () => transformEdges(graphData.connections, emptyHighlights, visualConfig),
    [graphData.connections, emptyHighlights, visualConfig]
  );

  const displayedStats = useMemo(() => getGraphStats(graphData), [graphData]);

  return {
    graphData,
    displayedGraphData: graphData,
    reagraphNodes,
    reagraphEdges,
    displayedStats,
  };
}
