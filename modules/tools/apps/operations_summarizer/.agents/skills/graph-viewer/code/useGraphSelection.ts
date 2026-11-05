import { useCallback, useMemo, useState } from "react";
import type { GraphEdge, GraphNode, GraphSelection } from "./types";

export interface UseGraphSelectionReturn {
  selection: GraphSelection;
  selectedNode: GraphNode | null;
  selectedEdge: GraphEdge | null;
  selectNode: (node: GraphNode | null) => void;
  selectEdge: (edge: GraphEdge | null) => void;
  clearSelection: () => void;
  isNodeSelected: (nodeId: string) => boolean;
  isEdgeSelected: (edgeId: string) => boolean;
}

export function useGraphSelection(): UseGraphSelectionReturn {
  const [selection, setSelection] = useState<GraphSelection>({
    type: null,
    id: null,
    node: undefined,
    edge: undefined,
  });

  const selectNode = useCallback((node: GraphNode | null) => {
    if (node) {
      setSelection({
        type: "node",
        id: node.id,
        node,
        edge: undefined,
      });
    } else {
      setSelection({
        type: null,
        id: null,
        node: undefined,
        edge: undefined,
      });
    }
  }, []);

  const selectEdge = useCallback((edge: GraphEdge | null) => {
    if (edge) {
      setSelection({
        type: "edge",
        id: edge.id,
        node: undefined,
        edge,
      });
    } else {
      setSelection({
        type: null,
        id: null,
        node: undefined,
        edge: undefined,
      });
    }
  }, []);

  const clearSelection = useCallback(() => {
    setSelection({
      type: null,
      id: null,
      node: undefined,
      edge: undefined,
    });
  }, []);

  const isNodeSelected = useCallback(
    (nodeId: string) => selection.type === "node" && selection.id === nodeId,
    [selection]
  );

  const isEdgeSelected = useCallback(
    (edgeId: string) => selection.type === "edge" && selection.id === edgeId,
    [selection]
  );

  const selectedNode = useMemo(
    () => (selection.type === "node" ? (selection.node ?? null) : null),
    [selection]
  );

  const selectedEdge = useMemo(
    () => (selection.type === "edge" ? (selection.edge ?? null) : null),
    [selection]
  );

  return {
    selection,
    selectedNode,
    selectedEdge,
    selectNode,
    selectEdge,
    clearSelection,
    isNodeSelected,
    isEdgeSelected,
  };
}
