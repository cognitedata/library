import type { Edge } from "@xyflow/react";

/** Applied to edges incident on the inspector-selected node. */
export const FLOW_EDGE_CONNECTED_CLASS = "disc-flow-edge--connected";

function withoutConnectedClass(className: string | undefined): string | undefined {
  if (!className) return undefined;
  const parts = className.split(/\s+/).filter((c) => c && c !== FLOW_EDGE_CONNECTED_CLASS);
  return parts.length > 0 ? parts.join(" ") : undefined;
}

function withConnectedClass(className: string | undefined): string {
  const parts = new Set(
    (className ?? "")
      .split(/\s+/)
      .filter((c) => c && c !== FLOW_EDGE_CONNECTED_CLASS)
  );
  parts.add(FLOW_EDGE_CONNECTED_CLASS);
  return [...parts].join(" ");
}

/** Highlight edges whose source or target is `selectedNodeId`. */
export function highlightEdgesConnectedToNode<E extends Edge>(
  edges: readonly E[],
  selectedNodeId: string | null | undefined
): E[] {
  return edges.map((edge) => {
    const connected =
      selectedNodeId != null &&
      selectedNodeId.length > 0 &&
      (edge.source === selectedNodeId || edge.target === selectedNodeId);
    const className = connected
      ? withConnectedClass(edge.className)
      : withoutConnectedClass(edge.className);
    if (className === edge.className) return edge;
    return { ...edge, className };
  });
}
