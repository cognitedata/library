/**
 * Preview-only: animate edges whose target node's task is active or completed
 * (local streamed run progress).
 */
export function runProgressAnimatedEdgeIds(
  edges: readonly { id: string; target: string }[],
  activeCanvasNodeIds: readonly string[],
  completedCanvasNodeIds: readonly string[]
): Set<string> {
  const hot = new Set<string>([...activeCanvasNodeIds, ...completedCanvasNodeIds]);
  if (hot.size === 0) return new Set();
  const out = new Set<string>();
  for (const e of edges) {
    if (hot.has(e.target)) out.add(e.id);
  }
  return out;
}
