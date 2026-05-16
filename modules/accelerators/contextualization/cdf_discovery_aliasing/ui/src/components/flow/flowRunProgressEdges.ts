/**
 * Preview-only: animate edges whose target node's task is active or completed
 * (local streamed run progress).
 */
import type { WorkflowCanvasDocument } from "../../types/workflowCanvas";

export function collectStartCanvasNodeIdsOnAnyPathToTarget(
  doc: WorkflowCanvasDocument,
  targetCanvasId: string
): string[] {
  const tid = (targetCanvasId ?? "").trim();
  if (!tid) return [];
  const byId = new Map(doc.nodes.map((n) => [n.id, n]));
  if (!byId.has(tid)) return [];

  const predecessors = new Map<string, string[]>();
  for (const e of doc.edges) {
    const s = String(e.source ?? "").trim();
    const t = String(e.target ?? "").trim();
    if (!s || !t) continue;
    const arr = predecessors.get(t);
    if (arr) arr.push(s);
    else predecessors.set(t, [s]);
  }

  const found = new Set<string>();
  const seen = new Set<string>();
  const stack = [tid];
  while (stack.length) {
    const cur = stack.pop()!;
    if (seen.has(cur)) continue;
    seen.add(cur);
    const node = byId.get(cur);
    if (node?.kind === "start") found.add(cur);
    for (const p of predecessors.get(cur) ?? []) {
      if (!seen.has(p)) stack.push(p);
    }
  }
  return [...found];
}

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
