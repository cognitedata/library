import type { Node } from "@xyflow/react";
import type { WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { newNodeId, orderFlowNodesForReactFlow } from "./flowDocumentBridge";
import { absoluteNodeRect, nodeFlowSize } from "./flowNodeGeometry";
import { assignFlowNodeSubflowParent } from "./subflowDropAssociation";
import { isWrapGroupableNodeType } from "./subflowMembership";

const HEADER = 40;
const PAD = 20;
const GAP = 24;
const MIN_SUB_W = 200;
const MIN_SUB_H = 140;

function parentDepth(nodes: Node[], id: string): number {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  let d = 0;
  let cur: string | undefined = id;
  const guard = new Set<string>();
  while (cur && !guard.has(cur)) {
    guard.add(cur);
    const p: string | undefined = byId.get(cur)?.parentId;
    if (!p) break;
    d++;
    cur = p;
  }
  return d;
}

/**
 * Detach selected nodes to absolute coordinates (deepest first), insert a new ``keaSubflow`` frame
 * (organizational grouping only — no boundary ports or graph I/O hubs), grid-layout members inside
 * the frame, and parent them under the subflow.
 *
 * ``selected`` should list the nodes to wrap (e.g. from ``resolveGroupableSelectionNodes``); they
 * need not have ``selected: true`` on the React Flow node when coming from box selection.
 */
export function wrapSelectionInNewSubflow(nodes: Node[], selected: Node[]): Node[] | null {
  const groupable = selected.filter((n) => isWrapGroupableNodeType(n.type));
  if (groupable.length < 1) return null;

  const selectedIds = new Set(groupable.map((n) => n.id));

  let next: Node[] = [...nodes];

  const sortedDetach = [...groupable].sort((a, b) => parentDepth(nodes, b.id) - parentDepth(nodes, a.id));
  for (const n of sortedDetach) {
    const cur = next.find((x) => x.id === n.id);
    if (cur?.parentId) {
      next = assignFlowNodeSubflowParent(next, n.id, null);
    }
  }

  const fresh = (id: string) => next.find((x) => x.id === id);
  const members = groupable
    .map((g) => fresh(g.id))
    .filter((x): x is Node => Boolean(x));

  const sizes = members.map((m) => nodeFlowSize(m));
  const maxW = Math.max(...sizes.map((s) => s.w), 120);
  const maxH = Math.max(...sizes.map((s) => s.h), 56);
  const n = members.length;
  const cols = Math.ceil(Math.sqrt(n));
  const rows = Math.ceil(n / cols);
  const cellW = maxW + GAP;
  const cellH = maxH + GAP;
  const innerW = cols * cellW - GAP;
  const innerH = rows * cellH - GAP;
  const laneLeft = PAD;
  const subW = Math.max(MIN_SUB_W, PAD * 2 + innerW);
  const subH = Math.max(MIN_SUB_H, HEADER + PAD * 2 + innerH);

  let minX = Infinity;
  let minY = Infinity;
  for (const m of members) {
    const r = absoluteNodeRect(next, m);
    minX = Math.min(minX, r.x);
    minY = Math.min(minY, r.y);
  }
  if (!Number.isFinite(minX) || !Number.isFinite(minY)) return null;

  const subId = newNodeId();
  const subAbsX = minX - PAD;
  const subAbsY = minY - PAD;

  const subflowData: WorkflowCanvasNodeData = {
    label: "Subflow",
  };

  const subflowNode: Node = {
    id: subId,
    type: "keaSubflow",
    position: { x: subAbsX, y: subAbsY },
    data: { ...subflowData } as Record<string, unknown>,
    style: { width: subW, height: subH },
  };

  const ordered = [...members].sort((a, b) => a.id.localeCompare(b.id));
  const updatedMembers: Node[] = ordered.map((m, i) => {
    const col = i % cols;
    const row = Math.floor(i / cols);
    const relX = laneLeft + col * cellW;
    const relY = HEADER + PAD + row * cellH;
    return {
      ...m,
      parentId: subId,
      extent: "parent" as const,
      expandParent: true,
      position: { x: relX, y: relY },
    };
  });

  const without = next.filter((x) => !selectedIds.has(x.id));
  return orderFlowNodesForReactFlow([...without, subflowNode, ...updatedMembers]);
}
