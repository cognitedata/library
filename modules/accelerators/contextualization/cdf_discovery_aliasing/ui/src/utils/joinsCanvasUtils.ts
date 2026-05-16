import type { WorkflowCanvasDocument, WorkflowCanvasNode } from "../types/workflowCanvas";

export function listJoinNodes(canvas: WorkflowCanvasDocument): WorkflowCanvasNode[] {
  return canvas.nodes.filter((n) => n.kind === "join");
}

export function joinNodeListLabel(node: WorkflowCanvasNode): string {
  const cfg = node.data?.config;
  if (cfg && typeof cfg === "object" && !Array.isArray(cfg)) {
    const d = (cfg as Record<string, unknown>).description;
    if (d != null && String(d).trim()) return String(d).trim();
  }
  return node.data?.label != null ? String(node.data.label) : node.id;
}
