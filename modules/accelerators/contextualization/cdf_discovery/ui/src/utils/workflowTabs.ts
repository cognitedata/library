import type { TreeNode, WorkflowRef } from "../types/discoveryNodes";

export function workflowRefFromNode(node: TreeNode): WorkflowRef | null {
  if (node.kind !== "workflow" || !node.meta) return null;
  const ext = node.meta.external_id;
  if (typeof ext !== "string" || !ext.trim()) return null;
  const name = typeof node.meta.name === "string" ? node.meta.name : undefined;
  return { external_id: ext.trim(), name };
}

export function workflowTabKey(ref: WorkflowRef): string {
  const ver = ref.version?.trim();
  return ver ? `wf:${ref.external_id}:${ver}` : `wf:${ref.external_id}`;
}

export function workflowTabLabel(ref: WorkflowRef): string {
  const base = ref.external_id;
  return ref.name?.trim() ? `${ref.name.trim()} — ${base}` : base;
}
