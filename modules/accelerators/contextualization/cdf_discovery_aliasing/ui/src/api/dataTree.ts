import type { TreeNode } from "../types/dataTree";

export async function fetchDataTreeChildren(nodeId: string, signal?: AbortSignal) {
  const r = await fetch(
    `/api/cdf/data-tree/children?${new URLSearchParams({ node_id: nodeId })}`,
    { signal }
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ nodes: TreeNode[]; stars?: string[] }>;
}
