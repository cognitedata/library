/** Canonical Discovery tree node ids (mirror ``ui/server/tree_node_ids.py``). */

export const CONNECTION_ROOT = "connection";
export const DATA_ROOT = "data";
export const DATA_SAVED_QUERIES = "data:sq";
export const FUSION_ROOT = "fusion";
export const FUSION_DM_ROOT = "fusion:dm";
export const FUSION_INTEGRATION_ROOT = "fusion:integration";
export const GOVERNANCE_ROOT = "gov";

export function dedupeNodeIds(nodeIds: readonly string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const nodeId of nodeIds) {
    const nid = nodeId.trim();
    if (!nid || seen.has(nid)) continue;
    seen.add(nid);
    out.push(nid);
  }
  return out;
}
