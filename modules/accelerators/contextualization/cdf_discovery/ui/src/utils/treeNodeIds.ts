/** Canonical Discovery tree node ids (mirror ``ui/server/tree_node_ids.py``). */

export const CONNECTION_ROOT = "connection";
export const DATA_ROOT = "data";
export const DATA_SAVED_QUERIES = "data:sq";
export const TRANSFORM_ROOT = "transform";
export const TRANSFORM_PIPELINES = "transform:pipelines";
export const TRANSFORM_PIPELINE_PREFIX = "transform:pipeline:";
export const TRANSFORM_TEMPLATES = "transform:templates";
export const TRANSFORM_TEMPLATE_PREFIX = "transform:template:";
export const TRANSFORM_WORKFLOW_PREFIX = "transform:workflow:";
export const FUSION_ROOT = "fusion";
export const FUSION_DM_ROOT = "fusion:dm";
export const FUSION_SPACES = "fusion:spaces";
export const FUSION_ADMIN = "fusion:admin";
export const FUSION_GROUPS = "fusion:admin:groups";
export const FUSION_INTEGRATION_ROOT = "fusion:integration";
export const GOVERNANCE_ROOT = "gov";
export const GOVERNANCE_SPACES = "gov:spaces";
export const GOVERNANCE_GROUPS = "gov:groups";
export const EXTRACT_ROOT = "extract";
export const MONITOR_ROOT = "monitor";

/** Sibling order under ``connection`` (tree root label shows the CDF project). */
export const CONNECTION_ROOT_CHILD_ORDER = [
  DATA_ROOT,
  FUSION_ROOT,
  GOVERNANCE_ROOT,
  EXTRACT_ROOT,
  TRANSFORM_ROOT,
  MONITOR_ROOT,
] as const;

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
