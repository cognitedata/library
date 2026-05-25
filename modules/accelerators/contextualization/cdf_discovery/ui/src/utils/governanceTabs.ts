import type { TreeNode } from "../types/discoveryNodes";
import { GOVERNANCE_GROUPS, GOVERNANCE_ROOT, GOVERNANCE_SPACES } from "./treeNodeIds";

/** Live CDF space/group detail tabs (under Fusion → Spaces / Groups). */
export function opensGovernanceCdfDetailTab(node: TreeNode): boolean {
  return node.kind === "gov_space" || node.kind === "gov_group";
}

/** Tree nodes that open a governance workspace tab (scope, configure, build, artifacts). */
export function opensGovernanceTab(node: TreeNode): boolean {
  if (node.id === GOVERNANCE_ROOT || node.id === GOVERNANCE_SPACES || node.id === GOVERNANCE_GROUPS) {
    return true;
  }
  if (node.meta?.live_cdf === true) {
    return false;
  }
  const ws = node.meta?.governance_workspace;
  if (ws === "scope" || ws === "spaces" || ws === "groups") {
    return true;
  }
  return node.kind === "gov_artifact_file";
}
