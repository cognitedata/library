import type { TreeNode } from "../types/discoveryNodes";
import { GOVERNANCE_ROOT } from "./treeNodeIds";

/** Tree nodes that open a governance document tab (scope, configure, live CDF, artifacts). */
export function opensGovernanceTab(node: TreeNode): boolean {
  if (
    node.id === GOVERNANCE_ROOT ||
    node.id === "gov:spaces" ||
    node.id === "gov:groups"
  ) {
    return true;
  }
  const ws = node.meta?.governance_workspace;
  if (ws === "scope" || ws === "spaces" || ws === "groups") {
    return true;
  }
  return (
    node.kind === "gov_space" ||
    node.kind === "gov_group" ||
    node.kind === "gov_artifact_file"
  );
}
