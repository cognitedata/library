import type { TreeNode } from "../types/discoveryNodes";
import { opensGovernanceCdfDetailTab, opensGovernanceTab } from "./governanceTabs";
import { canQueryTreeNode } from "./sqlQuerySeed";
import { opensTransformTab } from "./transformTabs";
import { opensExtractTab, opensMonitorTab } from "./workspaceTabs";

function opensDocumentTab(node: TreeNode): boolean {
  return (
    node.kind === "dm_data_model" ||
    node.kind === "workflow" ||
    node.kind === "transformation" ||
    node.kind === "function" ||
    node.kind === "saved_query" ||
    opensGovernanceTab(node) ||
    opensTransformTab(node) ||
    opensExtractTab(node) ||
    opensMonitorTab(node)
  );
}

/** True when the context menu should show **Open** (not query-only leaves). */
export function treeNodeOpensDocumentTab(node: TreeNode): boolean {
  if (opensDocumentTab(node) || opensGovernanceCdfDetailTab(node)) return true;
  return node.kind === "record_stream" && node.open_target?.type === "record_stream";
}

/** True when double-click / Open should invoke ``onOpenNode``. */
export function treeNodeOpenable(node: TreeNode): boolean {
  return treeNodeOpensDocumentTab(node) || canQueryTreeNode(node);
}
