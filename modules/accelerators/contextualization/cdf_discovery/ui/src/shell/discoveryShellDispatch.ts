import type { ReactNode } from "react";
import type { DocumentTab, TreeNode } from "../types/discoveryNodes";
import type { DiscoveryModule, OpenNodeContext, RenderTabContext } from "./discoveryShell";

export function openModuleNode(
  node: TreeNode,
  ctx: OpenNodeContext,
  modules: readonly DiscoveryModule[]
): boolean {
  for (const mod of modules) {
    if (mod.ownsNode(node) && mod.tryOpenNode(node, ctx)) {
      return true;
    }
  }
  return false;
}

export function renderModuleTab(
  tab: DocumentTab,
  ctx: RenderTabContext,
  modules: readonly DiscoveryModule[]
): ReactNode | null {
  for (const mod of modules) {
    if (mod.ownsTab(tab)) {
      const rendered = mod.renderTab(tab, ctx);
      if (rendered != null) {
        return rendered;
      }
    }
  }
  return null;
}
