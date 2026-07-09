import type { ReactNode } from "react";
import { ComingSoonPane } from "../../components/ComingSoonPane";
import type { DiscoveryModule, OpenNodeContext, RenderTabContext } from "../../shell/discoveryShell";
import type { DocumentTab, TreeNode } from "../../types/discoveryNodes";
import { EXTRACT_ROOT } from "../../utils/treeNodeIds";
import { createExtractTab, opensExtractTab } from "../../utils/workspaceTabs";
import { isExtractTab } from "../../types/discoveryNodes";

export const extractModule: DiscoveryModule = {
  id: "extract",
  treeRootId: EXTRACT_ROOT,
  labelKey: "tree.desc.extract",

  ownsNode(node: TreeNode): boolean {
    return opensExtractTab(node);
  },

  tryOpenNode(node: TreeNode, ctx: OpenNodeContext): boolean {
    if (!opensExtractTab(node)) return false;
    const id = EXTRACT_ROOT;
    ctx.setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === id);
      if (existing) {
        ctx.setActiveTabId(id);
        return prev;
      }
      const tab = createExtractTab(ctx.t("tree.extract"));
      ctx.setActiveTabId(id);
      return [...prev, tab];
    });
    ctx.setRowDetail(null);
    return true;
  },

  ownsTab(tab: DocumentTab): boolean {
    return isExtractTab(tab);
  },

  renderTab(tab: DocumentTab, _ctx: RenderTabContext): ReactNode | null {
    if (isExtractTab(tab)) {
      return <ComingSoonPane workspace="extract" />;
    }
    return null;
  },
};
