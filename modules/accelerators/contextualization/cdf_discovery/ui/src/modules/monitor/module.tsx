import type { ReactNode } from "react";
import { WorkflowStateDashboardPane } from "../../components/monitor/WorkflowStateDashboardPane";
import type { DiscoveryModule, OpenNodeContext, RenderTabContext } from "../../shell/discoveryShell";
import type { DocumentTab, TreeNode } from "../../types/discoveryNodes";
import { MONITOR_ROOT } from "../../utils/treeNodeIds";
import { createMonitorTab, opensMonitorTab } from "../../utils/workspaceTabs";
import { isMonitorTab } from "../../types/discoveryNodes";

export const monitorModule: DiscoveryModule = {
  id: "monitor",
  treeRootId: MONITOR_ROOT,
  labelKey: "tree.desc.monitor",

  ownsNode(node: TreeNode): boolean {
    return opensMonitorTab(node);
  },

  tryOpenNode(node: TreeNode, ctx: OpenNodeContext): boolean {
    if (!opensMonitorTab(node)) return false;
    const id = MONITOR_ROOT;
    ctx.setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === id);
      if (existing) {
        ctx.setActiveTabId(id);
        return prev;
      }
      const tab = createMonitorTab(ctx.t("tree.monitor"));
      ctx.setActiveTabId(id);
      return [...prev, tab];
    });
    ctx.setRowDetail(null);
    return true;
  },

  ownsTab(tab: DocumentTab): boolean {
    return isMonitorTab(tab);
  },

  renderTab(tab: DocumentTab, ctx: RenderTabContext): ReactNode | null {
    if (isMonitorTab(tab)) {
      return (
        <WorkflowStateDashboardPane
          activeSection={tab.activeSection}
          onActiveSectionChange={(next) =>
            ctx.setTabs((prev) =>
              prev.map((row) =>
                row.id === tab.id && isMonitorTab(row) ? { ...row, activeSection: next } : row
              )
            )
          }
        />
      );
    }
    return null;
  },
};
