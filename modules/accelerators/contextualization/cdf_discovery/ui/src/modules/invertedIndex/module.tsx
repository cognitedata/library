import type { ReactNode } from "react";
import type { TreeNode } from "../../types/discoveryNodes";
import { INDEX_ROOT } from "../../utils/treeNodeIds";
import type { DiscoveryModule, OpenNodeContext, RenderTabContext } from "../../shell/discoveryShell";
import { ConfigPane } from "./components/ConfigPane";
import { DashboardPane } from "./components/DashboardPane";
import { BuildAnnotationsPane } from "./components/BuildAnnotationsPane";
import { BuildMetadataPane } from "./components/BuildMetadataPane";
import { FileContextPane } from "./components/FileContextPane";
import { QueryPane } from "./components/QueryPane";
import { TagReusePane } from "./components/TagReusePane";
import { TargetDrivenPane } from "./components/TargetDrivenPane";
import { isInvertedIndexTab } from "./types";
import {
  createInvertedIndexTab,
  invertedIndexKindFromNode,
  isInvertedIndexBuildAnnotationsTab,
  isInvertedIndexBuildMetadataTab,
  isInvertedIndexConfigurationTab,
  isInvertedIndexDashboardTab,
  isInvertedIndexFileContextTab,
  isInvertedIndexQueryTab,
  isInvertedIndexTagReuseTab,
  isInvertedIndexTargetDrivenTab,
  tabIdForInvertedIndexKind,
} from "./utils/indexTabs";

export const invertedIndexModule: DiscoveryModule = {
  id: "inverted_index",
  treeRootId: INDEX_ROOT,
  labelKey: "tree.desc.index",

  ownsNode(node: TreeNode): boolean {
    if (node.id === INDEX_ROOT || node.id.startsWith("index:")) return true;
    return invertedIndexKindFromNode(node) != null;
  },

  tryOpenNode(node: TreeNode, ctx: OpenNodeContext): boolean {
    const tabKind = invertedIndexKindFromNode(node);
    if (!tabKind) return false;
    const id = tabIdForInvertedIndexKind(tabKind);
    const label = node.label?.trim() || tabKind;
    ctx.setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === id);
      if (existing) {
        ctx.setActiveTabId(id);
        return prev;
      }
      const tab = createInvertedIndexTab(tabKind, label, node.id);
      ctx.setActiveTabId(id);
      return [...prev, tab];
    });
    ctx.setRowDetail(null);
    return true;
  },

  ownsTab(tab): boolean {
    return isInvertedIndexTab(tab);
  },

  renderTab(tab, _ctx: RenderTabContext): ReactNode | null {
    if (!isInvertedIndexTab(tab)) return null;
    if (isInvertedIndexDashboardTab(tab)) {
      return <DashboardPane refreshKey={0} />;
    }
    if (isInvertedIndexConfigurationTab(tab)) {
      return <ConfigPane />;
    }
    if (isInvertedIndexBuildMetadataTab(tab)) {
      return <BuildMetadataPane />;
    }
    if (isInvertedIndexBuildAnnotationsTab(tab)) {
      return <BuildAnnotationsPane />;
    }
    if (isInvertedIndexTargetDrivenTab(tab)) {
      return <TargetDrivenPane />;
    }
    if (isInvertedIndexQueryTab(tab)) {
      return <QueryPane onSelectRow={() => undefined} />;
    }
    if (isInvertedIndexFileContextTab(tab)) {
      return <FileContextPane onSelectRow={() => undefined} />;
    }
    if (isInvertedIndexTagReuseTab(tab)) {
      return <TagReusePane onSelectRow={() => undefined} />;
    }
    return null;
  },
};
