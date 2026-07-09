import type { ReactNode } from "react";
import { GovernanceCdfGroupPane } from "../../components/governance/GovernanceCdfGroupPane";
import { GovernanceCdfSpacePane } from "../../components/governance/GovernanceCdfSpacePane";
import { GovernanceGroupsPane } from "../../components/governance/GovernanceGroupsPane";
import { GovernanceScopePane } from "../../components/governance/GovernanceScopePane";
import { GovernanceSpacesPane } from "../../components/governance/GovernanceSpacesPane";
import type { DiscoveryModule, OpenNodeContext, RenderTabContext } from "../../shell/discoveryShell";
import type {
  DocumentTab,
  GovernanceCdfGroupDocumentTab,
  GovernanceCdfSpaceDocumentTab,
  TreeNode,
} from "../../types/discoveryNodes";
import { opensGovernanceCdfDetailTab, opensGovernanceTab } from "../../utils/governanceTabs";
import { GOVERNANCE_ROOT } from "../../utils/treeNodeIds";
import {
  isGovernanceCdfGroupTab,
  isGovernanceCdfSpaceTab,
  isGovernanceGroupsTab,
  isGovernanceScopeTab,
  isGovernanceSpacesTab,
} from "../../types/discoveryNodes";

export const governanceModule: DiscoveryModule = {
  id: "governance",
  treeRootId: GOVERNANCE_ROOT,
  labelKey: "tree.desc.gov",

  ownsNode(node: TreeNode): boolean {
    return opensGovernanceTab(node) || opensGovernanceCdfDetailTab(node);
  },

  tryOpenNode(node: TreeNode, ctx: OpenNodeContext): boolean {
    const ws = node.meta?.governance_workspace as string | undefined;
    if (ws === "scope" || node.id === "gov") {
      const id = "gov:scope";
      ctx.setTabs((prev) => {
        const existing = prev.find((tab) => tab.id === id);
        if (existing) {
          ctx.setActiveTabId(id);
          return prev;
        }
        ctx.setActiveTabId(id);
        return [...prev, { kind: "governance_scope", id, label: ctx.t("governance.subtab.scope") }];
      });
      ctx.setRowDetail(null);
      return true;
    }
    if (ws === "spaces" || node.id === "gov:spaces") {
      ctx.openGovernanceWorkspaceTab("spaces", "configure");
      ctx.setRowDetail(null);
      return true;
    }
    if (ws === "groups" || node.id === "gov:groups") {
      ctx.openGovernanceWorkspaceTab("groups", "configure");
      ctx.setRowDetail(null);
      return true;
    }
    if (node.kind === "gov_artifact_file") {
      const rel = String(node.meta?.artifact_rel ?? "");
      const workspace = (node.meta?.governance_workspace as "spaces" | "groups") ?? "spaces";
      ctx.openGovernanceWorkspaceTab(workspace, "artifacts", rel);
      ctx.setRowDetail(null);
      return true;
    }
    if (node.kind === "gov_space") {
      const space = String(node.meta?.space ?? "");
      if (!space) return false;
      const id = `gov:space:tab:${space}`;
      ctx.setTabs((prev) => {
        const existing = prev.find((tab) => tab.id === id);
        if (existing) {
          ctx.setActiveTabId(id);
          return prev;
        }
        const tab: GovernanceCdfSpaceDocumentTab = {
          kind: "governance_cdf_space",
          id,
          label: node.label,
          space,
          detail: null,
          loading: true,
          error: null,
        };
        ctx.setActiveTabId(id);
        return [...prev, tab];
      });
      ctx.setRowDetail(null);
      return true;
    }
    if (node.kind === "gov_group") {
      const gid = node.meta?.id;
      const groupId = typeof gid === "number" ? gid : Number(gid);
      if (!Number.isFinite(groupId)) return false;
      const id = `gov:group:tab:${groupId}`;
      ctx.setTabs((prev) => {
        const existing = prev.find((tab) => tab.id === id);
        if (existing) {
          ctx.setActiveTabId(id);
          return prev;
        }
        const tab: GovernanceCdfGroupDocumentTab = {
          kind: "governance_cdf_group",
          id,
          label: node.label,
          groupId,
          detail: null,
          loading: true,
          error: null,
        };
        ctx.setActiveTabId(id);
        return [...prev, tab];
      });
      ctx.setRowDetail(null);
      return true;
    }
    return false;
  },

  ownsTab(tab: DocumentTab): boolean {
    return (
      isGovernanceScopeTab(tab) ||
      isGovernanceSpacesTab(tab) ||
      isGovernanceGroupsTab(tab) ||
      isGovernanceCdfSpaceTab(tab) ||
      isGovernanceCdfGroupTab(tab)
    );
  },

  renderTab(tab: DocumentTab, ctx: RenderTabContext): ReactNode | null {
    if (isGovernanceScopeTab(tab)) {
      return <GovernanceScopePane />;
    }
    if (isGovernanceSpacesTab(tab)) {
      return (
        <GovernanceSpacesPane
          initialSubTab={tab.activeSubTab as "configure"}
          initialArtifactRel={tab.artifactRel}
        />
      );
    }
    if (isGovernanceGroupsTab(tab)) {
      return (
        <GovernanceGroupsPane
          initialSubTab={tab.activeSubTab as "configure"}
          initialArtifactRel={tab.artifactRel}
        />
      );
    }
    if (isGovernanceCdfSpaceTab(tab)) {
      return (
        <GovernanceCdfSpacePane
          tab={tab}
          onTabUpdate={(updated) =>
            ctx.setTabs((prev) => prev.map((row) => (row.id === updated.id ? updated : row)))
          }
        />
      );
    }
    if (isGovernanceCdfGroupTab(tab)) {
      return (
        <GovernanceCdfGroupPane
          tab={tab}
          onTabUpdate={(updated) =>
            ctx.setTabs((prev) => prev.map((row) => (row.id === updated.id ? updated : row)))
          }
        />
      );
    }
    return null;
  },
};
