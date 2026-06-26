import type { IndexDocumentTab, OverviewSubTab, WorkspaceState } from "../types/indexWorkspace";
import { tabIdForKind } from "./indexTabs";

export function serializeWorkspace(
  tabs: IndexDocumentTab[],
  activeTabId: string | null
): WorkspaceState {
  return {
    active_tab_id: activeTabId,
    tabs: tabs.map((tab) => ({ ...tab })),
  };
}

const OVERVIEW_TAB_ID = tabIdForKind("overview");

export function restoreWorkspaceTabs(workspace: WorkspaceState): {
  tabs: IndexDocumentTab[];
  activeTabId: string | null;
  overviewSubTab: OverviewSubTab;
} {
  const rawTabs = (workspace.tabs ?? []).filter(
    (tab): tab is IndexDocumentTab =>
      typeof tab?.id === "string" &&
      typeof tab?.kind === "string" &&
      typeof tab?.label === "string" &&
      typeof tab?.navNodeId === "string"
  );

  let overviewSubTab: OverviewSubTab = "summary";
  const tabs: IndexDocumentTab[] = [];

  for (const tab of rawTabs) {
    if ((tab.kind as string) === "config") {
      overviewSubTab = "configuration";
      continue;
    }
    if (tab.kind === "overview") {
      if (tabs.some((existing) => existing.id === OVERVIEW_TAB_ID)) continue;
      tabs.push(tab);
      continue;
    }
    tabs.push(tab);
  }

  if (overviewSubTab === "configuration" && !tabs.some((tab) => tab.id === OVERVIEW_TAB_ID)) {
    tabs.unshift({
      id: OVERVIEW_TAB_ID,
      kind: "overview",
      label: "Overview",
      navNodeId: "inverted-index/overview",
    });
  }

  let activeTabId =
    workspace.active_tab_id && tabs.some((t) => t.id === workspace.active_tab_id)
      ? workspace.active_tab_id
      : tabs[0]?.id ?? null;

  if (workspace.active_tab_id === "tab:config") {
    activeTabId = OVERVIEW_TAB_ID;
    overviewSubTab = "configuration";
  }

  return { tabs, activeTabId, overviewSubTab };
}
