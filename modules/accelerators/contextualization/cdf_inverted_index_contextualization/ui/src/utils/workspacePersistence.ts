import type { IndexDocumentTab, WorkspaceState } from "../types/indexWorkspace";
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

const DASHBOARD_TAB_ID = tabIdForKind("dashboard");
const CONFIG_TAB_ID = tabIdForKind("configuration");

function migrateTab(tab: IndexDocumentTab): IndexDocumentTab | null {
  if ((tab.kind as string) === "config" || (tab.kind as string) === "overview") {
    return {
      id: DASHBOARD_TAB_ID,
      kind: "dashboard",
      label: tab.label,
      navNodeId: "inverted-index/dashboard",
    };
  }
  return tab;
}

export function restoreWorkspaceTabs(workspace: WorkspaceState): {
  tabs: IndexDocumentTab[];
  activeTabId: string | null;
} {
  const rawTabs = (workspace.tabs ?? []).filter(
    (tab): tab is IndexDocumentTab =>
      typeof tab?.id === "string" &&
      typeof tab?.kind === "string" &&
      typeof tab?.label === "string" &&
      typeof tab?.navNodeId === "string"
  );

  let openConfigFromLegacy = false;
  const tabs: IndexDocumentTab[] = [];

  for (const tab of rawTabs) {
    if ((tab.kind as string) === "config") {
      openConfigFromLegacy = true;
      continue;
    }
    const migrated = migrateTab(tab);
    if (!migrated) {
      tabs.push(tab);
      continue;
    }
    if (migrated.id === DASHBOARD_TAB_ID && tabs.some((existing) => existing.id === DASHBOARD_TAB_ID)) {
      continue;
    }
    tabs.push(migrated);
  }

  if (openConfigFromLegacy && !tabs.some((tab) => tab.id === CONFIG_TAB_ID)) {
    tabs.push({
      id: CONFIG_TAB_ID,
      kind: "configuration",
      label: "Configuration",
      navNodeId: "inverted-index/config",
    });
  }

  let activeTabId =
    workspace.active_tab_id && tabs.some((t) => t.id === workspace.active_tab_id)
      ? workspace.active_tab_id
      : tabs[0]?.id ?? null;

  if (workspace.active_tab_id === "tab:config" || workspace.active_tab_id === "tab:overview") {
    activeTabId = openConfigFromLegacy ? CONFIG_TAB_ID : DASHBOARD_TAB_ID;
  }

  return { tabs, activeTabId };
}
