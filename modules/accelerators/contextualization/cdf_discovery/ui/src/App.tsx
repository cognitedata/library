import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { fetchConnection, type ConnectionInfo } from "./api";
import { DocumentTabBar } from "./components/DocumentTabBar";
import { DataModelFlowPane } from "./components/DataModelFlowPane";
import { WorkflowFlowPane } from "./components/WorkflowFlowPane";
import { ObjectDiscovery } from "./components/ObjectDiscovery";
import { PanelDragHandle } from "./components/PanelDragHandle";
import { PanelDropOverlay } from "./components/PanelDropOverlay";
import { PropertiesPanel, type PropertiesPanelLayout } from "./components/PropertiesPanel";
import {
  useDiscoveryPanelLayout,
  type TreePanelSide,
} from "./hooks/useDiscoveryPanelLayout";
import { SqlQueryPane } from "./components/SqlQueryPane";
import { FunctionPane } from "./components/FunctionPane";
import { TransformationPane } from "./components/TransformationPane";
import { GovernanceScopePane } from "./components/governance/GovernanceScopePane";
import { GovernanceSpacesPane } from "./components/governance/GovernanceSpacesPane";
import { GovernanceGroupsPane } from "./components/governance/GovernanceGroupsPane";
import { GovernanceCdfSpacePane } from "./components/governance/GovernanceCdfSpacePane";
import { GovernanceCdfGroupPane } from "./components/governance/GovernanceCdfGroupPane";
import { CogniteLogo } from "./components/CogniteLogo";
import { useAppSettings } from "./context/AppSettingsContext";
import { useDiscoveryConfig } from "./context/DiscoveryConfigContext";
import { LOCALES } from "./i18n";
import {
  isDataModelTab,
  isFunctionTab,
  isSqlTab,
  isTransformationTab,
  isWorkflowTab,
  isGovernanceScopeTab,
  isGovernanceSpacesTab,
  isGovernanceGroupsTab,
  isGovernanceCdfSpaceTab,
  isGovernanceCdfGroupTab,
  type DataModelDocumentTab,
  type GovernanceSubTab,
  type GovernanceSpacesDocumentTab,
  type GovernanceGroupsDocumentTab,
  type GovernanceCdfSpaceDocumentTab,
  type GovernanceCdfGroupDocumentTab,
  type DataModelGraphView,
  type DocumentTab,
  type FunctionDocumentTab,
  type WorkflowDocumentTab,
  type TransformationDocumentTab,
  type OpenTarget,
  type SqlDocumentTab,
  type TreeNode,
} from "./types/discoveryNodes";
import { dataModelTabKey, dataModelTabLabel, dataModelRefFromNode } from "./utils/dataModelTabs";
import {
  workflowRefFromNode,
  workflowTabKey,
  workflowTabLabel,
} from "./utils/workflowTabs";
import {
  createFunctionTab,
  functionIdFromNode,
  functionLabelFromMeta,
  functionTabKey,
} from "./utils/functionTabs";
import {
  createTransformationTab,
  transformationIdFromNode,
  transformationLabelFromMeta,
  transformationTabKey,
} from "./utils/transformationTabs";
import {
  createSqlTab,
  createSqlTabForOpenTarget,
  createFileContentSqlTab,
  SQL_WORKSPACE_TAB_ID,
} from "./utils/sqlTabs";
import { canQueryTreeNode, labelForDmView, openTargetForDmView } from "./utils/sqlQuerySeed";
import { fileContentRefFromRow } from "./utils/queryableFileFromRow";
import {
  createSqlTabFromSavedQuery,
  savedQueryEntryFromSqlTab,
  savedQueryFromNode,
  slugifySavedQueryId,
  sqlSavedQueryTabId,
  uniqueSavedQueryId,
} from "./utils/savedQueries";
import { opensGovernanceTab } from "./utils/governanceTabs";
import { restoreWorkspaceTabs, serializeWorkspace } from "./utils/workspacePersistence";
import type { SavedQuery } from "./types/discoveryNodes";

export function App() {
  const { t, theme, setTheme, locale, setLocale } = useAppSettings();
  const {
    workspace,
    persistWorkspace,
    loading: configLoading,
    savedQueries,
    savedQueriesRevision,
    persistSavedQueries,
  } = useDiscoveryConfig();
  const [connection, setConnection] = useState<ConnectionInfo | null>(null);
  const [connError, setConnError] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [governanceArtifactsRevision, setGovernanceArtifactsRevision] = useState<{
    token: number;
    workspace: "spaces" | "groups";
  }>({ token: 0, workspace: "spaces" });
  const [selectedNode, setSelectedNode] = useState<TreeNode | null>(null);
  const [tabs, setTabs] = useState<DocumentTab[]>([]);
  const [activeTabId, setActiveTabId] = useState<string | null>(null);
  const [rowDetail, setRowDetail] = useState<unknown | null>(null);
  const panel = useDiscoveryPanelLayout();
  const workspaceRestored = useRef(false);

  const openSqlWorkspace = useCallback(() => {
    setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === SQL_WORKSPACE_TAB_ID);
      if (existing) {
        setActiveTabId(SQL_WORKSPACE_TAB_ID);
        return prev;
      }
      const tab = createSqlTab({ id: SQL_WORKSPACE_TAB_ID, label: t("sql.title") });
      setActiveTabId(tab.id);
      return [...prev, tab];
    });
    setRowDetail(null);
  }, [t]);

  useEffect(() => {
    if (configLoading || workspaceRestored.current) return;
    workspaceRestored.current = true;
    if (workspace.tabs.length) {
      const restored = restoreWorkspaceTabs(workspace, t("sql.title"));
      if (restored.tabs.length) {
        setTabs(restored.tabs);
        setActiveTabId(restored.activeTabId);
        return;
      }
    }
    openSqlWorkspace();
  }, [configLoading, workspace, t, openSqlWorkspace]);

  useEffect(() => {
    if (!workspaceRestored.current) return;
    const timer = window.setTimeout(() => {
      void persistWorkspace(serializeWorkspace(tabs, activeTabId)).catch(() => {
        /* best-effort; config errors surface in DiscoveryConfigContext */
      });
    }, 500);
    return () => window.clearTimeout(timer);
  }, [tabs, activeTabId, persistWorkspace]);

  const openSavedQuery = useCallback((query: SavedQuery) => {
    const tab = createSqlTabFromSavedQuery(query);
    setTabs((prev) => {
      const existing = prev.find((t) => t.id === tab.id);
      if (existing && isSqlTab(existing)) {
        setActiveTabId(tab.id);
        return prev.map((t) =>
          t.id === tab.id
            ? {
                ...existing,
                query: tab.query,
                label: tab.label,
                limit: tab.limit,
                convertToString: tab.convertToString,
                savedQueryId: tab.savedQueryId,
              }
            : t
        );
      }
      setActiveTabId(tab.id);
      return [...prev, tab];
    });
    setRowDetail(null);
  }, []);

  const saveSqlTab = useCallback(
    async (tab: SqlDocumentTab, mode: "save" | "saveAs") => {
      const existingIds = new Set(savedQueries.map((q) => q.id));
      let id = tab.savedQueryId;
      let name = tab.label.trim() || t("sql.title");

      if (mode === "saveAs" || !id) {
        const prompted = window.prompt(t("sql.saveAsPrompt"), name);
        if (prompted == null) return;
        name = prompted.trim();
        if (!name) return;
        const base = slugifySavedQueryId(name);
        id = uniqueSavedQueryId(base, existingIds);
      }

      const entry = savedQueryEntryFromSqlTab(tab, name, id!);
      const next =
        tab.savedQueryId && id === tab.savedQueryId
          ? savedQueries.map((q) => (q.id === id ? entry : q))
          : [...savedQueries.filter((q) => q.id !== id), entry];
      await persistSavedQueries(next);

      const newTabId = sqlSavedQueryTabId(id);
      setTabs((prev) =>
        prev.map((t) =>
          t.id === tab.id
            ? {
                ...(t as SqlDocumentTab),
                id: newTabId,
                label: name,
                savedQueryId: id,
              }
            : t
        )
      );
      setActiveTabId((current) => (current === tab.id ? newTabId : current));
    },
    [persistSavedQueries, savedQueries, t]
  );

  const deleteSavedQuery = useCallback(
    async (query: SavedQuery) => {
      if (!window.confirm(t("discovery.deleteSavedQueryConfirm", { name: query.name }))) {
        return;
      }
      const tabId = sqlSavedQueryTabId(query.id);
      await persistSavedQueries(savedQueries.filter((q) => q.id !== query.id));
      setTabs((prev) => {
        const next = prev.filter(
          (t) => t.id !== tabId && !(isSqlTab(t) && t.savedQueryId === query.id)
        );
        setActiveTabId((cur) => {
          if (!cur || next.some((tab) => tab.id === cur)) return cur;
          return next[next.length - 1]?.id ?? null;
        });
        return next;
      });
      setSelectedNode((current) => {
        if (current?.kind !== "saved_query") return current;
        const selected = savedQueryFromNode(current);
        return selected?.id === query.id ? null : current;
      });
    },
    [persistSavedQueries, savedQueries, t]
  );

  const openSqlForOpenTarget = useCallback((target: OpenTarget, label: string) => {
    const tab = createSqlTabForOpenTarget(target, label);
    if (!tab) return;
    setTabs((prev) => {
      const existing = prev.find((t) => t.id === tab.id);
      if (existing && isSqlTab(existing)) {
        setActiveTabId(tab.id);
        return prev.map((t) => (t.id === tab.id ? { ...existing, query: tab.query, label: tab.label } : t));
      }
      setActiveTabId(tab.id);
      return [...prev, tab];
    });
    setRowDetail(null);
  }, []);

  const openFileContentQueryFromRow = useCallback((row: Record<string, unknown>) => {
    const ref = fileContentRefFromRow(row);
    if (!ref) return;
    const tab = createFileContentSqlTab(ref);
    setTabs((prev) => {
      const existing = prev.find((t) => t.id === tab.id);
      if (existing && isSqlTab(existing)) {
        setActiveTabId(tab.id);
        return prev;
      }
      setActiveTabId(tab.id);
      return [...prev, tab];
    });
    setRowDetail(null);
  }, []);

  const queryDmView = useCallback(
    (view: DataModelGraphView) => {
      openSqlForOpenTarget(openTargetForDmView(view), labelForDmView(view));
    },
    [openSqlForOpenTarget]
  );

  const openGovernanceWorkspaceTab = useCallback(
    (which: "spaces" | "groups", subTab: GovernanceSubTab, artifactRel?: string) => {
      const id = which === "spaces" ? "gov:spaces" : "gov:groups";
      const label = which === "spaces" ? "Spaces" : "Groups";
      setTabs((prev) => {
        const existing = prev.find((tab) => tab.id === id);
        if (existing && isGovernanceSpacesTab(existing) && which === "spaces") {
          setActiveTabId(id);
          return prev.map((tab) =>
            tab.id === id
              ? {
                  ...existing,
                  activeSubTab: subTab,
                  artifactRel: artifactRel ?? existing.artifactRel,
                }
              : tab
          );
        }
        if (existing && isGovernanceGroupsTab(existing) && which === "groups") {
          setActiveTabId(id);
          return prev.map((tab) =>
            tab.id === id
              ? {
                  ...existing,
                  activeSubTab: subTab,
                  artifactRel: artifactRel ?? existing.artifactRel,
                }
              : tab
          );
        }
        if (existing) {
          setActiveTabId(id);
          return prev;
        }
        if (which === "spaces") {
          setActiveTabId("gov:spaces");
          const tab: GovernanceSpacesDocumentTab = {
            kind: "governance_spaces",
            id: "gov:spaces",
            label,
            activeSubTab: subTab,
            artifactRel: artifactRel ?? null,
          };
          return [...prev, tab];
        }
        setActiveTabId("gov:groups");
        const tab: GovernanceGroupsDocumentTab = {
          kind: "governance_groups",
          id: "gov:groups",
          label,
          activeSubTab: subTab,
          artifactRel: artifactRel ?? null,
        };
        return [...prev, tab];
      });
    },
    []
  );

  const openDiscoveryNode = useCallback(
    (node: TreeNode) => {
      if (node.kind === "saved_query") {
        const query = savedQueryFromNode(node);
        if (query) openSavedQuery(query);
        return;
      }
      if (node.kind === "workflow") {
        const ref = workflowRefFromNode(node);
        if (!ref) return;
        const id = workflowTabKey(ref);
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab: WorkflowDocumentTab = {
            kind: "workflow",
            id,
            label: workflowTabLabel(ref),
            workflow: ref,
            graph: null,
            loading: true,
            error: null,
          };
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "function") {
        const fnId = functionIdFromNode(node);
        if (fnId == null) return;
        const id = functionTabKey(fnId);
        const label = functionLabelFromMeta(node.meta);
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab = createFunctionTab(fnId, label);
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "transformation") {
        const txId = transformationIdFromNode(node);
        if (txId == null) return;
        const id = transformationTabKey(txId);
        const label = transformationLabelFromMeta(node.meta);
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab = createTransformationTab(txId, label);
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "dm_data_model") {
        const ref = dataModelRefFromNode(node);
        if (!ref) return;
        const id = dataModelTabKey(ref);
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          const tab: DocumentTab = {
            kind: "data_model",
            id,
            label: dataModelTabLabel(ref),
            dataModel: ref,
            graph: null,
            loading: true,
            error: null,
          };
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      const ws = node.meta?.governance_workspace as string | undefined;
      if (ws === "scope" || node.id === "gov") {
        const id = "gov:scope";
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
            return prev;
          }
          setActiveTabId(id);
          return [
            ...prev,
            { kind: "governance_scope", id, label: t("governance.subtab.scope") },
          ];
        });
        setRowDetail(null);
        return;
      }
      if (ws === "spaces" || node.id === "gov:spaces") {
        openGovernanceWorkspaceTab("spaces", "configure");
        setRowDetail(null);
        return;
      }
      if (ws === "groups" || node.id === "gov:groups") {
        openGovernanceWorkspaceTab("groups", "configure");
        setRowDetail(null);
        return;
      }
      if (node.kind === "gov_artifact_file") {
        const rel = String(node.meta?.artifact_rel ?? "");
        const workspace = (node.meta?.governance_workspace as "spaces" | "groups") ?? "spaces";
        openGovernanceWorkspaceTab(workspace, "artifacts", rel);
        setRowDetail(null);
        return;
      }
      if (node.kind === "gov_space") {
        const space = String(node.meta?.space ?? "");
        if (!space) return;
        const id = `gov:space:tab:${space}`;
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
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
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (node.kind === "gov_group") {
        const gid = node.meta?.id;
        const groupId = typeof gid === "number" ? gid : Number(gid);
        if (!Number.isFinite(groupId)) return;
        const id = `gov:group:tab:${groupId}`;
        setTabs((prev) => {
          const existing = prev.find((tab) => tab.id === id);
          if (existing) {
            setActiveTabId(id);
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
          setActiveTabId(id);
          return [...prev, tab];
        });
        setRowDetail(null);
        return;
      }
      if (canQueryTreeNode(node) && node.open_target) {
        openSqlForOpenTarget(node.open_target, node.label);
      }
    },
    [openGovernanceWorkspaceTab, openSavedQuery, openSqlForOpenTarget, t]
  );

  const loadConnection = useCallback(async () => {
    setConnError(null);
    try {
      const info = await fetchConnection();
      setConnection(info);
    } catch (e) {
      setConnection(null);
      setConnError(String(e));
    }
  }, []);

  useEffect(() => {
    void loadConnection();
  }, [loadConnection, refreshKey]);

  const activeTab = useMemo(
    () => tabs.find((tab) => tab.id === activeTabId) ?? null,
    [tabs, activeTabId]
  );

  const updateDataModelTab = useCallback((updated: DataModelDocumentTab) => {
    setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
  }, []);

  const updateSqlTab = useCallback((updated: SqlDocumentTab) => {
    setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
  }, []);

  const updateTransformationTab = useCallback((updated: TransformationDocumentTab) => {
    setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
  }, []);

  const updateFunctionTab = useCallback((updated: FunctionDocumentTab) => {
    setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
  }, []);

  const updateWorkflowTab = useCallback((updated: WorkflowDocumentTab) => {
    setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)));
  }, []);

  const reorderTabs = useCallback((fromIndex: number, toIndex: number) => {
    if (fromIndex === toIndex || fromIndex < 0 || toIndex < 0) return;
    setTabs((prev) => {
      if (fromIndex >= prev.length || toIndex >= prev.length) return prev;
      const next = [...prev];
      const [moved] = next.splice(fromIndex, 1);
      next.splice(toIndex, 0, moved);
      return next;
    });
  }, []);

  const closeTab = useCallback(
    (id: string) => {
      const nextTabs = tabs.filter((tab) => tab.id !== id);
      const nextActiveId =
        activeTabId === id
          ? nextTabs.length
            ? nextTabs[nextTabs.length - 1].id
            : null
          : activeTabId;
      setTabs(nextTabs);
      setActiveTabId(nextActiveId);
      if (workspaceRestored.current) {
        void persistWorkspace(serializeWorkspace(nextTabs, nextActiveId)).catch(() => {
          /* best-effort; config errors surface in DiscoveryConfigContext */
        });
      }
    },
    [tabs, activeTabId, persistWorkspace]
  );

  const connectionLabel = connection
    ? `${connection.project} @ ${connection.base_url || "CDF"}`
    : connError
      ? t("connection.failed", { detail: connError })
      : t("connection.loading");

  const shouldRenderSideColumn = (side: TreePanelSide): boolean => {
    if (panel.treeSide === side && !panel.treeHidden) return true;
    if (panel.propertiesDock === "left-bottom" && panel.treeSide === side) return true;
    if (panel.propertiesDock === "right" && side === "right") return true;
    return false;
  };

  const showLeftColumn = shouldRenderSideColumn("left");
  const showRightColumn = shouldRenderSideColumn("right");

  const columnWidthForSide = (side: TreePanelSide): number => {
    const hasTree = panel.treeSide === side && !panel.treeHidden;
    if (hasTree) return panel.treeWidth;
    if (panel.propertiesDock === "right" && side === "right") return panel.propertiesSize;
    return panel.sideColumnWidth;
  };

  const resizeHandlerForSide = (side: TreePanelSide) => {
    const hasTree = panel.treeSide === side && !panel.treeHidden;
    if (hasTree) return panel.onResizeTreeStart;
    if (panel.propertiesDock === "right" && side === "right") return panel.onResizePropertiesSideStart;
    return panel.onResizeTreeStart;
  };

  const renderDocumentPane = () => (
    <div className="disc-doc-pane" style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
      <DocumentTabBar
        tabs={tabs}
        activeId={activeTabId}
        onSelect={setActiveTabId}
        onClose={closeTab}
        onReorder={reorderTabs}
      />
      {activeTab && isDataModelTab(activeTab) ? (
        <DataModelFlowPane tab={activeTab} onTabUpdate={updateDataModelTab} onQueryView={queryDmView} />
      ) : activeTab && isWorkflowTab(activeTab) ? (
        <WorkflowFlowPane tab={activeTab} onTabUpdate={updateWorkflowTab} />
      ) : activeTab && isTransformationTab(activeTab) ? (
        <TransformationPane
          tab={activeTab}
          onTabUpdate={updateTransformationTab}
          onSelectRow={(row) => setRowDetail(row)}
          onQueryFile={openFileContentQueryFromRow}
        />
      ) : activeTab && isFunctionTab(activeTab) ? (
        <FunctionPane tab={activeTab} onTabUpdate={updateFunctionTab} />
      ) : activeTab && isGovernanceScopeTab(activeTab) ? (
        <GovernanceScopePane />
      ) : activeTab && isGovernanceSpacesTab(activeTab) ? (
        <GovernanceSpacesPane
          initialSubTab={activeTab.activeSubTab}
          initialArtifactRel={activeTab.artifactRel}
          onArtifactsChanged={(workspace) =>
            setGovernanceArtifactsRevision((prev) => ({
              token: prev.token + 1,
              workspace,
            }))
          }
        />
      ) : activeTab && isGovernanceGroupsTab(activeTab) ? (
        <GovernanceGroupsPane
          initialSubTab={activeTab.activeSubTab}
          initialArtifactRel={activeTab.artifactRel}
          onArtifactsChanged={(workspace) =>
            setGovernanceArtifactsRevision((prev) => ({
              token: prev.token + 1,
              workspace,
            }))
          }
        />
      ) : activeTab && isGovernanceCdfSpaceTab(activeTab) ? (
        <GovernanceCdfSpacePane
          tab={activeTab}
          onTabUpdate={(updated) => setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)))}
        />
      ) : activeTab && isGovernanceCdfGroupTab(activeTab) ? (
        <GovernanceCdfGroupPane
          tab={activeTab}
          onTabUpdate={(updated) => setTabs((prev) => prev.map((tab) => (tab.id === updated.id ? updated : tab)))}
        />
      ) : activeTab && isSqlTab(activeTab) ? (
        <SqlQueryPane
          tab={activeTab}
          onTabUpdate={updateSqlTab}
          onSelectRow={(row) => setRowDetail(row)}
          onQueryFile={openFileContentQueryFromRow}
          onSave={activeTab.engine === "file_content" ? undefined : () => void saveSqlTab(activeTab, "save")}
          onSaveAs={activeTab.engine === "file_content" ? undefined : () => void saveSqlTab(activeTab, "saveAs")}
        />
      ) : (
        <div
          className="disc-empty-hint"
          style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}
        >
          {t("tabs.empty")}
        </div>
      )}
    </div>
  );

  const renderPropertiesPanel = (layout: PropertiesPanelLayout) => (
    <PropertiesPanel
      collapsed={panel.propertiesCollapsed}
      onToggleCollapse={panel.togglePropertiesCollapsed}
      selectedNode={selectedNode}
      rowDetail={rowDetail}
      paneSize={panel.propertiesSize}
      layout={layout}
      isDragging={panel.draggingPanel === "properties"}
      onPanelDragStart={() => panel.beginPanelDrag("properties")}
      onPanelDragEnd={panel.endPanelDrag}
      onQueryFile={openFileContentQueryFromRow}
    />
  );

  const renderTreePane = () => (
    <aside className={`disc-tree-pane${panel.draggingPanel === "tree" ? " disc-panel--dragging" : ""}`}>
      <div className="disc-tree-pane-header">
        <PanelDragHandle
          panel="tree"
          labelKey="layout.dragHandle.tree"
          onDragStart={() => panel.beginPanelDrag("tree")}
          onDragEnd={panel.endPanelDrag}
        />
        <span className="disc-tree-pane-header__title">{t("discovery.title")}</span>
      </div>
      <ObjectDiscovery
        refreshKey={refreshKey}
        savedQueriesRevision={savedQueriesRevision}
        governanceArtifactsRevision={governanceArtifactsRevision}
        connectionLabel={connection ? `${connection.project}` : undefined}
        selectedId={selectedNode?.id ?? null}
        onSelectNode={(node) => {
          setSelectedNode(node);
          setRowDetail(null);
          if (node && opensGovernanceTab(node)) {
            openDiscoveryNode(node);
          }
        }}
        onOpenNode={openDiscoveryNode}
        onDeleteSavedQuery={deleteSavedQuery}
      />
    </aside>
  );

  const renderSideColumn = (side: TreePanelSide) => {
    const showTree = panel.treeSide === side && !panel.treeHidden;
    const showStackedProps = panel.propertiesDock === "left-bottom" && panel.treeSide === side;
    const showSideProps = panel.propertiesDock === "right" && side === "right";
    const columnWidth = columnWidthForSide(side);

    return (
      <div
        className={`disc-side-column disc-side-column--${side}`}
        style={{ width: columnWidth, minWidth: columnWidth, maxWidth: columnWidth }}
      >
        {showTree && renderTreePane()}
        {showStackedProps && (
          <>
            {showTree && !panel.propertiesCollapsed && (
              <div
                className="disc-resize-handle-v"
                role="separator"
                aria-orientation="horizontal"
                onMouseDown={panel.onResizePropertiesStackedStart}
              />
            )}
            {renderPropertiesPanel("stacked")}
          </>
        )}
        {showSideProps && (
          <>
            {showTree && !panel.propertiesCollapsed && (
              <div
                className="disc-resize-handle-v"
                role="separator"
                aria-orientation="horizontal"
                onMouseDown={panel.onResizePropertiesStackedStart}
              />
            )}
            {renderPropertiesPanel(showTree ? "stacked" : "side")}
          </>
        )}
      </div>
    );
  };

  const renderHorizontalResize = (side: TreePanelSide) => (
    <div
      className="disc-resize-handle-h"
      role="separator"
      aria-orientation="vertical"
      aria-valuenow={columnWidthForSide(side)}
      onMouseDown={resizeHandlerForSide(side)}
    />
  );

  const splitHiddenClass =
    panel.treeHidden && panel.propertiesDock !== "left-bottom" ? " disc-split-h--tree-hidden" : "";

  return (
    <div className="disc-app">
      <header className="disc-toolbar">
        <CogniteLogo />
        <h1 className="disc-toolbar__title">{t("app.title")}</h1>
        <span className="disc-connection-badge">{connectionLabel}</span>
        <button type="button" className="disc-btn" onClick={() => setRefreshKey((k) => k + 1)}>
          {t("toolbar.refresh")}
        </button>
        <button type="button" className="disc-btn" onClick={openSqlWorkspace}>
          {t("toolbar.sqlQuery")}
        </button>
        <button
          type="button"
          className="disc-btn disc-btn--tree-toggle"
          aria-pressed={!panel.treeHidden}
          onClick={panel.toggleTreeHidden}
        >
          {panel.treeHidden ? t("discovery.toggleTreeShow") : t("discovery.toggleTreeHide")}
        </button>
        <div className="disc-toolbar__controls">
          <label className="disc-toolbar__control" title={t("controls.theme.tooltip")}>
            <span className="disc-toolbar__control-label">{t("controls.theme")}</span>
            <span className="disc-theme-toggle" role="group">
              <button type="button" data-active={theme === "light"} onClick={() => setTheme("light")}>
                {t("controls.themeLight")}
              </button>
              <button type="button" data-active={theme === "dark"} onClick={() => setTheme("dark")}>
                {t("controls.themeDark")}
              </button>
            </span>
          </label>
          <label className="disc-toolbar__control" title={t("controls.language.tooltip")}>
            <span className="disc-toolbar__control-label">{t("controls.language")}</span>
            <select value={locale} onChange={(e) => setLocale(e.target.value as typeof locale)}>
              {LOCALES.map(({ code, label }) => (
                <option key={code} value={code}>
                  {label}
                </option>
              ))}
            </select>
          </label>
        </div>
      </header>
      {connError && <div className="disc-banner--error">{t("connection.failed", { detail: connError })}</div>}
      <div className={`disc-main${panel.draggingPanel ? " disc-main--panel-drag" : ""}`}>
        <div className={`disc-split-h${splitHiddenClass}`}>
          {showLeftColumn && renderSideColumn("left")}
          {showLeftColumn && renderHorizontalResize("left")}
          <div className="disc-workspace">
            {panel.propertiesDock === "bottom" ? (
              <div className="disc-split-v">
                {renderDocumentPane()}
                {!panel.propertiesCollapsed && (
                  <div
                    className="disc-resize-handle-v"
                    role="separator"
                    aria-orientation="horizontal"
                    onMouseDown={panel.onResizePropertiesBottomStart}
                  />
                )}
                {renderPropertiesPanel("bottom")}
              </div>
            ) : (
              renderDocumentPane()
            )}
          </div>
          {showRightColumn && renderHorizontalResize("right")}
          {showRightColumn && renderSideColumn("right")}
        </div>
        <PanelDropOverlay
          dragging={panel.draggingPanel}
          treeSide={panel.treeSide}
          treeWidth={panel.treeWidth}
          onDropTree={panel.dropTreeSide}
          onDropProperties={panel.dropPropertiesDock}
        />
      </div>
    </div>
  );
}
