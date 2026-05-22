import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { fetchConnection, type ConnectionInfo } from "./api";
import { DocumentTabBar } from "./components/DocumentTabBar";
import { DataModelFlowPane } from "./components/DataModelFlowPane";
import { WorkflowFlowPane } from "./components/WorkflowFlowPane";
import { ObjectExplorer } from "./components/ObjectExplorer";
import { PropertiesPanel } from "./components/PropertiesPanel";
import { SqlQueryPane } from "./components/SqlQueryPane";
import { FunctionPane } from "./components/FunctionPane";
import { TransformationPane } from "./components/TransformationPane";
import { useAppSettings } from "./context/AppSettingsContext";
import { useExplorerConfig } from "./context/ExplorerConfigContext";
import { LOCALES } from "./i18n";
import {
  isDataModelTab,
  isFunctionTab,
  isSqlTab,
  isTransformationTab,
  isWorkflowTab,
  type DataModelDocumentTab,
  type DataModelGraphView,
  type DocumentTab,
  type FunctionDocumentTab,
  type WorkflowDocumentTab,
  type TransformationDocumentTab,
  type OpenTarget,
  type SqlDocumentTab,
  type TreeNode,
} from "./types/explorerNodes";
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
  SQL_WORKSPACE_TAB_ID,
} from "./utils/sqlTabs";
import { canQueryTreeNode, labelForDmView, openTargetForDmView } from "./utils/sqlQuerySeed";
import {
  createSqlTabFromSavedQuery,
  savedQueryEntryFromSqlTab,
  savedQueryFromNode,
  slugifySavedQueryId,
  sqlSavedQueryTabId,
  uniqueSavedQueryId,
} from "./utils/savedQueries";
import { restoreWorkspaceTabs, serializeWorkspace } from "./utils/workspacePersistence";
import type { SavedQuery } from "./types/explorerNodes";

export function App() {
  const { t, theme, setTheme, locale, setLocale } = useAppSettings();
  const {
    workspace,
    persistWorkspace,
    loading: configLoading,
    savedQueries,
    savedQueriesRevision,
    persistSavedQueries,
  } = useExplorerConfig();
  const [connection, setConnection] = useState<ConnectionInfo | null>(null);
  const [connError, setConnError] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [selectedNode, setSelectedNode] = useState<TreeNode | null>(null);
  const [tabs, setTabs] = useState<DocumentTab[]>([]);
  const [activeTabId, setActiveTabId] = useState<string | null>(null);
  const [rowDetail, setRowDetail] = useState<unknown | null>(null);
  const [propsCollapsed, setPropsCollapsed] = useState(false);
  const [propsHeight, setPropsHeight] = useState(200);
  const [explorerWidth, setExplorerWidth] = useState(280);
  const workspaceRestored = useRef(false);

  useEffect(() => {
    if (configLoading || workspaceRestored.current) return;
    workspaceRestored.current = true;
    if (!workspace.tabs.length) return;
    const restored = restoreWorkspaceTabs(workspace, t("sql.title"));
    setTabs(restored.tabs);
    setActiveTabId(restored.activeTabId);
  }, [configLoading, workspace, t]);

  useEffect(() => {
    if (!workspaceRestored.current) return;
    const timer = window.setTimeout(() => {
      void persistWorkspace(serializeWorkspace(tabs, activeTabId)).catch(() => {
        /* best-effort; config errors surface in ExplorerConfigContext */
      });
    }, 500);
    return () => window.clearTimeout(timer);
  }, [tabs, activeTabId, persistWorkspace]);

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
      if (!window.confirm(t("explorer.deleteSavedQueryConfirm", { name: query.name }))) {
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

  const queryDmView = useCallback(
    (view: DataModelGraphView) => {
      openSqlForOpenTarget(openTargetForDmView(view), labelForDmView(view));
    },
    [openSqlForOpenTarget]
  );

  const openExplorerNode = useCallback(
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
      if (canQueryTreeNode(node) && node.open_target) {
        openSqlForOpenTarget(node.open_target, node.label);
      }
    },
    [openSavedQuery, openSqlForOpenTarget]
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
          /* best-effort; config errors surface in ExplorerConfigContext */
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

  return (
    <div className="exp-app">
      <header className="exp-toolbar">
        <h1>{t("app.title")}</h1>
        <span className="exp-connection-badge">{connectionLabel}</span>
        <button type="button" className="exp-btn" onClick={() => setRefreshKey((k) => k + 1)}>
          {t("toolbar.refresh")}
        </button>
        <button type="button" className="exp-btn" onClick={openSqlWorkspace}>
          {t("toolbar.sqlQuery")}
        </button>
        <div className="exp-toolbar__controls">
          <label className="exp-toolbar__control" title={t("controls.theme.tooltip")}>
            <span className="exp-toolbar__control-label">{t("controls.theme")}</span>
            <span className="exp-theme-toggle" role="group">
              <button type="button" data-active={theme === "light"} onClick={() => setTheme("light")}>
                {t("controls.themeLight")}
              </button>
              <button type="button" data-active={theme === "dark"} onClick={() => setTheme("dark")}>
                {t("controls.themeDark")}
              </button>
            </span>
          </label>
          <label className="exp-toolbar__control" title={t("controls.language.tooltip")}>
            <span className="exp-toolbar__control-label">{t("controls.language")}</span>
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
      {connError && <div className="exp-banner--error">{t("connection.failed", { detail: connError })}</div>}
      <div className="exp-main">
        <div className="exp-split-h">
          <aside
            className="exp-explorer-pane"
            style={{ width: explorerWidth, minWidth: explorerWidth, maxWidth: explorerWidth }}
          >
            <div className="exp-explorer-pane-header">{t("explorer.title")}</div>
            <ObjectExplorer
              refreshKey={refreshKey}
              savedQueriesRevision={savedQueriesRevision}
              connectionLabel={connection ? `${connection.project}` : undefined}
              selectedId={selectedNode?.id ?? null}
              onSelectNode={(node) => {
                setSelectedNode(node);
                setRowDetail(null);
              }}
              onOpenNode={openExplorerNode}
              onDeleteSavedQuery={deleteSavedQuery}
            />
          </aside>
          <div
            className="exp-resize-handle-h"
            role="separator"
            aria-orientation="vertical"
            aria-valuenow={explorerWidth}
            onMouseDown={(e) => {
              const startX = e.clientX;
              const startW = explorerWidth;
              const onMove = (ev: MouseEvent) => {
                setExplorerWidth(
                  Math.max(180, Math.min(480, Math.min(window.innerWidth * 0.55, startW + (ev.clientX - startX))))
                );
              };
              const onUp = () => {
                window.removeEventListener("mousemove", onMove);
                window.removeEventListener("mouseup", onUp);
              };
              window.addEventListener("mousemove", onMove);
              window.addEventListener("mouseup", onUp);
            }}
          />
          <div className="exp-workspace">
            <div className="exp-split-v">
              <div className="exp-doc-pane" style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
                <DocumentTabBar
                  tabs={tabs}
                  activeId={activeTabId}
                  onSelect={setActiveTabId}
                  onClose={closeTab}
                  onReorder={reorderTabs}
                />
                {activeTab && isDataModelTab(activeTab) ? (
                  <DataModelFlowPane
                    tab={activeTab}
                    onTabUpdate={updateDataModelTab}
                    onQueryView={queryDmView}
                  />
                ) : activeTab && isWorkflowTab(activeTab) ? (
                  <WorkflowFlowPane tab={activeTab} onTabUpdate={updateWorkflowTab} />
                ) : activeTab && isTransformationTab(activeTab) ? (
                  <TransformationPane
                    tab={activeTab}
                    onTabUpdate={updateTransformationTab}
                    onSelectRow={(row) => setRowDetail(row)}
                  />
                ) : activeTab && isFunctionTab(activeTab) ? (
                  <FunctionPane tab={activeTab} onTabUpdate={updateFunctionTab} />
                ) : activeTab && isSqlTab(activeTab) ? (
                  <SqlQueryPane
                    tab={activeTab}
                    onTabUpdate={updateSqlTab}
                    onSelectRow={(row) => setRowDetail(row)}
                    onSave={() => void saveSqlTab(activeTab, "save")}
                    onSaveAs={() => void saveSqlTab(activeTab, "saveAs")}
                  />
                ) : (
                  <div
                    className="exp-empty-hint"
                    style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}
                  >
                    {t("tabs.empty")}
                  </div>
                )}
              </div>
              <div
                className="exp-resize-handle-v"
                role="separator"
                onMouseDown={(e) => {
                  const startY = e.clientY;
                  const startH = propsHeight;
                  const onMove = (ev: MouseEvent) => {
                    setPropsHeight(Math.max(80, Math.min(window.innerHeight * 0.5, startH - (ev.clientY - startY))));
                  };
                  const onUp = () => {
                    window.removeEventListener("mousemove", onMove);
                    window.removeEventListener("mouseup", onUp);
                  };
                  window.addEventListener("mousemove", onMove);
                  window.addEventListener("mouseup", onUp);
                }}
              />
              <PropertiesPanel
                collapsed={propsCollapsed}
                onToggleCollapse={() => setPropsCollapsed((c) => !c)}
                selectedNode={selectedNode}
                rowDetail={rowDetail}
                propertiesHeight={propsHeight}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
