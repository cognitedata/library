import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { fetchConnection } from "./api";
import { BuildAnnotationsPane } from "./components/index/BuildAnnotationsPane";
import { BuildMetadataPane } from "./components/index/BuildMetadataPane";
import { FileContextPane } from "./components/index/FileContextPane";
import { OverviewPane } from "./components/index/OverviewPane";
import { QueryPane } from "./components/index/QueryPane";
import { TagReusePane } from "./components/index/TagReusePane";
import { TargetDrivenPane } from "./components/index/TargetDrivenPane";
import { AccessibleResizeHandle } from "./components/shell/AccessibleResizeHandle";
import { CogniteLogo } from "./components/shell/CogniteLogo";
import { DocumentTabBar } from "./components/shell/DocumentTabBar";
import { IndexNavTree } from "./components/shell/IndexNavTree";
import { PropertiesPanel } from "./components/shell/PropertiesPanel";
import { SettingsPane } from "./components/shell/SettingsPane";
import { useAppSettings } from "./context/AppSettingsContext";
import { useIndexWorkspace } from "./context/IndexWorkspaceContext";
import { useIndexPanelLayout } from "./hooks/useIndexPanelLayout";
import { LOCALES } from "./i18n";
import type { ConnectionInfo, IndexDocumentTab, IndexNavNode } from "./types/indexWorkspace";
import {
  createIndexTab,
  isBuildAnnotationsTab,
  isBuildMetadataTab,
  isFileContextTab,
  isOverviewTab,
  isQueryTab,
  isSettingsTab,
  isTagReuseTab,
  isTargetDrivenTab,
  tabIdForKind,
} from "./utils/indexTabs";
import type { MessageKey } from "./i18n";

export function App() {
  const { t, theme, setTheme, locale, setLocale } = useAppSettings();
  const { workspace, loading: workspaceLoading, overviewSubTab, persistWorkspace } = useIndexWorkspace();
  const panel = useIndexPanelLayout();
  const [connection, setConnection] = useState<ConnectionInfo | null>(null);
  const [connError, setConnError] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [tabs, setTabs] = useState<IndexDocumentTab[]>([]);
  const [activeTabId, setActiveTabId] = useState<string | null>(null);
  const [rowDetail, setRowDetail] = useState<unknown | null>(null);
  const workspaceRestored = useRef(false);

  useEffect(() => {
    if (workspaceLoading || workspaceRestored.current) return;
    workspaceRestored.current = true;
    setTabs(workspace.tabs);
    setActiveTabId(workspace.active_tab_id);
  }, [workspace, workspaceLoading]);

  useEffect(() => {
    if (!workspaceRestored.current) return;
    persistWorkspace(tabs, activeTabId);
  }, [tabs, activeTabId, persistWorkspace]);

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

  const connectionLabel = useMemo(() => {
    if (connError) return t("connection.error");
    if (!connection) return t("connection.loading");
    return t("connection.project", { project: connection.project });
  }, [connError, connection, t]);

  const openNavNode = useCallback(
    (node: IndexNavNode) => {
      if (!node.kind) return;
      const label = t(node.labelKey as MessageKey);
      const id = tabIdForKind(node.kind);
      setTabs((prev) => {
        const existing = prev.find((tab) => tab.id === id);
        if (existing) {
          setActiveTabId(id);
          return prev;
        }
        const tab = createIndexTab(node.kind!, label, node.id);
        setActiveTabId(id);
        return [...prev, tab];
      });
      setSelectedNodeId(node.id);
    },
    [t]
  );

  const openSettingsTab = useCallback(() => {
    const id = tabIdForKind("settings");
    setTabs((prev) => {
      const existing = prev.find((tab) => tab.id === id);
      if (existing) {
        setActiveTabId(id);
        return prev;
      }
      const tab = createIndexTab("settings", t("settings.title"), "inverted-index/settings");
      setActiveTabId(id);
      return [...prev, tab];
    });
  }, [t]);

  const closeTab = useCallback((id: string) => {
    setTabs((prev) => {
      const next = prev.filter((tab) => tab.id !== id);
      setActiveTabId((current) => {
        if (current !== id) return current;
        return next[next.length - 1]?.id ?? null;
      });
      return next;
    });
  }, []);

  const reorderTabs = useCallback((fromIndex: number, toIndex: number) => {
    setTabs((prev) => {
      const next = [...prev];
      const [moved] = next.splice(fromIndex, 1);
      if (!moved) return prev;
      next.splice(toIndex, 0, moved);
      return next;
    });
  }, []);

  const activeTab = tabs.find((tab) => tab.id === activeTabId) ?? null;

  const renderActiveTabContent = (tab: IndexDocumentTab) => {
    if (isOverviewTab(tab)) return <OverviewPane refreshKey={refreshKey} initialSubTab={overviewSubTab} />;
    if (isBuildMetadataTab(tab)) return <BuildMetadataPane />;
    if (isBuildAnnotationsTab(tab)) return <BuildAnnotationsPane />;
    if (isQueryTab(tab)) return <QueryPane onSelectRow={setRowDetail} />;
    if (isFileContextTab(tab)) return <FileContextPane onSelectRow={setRowDetail} />;
    if (isTargetDrivenTab(tab)) return <TargetDrivenPane />;
    if (isTagReuseTab(tab)) return <TagReusePane onSelectRow={setRowDetail} />;
    if (isSettingsTab(tab)) return <SettingsPane />;
    return null;
  };

  const sideWidth = panel.treeCollapsed ? 120 : panel.treeWidth;

  return (
    <div className="idx-app">
      <a href="#idx-main" className="idx-skip-link">
        {t("a11y.skipToMain")}
      </a>
      <header className="idx-toolbar">
        <CogniteLogo />
        <h1 className="idx-toolbar__title">{t("app.title")}</h1>
        <span className="idx-connection-badge">{connectionLabel}</span>
        <button type="button" className="idx-btn" onClick={() => setRefreshKey((k) => k + 1)}>
          {t("toolbar.refresh")}
        </button>
        <div className="idx-toolbar__controls">
          <label className="idx-toolbar__control" title={t("controls.theme.tooltip")}>
            <span className="idx-toolbar__control-label">{t("controls.theme")}</span>
            <span className="idx-theme-toggle" role="group">
              <button type="button" data-active={theme === "light"} onClick={() => setTheme("light")}>
                {t("controls.themeLight")}
              </button>
              <button type="button" data-active={theme === "dark"} onClick={() => setTheme("dark")}>
                {t("controls.themeDark")}
              </button>
            </span>
          </label>
          <label className="idx-toolbar__control" title={t("controls.language.tooltip")}>
            <span className="idx-toolbar__control-label">{t("controls.language")}</span>
            <span className="idx-toolbar__locale-settings">
              <select value={locale} onChange={(e) => setLocale(e.target.value as typeof locale)}>
                {LOCALES.map(({ code, label }) => (
                  <option key={code} value={code}>
                    {label}
                  </option>
                ))}
              </select>
              <button
                type="button"
                className="idx-btn idx-btn--ghost idx-toolbar__settings-btn"
                title={t("controls.settings.tooltip")}
                aria-label={t("controls.settings")}
                onClick={openSettingsTab}
              >
                <svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor" aria-hidden="true">
                  <path d="M8 4.75a3.25 3.25 0 1 0 0 6.5 3.25 3.25 0 0 0 0-6.5zm6.1 3.35.9-.65a.5.5 0 0 0 .12-.64l-.85-1.47a.5.5 0 0 0-.58-.22l-1.05.4a5.2 5.2 0 0 0-1.12-.65l-.16-1.12a.5.5 0 0 0-.49-.42H7.03a.5.5 0 0 0-.49.42l-.16 1.12c-.4.15-.77.37-1.12.65l-1.05-.4a.5.5 0 0 0-.58.22l-.85 1.47a.5.5 0 0 0 .12.64l.9.65a5.3 5.3 0 0 0 0 1.3l-.9.65a.5.5 0 0 0-.12.64l.85 1.47a.5.5 0 0 0 .58.22l1.05-.4c.35.28.72.5 1.12.65l.16 1.12a.5.5 0 0 0 .49.42h1.7a.5.5 0 0 0 .49-.42l.16-1.12c.4-.15.77-.37 1.12-.65l1.05.4a.5.5 0 0 0 .58-.22l.85-1.47a.5.5 0 0 0-.12-.64l-.9-.65a5.3 5.3 0 0 0 0-1.3z" />
                </svg>
              </button>
            </span>
          </label>
        </div>
      </header>
      {connError ? <div className="idx-banner--error">{connError}</div> : null}
      <div className="idx-main">
        <div className="idx-split-h">
          <div className="idx-side-column" style={{ width: sideWidth }}>
            <IndexNavTree
              selectedNodeId={selectedNodeId}
              onSelectNode={setSelectedNodeId}
              onOpenNode={openNavNode}
            />
            {!panel.propertiesCollapsed ? (
              <>
                <AccessibleResizeHandle
                  orientation="vertical"
                  value={panel.propertiesSize}
                  min={panel.propsMin}
                  max={panel.propsMaxHeight()}
                  labelKey="layout.resize.propertiesSize"
                  onMouseDown={panel.onResizePropertiesStart}
                  onValueChange={panel.setPropertiesSizeClamped}
                  className="idx-resize-handle idx-resize-handle--vertical"
                />
                <div style={{ height: panel.propertiesSize, minHeight: 0, display: "flex", flexDirection: "column" }}>
                  <PropertiesPanel detail={rowDetail} />
                </div>
              </>
            ) : null}
          </div>
          {!panel.treeCollapsed ? (
            <AccessibleResizeHandle
              orientation="horizontal"
              value={panel.treeWidth}
              min={panel.treeMin}
              max={panel.treeMax}
              labelKey="layout.resize.treeWidth"
              onMouseDown={panel.onResizeTreeStart}
              onValueChange={panel.setTreeWidthClamped}
              className="idx-resize-handle idx-resize-handle--horizontal"
            />
          ) : null}
          <main id="idx-main" className="idx-workspace">
            <DocumentTabBar
              tabs={tabs}
              activeId={activeTabId}
              onSelect={setActiveTabId}
              onClose={closeTab}
              onReorder={reorderTabs}
            />
            <div className="idx-tabpanel" role="tabpanel">
              {activeTab ? (
                renderActiveTabContent(activeTab)
              ) : (
                <div className="idx-tabpanel-empty">{t("tabs.empty")}</div>
              )}
            </div>
          </main>
        </div>
      </div>
    </div>
  );
}
