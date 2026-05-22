import { useCallback, useEffect, useMemo, useState, type MouseEvent } from "react";
import { createPortal } from "react-dom";
import {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import { fetchDataModelGraph } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import type { DataModelDocumentTab, DataModelGraphView } from "../types/explorerNodes";
import { filterViewsBySearch, viewMatchesSearch } from "../utils/dataModelFlowSearch";
import { graphToFlow } from "../utils/dataModelFlowLayout";
import { DataModelViewProperties } from "./DataModelViewProperties";
import { DmViewFlowNode } from "./flow/DmViewFlowNode";

const nodeTypes = { dmView: DmViewFlowNode };

type Props = {
  tab: DataModelDocumentTab;
  onTabUpdate: (tab: DataModelDocumentTab) => void;
  onQueryView: (view: DataModelGraphView) => void;
};

type CtxMenu = { x: number; y: number; view: DataModelGraphView } | null;

function FitViewOnLoad({ revision }: { revision: string }) {
  const { fitView } = useReactFlow();
  useEffect(() => {
    const id = window.requestAnimationFrame(() => {
      fitView({ padding: 0.2, duration: 200 });
    });
    return () => window.cancelAnimationFrame(id);
  }, [revision, fitView]);
  return null;
}

function FocusViewNode({ viewId }: { viewId: string | null }) {
  const { fitView } = useReactFlow();
  useEffect(() => {
    if (!viewId) return;
    const id = window.requestAnimationFrame(() => {
      fitView({ nodes: [{ id: viewId }], padding: 0.45, duration: 280, maxZoom: 1.15 });
    });
    return () => window.cancelAnimationFrame(id);
  }, [viewId, fitView]);
  return null;
}

function FlowInner({ tab, onTabUpdate, onQueryView }: Props) {
  const { t, theme } = useAppSettings();
  const [selectedViewId, setSelectedViewId] = useState<string | null>(null);
  const [selectedView, setSelectedView] = useState<DataModelGraphView | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [focusViewId, setFocusViewId] = useState<string | null>(null);
  const [ctxMenu, setCtxMenu] = useState<CtxMenu>(null);

  useEffect(() => {
    if (tab.graph != null || !tab.loading) return;
    let cancelled = false;
    const load = async () => {
      onTabUpdate({ ...tab, loading: true, error: null });
      try {
        const graph = await fetchDataModelGraph(tab.dataModel);
        if (cancelled) return;
        onTabUpdate({ ...tab, graph, loading: false, error: null });
      } catch (e) {
        if (cancelled) return;
        onTabUpdate({ ...tab, loading: false, error: String(e) });
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [tab.id, tab.loading, tab.graph]);

  const searchActive = searchQuery.trim().length > 0;
  const searchMatches = useMemo(
    () => (tab.graph ? filterViewsBySearch(tab.graph.views, searchQuery) : []),
    [tab.graph, searchQuery]
  );

  const { nodes: baseNodes, edges } = useMemo(
    () => (tab.graph ? graphToFlow(tab.graph) : { nodes: [] as Node[], edges: [] as Edge[] }),
    [tab.graph]
  );

  const nodes = useMemo(
    () =>
      baseNodes.map((n) => {
        const view = (n.data as { view?: DataModelGraphView }).view;
        const matches = view ? viewMatchesSearch(view, searchQuery) : true;
        return {
          ...n,
          data: {
            ...(n.data as object),
            selected: n.id === selectedViewId,
            dimmed: searchActive && !matches,
          },
        };
      }),
    [baseNodes, selectedViewId, searchQuery, searchActive]
  );

  const graphRevision = tab.graph ? `${tab.id}:${tab.graph.views.length}:${tab.graph.edges.length}` : tab.id;

  const selectView = useCallback((view: DataModelGraphView, focus = true) => {
    setSelectedViewId(view.id);
    setSelectedView(view);
    if (focus) setFocusViewId(view.id);
  }, []);

  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      const view = (node.data as { view?: DataModelGraphView }).view;
      if (!view) return;
      selectView(view, true);
    },
    [selectView]
  );

  const onPaneClick = useCallback(() => {
    setSelectedViewId(null);
    setSelectedView(null);
    setFocusViewId(null);
    setCtxMenu(null);
  }, []);

  const onNodeContextMenu = useCallback((e: MouseEvent, node: Node) => {
    e.preventDefault();
    const view = (node.data as { view?: DataModelGraphView }).view;
    if (!view) return;
    setCtxMenu({ x: e.clientX, y: e.clientY, view });
  }, []);

  useEffect(() => {
    if (!ctxMenu) return;
    const close = () => setCtxMenu(null);
    window.addEventListener("click", close);
    return () => window.removeEventListener("click", close);
  }, [ctxMenu]);

  const refresh = useCallback(() => {
    onTabUpdate({ ...tab, graph: null, loading: true, error: null });
    setSelectedViewId(null);
    setSelectedView(null);
    setSearchQuery("");
    setFocusViewId(null);
  }, [tab, onTabUpdate]);

  if (tab.error) {
    return (
      <div className="exp-doc-pane">
        <div className="exp-banner--error">{t("status.error", { detail: tab.error })}</div>
        <button type="button" className="exp-btn" onClick={refresh}>
          {t("dmViewer.refresh")}
        </button>
      </div>
    );
  }

  return (
    <div className="exp-doc-pane exp-dm-flow-pane">
      <div className="exp-doc-toolbar">
        <span className="exp-dm-flow-pane__title">{t("dmViewer.title")}</span>
        <span className="exp-dm-flow-pane__meta">
          {tab.dataModel.space} / {tab.dataModel.external_id} / {tab.dataModel.version}
          {tab.graph
            ? ` · ${tab.graph.views.length} ${t("dmViewer.views")} · ${tab.graph.edges.length} ${t("dmViewer.edges")}`
            : ""}
        </span>
        {tab.graph && tab.graph.views.length > 0 && (
          <label className="exp-dm-flow-search">
            <span className="exp-dm-flow-search__label">{t("dmViewer.search")}</span>
            <input
              className="exp-input exp-dm-flow-search__input"
              type="search"
              value={searchQuery}
              placeholder={t("dmViewer.searchPlaceholder")}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </label>
        )}
        <button type="button" className="exp-btn" disabled={tab.loading} onClick={refresh}>
          {t("dmViewer.refresh")}
        </button>
      </div>
      {searchActive && tab.graph && (
        <div className="exp-dm-flow-search-results">
          {searchMatches.length === 0 ? (
            <span className="exp-dm-flow-search-results__empty">{t("dmViewer.noSearchResults")}</span>
          ) : (
            <ul className="exp-dm-flow-search-results__list">
              {searchMatches.map((view) => (
                <li key={view.id}>
                  <button
                    type="button"
                    className={
                      selectedViewId === view.id
                        ? "exp-dm-flow-search-results__item exp-dm-flow-search-results__item--active"
                        : "exp-dm-flow-search-results__item"
                    }
                    onClick={() => selectView(view, true)}
                  >
                    <span className="exp-dm-flow-search-results__name">
                      {view.name?.trim() || view.external_id}
                    </span>
                    <span className="exp-dm-flow-search-results__meta">
                      {view.space} · {view.version}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
      <div className="exp-dm-flow-body">
        <div className="exp-dm-flow-canvas">
          {tab.loading && !tab.graph ? (
            <p className="exp-empty-hint">{t("dmViewer.loading")}</p>
          ) : tab.graph && tab.graph.views.length === 0 ? (
            <p className="exp-empty-hint">{t("dmViewer.noViews")}</p>
          ) : (
            <ReactFlow
              nodes={nodes}
              edges={edges}
              nodeTypes={nodeTypes}
              colorMode={theme}
              nodesDraggable
              nodesConnectable={false}
              elementsSelectable
              fitView
              proOptions={{ hideAttribution: true }}
              onNodeClick={onNodeClick}
              onNodeContextMenu={onNodeContextMenu}
              onPaneClick={onPaneClick}
            >
              <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
              <Controls showInteractive={false} />
              <MiniMap zoomable pannable nodeStrokeWidth={2} />
              <FitViewOnLoad revision={graphRevision} />
              <FocusViewNode viewId={focusViewId} />
            </ReactFlow>
          )}
        </div>
        <DataModelViewProperties graph={tab.graph} view={selectedView} />
      </div>
      {ctxMenu &&
        createPortal(
          <ul
            className="exp-ctx-menu"
            style={{ left: ctxMenu.x, top: ctxMenu.y }}
            onClick={(e) => e.stopPropagation()}
          >
            <li>
              <button
                type="button"
                onClick={() => {
                  onQueryView(ctxMenu.view);
                  setCtxMenu(null);
                }}
              >
                {t("explorer.query")}
              </button>
            </li>
          </ul>,
          document.body
        )}
    </div>
  );
}

export function DataModelFlowPane(props: Props) {
  return (
    <ReactFlowProvider>
      <FlowInner {...props} />
    </ReactFlowProvider>
  );
}
