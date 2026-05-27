import { useCallback, useEffect, useMemo, useRef, useState, type MouseEvent } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useOnSelectionChange,
  useReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import { fetchDataModelGraph } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import type { DataModelDocumentTab, DataModelGraphView } from "../types/discoveryNodes";
import {
  normalizeTransformCanvasEdgePathStyle,
  type TransformCanvasEdgePathStyle,
  type TransformCanvasHandleOrientation,
} from "../types/transformCanvas";
import { filterViewsBySearch, viewMatchesSearch } from "../utils/dataModelFlowSearch";
import { graphToFlow, layoutDmViewNodes } from "../utils/dataModelFlowLayout";
import { DataModelViewProperties } from "./DataModelViewProperties";
import { alignSelectedTransformFlowNodes, type AlignFlowSelectionMode } from "./transform/alignSelectedNodes";
import {
  connectionLineTypeForEdgePathStyle,
  defaultTransformFlowEdgeOptions,
  FlowHandleOrientationEdgeSync,
} from "./transform/FlowHandleOrientationEdgeSync";
import { TransformFlowLayoutControls } from "./transform/TransformFlowLayoutControls";
import { highlightEdgesConnectedToNode } from "./flow/highlightEdgesForSelectedNode";
import { DmViewFlowNode } from "./flow/DmViewFlowNode";
import { TreeContextMenuPortal } from "./governance/TreeContextMenu";
import { FlowHandleOrientationProvider } from "./transform/FlowHandleOrientationContext";
import { applyFlowHandleOrientationToNode } from "./transform/flowHandleOrientation";

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

function orientDmFlowNodes(
  nodes: Node[],
  orientation: TransformCanvasHandleOrientation
): Node[] {
  return nodes.map((node) => applyFlowHandleOrientationToNode(node, orientation));
}

function DmFlowSelectionBridge({
  onSelectionChange,
}: {
  onSelectionChange: (nodes: Node[]) => void;
}) {
  const onChange = useCallback(
    ({ nodes: sel }: { nodes: Node[] }) => {
      onSelectionChange(sel);
    },
    [onSelectionChange]
  );
  useOnSelectionChange({ onChange });
  return null;
}

function FlowInner({ tab, onTabUpdate, onQueryView }: Props) {
  const { t, theme } = useAppSettings();
  const { fitView, getEdges } = useReactFlow();
  const [selectedViewId, setSelectedViewId] = useState<string | null>(null);
  const [selectedView, setSelectedView] = useState<DataModelGraphView | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [focusViewId, setFocusViewId] = useState<string | null>(null);
  const [ctxMenu, setCtxMenu] = useState<CtxMenu>(null);
  const [handleOrientation, setHandleOrientation] = useState<TransformCanvasHandleOrientation>("lr");
  const [edgePathStyle, setEdgePathStyle] = useState<TransformCanvasEdgePathStyle>("smoothstep");
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const rfSelectionRef = useRef<Node[]>([]);
  const [alignableSelectionCount, setAlignableSelectionCount] = useState(0);

  const onFlowSelectionChange = useCallback((sel: Node[]) => {
    rfSelectionRef.current = sel;
    setAlignableSelectionCount(sel.length);
  }, []);

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

  useEffect(() => {
    if (!tab.graph) return;
    const seed = graphToFlow(tab.graph, { handleOrientation, edgePathStyle });
    setNodes(orientDmFlowNodes(seed.nodes, handleOrientation));
    setEdges(seed.edges);
    window.setTimeout(() => fitView({ padding: 0.2, duration: 0 }), 0);
  }, [tab.graph, setNodes, setEdges, fitView]);

  const applyAutoLayout = useCallback(
    (orientation: TransformCanvasHandleOrientation) => {
      const eds = getEdges();
      setNodes((nds) => {
        const laidOut = layoutDmViewNodes(nds, eds, orientation);
        return orientDmFlowNodes(laidOut, orientation);
      });
      window.setTimeout(() => fitView({ padding: 0.2, duration: 200 }), 0);
    },
    [getEdges, setNodes, fitView]
  );

  const handleAutoLayout = useCallback(() => {
    applyAutoLayout(handleOrientation);
  }, [applyAutoLayout, handleOrientation]);

  const onHandleOrientationChange = useCallback(
    (next: TransformCanvasHandleOrientation) => {
      setHandleOrientation(next);
      applyAutoLayout(next);
    },
    [applyAutoLayout]
  );

  const onEdgePathStyleChange = useCallback(
    (next: TransformCanvasEdgePathStyle) => {
      setEdgePathStyle(next);
      const rfType = normalizeTransformCanvasEdgePathStyle(next);
      setEdges((eds) =>
        eds.map((e) => ((e.type ?? rfType) === rfType ? e : { ...e, type: rfType }))
      );
    },
    [setEdges]
  );

  const applySelectionAlign = useCallback(
    (mode: AlignFlowSelectionMode) => {
      setNodes((nds) => {
        const next = alignSelectedTransformFlowNodes(nds, rfSelectionRef.current, mode);
        return next ?? nds;
      });
    },
    [setNodes]
  );

  const defaultEdgeOptions = useMemo(
    () => defaultTransformFlowEdgeOptions(edgePathStyle),
    [edgePathStyle]
  );

  const connectionLineType = useMemo(
    () => connectionLineTypeForEdgePathStyle(edgePathStyle),
    [edgePathStyle]
  );

  const displayNodes = useMemo(
    () =>
      nodes.map((n) => {
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
    [nodes, selectedViewId, searchQuery, searchActive]
  );

  const displayEdges = useMemo(
    () => highlightEdgesConnectedToNode(edges, selectedViewId),
    [edges, selectedViewId]
  );

  const graphRevision = tab.graph
    ? `${tab.id}:${tab.graph.views.length}:${tab.graph.edges.length}:${handleOrientation}:${edgePathStyle}`
    : tab.id;

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
      <div className="disc-doc-pane">
        <div className="disc-banner--error">{t("status.error", { detail: tab.error })}</div>
        <button type="button" className="disc-btn" onClick={refresh}>
          {t("dmViewer.refresh")}
        </button>
      </div>
    );
  }

  return (
    <div className="disc-doc-pane disc-dm-flow-pane">
      <div className="disc-doc-toolbar">
        <span className="disc-dm-flow-pane__title">{t("dmViewer.title")}</span>
        <span className="disc-dm-flow-pane__meta">
          {tab.dataModel.space} / {tab.dataModel.external_id} / {tab.dataModel.version}
          {tab.graph
            ? ` · ${tab.graph.views.length} ${t("dmViewer.views")} · ${tab.graph.edges.length} ${t("dmViewer.edges")}`
            : ""}
        </span>
        <button type="button" className="disc-btn" disabled={tab.loading} onClick={refresh}>
          {t("dmViewer.refresh")}
        </button>
      </div>
      {tab.graph && tab.graph.views.length > 0 && (
        <div className="transform-flow-search-row">
          <label className="disc-dm-flow-search transform-flow-search">
            <span className="disc-dm-flow-search__label">{t("dmViewer.search")}</span>
            <input
              className="disc-input disc-dm-flow-search__input"
              type="search"
              value={searchQuery}
              placeholder={t("dmViewer.searchPlaceholder")}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </label>
          <TransformFlowLayoutControls
            t={t}
            handleOrientation={handleOrientation}
            onHandleOrientationChange={onHandleOrientationChange}
            edgePathStyle={edgePathStyle}
            onEdgePathStyleChange={onEdgePathStyleChange}
            onAutoLayout={handleAutoLayout}
            onFitView={() => fitView({ padding: 0.2, duration: 200 })}
            alignDisabled={alignableSelectionCount < 2}
            onAlignSelection={applySelectionAlign}
          />
        </div>
      )}
      {tab.graph && tab.graph.views.length > 0 && (
        <section className="disc-flow-node-list" aria-label={t("dmViewer.nodeListLabel")}>
          {searchActive ? (
        <div className="disc-dm-flow-search-results">
          {searchMatches.length === 0 ? (
            <span className="disc-dm-flow-search-results__empty">{t("dmViewer.noSearchResults")}</span>
          ) : (
            <ul className="disc-dm-flow-search-results__list">
              {searchMatches.map((view) => (
                <li key={view.id}>
                  <button
                    type="button"
                    className={
                      selectedViewId === view.id
                        ? "disc-dm-flow-search-results__item disc-dm-flow-search-results__item--active"
                        : "disc-dm-flow-search-results__item"
                    }
                    onClick={() => selectView(view, true)}
                  >
                    <span className="disc-dm-flow-search-results__name">
                      {view.name?.trim() || view.external_id}
                    </span>
                    <span className="disc-dm-flow-search-results__meta">
                      {view.space} · {view.version}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
          ) : (
            <ul className="disc-dm-flow-search-results__list">
              {tab.graph.views.map((view) => (
                <li key={view.id}>
                  <button
                    type="button"
                    className={
                      selectedViewId === view.id
                        ? "disc-dm-flow-search-results__item disc-dm-flow-search-results__item--active"
                        : "disc-dm-flow-search-results__item"
                    }
                    onClick={() => selectView(view, true)}
                  >
                    <span className="disc-dm-flow-search-results__name">
                      {view.name?.trim() || view.external_id}
                    </span>
                    <span className="disc-dm-flow-search-results__meta">
                      {view.space} · {view.version}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </section>
      )}
      <div className="disc-dm-flow-body">
        <div className="disc-dm-flow-canvas">
          {tab.loading && !tab.graph ? (
            <p className="disc-empty-hint">{t("dmViewer.loading")}</p>
          ) : tab.graph && tab.graph.views.length === 0 ? (
            <p className="disc-empty-hint">{t("dmViewer.noViews")}</p>
          ) : (
            <FlowHandleOrientationProvider value={handleOrientation}>
              <ReactFlow
                nodes={displayNodes}
                edges={displayEdges}
                nodeTypes={nodeTypes}
                colorMode={theme}
                nodesDraggable
                nodesConnectable={false}
                elementsSelectable
                fitView
                connectionLineType={connectionLineType}
                defaultEdgeOptions={defaultEdgeOptions}
                proOptions={{ hideAttribution: true }}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onNodeClick={onNodeClick}
                onNodeContextMenu={onNodeContextMenu}
                onPaneClick={onPaneClick}
              >
                <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
                <Controls showInteractive={false} />
                <MiniMap zoomable pannable nodeStrokeWidth={2} />
                <FlowHandleOrientationEdgeSync
                  orientation={handleOrientation}
                  edgePathStyle={edgePathStyle}
                />
                <DmFlowSelectionBridge onSelectionChange={onFlowSelectionChange} />
                <FitViewOnLoad revision={graphRevision} />
                <FocusViewNode viewId={focusViewId} />
              </ReactFlow>
            </FlowHandleOrientationProvider>
          )}
        </div>
        <DataModelViewProperties graph={tab.graph} view={selectedView} />
      </div>
      <TreeContextMenuPortal
        menu={
          ctxMenu
            ? {
                x: ctxMenu.x,
                y: ctxMenu.y,
                items: [
                  {
                    id: "query",
                    label: t("discovery.query"),
                    onSelect: () => onQueryView(ctxMenu.view),
                  },
                ],
              }
            : null
        }
        onClose={() => setCtxMenu(null)}
        classPrefix="disc"
      />
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
