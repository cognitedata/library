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
import {
  graphToFlow,
  layoutDmViewNodesByMethod,
  normalizeDmFlowLayoutMethod,
  type DmFlowLayoutMethod,
} from "../utils/dataModelFlowLayout";
import { DM_FLOW_LAYOUT_METHOD_OPTIONS } from "../utils/dmFlowLayoutMethodOptions";
import { DataModelViewProperties } from "./DataModelViewProperties";
import { alignSelectedTransformFlowNodes, type AlignFlowSelectionMode } from "./transform/alignSelectedNodes";
import {
  connectionLineTypeForEdgePathStyle,
  defaultTransformFlowEdgeOptions,
  FlowHandleOrientationEdgeSync,
} from "./transform/FlowHandleOrientationEdgeSync";
import { FlowDocToolbarActions } from "./flow/FlowDocToolbarActions";
import { TransformFlowLayoutControls } from "./transform/TransformFlowLayoutControls";
import { highlightEdgesConnectedToNode } from "./flow/highlightEdgesForSelectedNode";
import { DmViewFlowNode } from "./flow/DmViewFlowNode";
import {
  TreeContextMenuPortal,
  treeCtxMenuSeparator,
  useTreeContextMenuState,
} from "./governance/TreeContextMenu";
import {
  flowLayoutContextMenuItems,
  transformFlowAlignContextMenuItems,
} from "./transform/transformFlowCanvasContextMenu";
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
  const [nodeCtxMenu, setNodeCtxMenu] = useState<CtxMenu>(null);
  const paneCtxMenu = useTreeContextMenuState();
  const [handleOrientation, setHandleOrientation] = useState<TransformCanvasHandleOrientation>("lr");
  const [layoutMethod, setLayoutMethod] = useState<DmFlowLayoutMethod>("dagre");
  const [edgePathStyle, setEdgePathStyle] = useState<TransformCanvasEdgePathStyle>("smoothstep");
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const rfSelectionRef = useRef<Node[]>([]);
  const [alignableSelectionCount, setAlignableSelectionCount] = useState(0);
  const [loadNonce, setLoadNonce] = useState(0);

  const onFlowSelectionChange = useCallback((sel: Node[]) => {
    rfSelectionRef.current = sel;
    setAlignableSelectionCount(sel.length);
  }, []);

  useEffect(() => {
    if (tab.graph != null) return;
    let cancelled = false;
    const load = async () => {
      onTabUpdate({ ...tab, loading: true, error: null });
      try {
        const graph = await fetchDataModelGraph(tab.dataModel);
        if (cancelled) return;
        onTabUpdate({ ...tab, graph, loading: false, error: null });
      } catch (e) {
        if (cancelled) return;
        onTabUpdate({ ...tab, graph: null, loading: false, error: String(e) });
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [
    tab.id,
    tab.graph,
    tab.dataModel.space,
    tab.dataModel.external_id,
    tab.dataModel.version,
    loadNonce,
  ]);

  const searchActive = searchQuery.trim().length > 0;
  const searchMatches = useMemo(
    () => (tab.graph ? filterViewsBySearch(tab.graph.views, searchQuery) : []),
    [tab.graph, searchQuery]
  );

  useEffect(() => {
    if (!tab.graph) return;
    const seed = graphToFlow(tab.graph, { handleOrientation, edgePathStyle, layoutMethod });
    setNodes(orientDmFlowNodes(seed.nodes, handleOrientation));
    setEdges(seed.edges);
    window.setTimeout(() => fitView({ padding: 0.2, duration: 0 }), 0);
  }, [tab.graph, layoutMethod, handleOrientation, edgePathStyle, setNodes, setEdges, fitView]);

  const applyAutoLayout = useCallback(
    (orientation: TransformCanvasHandleOrientation, method: DmFlowLayoutMethod = layoutMethod) => {
      const eds = getEdges();
      setNodes((nds) => {
        const laidOut = layoutDmViewNodesByMethod(nds, eds, orientation, method);
        return orientDmFlowNodes(laidOut, orientation);
      });
      window.setTimeout(() => fitView({ padding: 0.2, duration: 200 }), 0);
    },
    [getEdges, setNodes, fitView, layoutMethod]
  );

  const handleAutoLayout = useCallback(() => {
    applyAutoLayout(handleOrientation, layoutMethod);
  }, [applyAutoLayout, handleOrientation, layoutMethod]);

  const onLayoutMethodChange = useCallback(
    (next: string) => {
      const method = normalizeDmFlowLayoutMethod(next);
      setLayoutMethod(method);
      applyAutoLayout(handleOrientation, method);
    },
    [applyAutoLayout, handleOrientation]
  );

  const onHandleOrientationChange = useCallback(
    (next: TransformCanvasHandleOrientation) => {
      setHandleOrientation(next);
      applyAutoLayout(next, layoutMethod);
    },
    [applyAutoLayout, layoutMethod]
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
    ? `${tab.id}:${tab.graph.views.length}:${tab.graph.edges.length}:${handleOrientation}:${layoutMethod}:${edgePathStyle}`
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
    setNodeCtxMenu(null);
    paneCtxMenu.close();
  }, [paneCtxMenu]);

  const onPaneContextMenu = useCallback(
    (e: MouseEvent) => {
      e.preventDefault();
      setNodeCtxMenu(null);
      paneCtxMenu.open(e, [
        ...transformFlowAlignContextMenuItems({
          t,
          alignDisabled: alignableSelectionCount < 2,
          onAlignSelection: applySelectionAlign,
        }),
        treeCtxMenuSeparator("dm-sep-layout"),
        ...flowLayoutContextMenuItems({
          t,
          handleOrientation,
          onHandleOrientationChange,
          layoutMethod,
          layoutMethodOptions: DM_FLOW_LAYOUT_METHOD_OPTIONS,
          onLayoutMethodChange,
          edgePathStyle,
          onEdgePathStyleChange,
          onFitView: () => fitView({ padding: 0.2, duration: 200 }),
          onAutoLayout: handleAutoLayout,
        }),
      ]);
    },
    [
      paneCtxMenu,
      t,
      alignableSelectionCount,
      applySelectionAlign,
      handleOrientation,
      onHandleOrientationChange,
      layoutMethod,
      onLayoutMethodChange,
      edgePathStyle,
      onEdgePathStyleChange,
      fitView,
      handleAutoLayout,
    ]
  );

  const onNodeContextMenu = useCallback(
    (e: MouseEvent, node: Node) => {
      e.preventDefault();
      paneCtxMenu.close();
      const view = (node.data as { view?: DataModelGraphView }).view;
      if (!view) return;
      setNodeCtxMenu({ x: e.clientX, y: e.clientY, view });
    },
    [paneCtxMenu]
  );

  useEffect(() => {
    if (!nodeCtxMenu) return;
    const close = () => setNodeCtxMenu(null);
    window.addEventListener("click", close);
    return () => window.removeEventListener("click", close);
  }, [nodeCtxMenu]);

  const refresh = useCallback(() => {
    onTabUpdate({ ...tab, graph: null, loading: true, error: null });
    setLoadNonce((n) => n + 1);
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
        <FlowDocToolbarActions
          t={t}
          refreshLabelKey="dmViewer.refresh"
          onRefresh={refresh}
          refreshDisabled={tab.loading}
        />
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
            layoutAriaLabelKey="dmViewer.layout.aria"
            handleOrientation={handleOrientation}
            onHandleOrientationChange={onHandleOrientationChange}
            layoutMethod={layoutMethod}
            layoutMethodOptions={DM_FLOW_LAYOUT_METHOD_OPTIONS}
            layoutMethodLabelKey="dmViewer.layout.method"
            onLayoutMethodChange={onLayoutMethodChange}
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
                onPaneContextMenu={onPaneContextMenu}
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
      <TreeContextMenuPortal menu={paneCtxMenu.menu} onClose={paneCtxMenu.close} classPrefix="disc" />
      <TreeContextMenuPortal
        menu={
          nodeCtxMenu
            ? {
                x: nodeCtxMenu.x,
                y: nodeCtxMenu.y,
                items: [
                  {
                    id: "query",
                    label: t("discovery.query"),
                    onSelect: () => onQueryView(nodeCtxMenu.view),
                  },
                ],
              }
            : null
        }
        onClose={() => setNodeCtxMenu(null)}
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
