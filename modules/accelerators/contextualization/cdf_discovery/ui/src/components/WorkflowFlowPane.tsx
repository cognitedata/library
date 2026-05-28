import { useCallback, useEffect, useMemo, useRef, useState, type MouseEvent as ReactMouseEvent, type RefObject } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useNodesState,
  useReactFlow,
  type Edge,
  type Node,
  type Viewport,
} from "@xyflow/react";
import { fetchWorkflowGraph } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import type { WorkflowDocumentTab, WorkflowGraphTask } from "../types/discoveryNodes";
import { filterTasksBySearch, taskMatchesSearch } from "../utils/workflowFlowSearch";
import { layoutWfTaskNodes, workflowGraphToFlow } from "../utils/workflowFlowLayout";
import { FlowDocToolbarActions } from "./flow/FlowDocToolbarActions";
import { useFlowCanvasKeyboard } from "./flow/useFlowCanvasKeyboard";
import { useFlowLayoutHistory } from "./flow/useFlowLayoutHistory";
import {
  canvasViewportToFlowViewport,
  viewportToCanvasViewport,
} from "./transform/transformFlowHistory";
import { TransformFlowLayoutControls } from "./transform/TransformFlowLayoutControls";
import type { TransformCanvasViewport } from "../types/transformCanvasViewport";
import { workflowTaskKindLabel } from "../utils/workflowTaskKind";
import { WorkflowTaskProperties } from "./WorkflowTaskProperties";
import { highlightEdgesConnectedToNode } from "./flow/highlightEdgesForSelectedNode";
import { WfTaskFlowNode } from "./flow/WfTaskFlowNode";

const nodeTypes = { wfTask: WfTaskFlowNode };

type Props = {
  tab: WorkflowDocumentTab;
  onTabUpdate: (tab: WorkflowDocumentTab) => void;
  readOnly?: boolean;
  onOpenInTransform?: () => void;
  openInTransformBusy?: boolean;
  openInTransformError?: string | null;
};

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

function FocusTaskNode({ taskId }: { taskId: string | null }) {
  const { fitView } = useReactFlow();
  useEffect(() => {
    if (!taskId) return;
    const id = window.requestAnimationFrame(() => {
      fitView({ nodes: [{ id: taskId }], padding: 0.45, duration: 280, maxZoom: 1.15 });
    });
    return () => window.cancelAnimationFrame(id);
  }, [taskId, fitView]);
  return null;
}

function FlowInner({
  tab,
  onTabUpdate,
  readOnly = false,
  onOpenInTransform,
  openInTransformBusy = false,
  openInTransformError = null,
}: Props) {
  const { t, theme } = useAppSettings();
  const flowRootRef = useRef<HTMLDivElement>(null);
  const viewportRef = useRef<TransformCanvasViewport | null>(null);
  const viewportPersistTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [selectedTask, setSelectedTask] = useState<WorkflowGraphTask | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [focusTaskId, setFocusTaskId] = useState<string | null>(null);

  useEffect(() => {
    if (tab.graph != null) return;
    if (tab.error) return;
    if (!tab.loading) {
      onTabUpdate({ ...tab, graph: null, loading: true, error: null });
      return;
    }
    let cancelled = false;
    const load = async () => {
      onTabUpdate({ ...tab, loading: true, error: null });
      try {
        const graph = await fetchWorkflowGraph(tab.workflow);
        if (cancelled) return;
        const version = graph.workflow.version;
        onTabUpdate({
          ...tab,
          graph,
          loading: false,
          error: null,
          workflow: version ? { ...tab.workflow, version } : tab.workflow,
          label: version
            ? `${tab.workflow.external_id} (${version})`
            : tab.label,
        });
      } catch (e) {
        if (cancelled) return;
        onTabUpdate({ ...tab, loading: false, error: String(e) });
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [tab.id, tab.loading, tab.graph, tab.error, tab.workflow.external_id, onTabUpdate]);

  const searchActive = searchQuery.trim().length > 0;
  const searchMatches = useMemo(
    () => (tab.graph ? filterTasksBySearch(tab.graph.tasks, searchQuery, t) : []),
    [tab.graph, searchQuery, t]
  );

  const graphFlow = useMemo(
    () => (tab.graph ? workflowGraphToFlow(tab.graph) : { nodes: [] as Node[], edges: [] as Edge[] }),
    [tab.graph]
  );
  const [layoutNodes, setLayoutNodes, onNodesChange] = useNodesState<Node>([]);

  const layoutHistory = useFlowLayoutHistory({ nodes: layoutNodes, viewportRef });

  useEffect(() => {
    setLayoutNodes(graphFlow.nodes);
    viewportRef.current = null;
    layoutHistory.reset();
  }, [tab.graph, setLayoutNodes, graphFlow.nodes, layoutHistory.reset]);

  useEffect(() => {
    return () => {
      if (viewportPersistTimerRef.current) {
        window.clearTimeout(viewportPersistTimerRef.current);
      }
    };
  }, []);

  const edges = graphFlow.edges;

  const nodes = useMemo(
    () =>
      layoutNodes.map((n) => {
        const task = (n.data as { task?: WorkflowGraphTask }).task;
        const matches = task ? taskMatchesSearch(task, searchQuery, t) : true;
        return {
          ...n,
          data: {
            ...(n.data as object),
            selected: n.id === selectedTaskId,
            dimmed: searchActive && !matches,
          },
        };
      }),
    [layoutNodes, selectedTaskId, searchQuery, searchActive, t]
  );

  const { fitView, setViewport } = useReactFlow();

  const applyLayoutSnapshot = useCallback(
    (snap: ReturnType<typeof layoutHistory.undo>) => {
      if (!snap) return;
      viewportRef.current = snap.viewport;
      setLayoutNodes(snap.nodes);
      if (snap.viewport) {
        setViewport(canvasViewportToFlowViewport(snap.viewport));
      }
    },
    [setLayoutNodes, setViewport]
  );

  const handleUndo = useCallback(() => {
    applyLayoutSnapshot(layoutHistory.undo());
  }, [applyLayoutSnapshot, layoutHistory]);

  const handleRedo = useCallback(() => {
    applyLayoutSnapshot(layoutHistory.redo());
  }, [applyLayoutSnapshot, layoutHistory]);

  useFlowCanvasKeyboard({
    flowRootRef: flowRootRef as RefObject<HTMLElement | null>,
    readOnly,
    onUndo: handleUndo,
    onRedo: handleRedo,
    canUndo: layoutHistory.canUndo,
    canRedo: layoutHistory.canRedo,
  });

  const handleAutoLayout = useCallback(() => {
    if (readOnly) return;
    layoutHistory.recordBeforeChange();
    setLayoutNodes((nds) => layoutWfTaskNodes(nds, edges));
    window.setTimeout(() => fitView({ padding: 0.2, duration: 200 }), 0);
  }, [readOnly, layoutHistory, setLayoutNodes, edges, fitView]);

  const onNodeDragStart = useCallback(() => {
    if (readOnly) return;
    layoutHistory.recordBeforeChange();
  }, [readOnly, layoutHistory]);

  const onMoveEnd = useCallback(
    (_event: MouseEvent | TouchEvent | null, vp: Viewport) => {
      viewportRef.current = viewportToCanvasViewport(vp);
    },
    []
  );

  const displayEdges = useMemo(
    () => highlightEdgesConnectedToNode(edges, selectedTaskId),
    [edges, selectedTaskId]
  );

  const graphRevision = tab.graph
    ? `${tab.id}:${tab.graph.tasks.length}:${tab.graph.edges.length}`
    : tab.id;

  const selectTask = useCallback((task: WorkflowGraphTask, focus = true) => {
    setSelectedTaskId(task.id);
    setSelectedTask(task);
    if (focus) setFocusTaskId(task.id);
  }, []);

  const onNodeClick = useCallback(
    (_: ReactMouseEvent, node: Node) => {
      const task = (node.data as { task?: WorkflowGraphTask }).task;
      if (!task) return;
      selectTask(task, true);
    },
    [selectTask]
  );

  const onPaneClick = useCallback(() => {
    setSelectedTaskId(null);
    setSelectedTask(null);
    setFocusTaskId(null);
  }, []);

  const refresh = useCallback(() => {
    onTabUpdate({ ...tab, graph: null, loading: true, error: null });
    setSelectedTaskId(null);
    setSelectedTask(null);
    setSearchQuery("");
    setFocusTaskId(null);
  }, [tab, onTabUpdate]);

  if (tab.error) {
    return (
      <div className="disc-doc-pane">
        <div className="disc-banner--error">{t("status.error", { detail: tab.error })}</div>
        <button type="button" className="disc-btn" onClick={refresh}>
          {t("wfViewer.refresh")}
        </button>
      </div>
    );
  }

  return (
    <div className="disc-doc-pane disc-dm-flow-pane">
      {openInTransformError ? (
        <div className="disc-banner--error" role="alert">
          {t("status.error", { detail: openInTransformError })}
        </div>
      ) : null}
      <div className="disc-doc-toolbar">
        <span className="disc-dm-flow-pane__title">{t("wfViewer.title")}</span>
        <span className="disc-dm-flow-pane__meta">
          {tab.workflow.external_id}
          {tab.graph?.workflow.version ? ` / ${tab.graph.workflow.version}` : ""}
          {tab.graph
            ? ` · ${tab.graph.tasks.length} ${t("wfViewer.tasks")} · ${tab.graph.edges.length} ${t("wfViewer.edges")}`
            : ""}
        </span>
        <FlowDocToolbarActions
          t={t}
          refreshLabelKey="wfViewer.refresh"
          onRefresh={refresh}
          refreshDisabled={tab.loading}
          openInTransform={
            onOpenInTransform
              ? {
                  labelKey: "wfViewer.openInTransform",
                  busyLabelKey: "wfViewer.openInTransformBusy",
                  hintKey: "wfViewer.openInTransformHint",
                  busy: openInTransformBusy,
                  disabled: tab.loading,
                  onClick: onOpenInTransform,
                }
              : undefined
          }
        />
      </div>
      {tab.graph && tab.graph.tasks.length > 0 && (
        <div className="transform-flow-search-row">
          <label className="disc-dm-flow-search transform-flow-search">
            <span className="disc-dm-flow-search__label">{t("wfViewer.search")}</span>
            <input
              className="disc-input disc-dm-flow-search__input"
              type="search"
              value={searchQuery}
              placeholder={t("wfViewer.searchPlaceholder")}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </label>
          <TransformFlowLayoutControls
            t={t}
            readOnly={readOnly}
            mode="viewer"
            layoutAriaLabelKey="wfViewer.layout.aria"
            onFitView={() => fitView({ padding: 0.2, duration: 200 })}
            onAutoLayout={handleAutoLayout}
            canUndo={layoutHistory.canUndo}
            canRedo={layoutHistory.canRedo}
            onUndo={readOnly ? undefined : handleUndo}
            onRedo={readOnly ? undefined : handleRedo}
          />
        </div>
      )}
      {tab.graph && tab.graph.tasks.length > 0 && (
        <section className="disc-flow-node-list" aria-label={t("wfViewer.nodeListLabel")}>
          {searchActive ? (
        <div className="disc-dm-flow-search-results">
          {searchMatches.length === 0 ? (
            <span className="disc-dm-flow-search-results__empty">{t("wfViewer.noSearchResults")}</span>
          ) : (
            <ul className="disc-dm-flow-search-results__list">
              {searchMatches.map((task) => (
                <li key={task.id}>
                  <button
                    type="button"
                    className={
                      selectedTaskId === task.id
                        ? "disc-dm-flow-search-results__item disc-dm-flow-search-results__item--active"
                        : "disc-dm-flow-search-results__item"
                    }
                    onClick={() => selectTask(task, true)}
                  >
                    <span className="disc-dm-flow-search-results__name">
                      {task.label?.trim() || task.external_id}
                    </span>
                    <span className="disc-dm-flow-search-results__meta">
                      {workflowTaskKindLabel(task, t)}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
          ) : (
            <ul className="disc-dm-flow-search-results__list">
              {tab.graph.tasks.map((task) => (
                <li key={task.id}>
                  <button
                    type="button"
                    className={
                      selectedTaskId === task.id
                        ? "disc-dm-flow-search-results__item disc-dm-flow-search-results__item--active"
                        : "disc-dm-flow-search-results__item"
                    }
                    onClick={() => selectTask(task, true)}
                  >
                    <span className="disc-dm-flow-search-results__name">
                      {task.label?.trim() || task.external_id}
                    </span>
                    <span className="disc-dm-flow-search-results__meta">
                      {workflowTaskKindLabel(task, t)}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </section>
      )}
      <div className="disc-dm-flow-body">
        <div ref={flowRootRef} className="disc-dm-flow-canvas">
          {tab.loading && !tab.graph ? (
            <p className="disc-empty-hint">{t("wfViewer.loading")}</p>
          ) : tab.graph && tab.graph.tasks.length === 0 ? (
            <p className="disc-empty-hint">{t("wfViewer.noTasks")}</p>
          ) : (
            <ReactFlow
              nodes={nodes}
              edges={displayEdges}
              nodeTypes={nodeTypes}
              colorMode={theme}
              onNodesChange={onNodesChange}
              onNodeDragStart={onNodeDragStart}
              onMoveEnd={onMoveEnd}
              nodesDraggable={!readOnly}
              nodesConnectable={false}
              elementsSelectable
              proOptions={{ hideAttribution: true }}
              onNodeClick={onNodeClick}
              onPaneClick={onPaneClick}
            >
              <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
              <Controls showInteractive={false} />
              <MiniMap zoomable pannable nodeStrokeWidth={2} />
              <FitViewOnLoad revision={graphRevision} />
              <FocusTaskNode taskId={focusTaskId} />
            </ReactFlow>
          )}
        </div>
        <WorkflowTaskProperties graph={tab.graph} task={selectedTask} />
      </div>
    </div>
  );
}

export function WorkflowFlowPane(props: Props) {
  return (
    <ReactFlowProvider>
      <FlowInner {...props} />
    </ReactFlowProvider>
  );
}
