import { useCallback, useEffect, useMemo, useState, type MouseEvent as ReactMouseEvent } from "react";
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
import { fetchWorkflowGraph } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import type { WorkflowDocumentTab, WorkflowGraphTask } from "../types/explorerNodes";
import { filterTasksBySearch, taskMatchesSearch } from "../utils/workflowFlowSearch";
import { workflowGraphToFlow } from "../utils/workflowFlowLayout";
import { workflowTaskKindLabel } from "../utils/workflowTaskKind";
import { WorkflowTaskProperties } from "./WorkflowTaskProperties";
import { WfTaskFlowNode } from "./flow/WfTaskFlowNode";

const nodeTypes = { wfTask: WfTaskFlowNode };

type Props = {
  tab: WorkflowDocumentTab;
  onTabUpdate: (tab: WorkflowDocumentTab) => void;
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

function FlowInner({ tab, onTabUpdate }: Props) {
  const { t, theme } = useAppSettings();
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [selectedTask, setSelectedTask] = useState<WorkflowGraphTask | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [focusTaskId, setFocusTaskId] = useState<string | null>(null);

  useEffect(() => {
    if (tab.graph != null || !tab.loading) return;
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
  }, [tab.id, tab.loading, tab.graph]);

  const searchActive = searchQuery.trim().length > 0;
  const searchMatches = useMemo(
    () => (tab.graph ? filterTasksBySearch(tab.graph.tasks, searchQuery) : []),
    [tab.graph, searchQuery]
  );

  const { nodes: baseNodes, edges } = useMemo(
    () => (tab.graph ? workflowGraphToFlow(tab.graph) : { nodes: [] as Node[], edges: [] as Edge[] }),
    [tab.graph]
  );

  const nodes = useMemo(
    () =>
      baseNodes.map((n) => {
        const task = (n.data as { task?: WorkflowGraphTask }).task;
        const matches = task ? taskMatchesSearch(task, searchQuery) : true;
        return {
          ...n,
          data: {
            ...(n.data as object),
            selected: n.id === selectedTaskId,
            dimmed: searchActive && !matches,
          },
        };
      }),
    [baseNodes, selectedTaskId, searchQuery, searchActive]
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
      <div className="exp-doc-pane">
        <div className="exp-banner--error">{t("status.error", { detail: tab.error })}</div>
        <button type="button" className="exp-btn" onClick={refresh}>
          {t("wfViewer.refresh")}
        </button>
      </div>
    );
  }

  return (
    <div className="exp-doc-pane exp-dm-flow-pane">
      <div className="exp-doc-toolbar">
        <span className="exp-dm-flow-pane__title">{t("wfViewer.title")}</span>
        <span className="exp-dm-flow-pane__meta">
          {tab.workflow.external_id}
          {tab.graph?.workflow.version ? ` / ${tab.graph.workflow.version}` : ""}
          {tab.graph
            ? ` · ${tab.graph.tasks.length} ${t("wfViewer.tasks")} · ${tab.graph.edges.length} ${t("wfViewer.edges")}`
            : ""}
        </span>
        {tab.graph && tab.graph.tasks.length > 0 && (
          <label className="exp-dm-flow-search">
            <span className="exp-dm-flow-search__label">{t("wfViewer.search")}</span>
            <input
              className="exp-input exp-dm-flow-search__input"
              type="search"
              value={searchQuery}
              placeholder={t("wfViewer.searchPlaceholder")}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </label>
        )}
        <button type="button" className="exp-btn" disabled={tab.loading} onClick={refresh}>
          {t("wfViewer.refresh")}
        </button>
      </div>
      {searchActive && tab.graph && (
        <div className="exp-dm-flow-search-results">
          {searchMatches.length === 0 ? (
            <span className="exp-dm-flow-search-results__empty">{t("wfViewer.noSearchResults")}</span>
          ) : (
            <ul className="exp-dm-flow-search-results__list">
              {searchMatches.map((task) => (
                <li key={task.id}>
                  <button
                    type="button"
                    className={
                      selectedTaskId === task.id
                        ? "exp-dm-flow-search-results__item exp-dm-flow-search-results__item--active"
                        : "exp-dm-flow-search-results__item"
                    }
                    onClick={() => selectTask(task, true)}
                  >
                    <span className="exp-dm-flow-search-results__name">
                      {task.label?.trim() || task.external_id}
                    </span>
                    <span className="exp-dm-flow-search-results__meta">
                      {workflowTaskKindLabel(task)}
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
            <p className="exp-empty-hint">{t("wfViewer.loading")}</p>
          ) : tab.graph && tab.graph.tasks.length === 0 ? (
            <p className="exp-empty-hint">{t("wfViewer.noTasks")}</p>
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
