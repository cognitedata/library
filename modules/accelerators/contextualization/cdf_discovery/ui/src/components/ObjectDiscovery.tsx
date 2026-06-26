import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type DragEvent,
  type KeyboardEvent,
  type MouseEvent,
} from "react";
import { fetchTreeChildren } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import { useDiscoveryConfig } from "../context/DiscoveryConfigContext";
import type { TreeNode, WorkflowRef } from "../types/discoveryNodes";
import {
  ancestorChainTo,
  buildBreadcrumbTrail,
  collectDescendantIds,
  collectDescendantKeys,
  findNodeInTree,
  getDrillDownChildren,
  isLoadingPlaceholder,
  parentNodeId,
  searchLoadedTree,
  type DrillDownRow,
} from "../utils/treeFilter";
import { opensGovernanceTab } from "../utils/governanceTabs";
import {
  opensTransformTab,
  pipelineIdFromNode,
  templateIdFromNode,
} from "../utils/transformTabs";
import { workflowRefFromNode } from "../utils/workflowTabs";
import { opensExtractTab, opensMonitorTab } from "../utils/workspaceTabs";
import { canQueryTreeNode } from "../utils/sqlQuerySeed";
import { canDropDataTreeEntity } from "../utils/dataTreeEntityDrop";
import { canDragCdfResourceToTransformCanvas } from "../utils/cdfResourceDrop";
import { setCdfResourceDragData, setDataTreeEntityDragData } from "./transform/transformFlowDrag";
import {
  DATA_SAVED_QUERIES,
  TRANSFORM_PIPELINES,
  TRANSFORM_ROOT,
  TRANSFORM_TEMPLATES,
  isTransformWorkflowsSubtreeNodeId,
} from "../utils/treeNodeIds";
import {
  canDragTransformTreeItem,
  endTransformTreeDrag,
  getTransformTreeDragPayload,
  resolveTransformTreeDropTarget,
  setTransformTreeDragData,
  transformTreeDropAccepts,
} from "../utils/transformTreeDrag";
import { savedQueryFromNode } from "../utils/savedQueries";
import { treeNodeDisplayLabel } from "../utils/treeNodeLabels";
import { treeNodeDescription } from "../utils/treeNodeDescriptions";
import { TreeNavIcon } from "./TreeNavIcon";
import type { SavedQuery } from "../types/discoveryNodes";
import {
  resolveTreeToolbarNewAction,
  treeToolbarNewLabels,
  type TreeToolbarNewAction,
} from "../utils/treeToolbarNew";
import {
  TreeContextMenuPortal,
  type TreeCtxMenuItem,
} from "./governance/TreeContextMenu";

const ROOT_ID = "connection";

export type GovernanceArtifactsRevision = {
  token: number;
  workspace: "spaces" | "groups";
};

type Props = {
  refreshKey: number;
  savedQueriesRevision?: number;
  governanceArtifactsRevision?: GovernanceArtifactsRevision;
  transformPipelinesRevision?: number;
  transformTemplatesRevision?: number;
  connectionLabel?: string;
  selectedId: string | null;
  onSelectNode: (node: TreeNode | null) => void;
  onOpenNode: (node: TreeNode) => void;
  onDeleteSavedQuery?: (query: SavedQuery) => void;
  onTreeNew?: (action: TreeToolbarNewAction) => void;
  onDeletePipeline?: (pipelineId: string, label: string) => void;
  onDeleteTemplate?: (templateId: string, label: string) => void;
  onRenamePipeline?: (pipelineId: string, label: string) => void;
  onRenameTemplate?: (templateId: string, label: string) => void;
  onPipelineDropOnTemplates?: (pipelineId: string, pipelineLabel: string) => void;
  onTemplateDropOnPipelines?: (templateId: string, templateLabel: string) => void;
  onOpenWorkflowInTransform?: (ref: WorkflowRef) => void;
  onDeleteWorkflowInTransform?: (ref: WorkflowRef, label: string) => void;
  dataTreeDragEnabled?: boolean;
};

type CtxMenu = { x: number; y: number; node: TreeNode } | null;

export function ObjectDiscovery({
  refreshKey,
  savedQueriesRevision = 0,
  governanceArtifactsRevision,
  transformPipelinesRevision = 0,
  transformTemplatesRevision = 0,
  connectionLabel,
  selectedId,
  onSelectNode,
  onOpenNode,
  onDeleteSavedQuery,
  onTreeNew,
  onDeletePipeline,
  onDeleteTemplate,
  onRenamePipeline,
  onRenameTemplate,
  onPipelineDropOnTemplates,
  onTemplateDropOnPipelines,
  onOpenWorkflowInTransform,
  onDeleteWorkflowInTransform,
  dataTreeDragEnabled = false,
}: Props) {
  const { t } = useAppSettings();
  const { sortNodes, isStarred, toggleStar, starredIds } = useDiscoveryConfig();
  const [filter, setFilter] = useState("");
  const [currentParentId, setCurrentParentId] = useState(ROOT_ID);
  const [childrenByParent, setChildrenByParent] = useState<Map<string, TreeNode[]>>(new Map());
  const [loadedIds, setLoadedIds] = useState<Set<string>>(() => new Set());
  const [loading, setLoading] = useState<Set<string>>(new Set());
  const [errors, setErrors] = useState<Map<string, string>>(new Map());
  const [ctxMenu, setCtxMenu] = useState<CtxMenu>(null);
  const [focusedId, setFocusedId] = useState<string | null>(selectedId);
  const [transformTreeDropTargetId, setTransformTreeDropTargetId] = useState<string | null>(null);
  const abortByNode = useRef<Map<string, AbortController>>(new Map());
  const loadedRef = useRef(loadedIds);
  const loadingRef = useRef(loading);
  loadedRef.current = loadedIds;
  loadingRef.current = loading;

  const rootNode = useMemo<TreeNode>(
    () => ({
      id: ROOT_ID,
      label: connectionLabel?.trim() || t("connection.loading"),
      kind: "connection",
      has_children: true,
    }),
    [connectionLabel, t]
  );

  const nodeLabel = useCallback(
    (node: TreeNode) => treeNodeDisplayLabel(node, t),
    [t]
  );

  const nodeDescription = useCallback(
    (node: TreeNode) => treeNodeDescription(node, t),
    [t]
  );

  const invalidateSubtree = useCallback((nodeId: string) => {
    setChildrenByParent((prev) => {
      const next = new Map(prev);
      for (const key of collectDescendantKeys(next, nodeId)) {
        next.delete(key);
      }
      return next;
    });
    setLoadedIds((prev) => {
      const next = new Set(prev);
      for (const id of collectDescendantIds(prev, nodeId)) {
        next.delete(id);
      }
      return next;
    });
    setErrors((prev) => {
      const next = new Map(prev);
      for (const key of collectDescendantKeys(next, nodeId)) {
        next.delete(key);
      }
      return next;
    });
  }, []);

  const loadChildren = useCallback(async (nodeId: string, { force = false }: { force?: boolean } = {}) => {
    if (!force && (loadedRef.current.has(nodeId) || loadingRef.current.has(nodeId))) {
      return;
    }

    abortByNode.current.get(nodeId)?.abort();
    const controller = new AbortController();
    abortByNode.current.set(nodeId, controller);

    setLoading((prev) => new Set(prev).add(nodeId));
    setErrors((prev) => {
      const next = new Map(prev);
      next.delete(nodeId);
      return next;
    });

    try {
      const { nodes } = await fetchTreeChildren(nodeId, controller.signal);
      if (controller.signal.aborted) return;
      setChildrenByParent((prev) => {
        const next = new Map(prev);
        next.set(nodeId, sortNodes(nodes));
        return next;
      });
      setLoadedIds((prev) => new Set(prev).add(nodeId));
    } catch (e) {
      if (controller.signal.aborted) return;
      setErrors((prev) => new Map(prev).set(nodeId, String(e)));
    } finally {
      if (abortByNode.current.get(nodeId) === controller) {
        abortByNode.current.delete(nodeId);
      }
      setLoading((prev) => {
        const next = new Set(prev);
        next.delete(nodeId);
        return next;
      });
    }
  }, [sortNodes]);

  const preloadPath = useCallback(
    (targetParentId: string) => {
      for (const nodeId of ancestorChainTo(targetParentId, ROOT_ID)) {
        void loadChildren(nodeId);
      }
    },
    [loadChildren]
  );

  useEffect(() => {
    setChildrenByParent((prev) => {
      if (prev.size === 0) return prev;
      const next = new Map<string, TreeNode[]>();
      for (const [parentId, kids] of prev) {
        next.set(parentId, sortNodes(kids));
      }
      return next;
    });
  }, [starredIds, sortNodes]);

  useEffect(() => {
    for (const c of abortByNode.current.values()) {
      c.abort();
    }
    abortByNode.current.clear();
    setChildrenByParent(new Map());
    setLoadedIds(new Set());
    setCurrentParentId(ROOT_ID);
    setErrors(new Map());
    setLoading(new Set());
  }, [refreshKey]);

  useEffect(() => {
    if (!savedQueriesRevision) return;
    invalidateSubtree(DATA_SAVED_QUERIES);
    void loadChildren(DATA_SAVED_QUERIES, { force: true });
  }, [savedQueriesRevision, invalidateSubtree, loadChildren]);

  useEffect(() => {
    if (!governanceArtifactsRevision?.token) return;
    const workspace = governanceArtifactsRevision.workspace;
    const rootId = workspace === "spaces" ? "gov:spaces" : "gov:groups";
    const prefix = `${rootId}:`;
    invalidateSubtree(rootId);
    const reloadIds = new Set<string>([rootId]);
    for (const nodeId of loadedIds) {
      if (nodeId === rootId || nodeId.startsWith(prefix)) reloadIds.add(nodeId);
    }
    for (const nodeId of reloadIds) {
      void loadChildren(nodeId, { force: true });
    }
  }, [governanceArtifactsRevision, loadedIds, invalidateSubtree, loadChildren]);

  useEffect(() => {
    if (!transformPipelinesRevision) return;
    invalidateSubtree(TRANSFORM_ROOT);
    invalidateSubtree(TRANSFORM_PIPELINES);
    const reloadIds = new Set<string>([TRANSFORM_ROOT, TRANSFORM_PIPELINES]);
    for (const nodeId of loadedIds) {
      if (isTransformWorkflowsSubtreeNodeId(nodeId)) reloadIds.add(nodeId);
    }
    for (const nodeId of reloadIds) {
      void loadChildren(nodeId, { force: true });
    }
  }, [transformPipelinesRevision, loadedIds, invalidateSubtree, loadChildren]);

  useEffect(() => {
    if (!transformTemplatesRevision) return;
    invalidateSubtree(TRANSFORM_ROOT);
    invalidateSubtree(TRANSFORM_TEMPLATES);
    if (loadedIds.has(TRANSFORM_ROOT)) {
      void loadChildren(TRANSFORM_ROOT, { force: true });
    }
    if (loadedIds.has(TRANSFORM_TEMPLATES)) {
      void loadChildren(TRANSFORM_TEMPLATES, { force: true });
    }
  }, [transformTemplatesRevision, loadedIds, invalidateSubtree, loadChildren]);

  useEffect(
    () => () => {
      for (const c of abortByNode.current.values()) {
        c.abort();
      }
    },
    []
  );

  useEffect(() => {
    void loadChildren(currentParentId);
  }, [currentParentId, loadChildren]);

  useEffect(() => {
    preloadPath(currentParentId);
  }, [currentParentId, preloadPath]);

  const isFiltering = filter.trim().length > 0;

  const breadcrumb = useMemo(
    () =>
      isFiltering
        ? []
        : buildBreadcrumbTrail(currentParentId, childrenByParent, rootNode, nodeLabel),
    [childrenByParent, currentParentId, isFiltering, nodeLabel, rootNode]
  );

  const currentRows = useMemo((): DrillDownRow[] => {
    if (isFiltering) {
      return searchLoadedTree(childrenByParent, filter, rootNode, nodeLabel);
    }
    return getDrillDownChildren(
      currentParentId,
      childrenByParent,
      loadedIds,
      t("tree.loading")
    ).map((node) => ({ node, parentId: currentParentId }));
  }, [
    childrenByParent,
    currentParentId,
    filter,
    isFiltering,
    loadedIds,
    nodeLabel,
    rootNode,
    t,
  ]);

  const navigableRows = useMemo(
    () => currentRows.filter((row) => !isLoadingPlaceholder(row.node)),
    [currentRows]
  );

  useEffect(() => {
    if (selectedId) setFocusedId(selectedId);
  }, [selectedId]);

  useEffect(() => {
    if (!selectedId) return;
    const node = findNodeInTree(selectedId, childrenByParent, rootNode);
    if (!node) return;
    const nextParent = node.has_children ? node.id : parentNodeId(node.id, ROOT_ID);
    setCurrentParentId(nextParent);
    preloadPath(nextParent);
  }, [selectedId, childrenByParent, rootNode, preloadPath]);

  const selectedTreeNode = useMemo(
    () => (selectedId ? findNodeInTree(selectedId, childrenByParent, rootNode) : null),
    [childrenByParent, rootNode, selectedId]
  );

  const toolbarNewAction = useMemo(
    () => resolveTreeToolbarNewAction(selectedTreeNode),
    [selectedTreeNode]
  );

  const toolbarNewLabels = toolbarNewAction ? treeToolbarNewLabels(toolbarNewAction) : null;

  const opensDocumentTab = (node: TreeNode) =>
    node.kind === "dm_data_model" ||
    node.kind === "workflow" ||
    node.kind === "transformation" ||
    node.kind === "function" ||
    node.kind === "saved_query" ||
    opensGovernanceTab(node) ||
    opensTransformTab(node) ||
    opensExtractTab(node) ||
    opensMonitorTab(node);

  const openNode = (node: TreeNode) => {
    if (opensDocumentTab(node) || canQueryTreeNode(node)) onOpenNode(node);
  };

  const drillInto = useCallback(
    (node: TreeNode) => {
      if (!node.has_children || isLoadingPlaceholder(node)) return;
      setCurrentParentId(node.id);
      setFocusedId(null);
      void loadChildren(node.id);
    },
    [loadChildren]
  );

  const navigateToParent = useCallback(
    (parentId: string) => {
      setCurrentParentId(parentId);
      setFocusedId(null);
      void loadChildren(parentId);
    },
    [loadChildren]
  );

  const goBack = useCallback(() => {
    if (currentParentId === ROOT_ID) return;
    navigateToParent(parentNodeId(currentParentId, ROOT_ID));
  }, [currentParentId, navigateToParent]);

  const handleNodeActivate = useCallback(
    (node: TreeNode) => {
      if (isLoadingPlaceholder(node)) return;
      setFocusedId(node.id);
      onSelectNode(node);
      if (isFiltering) {
        const parent = parentNodeId(node.id, ROOT_ID);
        setCurrentParentId(node.has_children ? node.id : parent);
        setFilter("");
      } else if (node.has_children) {
        drillInto(node);
      }
    },
    [drillInto, isFiltering, onSelectNode]
  );

  const openContextMenuForNode = useCallback((node: TreeNode, anchor?: HTMLElement | null) => {
    const el = anchor ?? document.getElementById(`disc-treeitem-${node.id}`);
    const rect = el?.getBoundingClientRect();
    setCtxMenu({
      x: rect?.left ?? 8,
      y: (rect?.bottom ?? rect?.top ?? 8) + 4,
      node,
    });
  }, []);

  const focusTreeNode = useCallback((id: string) => {
    setFocusedId(id);
    requestAnimationFrame(() => {
      document.getElementById(`disc-treeitem-${id}`)?.focus();
    });
  }, []);

  const handleTreeKeyDown = useCallback(
    (e: KeyboardEvent<HTMLUListElement>) => {
      if (!isFiltering && (e.key === "Backspace" || e.key === "ArrowLeft") && currentParentId !== ROOT_ID) {
        const active = document.activeElement;
        if (
          active instanceof HTMLInputElement ||
          active instanceof HTMLTextAreaElement ||
          active instanceof HTMLSelectElement
        ) {
          return;
        }
        e.preventDefault();
        goBack();
        return;
      }

      const currentId = focusedId ?? selectedId ?? navigableRows[0]?.node.id;
      if (!currentId) return;
      const idx = navigableRows.findIndex((row) => row.node.id === currentId);
      if (idx < 0) return;
      const row = navigableRows[idx];
      const node = row.node;

      if (e.key === "ArrowDown") {
        e.preventDefault();
        const next = navigableRows[idx + 1];
        if (next) focusTreeNode(next.node.id);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        const prev = navigableRows[idx - 1];
        if (prev) focusTreeNode(prev.node.id);
      } else if (e.key === "ArrowRight") {
        if (node.has_children && !isLoadingPlaceholder(node)) {
          e.preventDefault();
          handleNodeActivate(node);
        }
      } else if (e.key === "Enter") {
        e.preventDefault();
        handleNodeActivate(node);
        if (!node.has_children) openNode(node);
      } else if (e.key === "ContextMenu" || (e.shiftKey && e.key === "F10")) {
        e.preventDefault();
        openContextMenuForNode(node, document.getElementById(`disc-treeitem-${node.id}`));
      }
    },
    [
      currentParentId,
      focusTreeNode,
      focusedId,
      goBack,
      handleNodeActivate,
      isFiltering,
      navigableRows,
      openContextMenuForNode,
      openNode,
      selectedId,
    ]
  );

  const handleTransformTreeDragOver = useCallback(
    (e: DragEvent, node: TreeNode) => {
      const dropKind = resolveTransformTreeDropTarget(node);
      if (!dropKind) return;
      const payload = getTransformTreeDragPayload(e);
      if (!payload || !transformTreeDropAccepts(dropKind, payload)) return;
      e.preventDefault();
      e.stopPropagation();
      e.dataTransfer.dropEffect = "copy";
      setTransformTreeDropTargetId(node.id);
    },
    []
  );

  const handleTransformTreeDragLeave = useCallback((e: DragEvent, node: TreeNode) => {
    const related = e.relatedTarget;
    if (related instanceof HTMLElement && e.currentTarget.contains(related)) return;
    setTransformTreeDropTargetId((prev) => (prev === node.id ? null : prev));
  }, []);

  const handleTransformTreeDrop = useCallback(
    (e: DragEvent, node: TreeNode) => {
      const dropKind = resolveTransformTreeDropTarget(node);
      const payload = getTransformTreeDragPayload(e);
      setTransformTreeDropTargetId(null);
      endTransformTreeDrag();
      if (!dropKind || !payload || !transformTreeDropAccepts(dropKind, payload)) return;
      e.preventDefault();
      e.stopPropagation();
      const savePipelineAsTemplate = dropKind === "templates" && payload.kind === "etl_pipeline";
      const createPipelineFromTemplate = dropKind === "pipelines" && payload.kind === "etl_template";
      if (savePipelineAsTemplate) {
        onPipelineDropOnTemplates?.(payload.pipelineId, payload.label);
      } else if (createPipelineFromTemplate) {
        onTemplateDropOnPipelines?.(payload.templateId, payload.label);
      }
    },
    [onPipelineDropOnTemplates, onTemplateDropOnPipelines]
  );

  const ctxMenuItems = useMemo((): TreeCtxMenuItem[] => {
    if (!ctxMenu) return [];
    const node = ctxMenu.node;
    const items: TreeCtxMenuItem[] = [];
    const newAction = resolveTreeToolbarNewAction(node);
    const newLabels = newAction ? treeToolbarNewLabels(newAction) : null;
    if (newAction && onTreeNew && newLabels) {
      items.push({
        id: "new",
        label: t(newLabels.labelKey),
        onSelect: () => onTreeNew(newAction),
      });
    }
    if (!isLoadingPlaceholder(node)) {
      items.push({
        id: "star",
        label: isStarred(node.id) ? t("discovery.unfavorite") : t("discovery.favorite"),
        onSelect: () => void toggleStar(node.id),
      });
    }
    if (opensDocumentTab(node)) {
      items.push({
        id: "open",
        label: t("discovery.open"),
        onSelect: () => openNode(node),
      });
    }
    if (node.kind === "workflow" && onOpenWorkflowInTransform) {
      const wfRef = workflowRefFromNode(node);
      if (wfRef) {
        items.push({
          id: "open-in-transform",
          label: t("wfViewer.openInTransform"),
          onSelect: () => onOpenWorkflowInTransform(wfRef),
        });
      }
    }
    const canDeleteWorkflowInTransform =
      node.kind === "workflow" && node.meta?.can_delete_in_transform === true;
    if (canDeleteWorkflowInTransform && onDeleteWorkflowInTransform) {
      const wfRef = workflowRefFromNode(node);
      if (wfRef) {
        items.push({
          id: "delete-workflow-in-transform",
          label: t("transform.pipelines.delete"),
          onSelect: () => onDeleteWorkflowInTransform(wfRef, node.label),
        });
      }
    }
    const delQuery =
      node.kind === "saved_query" && onDeleteSavedQuery ? savedQueryFromNode(node) : null;
    if (delQuery && onDeleteSavedQuery) {
      items.push({
        id: "delete-query",
        label: t("discovery.delete"),
        onSelect: () => onDeleteSavedQuery(delQuery),
      });
    }
    const pipeId = node.kind === "etl_pipeline" ? pipelineIdFromNode(node) : null;
    if (pipeId && onRenamePipeline) {
      items.push({
        id: "rename-pipeline",
        label: t("transform.pipelines.rename"),
        onSelect: () => onRenamePipeline(pipeId, node.label),
      });
    }
    if (pipeId && onDeletePipeline) {
      items.push({
        id: "delete-pipeline",
        label: t("transform.pipelines.delete"),
        onSelect: () => void onDeletePipeline(pipeId, node.label),
      });
    }
    const tplId = node.kind === "etl_template" ? templateIdFromNode(node) : null;
    if (tplId && onRenameTemplate) {
      items.push({
        id: "rename-template",
        label: t("transform.templates.rename"),
        onSelect: () => onRenameTemplate(tplId, node.label),
      });
    }
    if (tplId && onDeleteTemplate) {
      items.push({
        id: "delete-template",
        label: t("transform.templates.delete"),
        onSelect: () => void onDeleteTemplate(tplId, node.label),
      });
    }
    if (canQueryTreeNode(node)) {
      items.push({
        id: "query",
        label: t("discovery.query"),
        onSelect: () => openNode(node),
      });
    }
    if (node.has_children) {
      items.push({
        id: "refresh",
        label: t("discovery.refresh"),
        onSelect: () => {
          invalidateSubtree(node.id);
          void loadChildren(node.id, { force: true });
        },
      });
    }
    return items;
  }, [
    ctxMenu,
    invalidateSubtree,
    isStarred,
    loadChildren,
    onDeletePipeline,
    onDeleteSavedQuery,
    onDeleteTemplate,
    onDeleteWorkflowInTransform,
    onOpenWorkflowInTransform,
    onRenamePipeline,
    onRenameTemplate,
    onTreeNew,
    openNode,
    t,
    toggleStar,
  ]);

  const currentFolderLabel =
    breadcrumb.length > 0 ? breadcrumb[breadcrumb.length - 1]?.label : rootNode.label;

  return (
    <div className="disc-drill-nav">
      <div className="disc-tree-toolbar">
        <label className="disc-tree-toolbar__filter-label">
          <span className="disc-visually-hidden">{t("discovery.filter")}</span>
          <input
            className="disc-input disc-tree-toolbar__filter"
            type="search"
            placeholder={t("discovery.filter")}
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
        </label>
      </div>

      {!isFiltering && currentParentId !== ROOT_ID ? (
        <div className="disc-drill-header">
          <button
            type="button"
            className="disc-drill-back"
            onClick={goBack}
            aria-label={t("discovery.nav.back")}
          >
            <span className="disc-drill-back__icon" aria-hidden>
              ←
            </span>
            <span className="disc-drill-back__label">{t("discovery.nav.back")}</span>
          </button>
        </div>
      ) : null}

      {!isFiltering && breadcrumb.length > 1 ? (
        <nav className="disc-drill-breadcrumb" aria-label={t("a11y.discoveryBreadcrumb")}>
          <ol className="disc-drill-breadcrumb__list">
            {breadcrumb.map((segment, index) => {
              const isLast = index === breadcrumb.length - 1;
              return (
                <li key={segment.id} className="disc-drill-breadcrumb__item">
                  {index > 0 ? (
                    <span className="disc-drill-breadcrumb__sep" aria-hidden>
                      /
                    </span>
                  ) : null}
                  {isLast ? (
                    <span className="disc-drill-breadcrumb__current" aria-current="location">
                      {segment.label}
                    </span>
                  ) : (
                    <button
                      type="button"
                      className="disc-drill-breadcrumb__link"
                      onClick={() => navigateToParent(segment.id)}
                    >
                      {segment.label}
                    </button>
                  )}
                </li>
              );
            })}
          </ol>
        </nav>
      ) : null}

      {!isFiltering ? (
        <div className="disc-drill-folder-title" aria-hidden>
          {currentFolderLabel}
        </div>
      ) : (
        <div className="disc-drill-folder-title disc-drill-folder-title--search">
          {t("discovery.nav.searchResults")}
        </div>
      )}

      <div className="disc-tree disc-drill-panel">
        {currentRows.length === 0 ? (
          <p className="disc-empty-hint disc-drill-empty">{t("discovery.empty")}</p>
        ) : (
          <ul
            className="disc-drill-list"
            role="tree"
            aria-label={t("a11y.discoveryTreeLabel")}
            onKeyDown={handleTreeKeyDown}
          >
            {currentRows.map(({ node, pathLabel }) => {
              const isPlaceholder = isLoadingPlaceholder(node);
              const isSel = !isPlaceholder && selectedId === node.id;
              const err = errors.get(node.id);
              const isLoading = loading.has(node.id);
              const isDataTreeDraggable =
                dataTreeDragEnabled && !isPlaceholder && canDropDataTreeEntity(node);
              const isCdfResourceDraggable =
                dataTreeDragEnabled && !isPlaceholder && canDragCdfResourceToTransformCanvas(node);
              const isTransformTreeDraggable =
                !isPlaceholder && canDragTransformTreeItem(node);
              const isDraggable =
                isDataTreeDraggable || isCdfResourceDraggable || isTransformTreeDraggable;
              const transformDropKind = resolveTransformTreeDropTarget(node);
              const transformDropTarget =
                !isPlaceholder &&
                transformTreeDropTargetId === node.id &&
                transformDropKind != null;
              const transformDropHint =
                transformDropKind === "templates"
                  ? t("transform.treeDrag.dropPipelineOnTemplates")
                  : transformDropKind === "pipelines"
                    ? t("transform.treeDrag.dropTemplateOnPipelines")
                    : undefined;
              const isFolder = node.has_children && !isPlaceholder;
              const secondaryText = pathLabel ?? nodeDescription(node);
              return (
                <li key={node.id} className="disc-nav-item" role="none">
                  <div
                    className={`disc-nav-row-wrap${transformDropTarget ? " disc-tree-row--drop-target" : ""}`}
                    onDragOver={
                      transformDropKind
                        ? (e) => handleTransformTreeDragOver(e, node)
                        : undefined
                    }
                    onDragLeave={
                      transformDropKind
                        ? (e) => handleTransformTreeDragLeave(e, node)
                        : undefined
                    }
                    onDrop={
                      transformDropKind ? (e) => handleTransformTreeDrop(e, node) : undefined
                    }
                  >
                    {isPlaceholder ? (
                      <span className="disc-nav-row disc-nav-row--loading">
                        <span className="disc-nav-row__icon-box" aria-hidden />
                        <span className="disc-nav-row__text">
                          <span className="disc-nav-row__label">{node.label}</span>
                        </span>
                      </span>
                    ) : (
                      <button
                        type="button"
                        id={`disc-treeitem-${node.id}`}
                        role="treeitem"
                        aria-selected={isSel}
                        aria-expanded={isFolder ? false : undefined}
                        tabIndex={focusedId === node.id || (focusedId == null && isSel) ? 0 : -1}
                        className={`disc-nav-row${isSel ? " disc-nav-row--selected" : ""}${
                          node.starred || isStarred(node.id) ? " disc-nav-row--starred" : ""
                        }${isDraggable ? " disc-tree-node--draggable" : ""}${
                          transformDropTarget ? " disc-tree-node--drop-target" : ""
                        }${isFolder ? " disc-nav-row--folder" : ""}`}
                        draggable={isDraggable}
                        title={
                          transformDropHint ??
                          (isFolder
                            ? t("discovery.nav.drillInto", { name: nodeLabel(node) })
                            : isCdfResourceDraggable
                              ? t("transform.treeDrag.cdfResourceHint")
                              : isDataTreeDraggable
                                ? t("transform.treeDrag.hint")
                                : isTransformTreeDraggable
                                  ? t("transform.treeDrag.pipelineOrTemplate")
                                  : undefined)
                        }
                        onDragStart={
                          isDraggable
                            ? (e) => {
                                e.stopPropagation();
                                if (isTransformTreeDraggable) {
                                  setTransformTreeDragData(e, node);
                                } else if (isCdfResourceDraggable) {
                                  setCdfResourceDragData(e, node);
                                } else {
                                  setDataTreeEntityDragData(e, node);
                                }
                              }
                            : undefined
                        }
                        onDragEnd={
                          isTransformTreeDraggable
                            ? () => {
                                endTransformTreeDrag();
                              }
                            : undefined
                        }
                        onClick={() => handleNodeActivate(node)}
                        onDoubleClick={() => openNode(node)}
                        onContextMenu={(e: MouseEvent) => {
                          e.preventDefault();
                          openContextMenuForNode(node, e.currentTarget as HTMLElement);
                        }}
                      >
                        <span className="disc-nav-row__icon-box">
                          <TreeNavIcon node={node} className="disc-nav-row__icon" />
                        </span>
                        <span className="disc-nav-row__text">
                          <span className="disc-nav-row__label">
                            {(node.starred || isStarred(node.id)) && (
                              <span className="disc-tree-star" aria-hidden>
                                ★{" "}
                              </span>
                            )}
                            {nodeLabel(node)}
                            {err ? " ⚠" : ""}
                          </span>
                          <span className="disc-nav-row__desc">{secondaryText}</span>
                        </span>
                        {isFolder ? (
                          <span className="disc-nav-row__chevron" aria-hidden>
                            {isLoading ? "…" : "›"}
                          </span>
                        ) : null}
                      </button>
                    )}
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </div>
      {toolbarNewAction && onTreeNew && toolbarNewLabels ? (
        <div className="disc-nav-footer">
          <button
            type="button"
            className="disc-nav-footer__add"
            onClick={() => onTreeNew(toolbarNewAction)}
            title={t(toolbarNewLabels.titleKey)}
          >
            {t(toolbarNewLabels.labelKey)}
          </button>
        </div>
      ) : null}
      <TreeContextMenuPortal
        menu={ctxMenu ? { x: ctxMenu.x, y: ctxMenu.y, items: ctxMenuItems } : null}
        onClose={() => setCtxMenu(null)}
        classPrefix="disc"
      />
    </div>
  );
}
