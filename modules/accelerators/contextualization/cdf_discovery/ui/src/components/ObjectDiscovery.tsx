import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type DragEvent,
  type MouseEvent,
} from "react";
import { createPortal } from "react-dom";
import { fetchTreeChildren } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import { useDiscoveryConfig } from "../context/DiscoveryConfigContext";
import type { TreeNode } from "../types/discoveryNodes";
import {
  collectDescendantIds,
  collectDescendantKeys,
  flattenVisibleTree,
  isLoadingPlaceholder,
} from "../utils/treeFilter";
import { opensGovernanceTab } from "../utils/governanceTabs";
import { opensTransformTab, pipelineIdFromNode, templateIdFromNode } from "../utils/transformTabs";
import { opensExtractTab, opensMonitorTab } from "../utils/workspaceTabs";
import { canQueryTreeNode } from "../utils/sqlQuerySeed";
import { canDropDataTreeEntity } from "../utils/dataTreeEntityDrop";
import { setDataTreeEntityDragData } from "./transform/transformFlowDrag";
import { DATA_SAVED_QUERIES, TRANSFORM_ROOT, TRANSFORM_TEMPLATES } from "../utils/treeNodeIds";
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
import type { SavedQuery } from "../types/discoveryNodes";
import {
  resolveTreeToolbarNewAction,
  treeToolbarNewLabels,
  type TreeToolbarNewAction,
} from "../utils/treeToolbarNew";

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
  dataTreeDragEnabled = false,
}: Props) {
  const { t } = useAppSettings();
  const { sortNodes, isStarred, toggleStar, starredIds } = useDiscoveryConfig();
  const [filter, setFilter] = useState("");
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set([ROOT_ID]));
  const [childrenByParent, setChildrenByParent] = useState<Map<string, TreeNode[]>>(new Map());
  const [loadedIds, setLoadedIds] = useState<Set<string>>(() => new Set());
  const [loading, setLoading] = useState<Set<string>>(new Set());
  const [errors, setErrors] = useState<Map<string, string>>(new Map());
  const [ctxMenu, setCtxMenu] = useState<CtxMenu>(null);
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
    setExpanded(new Set([ROOT_ID]));
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
    for (const nodeId of expanded) {
      if (nodeId === rootId || nodeId.startsWith(prefix)) reloadIds.add(nodeId);
    }
    for (const nodeId of reloadIds) {
      void loadChildren(nodeId, { force: true });
    }
  }, [governanceArtifactsRevision, expanded, invalidateSubtree, loadChildren]);

  useEffect(() => {
    if (!transformPipelinesRevision) return;
    invalidateSubtree(TRANSFORM_ROOT);
    if (expanded.has(TRANSFORM_ROOT)) {
      void loadChildren(TRANSFORM_ROOT, { force: true });
    }
  }, [transformPipelinesRevision, expanded, invalidateSubtree, loadChildren]);

  useEffect(() => {
    if (!transformTemplatesRevision) return;
    invalidateSubtree(TRANSFORM_ROOT);
    invalidateSubtree(TRANSFORM_TEMPLATES);
    if (expanded.has(TRANSFORM_ROOT)) {
      void loadChildren(TRANSFORM_ROOT, { force: true });
    }
    if (expanded.has(TRANSFORM_TEMPLATES)) {
      void loadChildren(TRANSFORM_TEMPLATES, { force: true });
    }
  }, [transformTemplatesRevision, expanded, invalidateSubtree, loadChildren]);

  useEffect(
    () => () => {
      for (const c of abortByNode.current.values()) {
        c.abort();
      }
    },
    []
  );

  useEffect(() => {
    for (const nodeId of expanded) {
      void loadChildren(nodeId);
    }
  }, [expanded, loadChildren]);

  const toggleExpand = (node: TreeNode) => {
    if (isLoadingPlaceholder(node)) return;
    const next = new Set(expanded);
    if (next.has(node.id)) {
      next.delete(node.id);
    } else {
      next.add(node.id);
    }
    setExpanded(next);
  };

  const flat = useMemo(
    () =>
      flattenVisibleTree(ROOT_ID, childrenByParent, expanded, loadedIds, filter, rootNode, t("tree.loading")),
    [childrenByParent, expanded, loadedIds, filter, rootNode, t]
  );

  const selectedTreeNode = useMemo(
    () => (selectedId ? flat.find((row) => row.node.id === selectedId)?.node ?? null : null),
    [flat, selectedId]
  );

  const toolbarNewAction = useMemo(
    () => resolveTreeToolbarNewAction(selectedTreeNode),
    [selectedTreeNode]
  );

  const ctxNewAction = useMemo(
    () => (ctxMenu ? resolveTreeToolbarNewAction(ctxMenu.node) : null),
    [ctxMenu]
  );

  const toolbarNewLabels = toolbarNewAction ? treeToolbarNewLabels(toolbarNewAction) : null;
  const ctxNewLabels = ctxNewAction ? treeToolbarNewLabels(ctxNewAction) : null;

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

  useEffect(() => {
    if (!ctxMenu) return;
    const close = () => setCtxMenu(null);
    window.addEventListener("click", close);
    return () => window.removeEventListener("click", close);
  }, [ctxMenu]);

  const ctxDeleteQuery =
    ctxMenu?.node.kind === "saved_query" && onDeleteSavedQuery
      ? savedQueryFromNode(ctxMenu.node)
      : null;

  const ctxDeletePipeline =
    ctxMenu?.node.kind === "etl_pipeline" && onDeletePipeline
      ? (() => {
          const id = pipelineIdFromNode(ctxMenu.node);
          return id ? { id, label: ctxMenu.node.label } : null;
        })()
      : null;

  const ctxDeleteTemplate =
    ctxMenu?.node.kind === "etl_template" && onDeleteTemplate
      ? (() => {
          const id = templateIdFromNode(ctxMenu.node);
          return id ? { id, label: ctxMenu.node.label } : null;
        })()
      : null;

  const ctxRenamePipeline =
    ctxMenu?.node.kind === "etl_pipeline" && onRenamePipeline
      ? (() => {
          const id = pipelineIdFromNode(ctxMenu.node);
          return id ? { id, label: ctxMenu.node.label } : null;
        })()
      : null;

  const ctxRenameTemplate =
    ctxMenu?.node.kind === "etl_template" && onRenameTemplate
      ? (() => {
          const id = templateIdFromNode(ctxMenu.node);
          return id ? { id, label: ctxMenu.node.label } : null;
        })()
      : null;

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

  return (
    <div style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0 }}>
      <div className="disc-tree-toolbar">
        <input
          className="disc-input disc-tree-toolbar__filter"
          placeholder={t("discovery.filter")}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
        {toolbarNewAction && onTreeNew && toolbarNewLabels ? (
          <button
            type="button"
            className="disc-btn disc-btn--primary disc-tree-toolbar__new"
            onClick={() => onTreeNew(toolbarNewAction)}
            title={t(toolbarNewLabels.titleKey)}
          >
            {t(toolbarNewLabels.labelKey)}
          </button>
        ) : null}
      </div>
      <div className="disc-tree">
        {flat.length === 0 ? (
          <p className="disc-empty-hint" style={{ padding: "1rem" }}>
            {t("discovery.empty")}
          </p>
        ) : (
          <ul className="disc-tree-list">
            {flat.map(({ node, depth }) => {
              const isPlaceholder = isLoadingPlaceholder(node);
              const isSel = !isPlaceholder && selectedId === node.id;
              const isExp = expanded.has(node.id);
              const err = errors.get(node.id);
              const isLoading = loading.has(node.id);
              const isDataTreeDraggable =
                dataTreeDragEnabled && !isPlaceholder && canDropDataTreeEntity(node);
              const isTransformTreeDraggable =
                !isPlaceholder && canDragTransformTreeItem(node);
              const isDraggable = isDataTreeDraggable || isTransformTreeDraggable;
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
              return (
                <li
                  key={node.id}
                  className="disc-tree-item"
                  style={{ paddingLeft: depth * 14 }}
                >
                  <div
                    className={`disc-tree-row${transformDropTarget ? " disc-tree-row--drop-target" : ""}`}
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
                    {node.has_children && !isPlaceholder ? (
                      <button
                        type="button"
                        className="disc-tree-chevron"
                        aria-label={isExp ? t("artifacts.treeCollapse") : t("artifacts.treeExpand")}
                        onClick={() => toggleExpand(node)}
                      >
                        {isLoading ? "…" : isExp ? "▼" : "▶"}
                      </button>
                    ) : (
                      <span className="disc-tree-chevron-spacer" aria-hidden />
                    )}
                    {isPlaceholder ? (
                      <span className="disc-tree-node disc-tree-node--loading">{node.label}</span>
                    ) : (
                      <button
                        type="button"
                        className={`disc-tree-node${isSel ? " disc-tree-node--selected" : ""}${
                          node.starred || isStarred(node.id) ? " disc-tree-node--starred" : ""
                        }${isDraggable ? " disc-tree-node--draggable" : ""}${
                          transformDropTarget ? " disc-tree-node--drop-target" : ""
                        }`}
                        draggable={isDraggable}
                        title={
                          transformDropHint ??
                          (isDataTreeDraggable ? t("transform.treeDrag.hint") : isTransformTreeDraggable
                            ? t("transform.treeDrag.pipelineOrTemplate")
                            : undefined)
                        }
                        onDragStart={
                          isDraggable
                            ? (e) => {
                                e.stopPropagation();
                                if (isTransformTreeDraggable) {
                                  setTransformTreeDragData(e, node);
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
                        onClick={() => onSelectNode(node)}
                        onDoubleClick={() => openNode(node)}
                        onContextMenu={(e: MouseEvent) => {
                          e.preventDefault();
                          setCtxMenu({ x: e.clientX, y: e.clientY, node });
                        }}
                      >
                        {(node.starred || isStarred(node.id)) && (
                          <span className="disc-tree-star" aria-hidden>
                            ★{" "}
                          </span>
                        )}
                        {treeNodeDisplayLabel(node, t)}
                        {err ? " ⚠" : ""}
                      </button>
                    )}
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </div>
      {ctxMenu &&
        createPortal(
          <ul
            className="disc-ctx-menu"
            style={{ left: ctxMenu.x, top: ctxMenu.y }}
            onClick={(e) => e.stopPropagation()}
          >
            {ctxNewAction && onTreeNew && ctxNewLabels ? (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    onTreeNew(ctxNewAction);
                    setCtxMenu(null);
                  }}
                >
                  {t(ctxNewLabels.labelKey)}
                </button>
              </li>
            ) : null}
            {!isLoadingPlaceholder(ctxMenu.node) && (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    void toggleStar(ctxMenu.node.id).finally(() => setCtxMenu(null));
                  }}
                >
                  {isStarred(ctxMenu.node.id) ? t("discovery.unfavorite") : t("discovery.favorite")}
                </button>
              </li>
            )}
            {opensDocumentTab(ctxMenu.node) && (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    openNode(ctxMenu.node);
                    setCtxMenu(null);
                  }}
                >
                  {t("discovery.open")}
                </button>
              </li>
            )}
            {ctxDeleteQuery && onDeleteSavedQuery && (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    onDeleteSavedQuery(ctxDeleteQuery);
                    setCtxMenu(null);
                  }}
                >
                  {t("discovery.delete")}
                </button>
              </li>
            )}
            {ctxRenamePipeline && onRenamePipeline && (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    onRenamePipeline(ctxRenamePipeline.id, ctxRenamePipeline.label);
                    setCtxMenu(null);
                  }}
                >
                  {t("transform.pipelines.rename")}
                </button>
              </li>
            )}
            {ctxDeletePipeline && onDeletePipeline && (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    void onDeletePipeline(ctxDeletePipeline.id, ctxDeletePipeline.label);
                    setCtxMenu(null);
                  }}
                >
                  {t("transform.pipelines.delete")}
                </button>
              </li>
            )}
            {ctxRenameTemplate && onRenameTemplate && (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    onRenameTemplate(ctxRenameTemplate.id, ctxRenameTemplate.label);
                    setCtxMenu(null);
                  }}
                >
                  {t("transform.templates.rename")}
                </button>
              </li>
            )}
            {ctxDeleteTemplate && onDeleteTemplate && (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    void onDeleteTemplate(ctxDeleteTemplate.id, ctxDeleteTemplate.label);
                    setCtxMenu(null);
                  }}
                >
                  {t("transform.templates.delete")}
                </button>
              </li>
            )}
            {canQueryTreeNode(ctxMenu.node) && (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    openNode(ctxMenu.node);
                    setCtxMenu(null);
                  }}
                >
                  {t("discovery.query")}
                </button>
              </li>
            )}
            {ctxMenu.node.has_children && (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    invalidateSubtree(ctxMenu.node.id);
                    void loadChildren(ctxMenu.node.id, { force: true });
                    setCtxMenu(null);
                  }}
                >
                  {t("discovery.refresh")}
                </button>
              </li>
            )}
          </ul>,
          document.body
        )}
    </div>
  );
}
