import { useCallback, useEffect, useMemo, useRef, useState, type MouseEvent } from "react";
import { createPortal } from "react-dom";
import { fetchTreeChildren } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import { useExplorerConfig } from "../context/ExplorerConfigContext";
import type { TreeNode } from "../types/explorerNodes";
import {
  collectDescendantIds,
  collectDescendantKeys,
  flattenVisibleTree,
  isLoadingPlaceholder,
} from "../utils/treeFilter";
import { canQueryTreeNode } from "../utils/sqlQuerySeed";
import { savedQueryFromNode } from "../utils/savedQueries";
import type { SavedQuery } from "../types/explorerNodes";

const ROOT_ID = "connection";

type Props = {
  refreshKey: number;
  savedQueriesRevision?: number;
  connectionLabel?: string;
  selectedId: string | null;
  onSelectNode: (node: TreeNode | null) => void;
  onOpenNode: (node: TreeNode) => void;
  onDeleteSavedQuery?: (query: SavedQuery) => void;
};

type CtxMenu = { x: number; y: number; node: TreeNode } | null;

export function ObjectExplorer({
  refreshKey,
  savedQueriesRevision = 0,
  connectionLabel,
  selectedId,
  onSelectNode,
  onOpenNode,
  onDeleteSavedQuery,
}: Props) {
  const { t } = useAppSettings();
  const { sortNodes, isStarred, toggleStar, starredIds } = useExplorerConfig();
  const [filter, setFilter] = useState("");
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set());
  const [childrenByParent, setChildrenByParent] = useState<Map<string, TreeNode[]>>(new Map());
  const [loadedIds, setLoadedIds] = useState<Set<string>>(() => new Set());
  const [loading, setLoading] = useState<Set<string>>(new Set());
  const [errors, setErrors] = useState<Map<string, string>>(new Map());
  const [ctxMenu, setCtxMenu] = useState<CtxMenu>(null);
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
    setExpanded(new Set());
    setErrors(new Map());
    setLoading(new Set());
  }, [refreshKey]);

  useEffect(() => {
    if (!savedQueriesRevision) return;
    invalidateSubtree("sq");
    void loadChildren("sq", { force: true });
  }, [savedQueriesRevision, invalidateSubtree, loadChildren]);

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

  const opensDocumentTab = (node: TreeNode) =>
    node.kind === "dm_data_model" ||
    node.kind === "workflow" ||
    node.kind === "transformation" ||
    node.kind === "function" ||
    node.kind === "saved_query";

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

  return (
    <div style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0 }}>
      <div style={{ padding: "0.45rem 0.5rem" }}>
        <input
          className="exp-input"
          style={{ width: "100%" }}
          placeholder={t("explorer.filter")}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
      </div>
      <div className="exp-tree">
        {flat.length === 0 ? (
          <p className="exp-empty-hint" style={{ padding: "1rem" }}>
            {t("explorer.empty")}
          </p>
        ) : (
          <ul className="exp-tree-list">
            {flat.map(({ node, depth }) => {
              const isPlaceholder = isLoadingPlaceholder(node);
              const isSel = !isPlaceholder && selectedId === node.id;
              const isExp = expanded.has(node.id);
              const err = errors.get(node.id);
              const isLoading = loading.has(node.id);
              return (
                <li
                  key={node.id}
                  className="exp-tree-item"
                  style={{ paddingLeft: depth * 14 }}
                >
                  <div className="exp-tree-row">
                    {node.has_children && !isPlaceholder ? (
                      <button
                        type="button"
                        className="exp-tree-chevron"
                        aria-label={isExp ? "Collapse" : "Expand"}
                        onClick={() => toggleExpand(node)}
                      >
                        {isLoading ? "…" : isExp ? "▼" : "▶"}
                      </button>
                    ) : (
                      <span className="exp-tree-chevron-spacer" aria-hidden />
                    )}
                    {isPlaceholder ? (
                      <span className="exp-tree-node exp-tree-node--loading">{node.label}</span>
                    ) : (
                      <button
                        type="button"
                        className={`exp-tree-node${isSel ? " exp-tree-node--selected" : ""}${
                          node.starred || isStarred(node.id) ? " exp-tree-node--starred" : ""
                        }`}
                        onClick={() => onSelectNode(node)}
                        onDoubleClick={() => openNode(node)}
                        onContextMenu={(e: MouseEvent) => {
                          e.preventDefault();
                          setCtxMenu({ x: e.clientX, y: e.clientY, node });
                        }}
                      >
                        {(node.starred || isStarred(node.id)) && (
                          <span className="exp-tree-star" aria-hidden>
                            ★{" "}
                          </span>
                        )}
                        {node.label}
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
            className="exp-ctx-menu"
            style={{ left: ctxMenu.x, top: ctxMenu.y }}
            onClick={(e) => e.stopPropagation()}
          >
            {!isLoadingPlaceholder(ctxMenu.node) && (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    void toggleStar(ctxMenu.node.id).finally(() => setCtxMenu(null));
                  }}
                >
                  {isStarred(ctxMenu.node.id) ? t("explorer.unfavorite") : t("explorer.favorite")}
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
                  {t("explorer.open")}
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
                  {t("explorer.delete")}
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
                  {t("explorer.query")}
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
                  {t("explorer.refresh")}
                </button>
              </li>
            )}
          </ul>,
          document.body
        )}
    </div>
  );
}
