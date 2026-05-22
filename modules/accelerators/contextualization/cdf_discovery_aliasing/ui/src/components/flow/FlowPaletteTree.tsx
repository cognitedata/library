import { useCallback, useEffect, useMemo, useRef, useState, type MouseEvent } from "react";
import { fetchDataTreeChildren } from "../../api/dataTree";
import { TreeContextMenuPortal, useTreeContextMenuState } from "../TreeContextMenu";
import { usePaletteOperatorConfig } from "../../context/PaletteOperatorConfigContext";
import type { MessageKey } from "../../i18n";
import type { TreeNode } from "../../types/dataTree";
import {
  buildPaletteTreeChildrenByParent,
  PALETTE_DATA_ROOT,
  PALETTE_PIPELINE_ROOT,
  PALETTE_TREE_ROOT,
} from "../../utils/buildPaletteTreeNodes";
import { canDropDataTreeEntity } from "../../utils/dataTreeEntityDrop";
import { flattenVisibleTree, isLoadingPlaceholder } from "../../utils/dataTreeFilter";
import { setDataTreeEntityDragData, setPaletteDragData, type PaletteDragPayload } from "./FlowPalette";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  scopeDocument: Record<string, unknown>;
  readOnly?: boolean;
};

function isPaletteLeaf(node: TreeNode): boolean {
  return node.kind === "palette_leaf" && node.meta?.palette_payload != null;
}

function isDraggable(node: TreeNode): boolean {
  if (node.kind === "palette_hint" || isLoadingPlaceholder(node)) return false;
  if (isPaletteLeaf(node)) return true;
  return canDropDataTreeEntity(node);
}

function canFavoriteNode(node: TreeNode): boolean {
  return !isLoadingPlaceholder(node) && node.kind !== "palette_hint";
}

function sortStaticPaletteMap(
  map: Map<string, TreeNode[]>,
  sortNodes: (nodes: TreeNode[]) => TreeNode[]
): Map<string, TreeNode[]> {
  const next = new Map<string, TreeNode[]>();
  for (const [parentId, kids] of map) {
    next.set(parentId, sortNodes(kids));
  }
  return next;
}

export function FlowPaletteTree({ t, scopeDocument, readOnly = false }: Props) {
  const { sortNodes, isStarred, toggleStar, starredIds } = usePaletteOperatorConfig();
  const ctxMenu = useTreeContextMenuState();
  const [filter, setFilter] = useState("");
  const [expanded, setExpanded] = useState<Set<string>>(() => {
    const staticMap = buildPaletteTreeChildrenByParent(t, scopeDocument);
    const ids = new Set<string>([PALETTE_TREE_ROOT, PALETTE_DATA_ROOT, PALETTE_PIPELINE_ROOT]);
    for (const key of staticMap.keys()) {
      if (key.startsWith("palette:")) ids.add(key);
    }
    return ids;
  });
  const [childrenByParent, setChildrenByParent] = useState<Map<string, TreeNode[]>>(() =>
    sortStaticPaletteMap(buildPaletteTreeChildrenByParent(t, scopeDocument), sortNodes)
  );
  const [loadedIds, setLoadedIds] = useState<Set<string>>(() => {
    const ids = new Set<string>([PALETTE_TREE_ROOT, "palette:pipeline"]);
    for (const key of buildPaletteTreeChildrenByParent(t, scopeDocument).keys()) {
      if (key.startsWith("palette:")) ids.add(key);
    }
    return ids;
  });
  const [loading, setLoading] = useState<Set<string>>(new Set());
  const [errors, setErrors] = useState<Map<string, string>>(new Map());
  const abortByNode = useRef<Map<string, AbortController>>(new Map());
  const loadedRef = useRef(loadedIds);
  const loadingRef = useRef(loading);
  loadedRef.current = loadedIds;
  loadingRef.current = loading;

  useEffect(() => {
    const staticMap = buildPaletteTreeChildrenByParent(t, scopeDocument);
    setChildrenByParent((prev) => {
      const next = new Map(prev);
      for (const [k, v] of sortStaticPaletteMap(staticMap, sortNodes)) {
        if (k !== PALETTE_DATA_ROOT && !k.startsWith("dm:") && !k.startsWith("raw:") && k !== "classic") {
          next.set(k, v);
        }
      }
      return next;
    });
    setLoadedIds((prev) => {
      const next = new Set(prev);
      for (const key of staticMap.keys()) {
        if (key.startsWith("palette:") || key === PALETTE_TREE_ROOT) next.add(key);
      }
      return next;
    });
  }, [t, scopeDocument, sortNodes]);

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

  const loadChildren = useCallback(
    async (nodeId: string, { force = false }: { force?: boolean } = {}) => {
      if (nodeId.startsWith("palette:")) return;
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
        const { nodes } = await fetchDataTreeChildren(nodeId, controller.signal);
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
    },
    [sortNodes]
  );

  const toggleExpand = useCallback(
    (node: TreeNode) => {
      const id = node.id;
      setExpanded((prev) => {
        const next = new Set(prev);
        if (next.has(id)) {
          next.delete(id);
        } else {
          next.add(id);
          if (node.has_children && !id.startsWith("palette:")) {
            void loadChildren(id);
          }
        }
        return next;
      });
    },
    [loadChildren]
  );

  useEffect(() => {
    if (expanded.has(PALETTE_DATA_ROOT) && !loadedIds.has(PALETTE_DATA_ROOT)) {
      void loadChildren(PALETTE_DATA_ROOT);
    }
  }, [expanded, loadedIds, loadChildren]);

  /** Flatten CDF data + pipeline as top-level branches (skip redundant root row). */
  const flat = useMemo(() => {
    const data = flattenVisibleTree(PALETTE_DATA_ROOT, childrenByParent, expanded, loadedIds, filter, {
      id: PALETTE_DATA_ROOT,
      label: t("flow.paletteSectionCdfData"),
      kind: "folder",
      has_children: true,
      meta: { domain: "data" },
    });
    const pipeline = flattenVisibleTree(
      PALETTE_PIPELINE_ROOT,
      childrenByParent,
      expanded,
      loadedIds,
      filter,
      {
        id: PALETTE_PIPELINE_ROOT,
        label: t("flow.palettePipeline"),
        kind: "palette_folder",
        has_children: true,
      }
    );
    return [...data, ...pipeline];
  }, [childrenByParent, expanded, loadedIds, filter, t]);

  const onDragStart = useCallback(
    (e: React.DragEvent, node: TreeNode) => {
      if (readOnly || !isDraggable(node)) return;
      if (isPaletteLeaf(node)) {
        setPaletteDragData(e, node.meta!.palette_payload as PaletteDragPayload);
        return;
      }
      setDataTreeEntityDragData(e, node);
    },
    [readOnly]
  );

  const dragTitle = useCallback(
    (node: TreeNode) => {
      if (isPaletteLeaf(node)) return t("flow.paletteDragLeaf");
      if (canDropDataTreeEntity(node)) {
        return t("flow.paletteDragEntity", { label: node.label });
      }
      return undefined;
    },
    [t]
  );

  const onContextMenu = useCallback(
    (e: MouseEvent, node: TreeNode) => {
      if (readOnly || !canFavoriteNode(node)) return;
      const items = [
        {
          id: "favorite",
          label: isStarred(node.id) ? t("flow.paletteUnfavorite") : t("flow.paletteFavorite"),
          onSelect: () => {
            void toggleStar(node.id);
          },
        },
      ];
      ctxMenu.open(e, items);
    },
    [readOnly, isStarred, toggleStar, t, ctxMenu]
  );

  return (
    <div className="discovery-flow-palette-tree">
      <input
        className="discovery-input discovery-flow-palette-tree__filter"
        placeholder={t("flow.paletteTreeFilter")}
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
      />
      <div className="discovery-flow-palette-tree__scroll">
        {flat.length === 0 ? (
          <p className="discovery-hint discovery-flow-palette-tree__empty">{t("flow.paletteTreeEmpty")}</p>
        ) : (
          <ul className="discovery-flow-palette-tree__list">
            {flat.map(({ node, depth }) => {
              const isPlaceholder = isLoadingPlaceholder(node);
              const isExp = expanded.has(node.id);
              const isLoading = loading.has(node.id);
              const err = errors.get(node.id);
              const draggable = !readOnly && isDraggable(node);
              const starred = node.starred || isStarred(node.id);
              return (
                <li
                  key={node.id}
                  className="discovery-flow-palette-tree__item"
                  style={{ paddingLeft: depth * 14 }}
                >
                  <div className="discovery-flow-palette-tree__row">
                    {node.has_children && !isPlaceholder ? (
                      <button
                        type="button"
                        className="discovery-flow-palette-tree__chevron"
                        aria-label={isExp ? "Collapse" : "Expand"}
                        onClick={() => toggleExpand(node)}
                      >
                        {isLoading ? "…" : isExp ? "▼" : "▶"}
                      </button>
                    ) : (
                      <span className="discovery-flow-palette-tree__chevron-spacer" aria-hidden />
                    )}
                    {isPlaceholder ? (
                      <span className="discovery-flow-palette-tree__label discovery-flow-palette-tree__label--loading">
                        {node.label}
                      </span>
                    ) : (
                      <span
                        className={`discovery-flow-palette-tree__label${
                          draggable ? " discovery-flow-palette-tree__label--draggable" : ""
                        }${node.kind === "palette_hint" ? " discovery-flow-palette-tree__label--hint" : ""}${
                          starred ? " discovery-flow-palette-tree__label--starred" : ""
                        }`}
                        draggable={draggable}
                        onDragStart={(e) => onDragStart(e, node)}
                        onContextMenu={(e) => onContextMenu(e, node)}
                        title={dragTitle(node)}
                      >
                        {starred && (
                          <span className="discovery-flow-palette-tree__star" aria-hidden>
                            ★{" "}
                          </span>
                        )}
                        {node.label}
                        {err ? " ⚠" : ""}
                      </span>
                    )}
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </div>
      <TreeContextMenuPortal menu={ctxMenu.menu} onClose={ctxMenu.close} classPrefix="discovery" />
    </div>
  );
}
