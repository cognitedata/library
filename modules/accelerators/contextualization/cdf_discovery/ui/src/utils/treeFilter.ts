import type { TreeNode } from "../types/discoveryNodes";

export type TreeEntry = { node: TreeNode; depth: number; parentId: string | null };

export type BreadcrumbSegment = { id: string; label: string };

export type DrillDownRow = {
  node: TreeNode;
  parentId: string;
  pathLabel?: string;
};

const LOADING_KIND = "loading";

export function isLoadingPlaceholder(node: TreeNode): boolean {
  return node.kind === LOADING_KIND;
}

export function loadingPlaceholder(parentId: string, label: string): TreeNode {
  return {
    id: `${parentId}:__loading__`,
    label,
    kind: LOADING_KIND,
    has_children: false,
  };
}

/** Visible tree: only expanded nodes with loaded children are traversed deeper. */
export function flattenVisibleTree(
  rootId: string,
  childrenByParent: Map<string, TreeNode[]>,
  expanded: Set<string>,
  loadedIds: Set<string>,
  filter: string,
  rootNode?: TreeNode,
  loadingLabel = "Loading…"
): TreeEntry[] {
  const q = filter.trim().toLowerCase();
  const out: TreeEntry[] = [];

  const labelMatches = (node: TreeNode) => !q || node.label.toLowerCase().includes(q);

  const walk = (parentId: string, depth: number) => {
    const kids = childrenByParent.get(parentId) ?? [];
    for (const node of kids) {
      if (isLoadingPlaceholder(node)) {
        if (!q) out.push({ node, depth, parentId });
        continue;
      }

      const childList = childrenByParent.get(node.id);
      const isLoaded = loadedIds.has(node.id);
      const descendantLoaded = isLoaded && childList !== undefined;
      const showBecauseChild =
        q &&
        descendantLoaded &&
        childList!.some(
          (c) =>
            labelMatches(c) ||
            (loadedIds.has(c.id) && (childrenByParent.get(c.id)?.some((g) => labelMatches(g)) ?? false))
        );

      if (q && !labelMatches(node) && !showBecauseChild) {
        continue;
      }

      out.push({ node, depth, parentId });

      const isExp = expanded.has(node.id);
      if (!isExp || !node.has_children) {
        continue;
      }

      if (loadedIds.has(node.id)) {
        walk(node.id, depth + 1);
      } else if (!q) {
        out.push({ node: loadingPlaceholder(node.id, loadingLabel), depth: depth + 1, parentId: node.id });
      }
    }
  };

  if (rootNode) {
    const rootMatches = labelMatches(rootNode);
    const rootKids = childrenByParent.get(rootId) ?? [];
    const rootChildVisible =
      q &&
      loadedIds.has(rootId) &&
      rootKids.some((c) => labelMatches(c) || (loadedIds.has(c.id) && labelMatches(c)));

    if (!q || rootMatches || rootChildVisible) {
      out.push({ node: rootNode, depth: 0, parentId: null });
    }
    if (expanded.has(rootId) && loadedIds.has(rootId)) {
      walk(rootId, 1);
    } else if (expanded.has(rootId) && !loadedIds.has(rootId)) {
      if (!q) out.push({ node: loadingPlaceholder(rootId, loadingLabel), depth: 1, parentId: rootId });
    }
  } else {
    walk(rootId, 0);
  }

  return out;
}

/** Parent folder id for drill-down navigation (top-level nodes parent the connection root). */
export function parentNodeId(nodeId: string, rootId = "connection"): string {
  if (nodeId === rootId) return rootId;
  const idx = nodeId.lastIndexOf(":");
  if (idx < 0) return rootId;
  return nodeId.slice(0, idx);
}

/** Ordered ids from root through ``nodeId`` (inclusive). */
export function ancestorChainTo(nodeId: string, rootId = "connection"): string[] {
  const chain: string[] = [];
  let id = nodeId;
  while (id && id !== rootId) {
    chain.unshift(id);
    const parent = parentNodeId(id, rootId);
    if (parent === id) break;
    id = parent;
  }
  return [rootId, ...chain];
}

export function findNodeInTree(
  nodeId: string,
  childrenByParent: Map<string, TreeNode[]>,
  rootNode?: TreeNode
): TreeNode | null {
  if (rootNode && nodeId === rootNode.id) return rootNode;
  for (const kids of childrenByParent.values()) {
    const found = kids.find((n) => n.id === nodeId);
    if (found) return found;
  }
  return null;
}

export function buildBreadcrumbTrail(
  currentParentId: string,
  childrenByParent: Map<string, TreeNode[]>,
  rootNode: TreeNode,
  labelForNode: (node: TreeNode) => string
): BreadcrumbSegment[] {
  const ids = ancestorChainTo(currentParentId, rootNode.id);
  return ids.map((id) => {
    const node = findNodeInTree(id, childrenByParent, rootNode);
    return {
      id,
      label: node ? labelForNode(node) : id,
    };
  });
}

export function getDrillDownChildren(
  parentId: string,
  childrenByParent: Map<string, TreeNode[]>,
  loadedIds: Set<string>,
  loadingLabel: string
): TreeNode[] {
  if (!loadedIds.has(parentId)) {
    return [loadingPlaceholder(parentId, loadingLabel)];
  }
  return childrenByParent.get(parentId) ?? [];
}

export function searchLoadedTree(
  childrenByParent: Map<string, TreeNode[]>,
  filter: string,
  rootNode: TreeNode,
  labelForNode: (node: TreeNode) => string
): DrillDownRow[] {
  const q = filter.trim().toLowerCase();
  if (!q) return [];

  const out: DrillDownRow[] = [];
  const walk = (parentId: string, pathLabels: string[]) => {
    const kids = childrenByParent.get(parentId) ?? [];
    for (const node of kids) {
      if (isLoadingPlaceholder(node)) continue;
      const label = labelForNode(node);
      const nextPath = [...pathLabels, label];
      const matches = label.toLowerCase().includes(q) || node.id.toLowerCase().includes(q);
      if (matches) {
        out.push({
          node,
          parentId,
          pathLabel: pathLabels.length > 0 ? pathLabels.join(" › ") : undefined,
        });
      }
      if (node.has_children) {
        walk(node.id, nextPath);
      }
    }
  };

  walk(rootNode.id, []);
  return out;
}

export function ancestorIds(nodeId: string): string[] {
  const parts = nodeId.split(":");
  const acc: string[] = [];
  for (let i = 1; i < parts.length; i++) {
    acc.push(parts.slice(0, i).join(":"));
  }
  return acc;
}

export function collectDescendantKeys(map: Map<string, unknown>, nodeId: string): string[] {
  const prefix = `${nodeId}:`;
  return [...map.keys()].filter((k) => k === nodeId || k.startsWith(prefix));
}

export function collectDescendantIds(ids: Iterable<string>, nodeId: string): string[] {
  const prefix = `${nodeId}:`;
  return [...ids].filter((id) => id === nodeId || id.startsWith(prefix));
}
