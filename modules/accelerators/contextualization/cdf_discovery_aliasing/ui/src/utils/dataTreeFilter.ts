import type { TreeNode } from "../types/dataTree";

export type TreeEntry = { node: TreeNode; depth: number; parentId: string | null };

const LOADING_KIND = "loading";

export function isLoadingPlaceholder(node: TreeNode): boolean {
  return node.kind === "loading";
}

export function loadingPlaceholder(parentId: string): TreeNode {
  return {
    id: `${parentId}:__loading__`,
    label: "Loading…",
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
  rootNode?: TreeNode
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
        out.push({ node: loadingPlaceholder(node.id), depth: depth + 1, parentId: node.id });
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
      if (!q) out.push({ node: loadingPlaceholder(rootId), depth: 1, parentId: rootId });
    }
  } else {
    walk(rootId, 0);
  }

  return out;
}

export function collectDescendantKeys(map: Map<string, unknown>, nodeId: string): string[] {
  const prefix = `${nodeId}:`;
  return [...map.keys()].filter((k) => k === nodeId || k.startsWith(prefix));
}

export function collectDescendantIds(ids: Iterable<string>, nodeId: string): string[] {
  const prefix = `${nodeId}:`;
  return [...ids].filter((id) => id === nodeId || id.startsWith(prefix));
}
