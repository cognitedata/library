import type { TreeNode } from "../types/discoveryNodes";
import { CONNECTION_ROOT_CHILD_ORDER } from "./treeNodeIds";

const CONNECTION_ROOT_ORDER = new Map(
  CONNECTION_ROOT_CHILD_ORDER.map((id, index) => [id, index])
);

function isConnectionRootSiblings(nodes: TreeNode[]): boolean {
  if (nodes.length < 2) return false;
  return nodes.every((n) => CONNECTION_ROOT_ORDER.has(n.id));
}

function compareStarredThenPinned(
  a: TreeNode,
  b: TreeNode,
  starRank: Map<string, number>
): number {
  const aRank = starRank.get(a.id);
  const bRank = starRank.get(b.id);
  const aStar = aRank !== undefined;
  const bStar = bRank !== undefined;
  if (aStar && bStar) return aRank - bRank;
  if (aStar !== bStar) return aStar ? -1 : 1;
  const aPin = CONNECTION_ROOT_ORDER.get(a.id) ?? 999;
  const bPin = CONNECTION_ROOT_ORDER.get(b.id) ?? 999;
  return aPin - bPin;
}

/** Sort siblings: starred first (config order), then label A–Z (or pinned connection-root order). */
export function sortTreeNodes(nodes: TreeNode[], starredIds: readonly string[]): TreeNode[] {
  const starRank = new Map(starredIds.map((id, i) => [id, i]));
  if (isConnectionRootSiblings(nodes)) {
    return [...nodes].sort((a, b) => compareStarredThenPinned(a, b, starRank));
  }
  return [...nodes].sort((a, b) => {
    const aRank = starRank.get(a.id);
    const bRank = starRank.get(b.id);
    const aStar = aRank !== undefined;
    const bStar = bRank !== undefined;
    if (aStar && bStar) return aRank - bRank;
    if (aStar !== bStar) return aStar ? -1 : 1;
    return a.label.localeCompare(b.label, undefined, { sensitivity: "base" });
  });
}

export function applyStarredFlags(nodes: TreeNode[], starredSet: ReadonlySet<string>): TreeNode[] {
  return nodes.map((n) => ({
    ...n,
    starred: starredSet.has(n.id) ? true : undefined,
  }));
}
