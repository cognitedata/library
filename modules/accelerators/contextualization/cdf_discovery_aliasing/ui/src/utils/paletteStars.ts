import type { TreeNode } from "../types/dataTree";

/** Sort siblings: starred first (config order), then label A–Z. */
export function sortPaletteTreeNodes(nodes: TreeNode[], starredIds: readonly string[]): TreeNode[] {
  const starRank = new Map(starredIds.map((id, i) => [id, i]));
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

export function applyPaletteStarredFlags(nodes: TreeNode[], starredSet: ReadonlySet<string>): TreeNode[] {
  return nodes.map((n) => ({
    ...n,
    starred: starredSet.has(n.id) ? true : undefined,
  }));
}
