import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { fetchPaletteOperatorConfig, savePaletteStars } from "../api/paletteOperatorConfig";
import type { TreeNode } from "../types/dataTree";
import { applyPaletteStarredFlags, sortPaletteTreeNodes } from "../utils/paletteStars";

type PaletteOperatorConfigValue = {
  starredIds: string[];
  starredSet: ReadonlySet<string>;
  isStarred: (nodeId: string) => boolean;
  toggleStar: (nodeId: string) => Promise<void>;
  sortNodes: (nodes: TreeNode[]) => TreeNode[];
  loading: boolean;
  error: string | null;
};

const PaletteOperatorConfigContext = createContext<PaletteOperatorConfigValue | null>(null);

export function PaletteOperatorConfigProvider({ children }: { children: ReactNode }) {
  const [starredIds, setStarredIds] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    void fetchPaletteOperatorConfig()
      .then((cfg) => {
        if (cancelled) return;
        setStarredIds(cfg.stars?.node_ids ?? []);
        setError(null);
      })
      .catch((e) => {
        if (cancelled) return;
        setError(String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const starredSet = useMemo(() => new Set(starredIds), [starredIds]);

  const isStarred = useCallback((nodeId: string) => starredSet.has(nodeId), [starredSet]);

  const sortNodes = useCallback(
    (nodes: TreeNode[]) => applyPaletteStarredFlags(sortPaletteTreeNodes(nodes, starredIds), starredSet),
    [starredIds, starredSet]
  );

  const toggleStar = useCallback(
    async (nodeId: string) => {
      const next = starredSet.has(nodeId)
        ? starredIds.filter((id) => id !== nodeId)
        : [...starredIds, nodeId];
      setStarredIds(next);
      try {
        const saved = await savePaletteStars(next);
        setStarredIds(saved.stars.node_ids);
        setError(null);
      } catch (e) {
        setError(String(e));
        const cfg = await fetchPaletteOperatorConfig().catch(() => null);
        if (cfg) setStarredIds(cfg.stars?.node_ids ?? []);
        throw e;
      }
    },
    [starredIds, starredSet]
  );

  const value = useMemo(
    () => ({
      starredIds,
      starredSet,
      isStarred,
      toggleStar,
      sortNodes,
      loading,
      error,
    }),
    [starredIds, starredSet, isStarred, toggleStar, sortNodes, loading, error]
  );

  return (
    <PaletteOperatorConfigContext.Provider value={value}>{children}</PaletteOperatorConfigContext.Provider>
  );
}

export function usePaletteOperatorConfig(): PaletteOperatorConfigValue {
  const ctx = useContext(PaletteOperatorConfigContext);
  if (!ctx) {
    throw new Error("usePaletteOperatorConfig must be used within PaletteOperatorConfigProvider");
  }
  return ctx;
}
