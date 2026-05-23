import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import {
  fetchDiscoveryConfig,
  saveDiscoverySavedQueries,
  saveDiscoveryStars,
  saveDiscoveryWorkspace,
} from "../api";
import { applyStarredFlags, sortTreeNodes } from "../utils/discoveryStars";
import { dedupeNodeIds } from "../utils/treeNodeIds";
import type { SavedQuery, SavedWorkspace, TreeNode } from "../types/discoveryNodes";

const EMPTY_WORKSPACE: SavedWorkspace = { active_tab_id: null, tabs: [] };

type DiscoveryConfigValue = {
  starredIds: string[];
  starredSet: ReadonlySet<string>;
  isStarred: (nodeId: string) => boolean;
  toggleStar: (nodeId: string) => Promise<void>;
  sortNodes: (nodes: TreeNode[]) => TreeNode[];
  savedQueries: SavedQuery[];
  savedQueriesRevision: number;
  persistSavedQueries: (queries: SavedQuery[]) => Promise<SavedQuery[]>;
  workspace: SavedWorkspace;
  persistWorkspace: (workspace: SavedWorkspace) => Promise<void>;
  loading: boolean;
  error: string | null;
};

const DiscoveryConfigContext = createContext<DiscoveryConfigValue | null>(null);

export function DiscoveryConfigProvider({ children }: { children: ReactNode }) {
  const [starredIds, setStarredIds] = useState<string[]>([]);
  const [savedQueries, setSavedQueries] = useState<SavedQuery[]>([]);
  const [savedQueriesRevision, setSavedQueriesRevision] = useState(0);
  const [workspace, setWorkspace] = useState<SavedWorkspace>(EMPTY_WORKSPACE);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const persistWorkspaceChain = useRef(Promise.resolve());

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    void fetchDiscoveryConfig()
      .then((cfg) => {
        if (cancelled) return;
        setStarredIds(dedupeNodeIds(cfg.stars?.node_ids ?? []));
        setSavedQueries(cfg.saved_queries?.queries ?? []);
        setWorkspace(cfg.workspace ?? EMPTY_WORKSPACE);
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
    (nodes: TreeNode[]) => applyStarredFlags(sortTreeNodes(nodes, starredIds), starredSet),
    [starredIds, starredSet]
  );

  const persistWorkspace = useCallback(async (next: SavedWorkspace) => {
    const task = persistWorkspaceChain.current.then(async () => {
      const saved = await saveDiscoveryWorkspace(next);
      setWorkspace(saved.workspace);
      setError(null);
    });
    persistWorkspaceChain.current = task.catch(() => {});
    return task;
  }, []);

  const persistSavedQueries = useCallback(async (queries: SavedQuery[]) => {
    const saved = await saveDiscoverySavedQueries(queries);
    setSavedQueries(saved.saved_queries.queries);
    setSavedQueriesRevision((r) => r + 1);
    setError(null);
    return saved.saved_queries.queries;
  }, []);

  const toggleStar = useCallback(
    async (nodeId: string) => {
      const next = dedupeNodeIds(
        starredSet.has(nodeId)
          ? starredIds.filter((id) => id !== nodeId)
          : [...starredIds, nodeId]
      );
      setStarredIds(next);
      try {
        const saved = await saveDiscoveryStars(next);
        setStarredIds(saved.stars.node_ids);
        setError(null);
      } catch (e) {
        setError(String(e));
        const cfg = await fetchDiscoveryConfig().catch(() => null);
        if (cfg) setStarredIds(dedupeNodeIds(cfg.stars?.node_ids ?? []));
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
      savedQueries,
      savedQueriesRevision,
      persistSavedQueries,
      workspace,
      persistWorkspace,
      loading,
      error,
    }),
    [
      starredIds,
      starredSet,
      isStarred,
      toggleStar,
      sortNodes,
      savedQueries,
      savedQueriesRevision,
      persistSavedQueries,
      workspace,
      persistWorkspace,
      loading,
      error,
    ]
  );

  return (
    <DiscoveryConfigContext.Provider value={value}>{children}</DiscoveryConfigContext.Provider>
  );
}

export function useDiscoveryConfig(): DiscoveryConfigValue {
  const ctx = useContext(DiscoveryConfigContext);
  if (!ctx) throw new Error("useDiscoveryConfig must be used within DiscoveryConfigProvider");
  return ctx;
}
