import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { fetchWorkspace, saveWorkspace } from "../api";
import type { IndexDocumentTab, OverviewSubTab, WorkspaceState } from "../types/indexWorkspace";
import { restoreWorkspaceTabs, serializeWorkspace } from "../utils/workspacePersistence";

type IndexWorkspaceValue = {
  workspace: WorkspaceState;
  loading: boolean;
  overviewSubTab: OverviewSubTab;
  persistWorkspace: (tabs: IndexDocumentTab[], activeTabId: string | null) => void;
};

const IndexWorkspaceContext = createContext<IndexWorkspaceValue | null>(null);

export function IndexWorkspaceProvider({ children }: { children: ReactNode }) {
  const [workspace, setWorkspace] = useState<WorkspaceState>({ active_tab_id: null, tabs: [] });
  const [overviewSubTab, setOverviewSubTab] = useState<OverviewSubTab>("summary");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetchWorkspace();
        if (cancelled) return;
        const restored = restoreWorkspaceTabs({
          active_tab_id: res.workspace.active_tab_id,
          tabs: res.workspace.tabs as IndexDocumentTab[],
        });
        setOverviewSubTab(restored.overviewSubTab);
        setWorkspace(serializeWorkspace(restored.tabs, restored.activeTabId));
      } catch {
        if (!cancelled) setWorkspace({ active_tab_id: null, tabs: [] });
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const persistWorkspace = useCallback((tabs: IndexDocumentTab[], activeTabId: string | null) => {
    const next = serializeWorkspace(tabs, activeTabId);
    setWorkspace(next);
    void saveWorkspace(next).catch(() => {
      /* surface via connection banner if needed */
    });
  }, []);

  const value = useMemo(
    () => ({ workspace, loading, overviewSubTab, persistWorkspace }),
    [workspace, loading, overviewSubTab, persistWorkspace]
  );

  return <IndexWorkspaceContext.Provider value={value}>{children}</IndexWorkspaceContext.Provider>;
}

export function useIndexWorkspace(): IndexWorkspaceValue {
  const ctx = useContext(IndexWorkspaceContext);
  if (!ctx) throw new Error("useIndexWorkspace must be used within IndexWorkspaceProvider");
  return ctx;
}
