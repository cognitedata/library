import { useCallback, useEffect, useMemo, useState } from "react";
import type { WorkflowCanvasNodeData } from "../types/workflowCanvas";
import { readFilterConfigFromData } from "./filtersCanvasUtils";

/** Local draft for filter-node config; sync from props when the graph changes externally. */
export function useFilterNodeConfigDraft(
  data: WorkflowCanvasNodeData | undefined,
  persist: (config: Record<string, unknown>, label: string) => void
) {
  const configSig = useMemo(
    () => JSON.stringify(data?.config ?? null),
    [data?.config]
  );

  const [draftCfg, setDraftCfg] = useState(() => readFilterConfigFromData(data));

  useEffect(() => {
    setDraftCfg(readFilterConfigFromData(data));
  }, [configSig, data]);

  const persistDraft = useCallback(
    (next: Record<string, unknown>) => {
      setDraftCfg(next);
      const label =
        String(next.description ?? data?.label ?? "Instance filter").trim() || "Instance filter";
      persist(next, label);
    },
    [data?.label, persist]
  );

  const updateDraft = useCallback((next: Record<string, unknown>) => {
    setDraftCfg(next);
  }, []);

  return { draftCfg, updateDraft, persistDraft };
}
