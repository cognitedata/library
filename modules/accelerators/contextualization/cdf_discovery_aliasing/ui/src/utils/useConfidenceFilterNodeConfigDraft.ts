import { useCallback, useEffect, useMemo, useState } from "react";
import type { WorkflowCanvasNodeData } from "../types/workflowCanvas";
import { readConfidenceFilterConfigFromData } from "./confidenceFilterCanvasUtils";

export function useConfidenceFilterNodeConfigDraft(
  data: WorkflowCanvasNodeData | undefined,
  persist: (config: Record<string, unknown>, label: string) => void
) {
  const configSig = useMemo(
    () => JSON.stringify(data?.config ?? null),
    [data?.config]
  );

  const [draftCfg, setDraftCfg] = useState(() => readConfidenceFilterConfigFromData(data));

  useEffect(() => {
    setDraftCfg(readConfidenceFilterConfigFromData(data));
  }, [configSig, data]);

  const persistDraft = useCallback(
    (next: Record<string, unknown>) => {
      setDraftCfg(next);
      const label =
        String(next.description ?? data?.label ?? "Confidence filter").trim() ||
        "Confidence filter";
      persist(next, label);
    },
    [data?.label, persist]
  );

  const updateDraft = useCallback((next: Record<string, unknown>) => {
    setDraftCfg(next);
  }, []);

  return { draftCfg, updateDraft, persistDraft };
}
