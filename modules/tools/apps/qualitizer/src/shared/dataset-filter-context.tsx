import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import { useAppSdk } from "./auth";

export type DatasetSummary = {
  id: number;
  externalId?: string;
  name?: string;
};

const STORAGE_KEY = "qualitizer.datasetFilter";

function loadSelected(): number | null {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    if (raw === "null" || raw === "") return null;
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  } catch {
    return null;
  }
}

function saveSelected(value: number | null) {
  try {
    if (value == null) window.localStorage.removeItem(STORAGE_KEY);
    else window.localStorage.setItem(STORAGE_KEY, String(value));
  } catch {
    // ignore
  }
}

type DatasetFilterContextValue = {
  datasets: DatasetSummary[];
  isLoading: boolean;
  error: string | null;
  selectedDatasetId: number | null;
  setSelectedDatasetId: (id: number | null) => void;
  selectedDataset: DatasetSummary | null;
};

const DatasetFilterContext = createContext<DatasetFilterContextValue | null>(null);

export function DatasetFilterProvider({ children }: { children: React.ReactNode }) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const [datasets, setDatasets] = useState<DatasetSummary[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedDatasetId, setSelectedDatasetIdState] = useState<number | null>(loadSelected);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    setIsLoading(true);
    setError(null);
    (async () => {
      try {
        const items: DatasetSummary[] = [];
        let cursor: string | undefined;
        do {
          const response = await sdk.post<{
            items?: Array<{ id: number; externalId?: string; name?: string }>;
            nextCursor?: string | null;
          }>(`/api/v1/projects/${sdk.project}/datasets/list`, {
            data: { limit: 1000, cursor },
          });
          items.push(...(response.data?.items ?? []));
          cursor = response.data?.nextCursor ?? undefined;
        } while (cursor);
        if (cancelled) return;
        items.sort((a, b) => (a.name ?? a.externalId ?? "").localeCompare(b.name ?? b.externalId ?? ""));
        setDatasets(items);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load datasets");
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk]);

  const setSelectedDatasetId = useCallback((id: number | null) => {
    setSelectedDatasetIdState(id);
    saveSelected(id);
  }, []);

  const value = useMemo<DatasetFilterContextValue>(() => {
    const selected = selectedDatasetId == null ? null : datasets.find((d) => d.id === selectedDatasetId) ?? null;
    return {
      datasets,
      isLoading,
      error,
      selectedDatasetId,
      setSelectedDatasetId,
      selectedDataset: selected,
    };
  }, [datasets, isLoading, error, selectedDatasetId, setSelectedDatasetId]);

  return <DatasetFilterContext.Provider value={value}>{children}</DatasetFilterContext.Provider>;
}

export function useDatasetFilter() {
  const ctx = useContext(DatasetFilterContext);
  if (!ctx) throw new Error("useDatasetFilter must be used within DatasetFilterProvider");
  return ctx;
}
