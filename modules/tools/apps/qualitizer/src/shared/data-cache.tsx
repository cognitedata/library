import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { useAppSdk } from "@/shared/auth";

type LoadState = "idle" | "loading" | "success" | "error";

type DataModelSummary = {
  space: string;
  externalId: string;
  name?: string;
  description?: string;
  version?: string;
  createdTime?: number;
  lastUpdatedTime?: number;
  views?: Array<{ space: string; externalId: string; version?: string; name?: string }>;
};

type ViewSummary = {
  space: string;
  externalId: string;
  name?: string;
  description?: string;
  version?: string;
  createdTime?: number;
  lastUpdatedTime?: number;
  usedFor?: "node" | "edge" | "all";
};

type DataCacheContextValue = {
  dataModels: DataModelSummary[];
  dataModelsStatus: LoadState;
  dataModelsError: string | null;
  views: ViewSummary[];
  viewsStatus: LoadState;
  viewsError: string | null;
  dataModelDetails: Record<string, unknown>;
  loadDataModels: () => Promise<void>;
  loadViews: () => Promise<void>;
  retrieveDataModels: (
    params: Array<Record<string, unknown>>,
    options?: Record<string, unknown>
  ) => Promise<unknown>;
  clearCache: () => void;
};

const DataCacheContext = createContext<DataCacheContextValue | null>(null);

export function DataCacheProvider({ children }: { children: React.ReactNode }) {
  const { sdk } = useAppSdk();
  const [dataModels, setDataModels] = useState<DataModelSummary[]>([]);
  const [dataModelsStatus, setDataModelsStatus] = useState<LoadState>("idle");
  const [dataModelsError, setDataModelsError] = useState<string | null>(null);
  const [views, setViews] = useState<ViewSummary[]>([]);
  const [viewsStatus, setViewsStatus] = useState<LoadState>("idle");
  const [viewsError, setViewsError] = useState<string | null>(null);
  const [dataModelDetails, setDataModelDetails] = useState<Record<string, unknown>>({});
  const [hasHydrated, setHasHydrated] = useState(false);
  const storageKey = `qualitizer.dataCache.v1.${sdk.project}`;
  const [localStorageEnabled, setLocalStorageEnabled] = useState(true);

  useEffect(() => {
    if (typeof window === "undefined" || hasHydrated || !localStorageEnabled) return;
    const stored = window.localStorage.getItem(storageKey);
    if (!stored) {
      setHasHydrated(true);
      return;
    }
    try {
      const parsed = JSON.parse(stored) as {
        dataModels?: DataModelSummary[];
        views?: ViewSummary[];
        dataModelDetails?: Record<string, unknown>;
      };
      setDataModels(parsed.dataModels ?? []);
      setViews(parsed.views ?? []);
      setDataModelDetails(parsed.dataModelDetails ?? {});
      if (parsed.dataModels && parsed.dataModels.length > 0) {
        setDataModelsStatus("success");
      }
      if (parsed.views && parsed.views.length > 0) {
        setViewsStatus("success");
      }
    } catch {
      window.localStorage.removeItem(storageKey);
    } finally {
      setHasHydrated(true);
    }
  }, [hasHydrated, localStorageEnabled, storageKey]);

  useEffect(() => {
    setHasHydrated(false);
    setDataModels([]);
    setViews([]);
    setDataModelDetails({});
    setDataModelsStatus("idle");
    setViewsStatus("idle");
    setDataModelsError(null);
    setViewsError(null);
  }, [storageKey]);

  useEffect(() => {
    if (typeof window === "undefined" || !hasHydrated || !localStorageEnabled) return;
    try {
      const payload = JSON.stringify({
        dataModels,
        views,
        dataModelDetails,
      });
      window.localStorage.setItem(storageKey, payload);
    } catch (error) {
      if (error instanceof DOMException && error.name === "QuotaExceededError") {
        setLocalStorageEnabled(false);
        window.localStorage.removeItem(storageKey);
      } else {
        throw error;
      }
    }
  }, [dataModels, views, dataModelDetails, hasHydrated, localStorageEnabled, storageKey]);

  const loadDataModels = async () => {
    if (dataModelsStatus === "loading" || dataModelsStatus === "success") return;
    setDataModelsStatus("loading");
    setDataModelsError(null);
    try {
      const items: DataModelSummary[] = [];
      let cursor: string | undefined;
      do {
        const response = await sdk.dataModels.list({
          includeGlobal: true,
          allVersions: false,
          inlineViews: true,
          limit: 100,
          cursor,
        });
        items.push(...(response.items as DataModelSummary[]));
        cursor = response.nextCursor ?? undefined;
      } while (cursor);

      setDataModels(items);
      setDataModelsStatus("success");
    } catch (error) {
      setDataModelsError(error instanceof Error ? error.message : "Failed to load data models.");
      setDataModelsStatus("error");
    }
  };

  const loadViews = async () => {
    if (viewsStatus === "loading" || viewsStatus === "success") return;
    setViewsStatus("loading");
    setViewsError(null);
    try {
      const items: ViewSummary[] = [];
      let cursor: string | undefined;
      do {
        const response = await sdk.views.list({
          includeGlobal: true,
          allVersions: false,
          limit: 100,
          cursor,
        });
        items.push(...(response.items as ViewSummary[]));
        cursor = response.nextCursor ?? undefined;
      } while (cursor);

      setViews(items);
      setViewsStatus("success");
    } catch (error) {
      setViewsError(error instanceof Error ? error.message : "Failed to load views.");
      setViewsStatus("error");
    }
  };

  const retrieveDataModels = async (
    params: Array<Record<string, unknown>>,
    options?: Record<string, unknown>
  ) => {
    const key = `${sdk.project}:${JSON.stringify(params)}`;
    if (dataModelDetails[key]) {
      return dataModelDetails[key];
    }
    const response = await sdk.dataModels.retrieve(params as never, options as never);
    setDataModelDetails((prev) => ({ ...prev, [key]: response }));
    return response;
  };

  const clearCache = () => {
    setDataModels([]);
    setViews([]);
    setDataModelDetails({});
    setDataModelsStatus("idle");
    setViewsStatus("idle");
    setDataModelsError(null);
    setViewsError(null);
    if (typeof window !== "undefined" && localStorageEnabled) {
      window.localStorage.removeItem(storageKey);
    }
  };

  const value = useMemo(
    () => ({
      dataModels,
      dataModelsStatus,
      dataModelsError,
      views,
      viewsStatus,
      viewsError,
      dataModelDetails,
      loadDataModels,
      loadViews,
      retrieveDataModels,
      clearCache,
    }),
    [
      dataModels,
      dataModelsStatus,
      dataModelsError,
      views,
      viewsStatus,
      viewsError,
      dataModelDetails,
      clearCache,
    ]
  );

  return <DataCacheContext.Provider value={value}>{children}</DataCacheContext.Provider>;
}

export function useAppData() {
  const context = useContext(DataCacheContext);
  if (!context) {
    throw new Error("useAppData must be used within DataCacheProvider");
  }
  return context;
}
