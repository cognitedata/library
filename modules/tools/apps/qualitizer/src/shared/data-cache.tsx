import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import {
  cachedDataModelsRetrieve,
  cachedViewsRetrieve,
  listAllCachedDataModels,
  listAllCachedViews,
} from "@/shared/dms-catalog-cache";

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
  loadDataModels: () => Promise<void>;
  loadViews: () => Promise<void>;
  retrieveDataModels: (
    params: Array<Record<string, unknown>>,
    options?: Record<string, unknown>
  ) => Promise<unknown>;
  retrieveViews: (
    params: Array<Record<string, unknown>>,
    options?: Record<string, unknown>
  ) => Promise<unknown>;
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

  useEffect(() => {
    setDataModels([]);
    setDataModelsStatus("idle");
    setDataModelsError(null);
    setViews([]);
    setViewsStatus("idle");
    setViewsError(null);
  }, [sdk]);

  const loadDataModels = useCallback(async () => {
    if (dataModelsStatus === "loading" || dataModelsStatus === "success") return;
    setDataModelsStatus("loading");
    setDataModelsError(null);
    try {
      const items = (await listAllCachedDataModels(
        sdk,
        { includeGlobal: true, allVersions: false, inlineViews: true },
        { pageLimit: 100 }
      )) as DataModelSummary[];

      setDataModels(items);
      setDataModelsStatus("success");
    } catch (error) {
      setDataModelsError(error instanceof Error ? error.message : "Failed to load data models.");
      setDataModelsStatus("error");
    }
  }, [sdk, dataModelsStatus]);

  const loadViews = useCallback(async () => {
    if (viewsStatus === "loading" || viewsStatus === "success") return;
    setViewsStatus("loading");
    setViewsError(null);
    try {
      const items = (await listAllCachedViews(
        sdk,
        { includeGlobal: true, allVersions: false },
        { pageLimit: 100 }
      )) as ViewSummary[];

      setViews(items);
      setViewsStatus("success");
    } catch (error) {
      setViewsError(error instanceof Error ? error.message : "Failed to load views.");
      setViewsStatus("error");
    }
  }, [sdk, viewsStatus]);

  const retrieveDataModels = useCallback(
    async (params: Array<Record<string, unknown>>, options?: Record<string, unknown>) => {
      return cachedDataModelsRetrieve(sdk, params, options);
    },
    [sdk]
  );

  const retrieveViews = useCallback(
    async (params: Array<Record<string, unknown>>, options?: Record<string, unknown>) => {
      return cachedViewsRetrieve(sdk, params, options);
    },
    [sdk]
  );

  const value = useMemo(
    () => ({
      dataModels,
      dataModelsStatus,
      dataModelsError,
      views,
      viewsStatus,
      viewsError,
      loadDataModels,
      loadViews,
      retrieveDataModels,
      retrieveViews,
    }),
    [
      dataModels,
      dataModelsStatus,
      dataModelsError,
      views,
      viewsStatus,
      viewsError,
      loadDataModels,
      loadViews,
      retrieveDataModels,
      retrieveViews,
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
