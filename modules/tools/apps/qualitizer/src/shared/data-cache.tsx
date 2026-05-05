import { createContext, useCallback, useContext, useMemo, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import {
  cachedDataModelsList,
  cachedDataModelsRetrieve,
  cachedViewsList,
  cachedViewsRetrieve,
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

  const loadDataModels = useCallback(async () => {
    if (dataModelsStatus === "loading" || dataModelsStatus === "success" || dataModelsStatus === "error") return;
    setDataModelsStatus("loading");
    setDataModelsError(null);
    try {
      const items: DataModelSummary[] = [];
      let cursor: string | undefined;
      do {
        const params: Record<string, unknown> = {
          includeGlobal: true,
          allVersions: false,
          inlineViews: true,
          limit: 100,
        };
        if (cursor) params.cursor = cursor;
        const response = (await cachedDataModelsList(sdk, params)) as {
          items?: DataModelSummary[];
          nextCursor?: string;
        };
        items.push(...(response.items ?? []));
        cursor = response.nextCursor ?? undefined;
      } while (cursor);

      setDataModels(items);
      setDataModelsStatus("success");
    } catch (error) {
      setDataModelsError(error instanceof Error ? error.message : "Failed to load data models.");
      setDataModelsStatus("error");
    }
  }, [sdk, dataModelsStatus]);

  const loadViews = useCallback(async () => {
    if (viewsStatus === "loading" || viewsStatus === "success" || viewsStatus === "error") return;
    setViewsStatus("loading");
    setViewsError(null);
    try {
      const items: ViewSummary[] = [];
      let cursor: string | undefined;
      do {
        const params: Record<string, unknown> = {
          includeGlobal: true,
          allVersions: false,
          limit: 100,
        };
        if (cursor) params.cursor = cursor;
        const response = (await cachedViewsList(sdk, params)) as {
          items?: ViewSummary[];
          nextCursor?: string;
        };
        items.push(...(response.items ?? []));
        cursor = response.nextCursor ?? undefined;
      } while (cursor);

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
