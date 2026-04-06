import { createContext, useContext, useMemo, useState } from "react";
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
  viewDetails: Record<string, unknown>;
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
  const [dataModelDetails, setDataModelDetails] = useState<Record<string, unknown>>({});
  const [viewDetails, setViewDetails] = useState<Record<string, unknown>>({});

  const loadDataModels = async () => {
    if (dataModelsStatus === "loading" || dataModelsStatus === "success" || dataModelsStatus === "error") return;
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
    if (viewsStatus === "loading" || viewsStatus === "success" || viewsStatus === "error") return;
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

  const retrieveViews = async (
    params: Array<Record<string, unknown>>,
    options?: Record<string, unknown>
  ) => {
    const key = `${sdk.project}:${JSON.stringify(params)}:${JSON.stringify(options ?? {})}`;
    if (viewDetails[key]) {
      return viewDetails[key];
    }
    const response = await sdk.views.retrieve(params as never, options as never);
    setViewDetails((prev) => ({ ...prev, [key]: response }));
    return response;
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
      viewDetails,
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
      dataModelDetails,
      viewDetails,
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
