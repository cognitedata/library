import { useDune } from "@cognite/dune";
import { useEffect, useState } from "react";
import type { DataModelInfo, ViewReference } from "./types";

interface UseDataModelLoaderConfig {
  space: string;
  externalId: string;
  version: string;
}

interface UseDataModelLoaderReturn {
  dataModel: DataModelInfo | null;
  isLoading: boolean;
  error: string | null;
}

export function useDataModelLoader(
  config: UseDataModelLoaderConfig
): UseDataModelLoaderReturn {
  const { sdk, isLoading: isAuthLoading } = useDune();

  const [dataModel, setDataModel] = useState<DataModelInfo | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!sdk || isAuthLoading) return;

    let cancelled = false;

    async function load() {
      try {
        setIsLoading(true);
        setError(null);

        const response = await sdk.dataModels.retrieve([
          {
            space: config.space,
            externalId: config.externalId,
            version: config.version,
          },
        ]);

        if (cancelled) return;

        if (response.items.length === 0) {
          throw new Error(
            `Data model not found: ${config.space}/${config.externalId} v${config.version}`
          );
        }

        const model = response.items[0];
        const views: ViewReference[] = (model.views || []).map(
          (v: { space: string; externalId: string; version: string }) => ({
            space: v.space,
            externalId: v.externalId,
            version: v.version,
          })
        );

        setDataModel({
          space: model.space,
          externalId: model.externalId,
          name: model.name,
          description: model.description,
          version: model.version,
          views,
        });
      } catch (err) {
        if (!cancelled) {
          setError(
            err instanceof Error ? err.message : "Failed to load data model"
          );
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    load();

    return () => {
      cancelled = true;
    };
  }, [sdk, isAuthLoading, config.space, config.externalId, config.version]);

  return { dataModel, isLoading, error };
}
