import { useQuery } from "@tanstack/react-query";
import type { CogniteClient } from "@cognite/sdk";
import yaml from "js-yaml";
import type { PipelineConfig, ViewConfig, QueryConfig, PipelineFilter } from "../utils/types";
import { isLocalMockMode } from "@/runtime/authMode";
import { localPipelineConfigById, localPipelines } from "@/mocks/mockData";

/**
 * Interface for extraction pipeline from CDF API
 */
interface ExtractionPipeline {
  externalId: string;
  name?: string;
  description?: string;
}

/**
 * Interface for extraction pipeline config from CDF API
 */
interface ExtractionPipelineConfigResponse {
  externalId: string;
  config: string;
}

/**
 * Fetch extraction pipelines from CDF SDK
 */
async function fetchExtractionPipelines(
  sdk: CogniteClient,
  nameFilter = "file_annotation"
): Promise<string[]> {
  try {
    const extractionPipelinesApi = (sdk as unknown as { extractionPipelines?: { list?: (scope: { limit?: number }) => Promise<{ items?: ExtractionPipeline[] }> } }).extractionPipelines;
    if (extractionPipelinesApi?.list) {
      const response = await extractionPipelinesApi.list({ limit: 1000 });
      const items = response?.items ?? [];
      return items
        .filter((p) => p.externalId.includes(nameFilter))
        .map((p) => p.externalId)
        .sort();
    }

    // Fallback: extraction pipelines are not exposed in the SDK for this client.
    const response = await sdk.get<{ items?: ExtractionPipeline[] }>(
      `/api/v1/projects/${sdk.project}/extpipes`,
      { params: { limit: 1000 } }
    );

    const items = response.data?.items ?? [];
    return items
      .filter((p) => p.externalId.includes(nameFilter))
      .map((p) => p.externalId)
      .sort();
  } catch (error) {
    console.error("Failed to fetch extraction pipelines:", error);
    return [];
  }
}

/**
 * Fetch extraction pipeline configuration from CDF SDK
 */
async function fetchPipelineConfig(
  sdk: CogniteClient,
  pipelineExternalId: string
): Promise<PipelineConfig | null> {
  try {
    const extractionPipelinesApi = (sdk as unknown as { extractionPipelines?: { config?: { retrieve?: (externalId: string) => Promise<ExtractionPipelineConfigResponse> } } }).extractionPipelines;
    if (extractionPipelinesApi?.config?.retrieve) {
      const response = await extractionPipelinesApi.config.retrieve(pipelineExternalId);
      if (!response?.config) {
        return null;
      }
      const rawConfig = yaml.load(response.config) as Record<string, unknown>;
      return parseConfigToViewConfig(rawConfig);
    }

    // Fallback: extraction pipeline config is not exposed in the SDK for this client.
    const response = await sdk.get<ExtractionPipelineConfigResponse>(
      `/api/v1/projects/${sdk.project}/extpipes/config`,
      { params: { externalId: pipelineExternalId } }
    );

    if (!response.data?.config) {
      return null;
    }

    const rawConfig = yaml.load(response.data.config) as Record<string, unknown>;
    return parseConfigToViewConfig(rawConfig);
  } catch (error) {
    console.error("Failed to fetch pipeline config:", error);
    return null;
  }
}

/**
 * Parse raw config YAML to PipelineConfig
 * Handles the nested structure: dataModelViews, rawTables, launchFunction, finalizeFunction
 */
function parseConfigToViewConfig(rawConfig: Record<string, unknown>): PipelineConfig {
  // Extract dataModelViews (nested under this key in the actual config)
  const dataModelViews = (rawConfig.dataModelViews || rawConfig.data_model_views || {}) as Record<string, Record<string, unknown>>;
  
  // Extract rawTables (nested under this key)
  const rawTables = (rawConfig.rawTables || rawConfig.raw_tables || {}) as Record<string, string>;
  
  // Extract launchFunction config for file/asset resource properties
  const launchFunction = (rawConfig.launchFunction || rawConfig.launch_function || {}) as Record<string, unknown>;
  const dataModelService = (launchFunction.dataModelService || launchFunction.data_model_service || {}) as Record<string, unknown>;

  // Parse view configs from dataModelViews
  const annotationStateViewCfg = (dataModelViews.annotationStateView || dataModelViews.annotation_state_view) as Record<string, string> | undefined;
  const fileViewCfg = (dataModelViews.fileView || dataModelViews.file_view) as Record<string, string> | undefined;
  const assetViewCfg = (dataModelViews.targetEntityView || dataModelViews.asset_view || dataModelViews.assetView) as Record<string, string> | undefined;

  // Parse raw table names
  const rawDb = rawTables.rawDb || rawTables.raw_db || (rawConfig.rawDb as string) || (rawConfig.raw_db as string);
  const rawTablePatternTags = rawTables.rawTableDocPattern || rawTables.raw_table_doc_pattern || rawTables.rawTablePatternTags || rawTables.raw_table_pattern_tags;
  const rawTableAssetTags = rawTables.rawTableDocTag || rawTables.raw_table_doc_tag || rawTables.rawTableAssetTags || rawTables.raw_table_asset_tags;
  const rawTableFileTags = rawTables.rawTableDocDoc || rawTables.raw_table_doc_doc || rawTables.rawTableFileTags || rawTables.raw_table_file_tags;
  const rawTablePatternCache = rawTables.rawTableCache || rawTables.raw_table_cache || rawTables.rawTablePatternCache || rawTables.raw_table_pattern_cache;
  const rawManualPatternsCatalog = rawTables.rawManualPatternsCatalog || rawTables.raw_manual_patterns_catalog;

  // Parse file resource property from launchFunction
  const fileResourceProperty = 
    (launchFunction.fileResourceProperty as string) ||
    (launchFunction.file_resource_property as string) ||
    (rawConfig.fileResourceProperty as string) ||
    (rawConfig.file_resource_property as string) ||
    undefined;

  const assetResourceProperty = 
    (launchFunction.assetResourceProperty as string) ||
    (launchFunction.asset_resource_property as string) ||
    (rawConfig.assetResourceProperty as string) ||
    (rawConfig.asset_resource_property as string);

  const secondaryScopeProperty = 
    (launchFunction.secondaryScopeProperty as string) ||
    (launchFunction.secondary_scope_property as string) ||
    (rawConfig.secondaryScopeProperty as string) ||
    (rawConfig.secondary_scope_property as string);

  const primaryScopeProperty =
    (launchFunction.primaryScopeProperty as string) ||
    (launchFunction.primary_scope_property as string) ||
    (rawConfig.primaryScopeProperty as string) ||
    (rawConfig.primary_scope_property as string);

  const fileSearchProperty =
    (launchFunction.fileSearchProperty as string) ||
    (launchFunction.file_search_property as string);

  const targetEntitiesSearchProperty =
    (launchFunction.targetEntitiesSearchProperty as string) ||
    (launchFunction.target_entities_search_property as string) ||
    (launchFunction.assetSearchProperty as string) ||
    (launchFunction.asset_search_property as string);

  const targetEntitiesQueryRaw =
    (dataModelService.getTargetEntitiesQuery as Record<string, unknown> | Record<string, unknown>[]) ||
    (dataModelService.get_target_entities_query as Record<string, unknown> | Record<string, unknown>[]) ||
    (launchFunction.getTargetEntitiesQuery as Record<string, unknown> | Record<string, unknown>[]) ||
    (launchFunction.get_target_entities_query as Record<string, unknown> | Record<string, unknown>[]);

  const fileEntitiesQueryRaw =
    (dataModelService.getFileEntitiesQuery as Record<string, unknown> | Record<string, unknown>[]) ||
    (dataModelService.get_file_entities_query as Record<string, unknown> | Record<string, unknown>[]) ||
    (launchFunction.getFileEntitiesQuery as Record<string, unknown> | Record<string, unknown>[]) ||
    (launchFunction.get_file_entities_query as Record<string, unknown> | Record<string, unknown>[]);

  const targetEntitiesQuery = parseQueryConfig(targetEntitiesQueryRaw);
  const fileEntitiesQuery = parseQueryConfig(fileEntitiesQueryRaw);

  return {
    annotationStateView: annotationStateViewCfg
      ? parseViewConfig(annotationStateViewCfg)
      : {
          schemaSpace: "file_annotation",
          externalId: "AnnotationState",
          version: "1",
          instanceSpace: "file_annotation",
        },
    fileView: fileViewCfg
      ? parseViewConfig(fileViewCfg)
      : {
          schemaSpace: "cdf_cdm",
          externalId: "CogniteFile",
          version: "v1",
          instanceSpace: "file_annotation",
        },
    assetView: assetViewCfg ? parseViewConfig(assetViewCfg) : undefined,
    fileResourceProperty,
    assetResourceProperty,
    primaryScopeProperty,
    secondaryScopeProperty,
    fileSearchProperty,
    targetEntitiesSearchProperty,
    targetEntitiesQuery,
    fileEntitiesQuery,
    rawDb,
    rawTablePatternTags,
    rawTableAssetTags,
    rawTableFileTags,
    rawTablePatternCache,
    rawManualPatternsCatalog,
  };
}

/**
 * Parse individual view config from raw config
 */
function parseViewConfig(viewConfig: Record<string, unknown>): ViewConfig {
  return {
    schemaSpace: (viewConfig.schemaSpace as string) || (viewConfig.schema_space as string) || (viewConfig.space as string) || "",
    externalId: (viewConfig.externalId as string) || (viewConfig.external_id as string) || "",
    version: String((viewConfig.version as string) || "1"),
    instanceSpace: (viewConfig.instanceSpace as string) || (viewConfig.instance_space as string),
  };
}

function parseQueryConfig(
  queryRaw?: Record<string, unknown> | Record<string, unknown>[]
): QueryConfig | QueryConfig[] | undefined {
  if (!queryRaw) return undefined;
  if (Array.isArray(queryRaw)) {
    const parsed = queryRaw.map((entry) => parseQueryConfig(entry)).filter(Boolean) as QueryConfig[];
    return parsed.length > 0 ? parsed : undefined;
  }

  const targetView = (queryRaw.targetView || queryRaw.target_view) as Record<string, unknown> | undefined;
  const filtersRaw = (queryRaw.filters || queryRaw.filter || queryRaw.filters_list) as
    | Array<Record<string, unknown>>
    | Record<string, unknown>
    | undefined;
  const limit = (queryRaw.limit as number) ?? (queryRaw.limit_value as number);

  const filtersArray = Array.isArray(filtersRaw) ? filtersRaw : filtersRaw ? [filtersRaw] : [];
  const filters: PipelineFilter[] = filtersArray
    .map((filter) => {
      if (!filter) return undefined;
      return {
        targetProperty: (filter.targetProperty as string) || (filter.target_property as string),
        operator: (filter.operator as string) || (filter.op as string),
        values:
          (filter.values as string | string[]) ??
          (filter.value as string | string[]) ??
          (filter.values_list as string | string[]),
        negate: Boolean((filter.negate as boolean) ?? (filter.not as boolean)),
      } as PipelineFilter;
    })
    .filter(Boolean) as PipelineFilter[];

  return {
    targetView: targetView ? parseViewConfig(targetView) : undefined,
    filters: filters.length > 0 ? filters : undefined,
    limit,
  };
}

/**
 * Hook to fetch available pipeline IDs from CDF
 */
export function useAvailablePipelines(sdk: CogniteClient | null) {
  return useQuery({
    queryKey: ["extractionPipelines", isLocalMockMode ? "local" : sdk?.project],
    queryFn: () => {
      if (isLocalMockMode) {
        return Promise.resolve(localPipelines);
      }
      return sdk ? fetchExtractionPipelines(sdk) : Promise.resolve([]);
    },
    enabled: isLocalMockMode || !!sdk,
    staleTime: 1000 * 60 * 60, // 1 hour
  });
}

/**
 * Hook to get pipeline configuration
 */
export function usePipelineConfig(sdk: CogniteClient | null, pipelineId: string | null) {
  return useQuery({
    queryKey: ["pipelineConfig", isLocalMockMode ? "local" : sdk?.project, pipelineId],
    queryFn: () => {
      if (isLocalMockMode) {
        return Promise.resolve(pipelineId ? localPipelineConfigById[pipelineId] || null : null);
      }
      return sdk && pipelineId ? fetchPipelineConfig(sdk, pipelineId) : Promise.resolve(null);
    },
    enabled: (isLocalMockMode && !!pipelineId) || (!!sdk && !!pipelineId),
    staleTime: 1000 * 60 * 60, // 1 hour
  });
}

/**
 * Create a ViewConfig from parts
 */
export function createViewConfig(
  schemaSpace: string,
  externalId: string,
  version: string,
  instanceSpace?: string
): ViewConfig {
  return {
    schemaSpace,
    externalId,
    version,
    instanceSpace,
  };
}
