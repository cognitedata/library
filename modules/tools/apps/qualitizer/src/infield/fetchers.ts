import type { CogniteClient } from "@cognite/sdk";
import { cachedInstancesByIds, cachedInstancesList } from "@/shared/instances-cache";
import { cachedViewsRetrieve } from "@/shared/dms-catalog-cache";
import { withTransientRetries } from "@/shared/transient-http-retry";
import { searchNodesByExternalId } from "@/data-catalog/doc-lookup/doc-lookup-fetchers";
import type {
  LocationConfigValidation,
  DataStorageCounts,
  DataStorageReference,
  LegacyConfigData,
  LegacyConfigKey,
  LegacyConfigLocation,
  LegacyConfigLoadProgress,
  LegacyAssetLookupResult,
  LegacyLocationEstimates,
  LegacyLocationMigrationCounts,
  CellLoadOutcome,
  LegacyEstimateColumnKey,
  LegacyMigrationViewCount,
  LocationConfigNode,
  SampleCountEstimate,
  SampledInstanceRow,
  InfieldDataLocationOption,
  LegacyDataQualityReport,
  LegacyDataQualityProgress,
  LegacyViewSpaceCheckResult,
  RelevantObjectCountProgress,
  SpaceCountBreakdown,
  SpaceMetricPair,
  MappingCountMetrics,
  MappingSpaceProbeMetrics,
  LocationSpaceProbeResult,
  SpaceProbeMetric,
  SpaceProbeApiCall,
  MappingInstanceSpaceProbeMetric,
  ConfiguredSpaceUsageStatus,
  ViewCountResult,
  ViewExistenceResult,
  ViewMappingReference,
  ViewSource,
} from "./types";
import {
  deriveLegacySiteCode,
  getWaveLabel,
  getWaveSortRank,
  resolveLegacyInstanceSpace,
} from "./migration-scripts";

export { deriveLegacySiteCode, getWaveLabel, getWaveSortRank };

export const INFIELD_LOCATION_CONFIG_VIEW = {
  space: "cdf_infield",
  externalId: "InFieldCDMLocationConfig",
  version: "v1",
} as const;

export const OOTB_COGNITE_ASSET_VIEW: ViewSource = {
  type: "view",
  space: "cdf_cdm",
  externalId: "CogniteAsset",
  version: "v1",
};

export const OOTB_COGNITE_EQUIPMENT_VIEW: ViewSource = {
  type: "view",
  space: "cdf_cdm",
  externalId: "CogniteEquipment",
  version: "v1",
};

export const INFIELD_TIMESERIES_VIEW: ViewSource = {
  type: "view",
  space: "cdf_cdm",
  externalId: "CogniteTimeSeries",
  version: "v1",
};

export const DEFAULT_VIEW_MAPPINGS: Record<string, ViewSource> = {
  asset: { type: "view", space: "cdf_cdm", externalId: "CogniteAsset", version: "v1" },
  file: { type: "view", space: "cdf_cdm", externalId: "CogniteFile", version: "v1" },
  operation: { type: "view", space: "cdf_idm", externalId: "CogniteOperation", version: "v1" },
  observation: {
    type: "view",
    space: "cdf_infield",
    externalId: "FieldObservation",
    version: "v1",
  },
  notification: { type: "view", space: "cdf_idm", externalId: "CogniteNotification", version: "v1" },
  maintenanceOrder: {
    type: "view",
    space: "cdf_idm",
    externalId: "CogniteMaintenanceOrder",
    version: "v1",
  },
  timeseries: INFIELD_TIMESERIES_VIEW,
};

const VIEW_MAPPING_SAMPLE_ORDER: Record<string, number> = {
  file: 0,
  asset: 1,
  operation: 2,
  observation: 3,
  notification: 4,
  maintenanceOrder: 5,
  timeseries: 6,
};

export type ViewMappingSampleTask = {
  mappingKey: string;
  view: ViewSource;
  instanceSpace: string;
  mappingVariant: "configured" | "default";
  defaultView?: ViewSource;
};

export function isDefaultViewMapping(mappingKey: string, view: ViewSource): boolean {
  const defaultView = DEFAULT_VIEW_MAPPINGS[mappingKey];
  if (defaultView === undefined) return false;
  return (
    defaultView.space === view.space &&
    defaultView.externalId === view.externalId &&
    defaultView.version === view.version
  );
}

export const DEFAULT_SAMPLE_CAP = 500;

/** @deprecated Use DEFAULT_SAMPLE_CAP or SampleCapValue */
export const SAMPLE_CAP = DEFAULT_SAMPLE_CAP;

/** null means no cap — count until pagination ends */
export type SampleCapValue = number | null;

export type SampleCapPreset = 500 | 5000 | 25000 | 100000 | "all";

export const SAMPLE_CAP_PRESETS: readonly SampleCapPreset[] = [500, 5000, 25000, 100000, "all"];

export const DEFAULT_SAMPLE_CAP_PRESET: SampleCapPreset = 500;

export function isSampleCapPreset(value: unknown): value is SampleCapPreset {
  return (
    value === 500 ||
    value === 5000 ||
    value === 25000 ||
    value === 100000 ||
    value === "all"
  );
}

export function sampleCapFromPreset(preset: SampleCapPreset): SampleCapValue {
  return preset === "all" ? null : preset;
}

export function formatSampleCapPresetLabel(preset: SampleCapPreset): string {
  switch (preset) {
    case 500:
      return "500";
    case 5000:
      return "5K";
    case 25000:
      return "25K";
    case 100000:
      return "100K";
    case "all":
      return "All";
  }
}

export function formatSampleCapHint(cap: SampleCapValue): string {
  if (cap === null) return "all items";
  return `${cap.toLocaleString()} max items`;
}

export function resolveSampleCapOption(sampleCap: SampleCapValue | undefined): SampleCapValue {
  return sampleCap !== undefined ? sampleCap : DEFAULT_SAMPLE_CAP;
}

function capSampleCount(
  count: number,
  sampleCap: SampleCapValue
): { count: number; capped: boolean } {
  if (sampleCap !== null && count >= sampleCap) {
    return { count: sampleCap, capped: true };
  }
  return { count, capped: false };
}

function hasRecordShape(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function getStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string");
}

function getAssetSubtreeExternalIds(value: unknown): string[] {
  if (!hasRecordShape(value)) return [];
  return getStringList(value.assetSubtreeExternalIds);
}

function parseLegacyConfigLocation(value: unknown, fallbackExternalId: string): LegacyConfigLocation | null {
  if (!hasRecordShape(value)) return null;

  const externalId = value.externalId;
  const parsedExternalId = typeof externalId === "string" && externalId.length > 0 ? externalId : "";
  const dataSetId = typeof value.dataSetId === "number" ? value.dataSetId : null;
  const assetExternalId = typeof value.assetExternalId === "string" ? value.assetExternalId : "";
  const appDataInstanceSpace =
    typeof value.appDataInstanceSpace === "string" ? value.appDataInstanceSpace : "";
  const sourceDataInstanceSpace =
    typeof value.sourceDataInstanceSpace === "string" ? value.sourceDataInstanceSpace : "";
  const templateAdmins = getStringList(value.templateAdmins);
  const checklistAdmins = getStringList(value.checklistAdmins);

  const dataFilters = value.dataFilters;
  const filesFilter = hasRecordShape(dataFilters) ? dataFilters.files : undefined;
  const assetsFilter = hasRecordShape(dataFilters) ? dataFilters.assets : undefined;
  const generalFilter = hasRecordShape(dataFilters) ? dataFilters.general : undefined;
  const timeseriesFilter = hasRecordShape(dataFilters) ? dataFilters.timeseries : undefined;

  return {
    rowId: fallbackExternalId,
    externalId: parsedExternalId,
    dataSetId,
    assetExternalId,
    appDataInstanceSpace,
    sourceDataInstanceSpace,
    templateAdmins,
    checklistAdmins,
    fileFilter: getAssetSubtreeExternalIds(filesFilter),
    assetFilter: getAssetSubtreeExternalIds(assetsFilter),
    generalFilter: getAssetSubtreeExternalIds(generalFilter),
    timeseriesFilter: getAssetSubtreeExternalIds(timeseriesFilter),
    raw: value,
  };
}

function getRootLocationConfigurationsFromRow(row: unknown): unknown[] {
  if (!hasRecordShape(row)) return [];

  const properties = row.properties;
  if (!hasRecordShape(properties)) return [];

  const apmConfigSpace = properties.APM_Config;
  if (!hasRecordShape(apmConfigSpace)) return [];

  const apmConfigView = apmConfigSpace["APM_Config/1"];
  if (!hasRecordShape(apmConfigView)) return [];

  const featureConfiguration = apmConfigView.featureConfiguration;
  if (hasRecordShape(featureConfiguration) && Array.isArray(featureConfiguration.rootLocationConfigurations)) {
    return featureConfiguration.rootLocationConfigurations;
  }

  if (Array.isArray(apmConfigView.rootLocationConfigurations)) {
    return apmConfigView.rootLocationConfigurations;
  }

  const rootLocationConfigurations = apmConfigView.rootLocationConfigurations;
  if (
    hasRecordShape(rootLocationConfigurations) &&
    Array.isArray(rootLocationConfigurations.rootLocationConfigurations)
  ) {
    return rootLocationConfigurations.rootLocationConfigurations;
  }

  return [];
}

function parseLegacyConfigLocations(queryItems: unknown): LegacyConfigLocation[] {
  if (!hasRecordShape(queryItems)) return [];

  const viewRows = queryItems["APM_Config/1"];
  if (!Array.isArray(viewRows) || viewRows.length === 0) return [];

  const parsedLocations: LegacyConfigLocation[] = [];
  for (const [rowIndex, row] of viewRows.entries()) {
    const rowLocations = getRootLocationConfigurationsFromRow(row);
    for (const [index, location] of rowLocations.entries()) {
      const parsed = parseLegacyConfigLocation(location, `unknown-location-${rowIndex + 1}-${index + 1}`);
      if (parsed !== null) parsedLocations.push(parsed);
    }
  }

  return parsedLocations;
}

async function fetchLegacyConfigLocationsByExternalId(
  sdk: CogniteClient,
  configExternalId: LegacyConfigKey
): Promise<{ locations: LegacyConfigLocation[]; responseItems: unknown }> {
  const response = await withTransientRetries(() =>
    sdk.instances.query({
      with: {
        "APM_Config/1": {
          sort: [],
          nodes: {
            filter: {
              and: [
                { in: { property: ["node", "space"], values: ["APM_Config"] } },
                {
                  hasData: [
                    {
                      externalId: "APM_Config",
                      space: "APM_Config",
                      version: "1",
                      type: "view",
                    },
                  ],
                },
                {
                  equals: {
                    property: ["node", "externalId"],
                    value: configExternalId,
                  },
                },
              ],
            },
          },
        },
      },
      select: {
        "APM_Config/1": {
          sources: [
            {
              source: {
                externalId: "APM_Config",
                space: "APM_Config",
                version: "1",
                type: "view",
              },
              properties: ["*"],
            },
          ],
        },
      },
    })
  );

  return {
    locations: parseLegacyConfigLocations(response.items),
    responseItems: response.items,
  };
}

export async function fetchLegacyConfigData(
  sdk: CogniteClient,
  options?: {
    onProgress?: (progress: LegacyConfigLoadProgress) => void;
    onResult?: (result: LegacyConfigData) => void;
  }
): Promise<LegacyConfigData[]> {
  const configKeys: LegacyConfigKey[] = ["APP_CONFIG_V2", "default-config"];
  const results: LegacyConfigData[] = [];

  for (let index = 0; index < configKeys.length; index += 1) {
    const key = configKeys[index];
    options?.onProgress?.({ current: index, total: configKeys.length, configKey: key });

    try {
      const { locations, responseItems } = await fetchLegacyConfigLocationsByExternalId(sdk, key);
      const entry: LegacyConfigData = { key, locations, responseItems, error: null };
      results.push(entry);
      options?.onResult?.(entry);
    } catch (error: unknown) {
      const entry: LegacyConfigData = { key, locations: [], responseItems: null, error };
      results.push(entry);
      options?.onResult?.(entry);
    }

    options?.onProgress?.({ current: index + 1, total: configKeys.length, configKey: key });
  }

  return results;
}

export async function fetchLegacyAssetDescriptions(
  sdk: CogniteClient,
  assetExternalIds: string[]
): Promise<Record<string, string>> {
  const uniqueExternalIds = [...new Set(assetExternalIds.filter((id) => id.length > 0))];
  if (uniqueExternalIds.length === 0) return {};

  const requestBody = uniqueExternalIds.map((externalId) => ({ externalId }));
  const assets = await withTransientRetries(() => sdk.assets.retrieve(requestBody));

  return Object.fromEntries(
    uniqueExternalIds.map((externalId) => {
      const asset = assets.find((item) => item.externalId === externalId);
      const description =
        typeof asset?.description === "string" && asset.description.length > 0 ? asset.description : "—";
      return [externalId, description];
    })
  );
}

export async function fetchLegacyAssetDetails(
  sdk: CogniteClient,
  externalId: string,
  instanceSpaces: string[] = []
): Promise<LegacyAssetLookupResult | null> {
  const trimmed = externalId.trim();
  if (trimmed.length === 0) return null;

  const assets = await withTransientRetries(() => sdk.assets.retrieve([{ externalId: trimmed }]));
  if (assets[0] !== undefined) {
    return { kind: "legacy", data: assets[0] };
  }

  const uniqueSpaces = [...new Set(instanceSpaces.map((space) => space.trim()).filter((space) => space.length > 0))];
  if (uniqueSpaces.length === 0) return null;

  for (const space of uniqueSpaces) {
    try {
      const response = await withTransientRetries(() =>
        sdk.instances.retrieve({
          items: [{ externalId: trimmed, space, instanceType: "node" }],
          sources: [{ source: OOTB_COGNITE_ASSET_VIEW }],
        })
      );
      if (response.items.length > 0) {
        return { kind: "dm", data: response.items[0] };
      }
    } catch {
      // try next space
    }
  }

  try {
    const searchResponse = await withTransientRetries(() =>
      sdk.instances.search({
        instanceType: "node",
        view: OOTB_COGNITE_ASSET_VIEW,
        query: trimmed,
        filter: {
          and: [
            { equals: { property: ["node", "externalId"], value: trimmed } },
            { in: { property: ["node", "space"], values: uniqueSpaces } },
          ],
        },
        limit: 1,
      })
    );
    if (searchResponse.items.length > 0) {
      return { kind: "dm", data: searchResponse.items[0] };
    }
  } catch {
    // not found
  }

  return null;
}

export async function fetchLegacyDataSetNames(
  sdk: CogniteClient,
  dataSetIds: number[]
): Promise<Record<number, string>> {
  const uniqueIds = [...new Set(dataSetIds)];
  if (uniqueIds.length === 0) return {};

  const requestBody = uniqueIds.map((id) => ({ id }));
  const result: unknown = await withTransientRetries(() => sdk.datasets.retrieve(requestBody));

  const items = Array.isArray(result)
    ? result
    : hasRecordShape(result) && Array.isArray(result.items)
      ? result.items
      : [];

  return Object.fromEntries(
    uniqueIds.map((id) => {
      const matched = items.find((item: unknown) => hasRecordShape(item) && item.id === id);
      const name =
        hasRecordShape(matched) && typeof matched.name === "string" && matched.name.length > 0
          ? matched.name
          : `${id}`;
      return [id, name];
    })
  );
}

function getPropertiesFromNode(value: unknown): Record<string, unknown> {
  if (!hasRecordShape(value)) return {};

  const propertiesCandidate = value.properties;
  if (!hasRecordShape(propertiesCandidate)) return {};

  const viewKey = `${INFIELD_LOCATION_CONFIG_VIEW.externalId}/${INFIELD_LOCATION_CONFIG_VIEW.version}`;
  const spaceProperties = propertiesCandidate[INFIELD_LOCATION_CONFIG_VIEW.space];
  if (!hasRecordShape(spaceProperties)) return {};

  const viewProperties = spaceProperties[viewKey];
  if (!hasRecordShape(viewProperties)) return {};

  return viewProperties;
}

function toLocationConfigNode(value: unknown): LocationConfigNode | null {
  if (!hasRecordShape(value)) return null;

  const space = value.space;
  const externalId = value.externalId;
  if (typeof space !== "string" || typeof externalId !== "string") return null;

  return {
    space,
    externalId,
    createdTime: typeof value.createdTime === "number" ? value.createdTime : undefined,
    lastUpdatedTime: typeof value.lastUpdatedTime === "number" ? value.lastUpdatedTime : undefined,
    properties: getPropertiesFromNode(value),
  };
}

export async function fetchAllLocationConfigs(
  sdk: CogniteClient,
  options?: { onProgress?: (loadedCount: number) => void }
): Promise<LocationConfigNode[]> {
  const locationConfigs: LocationConfigNode[] = [];
  let cursor: string | undefined;

  do {
    const response = await withTransientRetries(() =>
      cachedInstancesList(sdk, {
        instanceType: "node",
        sources: [{ source: { type: "view", ...INFIELD_LOCATION_CONFIG_VIEW } }],
        limit: 1000,
        cursor,
      })
    );

    for (const item of response.items) {
      const node = toLocationConfigNode(item);
      if (node !== null) locationConfigs.push(node);
    }

    options?.onProgress?.(locationConfigs.length);
    cursor = response.nextCursor;
  } while (cursor !== undefined);

  return locationConfigs;
}

export const INFIELD_DATA_SAMPLE_LIMIT = 500;

export const LEGACY_APM_SCHEMA_SPACE = "cdf_apm";

export const INFIELD_CDM_SCHEMA_SPACE = "cdf_infield";

export const INFIELD_CDM_DATA_MODEL = {
  space: "cdf_infield",
  externalId: "InFieldOnCDM",
  version: "v1",
} as const;

/** Node views in cdf_infield from the InFieldOnCDM data model (app instance data). */
export const INFIELD_CDM_VIEWS = [
  { externalId: "FieldObservation", version: "v1" },
  { externalId: "Checklist", version: "v1" },
  { externalId: "ChecklistItem", version: "v1" },
  { externalId: "MeasurementReading", version: "v1" },
  { externalId: "Template", version: "v1" },
  { externalId: "TemplateItem", version: "v1" },
  { externalId: "Schedule", version: "v1" },
  { externalId: "ConditionalAction", version: "v1" },
  { externalId: "Action", version: "v1" },
  { externalId: "Condition", version: "v1" },
  { externalId: "Asset", version: "v1" },
  { externalId: "InfieldTimeSeries", version: "v1" },
] as const;

export function getInfieldCdmViewSources(): ViewSource[] {
  return INFIELD_CDM_VIEWS.map((view) => ({
    type: "view" as const,
    space: INFIELD_CDM_SCHEMA_SPACE,
    externalId: view.externalId,
    version: view.version,
  }));
}

export const LEGACY_APM_VIEWS = [
  { externalId: "ConditionalAction", version: "v1" },
  { externalId: "Observation", version: "v5" },
  { externalId: "Checklist", version: "v7" },
  { externalId: "MeasurementReading", version: "v4" },
  { externalId: "Template", version: "v8" },
  { externalId: "Action", version: "v1" },
  { externalId: "Condition", version: "v1" },
  { externalId: "ChecklistItem", version: "v7" },
  { externalId: "TemplateItem", version: "v7" },
  { externalId: "Schedule", version: "v4" },
] as const;

/** Legacy cdf_apm view externalId → matching cdf_infield view for Infield CDM data on the same node. */
export const LEGACY_TO_INFIELD_CDM_VIEW: Record<string, { externalId: string; version: string }> = {
  ConditionalAction: { externalId: "ConditionalAction", version: "v1" },
  Observation: { externalId: "FieldObservation", version: "v1" },
  Checklist: { externalId: "Checklist", version: "v1" },
  MeasurementReading: { externalId: "MeasurementReading", version: "v1" },
  Template: { externalId: "Template", version: "v1" },
  Action: { externalId: "Action", version: "v1" },
  Condition: { externalId: "Condition", version: "v1" },
  ChecklistItem: { externalId: "ChecklistItem", version: "v1" },
  TemplateItem: { externalId: "TemplateItem", version: "v1" },
  Schedule: { externalId: "Schedule", version: "v1" },
};

export function getInfieldCdmViewForLegacyView(legacyView: ViewSource): ViewSource | null {
  const mapped = LEGACY_TO_INFIELD_CDM_VIEW[legacyView.externalId];
  if (mapped === undefined) return null;
  return {
    type: "view",
    space: INFIELD_CDM_SCHEMA_SPACE,
    externalId: mapped.externalId,
    version: mapped.version,
  };
}

/** Source views for `cdf migrate infield-data` (toolkit), in migration order. */
export const MIGRATION_SOURCE_VIEWS: Array<{ mappingKey: string; label: string; view: ViewSource }> = [
  {
    mappingKey: "migrate:CogniteSolutionTag",
    label: "CogniteSolutionTag",
    view: { type: "view", space: "cdf_apps_shared", externalId: "CogniteSolutionTag", version: "v1" },
  },
  ...LEGACY_APM_VIEWS.map((legacyView) => ({
    mappingKey: `migrate:${legacyView.externalId}`,
    label: legacyView.externalId,
    view: {
      type: "view" as const,
      space: LEGACY_APM_SCHEMA_SPACE,
      externalId: legacyView.externalId,
      version: legacyView.version,
    },
  })),
];

export const LEGACY_VIEW_SAMPLE_CAP = 1000;
export const LEGACY_PREVIEW_ROW_LIMIT = 5;

function toSampledInstanceRow(
  value: unknown,
  options?: { includeProperties?: boolean; viewSource?: ViewSource }
): SampledInstanceRow | null {
  if (!hasRecordShape(value)) return null;

  const space = value.space;
  const externalId = value.externalId;
  if (typeof space !== "string" || typeof externalId !== "string") return null;

  const instanceType = typeof value.instanceType === "string" ? value.instanceType : "node";
  const lastUpdatedTime = typeof value.lastUpdatedTime === "number" ? value.lastUpdatedTime : undefined;
  const typeValue = value.type;
  const type =
    hasRecordShape(typeValue) &&
    typeof typeValue.space === "string" &&
    typeof typeValue.externalId === "string"
      ? { space: typeValue.space, externalId: typeValue.externalId }
      : undefined;
  const properties =
    options?.includeProperties === true && hasRecordShape(value.properties) ? value.properties : undefined;

  return {
    space,
    externalId,
    lastUpdatedTime,
    instanceType,
    type,
    viewSource: options?.viewSource,
    properties,
  };
}

function extractViewProperties(
  properties: Record<string, unknown>,
  viewSource: ViewSource
): Record<string, unknown> | null {
  const spaceProperties = properties[viewSource.space];
  if (!hasRecordShape(spaceProperties)) return null;

  const viewKey = `${viewSource.externalId}/${viewSource.version}`;
  const viewProperties = spaceProperties[viewKey];
  return hasRecordShape(viewProperties) ? viewProperties : null;
}

export function extractViewPropertiesForSource(
  properties: Record<string, unknown>,
  viewSource: ViewSource
): Record<string, unknown> | null {
  return extractViewProperties(properties, viewSource);
}

export function getConnectivityViewSources(): ViewSource[] {
  const legacyViews: ViewSource[] = LEGACY_APM_VIEWS.map((view) => ({
    type: "view",
    space: LEGACY_APM_SCHEMA_SPACE,
    externalId: view.externalId,
    version: view.version,
  }));
  return [...getInfieldCdmViewSources(), ...legacyViews];
}

export function formatSampledNodeDetails(
  item: Record<string, unknown>,
  viewSource?: ViewSource
): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    instanceType: item.instanceType,
    space: item.space,
    externalId: item.externalId,
    createdTime: item.createdTime,
    lastUpdatedTime: item.lastUpdatedTime,
    type: item.type,
  };

  if (!hasRecordShape(item.properties)) return payload;

  if (viewSource !== undefined) {
    const viewProperties = extractViewProperties(item.properties, viewSource);
    if (viewProperties !== null) {
      return { ...payload, properties: viewProperties };
    }
  }

  return { ...payload, properties: item.properties };
}

async function sampleNodesInSpaceViaQuery(
  sdk: CogniteClient,
  space: string,
  maxItems: number
): Promise<{ items: SampledInstanceRow[]; capped: boolean }> {
  const items: SampledInstanceRow[] = [];
  let cursor: string | undefined;
  let capped = false;

  do {
    const response = await withTransientRetries(() =>
      sdk.instances.query({
        with: {
          space_nodes: {
            nodes: {
              filter: {
                equals: {
                  property: ["node", "space"],
                  value: space,
                },
              },
            },
          },
        },
        select: {
          space_nodes: {},
        },
        parameters: {
          limit: Math.min(1000, maxItems - items.length),
        },
        ...(cursor !== undefined ? { cursors: { space_nodes: cursor } } : {}),
      })
    );

    const batch = response.items.space_nodes ?? [];
    for (const item of batch) {
      const row = toSampledInstanceRow(item);
      if (row !== null) items.push(row);
      if (items.length >= maxItems) {
        capped = true;
        break;
      }
    }

    if (capped) break;
    cursor = response.nextCursor?.space_nodes;
  } while (cursor !== undefined);

  return { items, capped };
}

async function sampleNodesInSpaceViaList(
  sdk: CogniteClient,
  space: string,
  maxItems: number
): Promise<{ items: SampledInstanceRow[]; capped: boolean }> {
  const items: SampledInstanceRow[] = [];
  let cursor: string | undefined;
  let capped = false;

  do {
    const response = await withTransientRetries(() =>
      cachedInstancesList(sdk, {
        instanceType: "node",
        limit: Math.min(1000, maxItems - items.length),
        cursor,
        filter: { in: { property: ["node", "space"], values: [space] } },
        sort: [{ property: ["node", "externalId"], direction: "ascending" }],
      })
    );

    for (const item of response.items) {
      const row = toSampledInstanceRow(item);
      if (row !== null) items.push(row);
      if (items.length >= maxItems) {
        capped = true;
        break;
      }
    }

    if (capped) break;
    cursor = response.nextCursor;
  } while (cursor !== undefined);

  return { items, capped };
}

export async function sampleNodesInSpace(
  sdk: CogniteClient,
  space: string,
  maxItems = INFIELD_DATA_SAMPLE_LIMIT
): Promise<{ items: SampledInstanceRow[]; capped: boolean }> {
  if (space.trim().length === 0) {
    return { items: [], capped: false };
  }

  try {
    return await sampleNodesInSpaceViaQuery(sdk, space, maxItems);
  } catch {
    return sampleNodesInSpaceViaList(sdk, space, maxItems);
  }
}

export async function retrieveSampledNodeDetails(
  sdk: CogniteClient,
  node: Pick<SampledInstanceRow, "space" | "externalId" | "instanceType" | "viewSource" | "properties">
): Promise<Record<string, unknown>> {
  if (node.properties !== undefined && Object.keys(node.properties).length > 0) {
    return formatSampledNodeDetails(
      {
        space: node.space,
        externalId: node.externalId,
        instanceType: node.instanceType,
        properties: node.properties,
      },
      node.viewSource
    );
  }

  const retrieveBody: {
    items: Array<{ space: string; externalId: string; instanceType: string }>;
    sources?: Array<{ source: ViewSource }>;
  } = {
    items: [
      {
        space: node.space,
        externalId: node.externalId,
        instanceType: node.instanceType,
      },
    ],
  };

  if (node.viewSource !== undefined) {
    retrieveBody.sources = [{ source: node.viewSource }];
  }

  const response = await withTransientRetries(() => cachedInstancesByIds(sdk, retrieveBody));

  const item = response.items?.[0];
  if (!hasRecordShape(item)) return {};
  return formatSampledNodeDetails(item, node.viewSource);
}

function findMatchingLegacyLocation(
  location: LocationConfigNode,
  legacyLocations: LegacyConfigLocation[]
): LegacyConfigLocation | null {
  const reference = getDataStorageReference(location);
  const rootLocationExternalId = reference.rootLocationExternalId ?? "";
  const locationExternalId = location.externalId;
  const locationName = getLocationName(location).toLowerCase();

  const exactMatch = legacyLocations.find(
    (legacy) =>
      legacy.assetExternalId === rootLocationExternalId ||
      legacy.externalId === locationExternalId ||
      legacy.assetExternalId === locationExternalId
  );
  if (exactMatch !== undefined) return exactMatch;

  const fuzzyMatch = legacyLocations.find((legacy) => {
    const assetId = legacy.assetExternalId.toLowerCase();
    return (
      (rootLocationExternalId.length > 0 && assetId.includes(rootLocationExternalId.toLowerCase())) ||
      (locationName.length > 0 && assetId.includes(locationName)) ||
      assetId.includes(locationExternalId.toLowerCase())
    );
  });
  return fuzzyMatch ?? null;
}

function resolveLegacyApmaInstanceSpace(legacy: LegacyConfigLocation): string | null {
  return resolveLegacyInstanceSpace(legacy);
}

export function getLegacyApmaInstanceSpace(location: LegacyConfigLocation): string | null {
  return resolveLegacyApmaInstanceSpace(location);
}

export function getLegacyApmaInstanceSpaceForLocation(
  location: LocationConfigNode,
  legacyLocations: LegacyConfigLocation[]
): string | null {
  const legacy = findMatchingLegacyLocation(location, legacyLocations);
  if (legacy === null) return null;
  return resolveLegacyApmaInstanceSpace(legacy);
}

export function getAllLegacyLocations(legacyConfigData: LegacyConfigData[]): LegacyConfigLocation[] {
  const byRowId = new Map<string, LegacyConfigLocation>();
  for (const config of legacyConfigData) {
    for (const location of config.locations) {
      byRowId.set(location.rowId, location);
    }
  }
  return [...byRowId.values()];
}

async function sampleViewInSpace(
  sdk: CogniteClient,
  instanceSpace: string,
  view: ViewSource,
  sampleCap = LEGACY_VIEW_SAMPLE_CAP
): Promise<Pick<LegacyViewSpaceCheckResult, "count" | "capped" | "previewRows" | "errorMessage">> {
  let cursor: string | undefined;
  let count = 0;
  let capped = false;
  const previewRows: SampledInstanceRow[] = [];

  try {
    do {
      const response = await withTransientRetries(() =>
        cachedInstancesList(sdk, {
          instanceType: "node",
          sources: [{ source: view }],
          limit: 1000,
          cursor,
          filter: { in: { property: ["node", "space"], values: [instanceSpace] } },
          sort: [{ property: ["node", "externalId"], direction: "ascending" }],
        })
      );

      for (const item of response.items) {
        if (previewRows.length < LEGACY_PREVIEW_ROW_LIMIT) {
          const row = toSampledInstanceRow(item, { includeProperties: true, viewSource: view });
          if (row !== null) previewRows.push(row);
        }
        count += 1;
        if (count >= sampleCap) {
          count = sampleCap;
          capped = true;
          break;
        }
      }

      if (capped) break;
      cursor = response.nextCursor;
    } while (cursor !== undefined);

    return { count, capped, previewRows, errorMessage: null };
  } catch (error) {
    return {
      count: null,
      capped: false,
      previewRows: [],
      errorMessage: error instanceof Error ? error.message : "Failed to sample view data.",
    };
  }
}

function viewSourceKey(view: ViewSource): string {
  return `${view.space}/${view.externalId}/${view.version}`;
}

async function buildViewDataQualityReport(
  sdk: CogniteClient,
  instanceSpaces: string[],
  views: ViewSource[],
  options?: {
    onProgress?: (progress: LegacyDataQualityProgress) => void;
    onResult?: (result: LegacyViewSpaceCheckResult) => void;
  }
): Promise<LegacyDataQualityReport> {
  const sampleCache = new Map<
    string,
    Promise<Pick<LegacyViewSpaceCheckResult, "count" | "capped" | "previewRows" | "errorMessage">>
  >();

  function getCachedSample(instanceSpace: string, view: ViewSource) {
    const cacheKey = `${instanceSpace}:${viewSourceKey(view)}`;
    const cached = sampleCache.get(cacheKey);
    if (cached !== undefined) return cached;

    const promise = sampleViewInSpace(sdk, instanceSpace, view);
    sampleCache.set(cacheKey, promise);
    return promise;
  }

  const tasks = views.flatMap((view) => instanceSpaces.map((instanceSpace) => ({ view, instanceSpace })));
  const results: LegacyViewSpaceCheckResult[] = [];

  for (let index = 0; index < tasks.length; index += 1) {
    const { view, instanceSpace } = tasks[index];
    const viewKey = `${view.externalId}/${view.version}`;
    options?.onProgress?.({ current: index, total: tasks.length, viewKey, instanceSpace });

    const sample = await getCachedSample(instanceSpace, view);
    const result: LegacyViewSpaceCheckResult = {
      viewKey,
      view,
      instanceSpace,
      ...sample,
    };
    results.push(result);
    options?.onResult?.(result);
    options?.onProgress?.({ current: index + 1, total: tasks.length, viewKey, instanceSpace });
  }

  return { instanceSpaces, results };
}

export async function buildInfieldCdmDataQualityReport(
  sdk: CogniteClient,
  instanceSpaces: string[],
  options?: {
    onProgress?: (progress: LegacyDataQualityProgress) => void;
    onResult?: (result: LegacyViewSpaceCheckResult) => void;
  }
): Promise<LegacyDataQualityReport> {
  const views = getInfieldCdmViewSources();
  return buildViewDataQualityReport(sdk, instanceSpaces, views, options);
}

export async function buildInfieldViewMappingQualityReport(
  sdk: CogniteClient,
  location: LocationConfigNode,
  options?: {
    onProgress?: (progress: LegacyDataQualityProgress) => void;
    onResult?: (result: LegacyViewSpaceCheckResult) => void;
  }
): Promise<LegacyDataQualityReport> {
  const tasks = buildViewMappingSampleTasks(location);
  const instanceSpaces = [...new Set(tasks.map((task) => task.instanceSpace))];
  const sampleCache = new Map<
    string,
    Promise<Pick<LegacyViewSpaceCheckResult, "count" | "capped" | "previewRows" | "errorMessage">>
  >();

  function getCachedSample(instanceSpace: string, view: ViewSource) {
    const cacheKey = `${instanceSpace}:${viewSourceKey(view)}`;
    const cached = sampleCache.get(cacheKey);
    if (cached !== undefined) return cached;

    const promise = sampleViewInSpace(sdk, instanceSpace, view);
    sampleCache.set(cacheKey, promise);
    return promise;
  }

  const results: LegacyViewSpaceCheckResult[] = [];

  for (let index = 0; index < tasks.length; index += 1) {
    const task = tasks[index];
    const viewKey = `${task.mappingKey}/${task.view.externalId}/${task.view.version}`;
    options?.onProgress?.({
      current: index,
      total: tasks.length,
      viewKey,
      instanceSpace: task.instanceSpace,
    });

    const sample = await getCachedSample(task.instanceSpace, task.view);
    const result: LegacyViewSpaceCheckResult = {
      viewKey,
      view: task.view,
      instanceSpace: task.instanceSpace,
      mappingKey: task.mappingKey,
      mappingVariant: task.mappingVariant,
      defaultView: task.mappingVariant === "configured" ? task.defaultView : undefined,
      ...sample,
    };
    results.push(result);
    options?.onResult?.(result);
    options?.onProgress?.({
      current: index + 1,
      total: tasks.length,
      viewKey,
      instanceSpace: task.instanceSpace,
    });
  }

  return { instanceSpaces, results };
}

export async function buildLegacyDataQualityReport(
  sdk: CogniteClient,
  instanceSpaces: string[],
  options?: {
    onProgress?: (progress: LegacyDataQualityProgress) => void;
    onResult?: (result: LegacyViewSpaceCheckResult) => void;
  }
): Promise<LegacyDataQualityReport> {
  const views: ViewSource[] = LEGACY_APM_VIEWS.map((legacyView) => ({
    type: "view" as const,
    space: LEGACY_APM_SCHEMA_SPACE,
    externalId: legacyView.externalId,
    version: legacyView.version,
  }));
  return buildViewDataQualityReport(sdk, instanceSpaces, views, options);
}

export function getInfieldDataLocationOptions(
  locations: LocationConfigNode[],
  legacyLocations: LegacyConfigLocation[] = []
): InfieldDataLocationOption[] {
  const options: InfieldDataLocationOption[] = [];

  for (const location of locations) {
    const reference = getDataStorageReference(location);
    if (reference.appInstanceSpace === null || reference.appInstanceSpace.length === 0) continue;

    options.push({
      locationExternalId: location.externalId,
      locationName: getLocationName(location),
      appInstanceSpace: reference.appInstanceSpace,
      legacyApmaInstanceSpace: getLegacyApmaInstanceSpaceForLocation(location, legacyLocations),
      location,
    });
  }

  return options.sort((a, b) => a.locationName.localeCompare(b.locationName));
}

export function findInfieldLocationForLegacy(
  legacy: LegacyConfigLocation,
  infieldLocations: LocationConfigNode[]
): LocationConfigNode | null {
  for (const location of infieldLocations) {
    if (findMatchingLegacyLocation(location, [legacy]) !== null) {
      return location;
    }
  }
  return null;
}

export function getInfield2DataLocationOptions(
  legacyLocations: LegacyConfigLocation[],
  infieldLocations: LocationConfigNode[] = []
): InfieldDataLocationOption[] {
  const options: InfieldDataLocationOption[] = [];

  for (const legacy of legacyLocations) {
    const instanceSpace = getLegacyApmaInstanceSpace(legacy);
    if (instanceSpace === null || instanceSpace.length === 0) continue;

    const matchedLocation = findInfieldLocationForLegacy(legacy, infieldLocations);

    options.push({
      locationExternalId: legacy.rowId,
      locationName: legacy.externalId || legacy.assetExternalId || legacy.rowId,
      appInstanceSpace: instanceSpace,
      legacyApmaInstanceSpace: instanceSpace,
      location: matchedLocation ?? undefined,
    });
  }

  return options.sort((a, b) => a.locationName.localeCompare(b.locationName));
}

function normalizeMappingKey(mappingKey: string): string {
  const canonical = mappingKey.toLowerCase().replace(/[^a-z0-9]/g, "");

  if (canonical === "file" || canonical === "files") return "file";
  if (canonical === "asset" || canonical === "assets") return "asset";
  if (canonical === "timeseries") return "timeseries";
  if (canonical === "operation" || canonical === "operations") return "operation";
  if (canonical === "notification" || canonical === "notifications") return "notification";
  if (canonical === "maintenanceorder" || canonical === "maintenanceorders") return "maintenanceOrder";
  if (canonical === "observation" || canonical === "observations") return "observation";

  return mappingKey;
}

function parseViewSourceFromMappingValue(value: unknown): ViewSource | null {
  if (!hasRecordShape(value)) return null;
  const space = value.space;
  const externalId = value.externalId;
  const version = value.version;
  if (typeof space !== "string" || typeof externalId !== "string" || typeof version !== "string") {
    return null;
  }
  return { type: "view", space, externalId, version };
}

export function getInstanceSpacesForViewMapping(
  location: LocationConfigNode,
  mappingKey: string
): string[] {
  const dataFilters = location.properties.dataFilters;
  const filterKey = MAPPING_TO_DATA_FILTER_KEY[mappingKey] ?? mappingKey;
  const reference = getDataStorageReference(location);

  if (hasRecordShape(dataFilters)) {
    const targetFilter = dataFilters[filterKey];
    if (hasRecordShape(targetFilter)) {
      const instanceSpaces = targetFilter.instanceSpaces;
      if (Array.isArray(instanceSpaces)) {
        const spaces = instanceSpaces.filter(
          (value): value is string => typeof value === "string" && value.length > 0
        );
        if (spaces.length > 0) return spaces;
      }
    }
  }

  if (reference.appInstanceSpace !== null && reference.appInstanceSpace.length > 0) {
    return [reference.appInstanceSpace];
  }

  return [location.space];
}

export function buildViewMappingSampleTasks(location: LocationConfigNode): ViewMappingSampleTask[] {
  const tasks: ViewMappingSampleTask[] = [];
  const seen = new Set<string>();

  function addTask(
    mappingKey: string,
    view: ViewSource,
    instanceSpace: string,
    mappingVariant: "configured" | "default",
    defaultView?: ViewSource
  ) {
    const key = `${mappingKey}|${mappingVariant}|${viewSourceKey(view)}|${instanceSpace}`;
    if (seen.has(key)) return;
    seen.add(key);
    tasks.push({ mappingKey, view, instanceSpace, mappingVariant, defaultView });
  }

  for (const { mappingKey, view } of getViewMappings(location)) {
    const defaultView = DEFAULT_VIEW_MAPPINGS[mappingKey];
    const spaces = getInstanceSpacesForViewMapping(location, mappingKey);
    for (const instanceSpace of spaces) {
      addTask(mappingKey, view, instanceSpace, "configured", defaultView);
      if (defaultView !== undefined && !isDefaultViewMapping(mappingKey, view)) {
        addTask(mappingKey, defaultView, instanceSpace, "default", defaultView);
      }
    }
  }

  return tasks.sort((a, b) => {
    const aOrder = VIEW_MAPPING_SAMPLE_ORDER[a.mappingKey] ?? 99;
    const bOrder = VIEW_MAPPING_SAMPLE_ORDER[b.mappingKey] ?? 99;
    if (aOrder !== bOrder) return aOrder - bOrder;
    if (a.mappingVariant !== b.mappingVariant) {
      return a.mappingVariant === "configured" ? -1 : 1;
    }
    const viewCompare = viewSourceKey(a.view).localeCompare(viewSourceKey(b.view));
    if (viewCompare !== 0) return viewCompare;
    return a.instanceSpace.localeCompare(b.instanceSpace);
  });
}

export function getLocationName(location: LocationConfigNode): string {
  const name = location.properties.name;
  if (typeof name === "string" && name.length > 0) return name;
  return location.externalId;
}

export function getLocationConfigNodeKey(node: Pick<LocationConfigNode, "space" | "externalId">): string {
  return `${node.space}:${node.externalId}`;
}

export function getLocationDescription(location: LocationConfigNode): string {
  const description = location.properties.description;
  if (typeof description === "string" && description.length > 0) return description;

  const summary = location.properties.summary;
  if (typeof summary === "string" && summary.length > 0) return summary;

  return "—";
}

export function formatTimestamp(timestamp: number | undefined): string {
  if (timestamp === undefined) return "—";
  return new Date(timestamp).toISOString();
}

export type InfieldCdmAssetInstanceSpacesConfig = {
  spaces: string[];
  error: string | null;
};

export function resolveInfieldCdmAssetInstanceSpaces(
  location: LocationConfigNode | undefined
): InfieldCdmAssetInstanceSpacesConfig {
  if (location === undefined) {
    return {
      spaces: [],
      error: "No matched Infield CDM location config for this legacy site.",
    };
  }

  const locationRef = `${location.space}/${location.externalId}`;
  const dataFilters = location.properties.dataFilters;
  if (!hasRecordShape(dataFilters)) {
    return {
      spaces: [],
      error: `Infield CDM location ${locationRef}: dataFilters missing from config.`,
    };
  }

  const assetsFilter = dataFilters.assets;
  if (!hasRecordShape(assetsFilter)) {
    return {
      spaces: [],
      error: `Infield CDM location ${locationRef}: dataFilters.assets missing from config.`,
    };
  }

  const instanceSpaces = assetsFilter.instanceSpaces;
  if (!Array.isArray(instanceSpaces)) {
    return {
      spaces: [],
      error: `Infield CDM location ${locationRef}: dataFilters.assets.instanceSpaces missing from config.`,
    };
  }

  const spaces = instanceSpaces.filter(
    (value): value is string => typeof value === "string" && value.length > 0
  );
  if (spaces.length === 0) {
    return {
      spaces: [],
      error: `Infield CDM location ${locationRef}: dataFilters.assets.instanceSpaces is empty.`,
    };
  }

  return { spaces, error: null };
}

export function getAssetInstanceSpaces(location: LocationConfigNode): string[] {
  return resolveInfieldCdmAssetInstanceSpaces(location).spaces;
}

function getAppInstanceSpaces(location: LocationConfigNode): string[] {
  const reference = getDataStorageReference(location);
  if (reference.appInstanceSpace === null || reference.appInstanceSpace.length === 0) return [];
  return [reference.appInstanceSpace];
}

export const DATA_FILTER_KEYS = [
  "files",
  "assets",
  "operations",
  "timeseries",
  "notifications",
  "maintenanceOrders",
  "observations",
] as const;

export const MAPPING_TO_DATA_FILTER_KEY: Record<string, string> = {
  file: "files",
  asset: "assets",
  operation: "operations",
  notification: "notifications",
  maintenanceOrder: "maintenanceOrders",
  timeseries: "timeseries",
  observation: "observations",
};

// These types are not stored in the app instance space; their instances live in the configured
// dataFilters.instanceSpaces. Probe those configured spaces only (not appInstanceSpace).
export const CONFIGURED_INSTANCE_SPACE_MAPPING_KEYS = new Set([
  "asset",
  "operation",
  "notification",
  "maintenanceOrder",
]);

function getDataFilterInstanceSpaceEntries(
  location: LocationConfigNode
): Array<{ space: string; filterKeys: string[] }> {
  const dataFilters = location.properties.dataFilters;
  if (!hasRecordShape(dataFilters)) return [];

  const spaceToKeys = new Map<string, string[]>();
  for (const filterKey of DATA_FILTER_KEYS) {
    const targetFilter = dataFilters[filterKey];
    if (!hasRecordShape(targetFilter)) continue;

    const instanceSpaces = targetFilter.instanceSpaces;
    if (!Array.isArray(instanceSpaces)) continue;

    for (const space of instanceSpaces) {
      if (typeof space !== "string" || space.length === 0) continue;
      const keys = spaceToKeys.get(space) ?? [];
      keys.push(filterKey);
      spaceToKeys.set(space, keys);
    }
  }

  return [...spaceToKeys.entries()]
    .map(([space, filterKeys]) => ({ space, filterKeys: [...new Set(filterKeys)] }))
    .sort((a, b) => a.space.localeCompare(b.space));
}

function getSpacesForMapping(location: LocationConfigNode, mappingKey: string): string[] {
  const dataFilters = location.properties.dataFilters;
  if (!hasRecordShape(dataFilters)) return [location.space];

  const filterKey = MAPPING_TO_DATA_FILTER_KEY[mappingKey] ?? mappingKey;
  const targetFilter = dataFilters[filterKey];
  if (!hasRecordShape(targetFilter)) return [location.space];

  const instanceSpaces = targetFilter.instanceSpaces;
  if (!Array.isArray(instanceSpaces)) return [location.space];

  const spaces = instanceSpaces.filter((value): value is string => typeof value === "string" && value.length > 0);
  return spaces.length > 0 ? spaces : [location.space];
}

function hasTimeseriesDataFilter(location: LocationConfigNode): boolean {
  const dataFilters = location.properties.dataFilters;
  if (!hasRecordShape(dataFilters)) return false;

  const timeseriesFilter = dataFilters.timeseries;
  if (!hasRecordShape(timeseriesFilter)) return false;

  const instanceSpaces = timeseriesFilter.instanceSpaces;
  if (!Array.isArray(instanceSpaces)) return false;

  return instanceSpaces.some((space) => typeof space === "string" && space.length > 0);
}

function getViewMappings(location: LocationConfigNode): Array<{ mappingKey: string; view: ViewSource }> {
  const viewMappings = location.properties.viewMappings;
  const mappings: Array<{ mappingKey: string; view: ViewSource }> = [];

  if (hasRecordShape(viewMappings)) {
    for (const [mappingKey, mappingValue] of Object.entries(viewMappings)) {
      const normalizedKey = normalizeMappingKey(mappingKey);
      if (Array.isArray(mappingValue)) {
        for (const item of mappingValue) {
          const view = parseViewSourceFromMappingValue(item);
          if (view !== null) mappings.push({ mappingKey: normalizedKey, view });
        }
        continue;
      }

      const view = parseViewSourceFromMappingValue(mappingValue);
      if (view !== null) mappings.push({ mappingKey: normalizedKey, view });
    }
  }

  const hasTimeseriesMapping = mappings.some((mapping) => mapping.mappingKey === "timeseries");
  if (!hasTimeseriesMapping && hasTimeseriesDataFilter(location)) {
    mappings.push({
      mappingKey: "timeseries",
      view: INFIELD_TIMESERIES_VIEW,
    });
  }

  return mappings;
}

export function getViewMappingsForLocation(node: LocationConfigNode | null): ViewMappingReference[] {
  if (node === null) return [];

  const mappings: ViewMappingReference[] = [];
  for (const { mappingKey, view } of getViewMappings(node)) {
    mappings.push({
      key: mappingKey,
      space: view.space,
      externalId: view.externalId,
      version: view.version,
    });
  }

  return mappings;
}

export function getDataStorageReference(node: LocationConfigNode | null): DataStorageReference {
  if (node === null) {
    return { rootLocationSpace: null, rootLocationExternalId: null, appInstanceSpace: null };
  }

  const dataStorage = node.properties.dataStorage;
  if (!hasRecordShape(dataStorage)) {
    return { rootLocationSpace: null, rootLocationExternalId: null, appInstanceSpace: null };
  }

  const appInstanceSpace = typeof dataStorage.appInstanceSpace === "string" ? dataStorage.appInstanceSpace : null;
  const rootLocation = dataStorage.rootLocation;
  if (!hasRecordShape(rootLocation)) {
    return { rootLocationSpace: null, rootLocationExternalId: null, appInstanceSpace };
  }

  return {
    rootLocationSpace: typeof rootLocation.space === "string" ? rootLocation.space : null,
    rootLocationExternalId: typeof rootLocation.externalId === "string" ? rootLocation.externalId : null,
    appInstanceSpace,
  };
}

function formatViewSource(view: ViewSource): string {
  return `${view.space}/${view.externalId}:${view.version}`;
}

export function formatViewReferenceLabel(view: ViewSource): string {
  return formatViewSource(view);
}

export async function fetchViewDefinition(sdk: CogniteClient, view: ViewSource): Promise<unknown> {
  const response = (await cachedViewsRetrieve(
    sdk,
    [
      {
        space: view.space,
        externalId: view.externalId,
        version: view.version,
      },
    ],
    { includeInheritedProperties: true }
  )) as { items?: unknown[] };

  const item = response.items?.[0];
  if (item === undefined) {
    throw new Error(`View not found: ${formatViewReferenceLabel(view)}`);
  }

  return item;
}

async function countAllNodesForSpace(
  sdk: CogniteClient,
  space: string,
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<{ count: number; capped: boolean }> {
  let cursor: string | undefined;
  let count = 0;
  let capped = false;

  do {
    const response = await withTransientRetries(() =>
      cachedInstancesList(sdk, {
        instanceType: "node",
        limit: 1000,
        cursor,
        filter: { in: { property: ["node", "space"], values: [space] } },
        sort: [{ property: ["node", "externalId"], direction: "ascending" }],
      })
    );

    count += response.items.length;
    const cappedResult = capSampleCount(count, sampleCap);
    count = cappedResult.count;
    if (cappedResult.capped) {
      capped = true;
      break;
    }

    cursor = response.nextCursor;
  } while (cursor !== undefined);

  return { count, capped };
}

async function countNodesForViewInSpace(
  sdk: CogniteClient,
  space: string,
  view: ViewSource,
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<{ count: number; capped: boolean }> {
  let cursor: string | undefined;
  let count = 0;
  let capped = false;

  do {
    const response = await withTransientRetries(() =>
      cachedInstancesList(sdk, {
        instanceType: "node",
        sources: [{ source: view }],
        limit: 1000,
        cursor,
        filter: { in: { property: ["node", "space"], values: [space] } },
        sort: [{ property: ["node", "externalId"], direction: "ascending" }],
      })
    );

    count += response.items.length;
    const cappedResult = capSampleCount(count, sampleCap);
    count = cappedResult.count;
    if (cappedResult.capped) {
      capped = true;
      break;
    }

    cursor = response.nextCursor;
  } while (cursor !== undefined);

  return { count, capped };
}

function toSpaceBreakdown(
  spaces: string[],
  counts: Array<{ count: number; capped: boolean }>
): SpaceCountBreakdown[] {
  return spaces.map((space, index) => ({
    space,
    count: counts[index]?.count ?? 0,
    capped: counts[index]?.capped ?? false,
  }));
}

type AggregateCountResponse = {
  items?: Array<{ aggregates?: Array<{ value?: number }> }>;
};

async function aggregateCountAllNodesInSpace(_sdk: CogniteClient, _space: string): Promise<number | null> {
  // CDF instances/aggregate requires a view; all-node counts use list sampling instead.
  return null;
}

async function aggregateCountViewInSpace(
  sdk: CogniteClient,
  space: string,
  view: ViewSource
): Promise<number | null> {
  if (view.space.length === 0 || view.externalId.length === 0 || view.version.length === 0) {
    return null;
  }

  try {
    const response = (await withTransientRetries(() =>
      (sdk.instances.aggregate as (params: unknown) => Promise<unknown>)({
        instanceType: "node",
        view: {
          type: "view",
          space: view.space,
          externalId: view.externalId,
          version: view.version,
        },
        aggregates: [{ count: { property: "externalId" } }],
        filter: { in: { property: ["node", "space"], values: [space] } },
      })
    )) as AggregateCountResponse;
    return response.items?.[0]?.aggregates?.[0]?.value ?? 0;
  } catch {
    return null;
  }
}

function toCellLoadError(error: unknown): string {
  return error instanceof Error ? error.message : "Request failed.";
}

export async function fetchAccurateSpaceBreakdown(
  sdk: CogniteClient,
  spaces: string[],
  view: ViewSource | null,
  sampledBreakdown: SpaceCountBreakdown[]
): Promise<SpaceCountBreakdown[]> {
  const results = await Promise.all(
    spaces.map(async (space) => {
      const sampled = sampledBreakdown.find((entry) => entry.space === space);
      const accurateCount =
        view === null
          ? await aggregateCountAllNodesInSpace(sdk, space)
          : await aggregateCountViewInSpace(sdk, space, view);

      if (accurateCount !== null) {
        return { space, count: accurateCount, capped: false, accurate: true };
      }

      return {
        space,
        count: sampled?.count ?? 0,
        capped: sampled?.capped ?? false,
        accurate: false,
      };
    })
  );

  return results;
}

async function countRootAnchorNode(
  sdk: CogniteClient,
  rootLocationSpace: string,
  rootLocationExternalId: string,
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<{ count: number; capped: boolean }> {
  let cursor: string | undefined;
  let count = 0;
  let capped = false;

  do {
    const response = await withTransientRetries(() =>
      cachedInstancesList(sdk, {
        instanceType: "node",
        limit: 1000,
        cursor,
        filter: {
          and: [
            { in: { property: ["node", "space"], values: [rootLocationSpace] } },
            { equals: { property: ["node", "externalId"], value: rootLocationExternalId } },
          ],
        },
        sort: [{ property: ["node", "externalId"], direction: "ascending" }],
      })
    );

    count += response.items.length;
    const cappedResult = capSampleCount(count, sampleCap);
    count = cappedResult.count;
    if (cappedResult.capped) {
      capped = true;
      break;
    }

    cursor = response.nextCursor;
  } while (cursor !== undefined);

  return { count, capped };
}

export async function getDataStorageCounts(
  sdk: CogniteClient,
  reference: DataStorageReference,
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<DataStorageCounts> {
  const rootLocationSpaceCountPromise =
    reference.rootLocationSpace !== null
      ? countAllNodesForSpace(sdk, reference.rootLocationSpace, sampleCap)
      : Promise.resolve(null);
  const appInstanceSpaceCountPromise =
    reference.appInstanceSpace !== null
      ? countAllNodesForSpace(sdk, reference.appInstanceSpace, sampleCap)
      : Promise.resolve(null);
  const rootAnchorCountPromise =
    reference.rootLocationSpace !== null && reference.rootLocationExternalId !== null
      ? countRootAnchorNode(
          sdk,
          reference.rootLocationSpace,
          reference.rootLocationExternalId,
          sampleCap
        )
      : Promise.resolve(null);

  const [rootLocationSpaceCount, appInstanceSpaceCount, rootAnchorCount] = await Promise.all([
    rootLocationSpaceCountPromise,
    appInstanceSpaceCountPromise,
    rootAnchorCountPromise,
  ]);

  return {
    rootLocationSpaceNodeCount: rootLocationSpaceCount?.count ?? null,
    rootLocationSpaceNodeCountCapped: rootLocationSpaceCount?.capped ?? false,
    appInstanceSpaceNodeCount: appInstanceSpaceCount?.count ?? null,
    appInstanceSpaceNodeCountCapped: appInstanceSpaceCount?.capped ?? false,
    rootAnchorNodeCount: rootAnchorCount?.count ?? null,
    rootAnchorNodeCountCapped: rootAnchorCount?.capped ?? false,
  };
}

export async function resolveViewMappingsExistence(
  sdk: CogniteClient,
  mappings: ViewMappingReference[]
): Promise<ViewExistenceResult[]> {
  if (mappings.length === 0) return [];

  const requestedViews = mappings.map((mapping) => ({
    space: mapping.space,
    externalId: mapping.externalId,
    version: mapping.version,
  }));

  const response = (await cachedViewsRetrieve(sdk, requestedViews)) as {
    items?: Array<Record<string, unknown>>;
  };
  const items = response.items ?? [];

  const existingSet = new Set(
    items
      .map((item) => {
        const space = item.space;
        const externalId = item.externalId;
        const version = item.version;
        if (typeof space !== "string" || typeof externalId !== "string" || typeof version !== "string") {
          return null;
        }
        return `${space}:${externalId}:${version}`;
      })
      .filter((value): value is string => value !== null)
  );

  const responseByKey = new Map<string, unknown>();
  for (const item of items) {
    const space = item.space;
    const externalId = item.externalId;
    const version = item.version;
    if (typeof space !== "string" || typeof externalId !== "string" || typeof version !== "string") continue;
    responseByKey.set(`${space}:${externalId}:${version}`, item);
  }

  return mappings.map((mapping) => {
    const viewKey = `${mapping.space}:${mapping.externalId}:${mapping.version}`;
    return {
      reference: mapping,
      exists: existingSet.has(viewKey),
      view: responseByKey.get(viewKey) ?? null,
    };
  });
}

export function collectConfiguredSpacesFromLocation(location: LocationConfigNode): string[] {
  const reference = getDataStorageReference(location);
  const spaces: string[] = [];
  if (reference.rootLocationSpace !== null && reference.rootLocationSpace.length > 0) {
    spaces.push(reference.rootLocationSpace);
  }
  if (reference.appInstanceSpace !== null && reference.appInstanceSpace.length > 0) {
    spaces.push(reference.appInstanceSpace);
  }
  for (const entry of getDataFilterInstanceSpaceEntries(location)) {
    spaces.push(entry.space);
  }
  return [...new Set(spaces)];
}

export function collectConfiguredSpacesFromLocations(locations: LocationConfigNode[]): string[] {
  const spaces = new Set<string>();
  for (const location of locations) {
    for (const space of collectConfiguredSpacesFromLocation(location)) {
      spaces.add(space);
    }
  }
  return [...spaces].sort((a, b) => a.localeCompare(b));
}

const REFERENCE_DATA_MAPPING_KEYS = ["operation", "notification", "maintenanceOrder"] as const;

function getInstanceSpacesForFilterKey(location: LocationConfigNode, filterKey: string): string[] {
  const dataFilters = location.properties.dataFilters;
  if (!hasRecordShape(dataFilters)) return [];
  const targetFilter = dataFilters[filterKey];
  if (!hasRecordShape(targetFilter)) return [];
  const instanceSpaces = targetFilter.instanceSpaces;
  if (!Array.isArray(instanceSpaces)) return [];
  return [...new Set(instanceSpaces.filter((value): value is string => typeof value === "string" && value.length > 0))];
}

type ReferenceDataSpaceInfo = {
  space: string | null;
  configuredKeys: string[];
  distinctSpaces: string[];
  mismatch: boolean;
};

function resolveReferenceDataSpace(location: LocationConfigNode): ReferenceDataSpaceInfo {
  const configuredKeys: string[] = [];
  const distinct = new Set<string>();
  let anyMultiSpace = false;

  for (const key of REFERENCE_DATA_MAPPING_KEYS) {
    const filterKey = MAPPING_TO_DATA_FILTER_KEY[key] ?? key;
    const spaces = getInstanceSpacesForFilterKey(location, filterKey);
    if (spaces.length === 0) continue;
    configuredKeys.push(key);
    if (spaces.length > 1) anyMultiSpace = true;
    for (const space of spaces) distinct.add(space);
  }

  const distinctSpaces = [...distinct];
  const anyConfigured = configuredKeys.length > 0;
  const mismatch =
    anyConfigured && (configuredKeys.length !== REFERENCE_DATA_MAPPING_KEYS.length || distinctSpaces.length !== 1 || anyMultiSpace);

  return {
    space: distinctSpaces.length === 1 ? distinctSpaces[0] : null,
    configuredKeys,
    distinctSpaces,
    mismatch,
  };
}

type SpaceUsage = { locationExternalId: string; locationName: string; role: "config" | "app" | "reference-data" };

function describeSpaceUsage(usage: SpaceUsage): string {
  return `${usage.role} space of "${usage.locationName}"`;
}

// Cross-location space-uniqueness checks (rules 1, 3, 4, 5). The externalId lookup (rule 2) is
// added separately via checkConfigExternalIdInOtherSpaces since it requires DMS calls.
export function buildLocationConfigValidations(locations: LocationConfigNode[]): LocationConfigValidation[] {
  const perLocation = locations.map((location) => {
    const reference = getDataStorageReference(location);
    const appInstanceSpace =
      reference.appInstanceSpace !== null && reference.appInstanceSpace.length > 0 ? reference.appInstanceSpace : null;
    return {
      location,
      locationName: getLocationName(location),
      configSpace: location.space,
      appInstanceSpace,
      referenceData: resolveReferenceDataSpace(location),
    };
  });

  const usagesBySpace = new Map<string, SpaceUsage[]>();
  const addUsage = (space: string | null, usage: SpaceUsage) => {
    if (space === null || space.length === 0) return;
    const list = usagesBySpace.get(space) ?? [];
    list.push(usage);
    usagesBySpace.set(space, list);
  };
  for (const entry of perLocation) {
    const base = { locationExternalId: entry.location.externalId, locationName: entry.locationName };
    addUsage(entry.configSpace, { ...base, role: "config" });
    addUsage(entry.appInstanceSpace, { ...base, role: "app" });
    addUsage(entry.referenceData.space, { ...base, role: "reference-data" });
  }

  const collisionsFor = (
    space: string | null,
    self: { locationExternalId: string; role: SpaceUsage["role"] },
    predicate: (usage: SpaceUsage) => boolean
  ): SpaceUsage[] => {
    if (space === null) return [];
    return (usagesBySpace.get(space) ?? []).filter(
      (usage) => !(usage.locationExternalId === self.locationExternalId && usage.role === self.role) && predicate(usage)
    );
  };

  return perLocation.map((entry) => {
    const errors: string[] = [];
    const selfId = entry.location.externalId;

    // Rule 1: config space must be unique across all locations and roles.
    const configCollisions = collisionsFor(entry.configSpace, { locationExternalId: selfId, role: "config" }, () => true);
    if (configCollisions.length > 0) {
      errors.push(
        `Config space "${entry.configSpace}" must be unique, but is also the ${configCollisions
          .map(describeSpaceUsage)
          .join(", ")}.`
      );
    }

    // Rule 3: operation/notification/maintenanceOrder must all share one instance space.
    if (entry.referenceData.mismatch) {
      const detail =
        entry.referenceData.distinctSpaces.length > 0
          ? ` (found: ${entry.referenceData.distinctSpaces.join(", ")})`
          : "";
      errors.push(
        `operation, notification and maintenanceOrder must all be configured with the same single instance space${detail}.`
      );
    }
    // Rule 3: that reference-data space must be unique across all locations and roles (view spaces
    // excluded since only instance/config spaces are indexed).
    if (entry.referenceData.space !== null) {
      const refCollisions = collisionsFor(
        entry.referenceData.space,
        { locationExternalId: selfId, role: "reference-data" },
        () => true
      );
      if (refCollisions.length > 0) {
        errors.push(
          `Reference-data space "${entry.referenceData.space}" must be unique, but is also the ${refCollisions
            .map(describeSpaceUsage)
            .join(", ")}.`
        );
      }
    }

    // Rules 4 & 5: appInstanceSpace must exist, be unique per location, and differ from config/reference-data.
    if (entry.appInstanceSpace === null) {
      errors.push("appInstanceSpace is not configured in dataStorage.");
    } else {
      const appCollisions = collisionsFor(entry.appInstanceSpace, { locationExternalId: selfId, role: "app" }, () => true);
      if (appCollisions.length > 0) {
        errors.push(
          `appInstanceSpace "${entry.appInstanceSpace}" must be unique per location and differ from config and reference-data spaces, but is also the ${appCollisions
            .map(describeSpaceUsage)
            .join(", ")}.`
        );
      }
    }

    return {
      locationExternalId: selfId,
      locationName: entry.locationName,
      configSpace: entry.configSpace,
      configExternalId: entry.location.externalId,
      appInstanceSpace: entry.appInstanceSpace,
      referenceDataSpace: entry.referenceData.space,
      errors,
      externalIdChecked: false,
    };
  });
}

// Rule 2: the config object externalId must only exist in its own config space. Uses the same
// externalId lookup as the Doc Lookup page.
export async function checkConfigExternalIdInOtherSpaces(
  sdk: CogniteClient,
  externalId: string,
  configSpace: string
): Promise<{ otherSpaces: string[]; truncated: boolean }> {
  const { nodes, truncated } = await searchNodesByExternalId(sdk, externalId);
  const otherSpaces = [
    ...new Set(nodes.map((node) => node.space).filter((space) => space !== configSpace)),
  ].sort((a, b) => a.localeCompare(b));
  return { otherSpaces, truncated };
}

export function formatConfiguredSpaceProbeStatus(status: ConfiguredSpaceUsageStatus): string {
  switch (status) {
    case "missing":
      return "Missing";
    case "empty":
      return "Empty";
    case "in_use":
      return "In use";
  }
}

export function formatSpaceProbeStatusDetail(
  metric: SpaceProbeMetric,
  view: ViewSource | null | undefined
): string {
  if (metric.statusDetail !== undefined && metric.statusDetail.length > 0) {
    return metric.statusDetail;
  }

  const viewLabel = view !== null && view !== undefined ? formatViewSource(view) : "the configured view";
  const space = metric.space;

  switch (metric.status) {
    case "missing":
      return `DMS space "${space}" was not found, or nodes for view ${viewLabel} could not be queried.`;
    case "empty":
      return `Space "${space}" exists but has no nodes of view ${viewLabel}.`;
    case "in_use":
      return metric.sampleExternalId !== null
        ? `Found node ${metric.sampleExternalId} for view ${viewLabel} in space "${space}".`
        : `Found at least one node for view ${viewLabel} in space "${space}".`;
  }
}

export function configuredSpaceProbeSortRank(status: ConfiguredSpaceUsageStatus): number {
  switch (status) {
    case "in_use":
      return 2;
    case "empty":
      return 1;
    case "missing":
      return 0;
  }
}

function probeSpaceViewCacheKey(space: string, view: ViewSource): string {
  return `${space}:${view.space}:${view.externalId}:${view.version}`;
}

function collectSpaceViewProbesFromLocation(
  location: LocationConfigNode
): Array<{ space: string; view: ViewSource }> {
  const pairs: Array<{ space: string; view: ViewSource }> = [];
  const appInstanceSpaces = getAppInstanceSpaces(location);
  const dataFilterSpaceEntries = getDataFilterInstanceSpaceEntries(location);

  for (const { mappingKey, view } of getViewMappings(location)) {
    const filterKey = MAPPING_TO_DATA_FILTER_KEY[mappingKey] ?? mappingKey;
    const dataFilterEntries = dataFilterSpaceEntries.filter((entry) => entry.filterKeys.includes(filterKey));
    const primarySpaces = CONFIGURED_INSTANCE_SPACE_MAPPING_KEYS.has(mappingKey)
      ? dataFilterEntries.map((entry) => entry.space)
      : appInstanceSpaces;

    for (const space of primarySpaces) {
      pairs.push({ space, view });
    }
    for (const entry of dataFilterEntries) {
      pairs.push({ space: entry.space, view });
    }
  }

  return pairs;
}

function collectSpaceViewProbesFromLocations(
  locations: LocationConfigNode[]
): Array<{ space: string; view: ViewSource }> {
  const seen = new Set<string>();
  const pairs: Array<{ space: string; view: ViewSource }> = [];

  for (const location of locations) {
    for (const pair of collectSpaceViewProbesFromLocation(location)) {
      const key = probeSpaceViewCacheKey(pair.space, pair.view);
      if (seen.has(key)) continue;
      seen.add(key);
      pairs.push(pair);
    }
  }

  return pairs;
}

async function batchProbeSpaceExistence(
  sdk: CogniteClient,
  spaces: string[]
): Promise<{
  existence: Map<string, boolean>;
  retrieveCalls: Map<string, SpaceProbeApiCall>;
}> {
  const uniqueSpaces = [...new Set(spaces.map((space) => space.trim()).filter((space) => space.length > 0))];
  const existence = new Map<string, boolean>();
  const retrieveCalls = new Map<string, SpaceProbeApiCall>();

  for (let index = 0; index < uniqueSpaces.length; index += 100) {
    const batch = uniqueSpaces.slice(index, index + 100);
    const response = await withTransientRetries(() => sdk.spaces.retrieve(batch));
    const responsePayload = { items: response.items ?? [] };
    const found = new Set((response.items ?? []).map((space) => space.space));
    const retrieveCall: SpaceProbeApiCall = {
      api: "POST /models/spaces/byids",
      request: batch,
      response: responsePayload,
    };
    for (const space of batch) {
      existence.set(space, found.has(space));
      retrieveCalls.set(space, retrieveCall);
    }
  }

  return { existence, retrieveCalls };
}

async function probeSpaceForView(
  sdk: CogniteClient,
  space: string,
  view: ViewSource,
  existence: Map<string, boolean>,
  retrieveCalls: Map<string, SpaceProbeApiCall>
): Promise<SpaceProbeMetric> {
  const apiCalls: SpaceProbeApiCall[] = [];
  const retrieveCall = retrieveCalls.get(space);
  if (retrieveCall !== undefined) apiCalls.push(retrieveCall);

  if (!existence.get(space)) {
    return {
      space,
      status: "missing",
      sampleExternalId: null,
      apiCalls,
      statusDetail: `DMS space "${space}" was not found.`,
    };
  }

  const viewLabel = formatViewSource(view);
  const listRequest = {
    instanceType: "node",
    sources: [{ source: view }],
    limit: 1,
    filter: { in: { property: ["node", "space"], values: [space] } },
    sort: [{ property: ["node", "externalId"], direction: "ascending" }],
  };

  try {
    const response = await withTransientRetries(() => cachedInstancesList(sdk, listRequest));
    apiCalls.push({
      api: "POST /models/instances/list",
      request: listRequest,
      response,
    });
    const first = response.items[0];
    if (first === undefined) {
      return {
        space,
        status: "empty",
        sampleExternalId: null,
        apiCalls,
        statusDetail: `Space "${space}" exists but has no nodes of view ${viewLabel}.`,
      };
    }
    const externalId = typeof first.externalId === "string" ? first.externalId : null;
    return {
      space,
      status: "in_use",
      sampleExternalId: externalId,
      apiCalls,
      statusDetail:
        externalId !== null
          ? `Found node ${externalId} for view ${viewLabel} in space "${space}".`
          : `Found at least one node for view ${viewLabel} in space "${space}".`,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to list nodes.";
    apiCalls.push({
      api: "POST /models/instances/list",
      request: listRequest,
      response: { error: message },
    });
    return {
      space,
      status: "missing",
      sampleExternalId: null,
      apiCalls,
      statusDetail: `Could not query view ${viewLabel} in space "${space}": ${message}`,
    };
  }
}

export async function buildLocationSpaceProbeResults(
  sdk: CogniteClient,
  locationConfigs: LocationConfigNode[],
  options?: {
    onProgress?: (progress: RelevantObjectCountProgress) => void;
    onResult?: (result: LocationSpaceProbeResult) => void;
  }
): Promise<{ results: LocationSpaceProbeResult[] }> {
  const configuredSpaces = collectConfiguredSpacesFromLocations(locationConfigs);
  const { existence, retrieveCalls } = await batchProbeSpaceExistence(sdk, configuredSpaces);
  const probesBySpaceView = new Map<string, SpaceProbeMetric>();
  const probePairs = collectSpaceViewProbesFromLocations(locationConfigs);
  let completedProbes = 0;

  if (probePairs.length > 0) {
    options?.onProgress?.({
      current: 0,
      total: probePairs.length,
      locationName: "Probing instance spaces",
    });
  }

  async function getCachedSpaceViewProbe(space: string, view: ViewSource): Promise<SpaceProbeMetric> {
    const cacheKey = probeSpaceViewCacheKey(space, view);
    const cached = probesBySpaceView.get(cacheKey);
    if (cached !== undefined) return cached;

    const metric = await probeSpaceForView(sdk, space, view, existence, retrieveCalls);
    probesBySpaceView.set(cacheKey, metric);
    completedProbes += 1;
    options?.onProgress?.({
      current: completedProbes,
      total: probePairs.length,
      locationName: `${space} · ${formatViewSource(view)}`,
    });
    return metric;
  }

  const results: LocationSpaceProbeResult[] = [];

  for (let index = 0; index < locationConfigs.length; index += 1) {
    const location = locationConfigs[index];
    const locationName = getLocationName(location);

    const appInstanceSpaces = getAppInstanceSpaces(location);
    const appInstanceSpace = appInstanceSpaces[0] ?? null;
    const dataFilterSpaceEntries = getDataFilterInstanceSpaceEntries(location);
    const dataFilterSpaces = dataFilterSpaceEntries.map((entry) => entry.space);
    const appInstanceSpaceNotInDataFilters =
      appInstanceSpace !== null && !dataFilterSpaces.includes(appInstanceSpace);

    const mappings = getViewMappings(location);
    const mappingMetrics: MappingSpaceProbeMetrics[] = [];

    const base = {
      locationExternalId: location.externalId,
      locationName,
      locationDescription: getLocationDescription(location),
      locationUpdated: formatTimestamp(location.lastUpdatedTime),
      location,
      appInstanceSpace,
      appInstanceSpaceNotInDataFilters,
    };

    for (const { mappingKey, view } of mappings) {
      const filterKey = MAPPING_TO_DATA_FILTER_KEY[mappingKey] ?? mappingKey;
      const dataFilterEntries = dataFilterSpaceEntries.filter((entry) => entry.filterKeys.includes(filterKey));
      const usesConfiguredSpaces = CONFIGURED_INSTANCE_SPACE_MAPPING_KEYS.has(mappingKey);

      // Ordered, deduplicated list of instance spaces for this column. For asset and reference-data
      // types, only dataFilters.instanceSpaces are probed; other mappings use appInstanceSpace plus
      // any additional configured filter spaces.
      const orderedSpaces: Array<{ space: string; filterKeys: string[]; isAppInstanceSpace: boolean }> = [];
      const seenSpaces = new Map<string, { filterKeys: string[]; isAppInstanceSpace: boolean }>();
      const addSpace = (space: string, filterKeys: string[], isAppInstanceSpace: boolean) => {
        const existing = seenSpaces.get(space);
        if (existing !== undefined) {
          existing.filterKeys = [...new Set([...existing.filterKeys, ...filterKeys])];
          return;
        }
        const entry = { space, filterKeys: [...filterKeys], isAppInstanceSpace };
        seenSpaces.set(space, entry);
        orderedSpaces.push(entry);
      };

      if (!usesConfiguredSpaces) {
        for (const space of appInstanceSpaces) addSpace(space, [], true);
      }
      for (const entry of dataFilterEntries) addSpace(entry.space, entry.filterKeys, false);

      const instanceSpaceMetrics: MappingInstanceSpaceProbeMetric[] = await Promise.all(
        orderedSpaces.map(async (entry) => ({
          ...(await getCachedSpaceViewProbe(entry.space, view)),
          filterKeys: entry.filterKeys,
          isAppInstanceSpace: entry.isAppInstanceSpace,
        }))
      );

      mappingMetrics.push({
        mappingKey,
        viewLabel: formatViewSource(view),
        view,
        instanceSpaceMetrics,
      });
    }

    const result = { ...base, mappingMetrics };
    results.push(result);
    options?.onResult?.(result);
    if (probePairs.length === 0) {
      options?.onProgress?.({ current: index + 1, total: locationConfigs.length, locationName });
    }
  }

  return { results };
}

export function getMappingProbeMetrics(
  result: LocationSpaceProbeResult,
  mappingKey: string
): MappingSpaceProbeMetrics | undefined {
  return result.mappingMetrics.find((metrics) => metrics.mappingKey === mappingKey);
}

export async function buildRelevantObjectCountResults(
  sdk: CogniteClient,
  locationConfigs: LocationConfigNode[],
  options?: {
    sampleCap?: SampleCapValue;
    onProgress?: (progress: RelevantObjectCountProgress) => void;
    onResult?: (result: ViewCountResult) => void;
  }
): Promise<ViewCountResult[]> {
  const sampleCap = resolveSampleCapOption(options?.sampleCap);
  const allNodesCountCache = new Map<string, Promise<{ count: number; capped: boolean }>>();
  const viewCountCache = new Map<string, Promise<{ count: number; capped: boolean }>>();

  async function getCachedAllNodesCount(locationSpace: string) {
    const cached = allNodesCountCache.get(locationSpace);
    if (cached !== undefined) return cached;
    const countPromise = countAllNodesForSpace(sdk, locationSpace, sampleCap);
    allNodesCountCache.set(locationSpace, countPromise);
    return countPromise;
  }

  async function getCachedViewCount(locationSpace: string, view: ViewSource) {
    const cacheKey = `${locationSpace}:${view.space}:${view.externalId}:${view.version}`;
    const cached = viewCountCache.get(cacheKey);
    if (cached !== undefined) return cached;
    const countPromise = countNodesForViewInSpace(sdk, locationSpace, view, sampleCap);
    viewCountCache.set(cacheKey, countPromise);
    return countPromise;
  }

  async function buildSpaceMetricPairs(spaces: string[], view: ViewSource): Promise<SpaceMetricPair[]> {
    if (spaces.length === 0) return [];

    return Promise.all(
      spaces.map(async (space) => {
        const [allNodes, viewNodes] = await Promise.all([
          getCachedAllNodesCount(space),
          getCachedViewCount(space, view),
        ]);
        return {
          space,
          viewCount: viewNodes.count,
          viewCountCapped: viewNodes.capped,
          viewSpaceBreakdown: toSpaceBreakdown([space], [viewNodes]),
          allNodesCount: allNodes.count,
          allNodesCountCapped: allNodes.capped,
          allNodesSpaceBreakdown: toSpaceBreakdown([space], [allNodes]),
        };
      })
    );
  }

  async function buildResultForLocation(
    location: LocationConfigNode,
    onPartialResult?: (result: ViewCountResult) => void
  ): Promise<ViewCountResult> {
    const appInstanceSpaces = getAppInstanceSpaces(location);
    const appInstanceSpace = appInstanceSpaces[0] ?? null;
    const dataFilterSpaceEntries = getDataFilterInstanceSpaceEntries(location);
    const dataFilterSpaces = dataFilterSpaceEntries.map((entry) => entry.space);
    const appInstanceSpaceNotInDataFilters =
      appInstanceSpace !== null && !dataFilterSpaces.includes(appInstanceSpace);

    const mappings = getViewMappings(location);
    const mappingMetrics: MappingCountMetrics[] = [];

    const base = {
      locationExternalId: location.externalId,
      locationName: getLocationName(location),
      locationDescription: getLocationDescription(location),
      locationUpdated: formatTimestamp(location.lastUpdatedTime),
      location,
      appInstanceSpace,
      appInstanceSpaceNotInDataFilters,
    };

    for (const { mappingKey, view } of mappings) {
      const filterKey = MAPPING_TO_DATA_FILTER_KEY[mappingKey] ?? mappingKey;
      const dataFilterEntries = dataFilterSpaceEntries.filter((entry) => entry.filterKeys.includes(filterKey));
      const dataFilterSpacesForMapping = dataFilterEntries.map((entry) => entry.space);
      const [appInstanceSpaceMetrics, dataFilterMetricPairs] = await Promise.all([
        buildSpaceMetricPairs(appInstanceSpaces, view),
        buildSpaceMetricPairs(dataFilterSpacesForMapping, view),
      ]);

      const dataFilterMetrics = dataFilterMetricPairs.map((pair, index) => ({
        ...pair,
        filterKeys: dataFilterEntries[index]?.filterKeys ?? [filterKey],
      }));

      mappingMetrics.push({
        mappingKey,
        viewLabel: formatViewSource(view),
        view,
        appInstanceSpaceMetrics,
        dataFilterMetrics,
      });

      onPartialResult?.({ ...base, mappingMetrics: [...mappingMetrics] });
    }

    return { ...base, mappingMetrics };
  }

  const results: ViewCountResult[] = [];

  for (let index = 0; index < locationConfigs.length; index += 1) {
    const location = locationConfigs[index];
    const locationName = getLocationName(location);
    options?.onProgress?.({ current: index, total: locationConfigs.length, locationName });

    const result = await buildResultForLocation(location, options?.onResult);
    results.push(result);
    options?.onProgress?.({ current: index + 1, total: locationConfigs.length, locationName });
  }

  return results;
}

export async function fetchLegacyMigrationCounts(
  sdk: CogniteClient,
  locations: LegacyConfigLocation[],
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<Record<string, LegacyLocationMigrationCounts>> {
  const viewCountCache = new Map<string, Promise<{ count: number; capped: boolean }>>();

  async function getCachedViewCount(space: string, view: ViewSource) {
    const cacheKey = `${space}:${view.space}:${view.externalId}:${view.version}`;
    const cached = viewCountCache.get(cacheKey);
    if (cached !== undefined) return cached;
    const countPromise = countNodesForViewInSpace(sdk, space, view, sampleCap);
    viewCountCache.set(cacheKey, countPromise);
    return countPromise;
  }

  const emptyMetrics = {
    space: "",
    viewCount: null,
    viewCountCapped: false,
    viewSpaceBreakdown: [] as SpaceCountBreakdown[],
    allNodesCount: null,
    allNodesCountCapped: false,
    allNodesSpaceBreakdown: [] as SpaceCountBreakdown[],
  };

  const results: Record<string, LegacyLocationMigrationCounts> = {};

  for (const location of locations) {
    const instanceSpace = getLegacyApmaInstanceSpace(location);
    const spaces = instanceSpace !== null && instanceSpace.length > 0 ? [instanceSpace] : [];

    const views = await Promise.all(
      MIGRATION_SOURCE_VIEWS.map(async ({ mappingKey, view }) => {
        if (spaces.length === 0) {
          return {
            mappingKey,
            viewLabel: formatViewSource(view),
            view,
            metrics: emptyMetrics,
          };
        }

        const viewNodes = await getCachedViewCount(spaces[0], view);

        return {
          mappingKey,
          viewLabel: formatViewSource(view),
          view,
          metrics: {
            space: spaces[0],
            viewCount: viewNodes.count,
            viewCountCapped: viewNodes.capped,
            viewSpaceBreakdown: toSpaceBreakdown(spaces, [viewNodes]),
            allNodesCount: null,
            allNodesCountCapped: false,
            allNodesSpaceBreakdown: [],
          },
        };
      })
    );

    results[location.rowId] = {
      rowId: location.rowId,
      instanceSpace,
      views,
    };
  }

  return results;
}

export async function fetchLegacyMigrationColumnCounts(
  sdk: CogniteClient,
  locations: LegacyConfigLocation[],
  mappingKey: string,
  options?: {
    sampleCap?: SampleCapValue;
    onProgress?: (progress: { current: number; total: number; detail?: string }) => void;
    onResult?: (
      rowId: string,
      outcome: CellLoadOutcome<LegacyMigrationViewCount>
    ) => void;
  }
): Promise<Record<string, CellLoadOutcome<LegacyMigrationViewCount>>> {
  const sampleCap = resolveSampleCapOption(options?.sampleCap);
  const sourceView = MIGRATION_SOURCE_VIEWS.find((entry) => entry.mappingKey === mappingKey);
  if (sourceView === undefined) return {};

  const { view } = sourceView;
  const viewCountCache = new Map<string, Promise<{ count: number; capped: boolean }>>();

  async function getCachedViewCount(space: string) {
    const cacheKey = `${space}:${view.space}:${view.externalId}:${view.version}`;
    const cached = viewCountCache.get(cacheKey);
    if (cached !== undefined) return cached;
    const countPromise = countNodesForViewInSpace(sdk, space, view, sampleCap);
    viewCountCache.set(cacheKey, countPromise);
    return countPromise;
  }

  const results: Record<string, CellLoadOutcome<LegacyMigrationViewCount>> = {};

  for (let index = 0; index < locations.length; index++) {
    const location = locations[index];
    const detail = location.externalId || location.assetExternalId || location.rowId;
    options?.onProgress?.({ current: index, total: locations.length, detail });

    try {
      const instanceSpace = getLegacyApmaInstanceSpace(location);
      const spaces = instanceSpace !== null && instanceSpace.length > 0 ? [instanceSpace] : [];

      if (spaces.length === 0) {
        const outcome: CellLoadOutcome<LegacyMigrationViewCount> = {
          ok: true,
          data: {
            view,
            viewLabel: formatViewSource(view),
            metrics: {
              space: "",
              viewCount: null,
              viewCountCapped: false,
              viewSpaceBreakdown: [],
              allNodesCount: null,
              allNodesCountCapped: false,
              allNodesSpaceBreakdown: [],
            },
          },
        };
        results[location.rowId] = outcome;
        options?.onResult?.(location.rowId, outcome);
        continue;
      }

      const viewNodes = await getCachedViewCount(spaces[0]);

      const outcome: CellLoadOutcome<LegacyMigrationViewCount> = {
        ok: true,
        data: {
          view,
          viewLabel: formatViewSource(view),
          metrics: {
            space: spaces[0],
            viewCount: viewNodes.count,
            viewCountCapped: viewNodes.capped,
            viewSpaceBreakdown: toSpaceBreakdown(spaces, [viewNodes]),
            allNodesCount: null,
            allNodesCountCapped: false,
            allNodesSpaceBreakdown: [],
          },
        },
      };
      results[location.rowId] = outcome;
      options?.onResult?.(location.rowId, outcome);
    } catch (error) {
      const outcome: CellLoadOutcome<LegacyMigrationViewCount> = { ok: false, error: toCellLoadError(error) };
      results[location.rowId] = outcome;
      options?.onResult?.(location.rowId, outcome);
    }
  }

  options?.onProgress?.({ current: locations.length, total: locations.length });
  return results;
}

export function getMappingMetrics(
  result: ViewCountResult,
  mappingKey: string
): MappingCountMetrics | undefined {
  return result.mappingMetrics.find((metrics) => metrics.mappingKey === mappingKey);
}

export function getPrimaryFilterValues(location: LegacyConfigLocation): string[] {
  if (location.fileFilter.length > 0) return location.fileFilter;
  if (location.assetFilter.length > 0) return location.assetFilter;
  if (location.generalFilter.length > 0) return location.generalFilter;
  if (location.timeseriesFilter.length > 0) return location.timeseriesFilter;
  return [];
}

export function displayList(values: string[]): string {
  if (values.length === 0) return "—";
  return values.join(", ");
}

export function normalizeFilter(values: string[]): string {
  return [...values].sort((a, b) => a.localeCompare(b)).join("|");
}

export function hasFilterDivergence(location: LegacyConfigLocation): boolean {
  const normalized = [
    normalizeFilter(location.fileFilter),
    normalizeFilter(location.assetFilter),
    normalizeFilter(location.generalFilter),
    normalizeFilter(location.timeseriesFilter),
  ];
  return new Set(normalized).size > 1;
}

export function shouldKeepFilterTokenUnbroken(value: string): boolean {
  return value.length <= 8;
}

function toSampleEstimate(
  sampled: { count: number; capped: boolean },
  accurateCount: number | null
): SampleCountEstimate {
  if (accurateCount !== null) {
    return { count: accurateCount, capped: false, accurate: true };
  }
  return { count: sampled.count, capped: sampled.capped, accurate: false };
}

export function getLegacyFilterRoots(location: LegacyConfigLocation): string[] {
  return [
    ...new Set([
      ...location.fileFilter,
      ...location.assetFilter,
      ...location.generalFilter,
      ...location.timeseriesFilter,
    ]),
  ];
}

async function countSampledAssetsList(
  sdk: CogniteClient,
  listParams: { filter?: Record<string, unknown> },
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<{ count: number; capped: boolean } | null> {
  if (typeof sdk.assets?.list !== "function") return null;

  let cursor: string | undefined;
  let count = 0;
  let capped = false;

  try {
    do {
      const response = await withTransientRetries(() =>
        sdk.assets.list({
          ...listParams,
          limit: 1000,
          cursor,
        } as never)
      );

      count += response.items.length;
      const cappedResult = capSampleCount(count, sampleCap);
      count = cappedResult.count;
      if (cappedResult.capped) {
        capped = true;
        break;
      }

      cursor = response.nextCursor;
    } while (cursor !== undefined);
  } catch {
    return null;
  }

  return { count, capped };
}

async function countSampledTimeSeriesList(
  sdk: CogniteClient,
  listParams: { filter?: Record<string, unknown> },
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<{ count: number; capped: boolean } | null> {
  if (typeof sdk.timeseries?.list !== "function") return null;

  let cursor: string | undefined;
  let count = 0;
  let capped = false;

  try {
    do {
      const response = await withTransientRetries(() =>
        sdk.timeseries.list({
          ...listParams,
          limit: 1000,
          cursor,
        } as never)
      );

      count += response.items.length;
      const cappedResult = capSampleCount(count, sampleCap);
      count = cappedResult.count;
      if (cappedResult.capped) {
        capped = true;
        break;
      }

      cursor = response.nextCursor;
    } while (cursor !== undefined);
  } catch {
    return null;
  }

  return { count, capped };
}

async function countAssetsInSubtree(
  sdk: CogniteClient,
  assetExternalId: string,
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<{ count: number; capped: boolean } | null> {
  if (assetExternalId.length === 0) return null;
  return countSampledAssetsList(
    sdk,
    {
      filter: { assetSubtreeIds: [{ externalId: assetExternalId }] },
    },
    sampleCap
  );
}

async function countAssetsMatchingFilterRoots(
  sdk: CogniteClient,
  filterRoots: string[],
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<{ count: number; capped: boolean } | null> {
  if (filterRoots.length === 0) return null;
  return countSampledAssetsList(
    sdk,
    {
      filter: { assetSubtreeIds: filterRoots.map((externalId) => ({ externalId })) },
    },
    sampleCap
  );
}

async function countAssetsInDataSet(
  sdk: CogniteClient,
  dataSetId: number,
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<{ count: number; capped: boolean } | null> {
  return countSampledAssetsList(
    sdk,
    {
      filter: { dataSetIds: [{ id: dataSetId }] },
    },
    sampleCap
  );
}

async function countTimeSeriesInDataSet(
  sdk: CogniteClient,
  dataSetId: number,
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<{ count: number; capped: boolean } | null> {
  return countSampledTimeSeriesList(
    sdk,
    {
      filter: { dataSetIds: [{ id: dataSetId }] },
    },
    sampleCap
  );
}

function emptyLegacyLocationEstimates(filterRootCount: number): LegacyLocationEstimates {
  return {
    appSpaceNodes: null,
    sourceSpaceNodes: null,
    assetSubtreeAssets: null,
    filterRootCount,
    filterSubtreeAssets: null,
    dataSetAssets: null,
    dataSetTimeSeries: null,
  };
}

export async function fetchLegacyEstimateColumn(
  sdk: CogniteClient,
  locations: LegacyConfigLocation[],
  columnKey: LegacyEstimateColumnKey,
  options?: {
    sampleCap?: SampleCapValue;
    onProgress?: (progress: { current: number; total: number; detail?: string }) => void;
    onResult?: (
      rowId: string,
      outcome: CellLoadOutcome<Partial<LegacyLocationEstimates>>
    ) => void;
  }
): Promise<Record<string, CellLoadOutcome<Partial<LegacyLocationEstimates>>>> {
  const sampleCap = resolveSampleCapOption(options?.sampleCap);
  const spaceSampleCache = new Map<string, Promise<{ count: number; capped: boolean }>>();
  const spaceAccurateCache = new Map<string, Promise<number | null>>();
  const assetSubtreeCache = new Map<string, Promise<{ count: number; capped: boolean } | null>>();
  const filterRootsCache = new Map<string, Promise<{ count: number; capped: boolean } | null>>();
  const dataSetAssetsCache = new Map<number, Promise<{ count: number; capped: boolean } | null>>();
  const dataSetTimeSeriesCache = new Map<number, Promise<{ count: number; capped: boolean } | null>>();

  function getSpaceSample(space: string) {
    if (space.length === 0) return Promise.resolve({ count: 0, capped: false });
    const cached = spaceSampleCache.get(space);
    if (cached !== undefined) return cached;
    const promise = countAllNodesForSpace(sdk, space, sampleCap);
    spaceSampleCache.set(space, promise);
    return promise;
  }

  function getSpaceAccurate(space: string) {
    if (space.length === 0) return Promise.resolve(null);
    const cached = spaceAccurateCache.get(space);
    if (cached !== undefined) return cached;
    const promise = aggregateCountAllNodesInSpace(sdk, space);
    spaceAccurateCache.set(space, promise);
    return promise;
  }

  function getAssetSubtree(assetExternalId: string) {
    const cached = assetSubtreeCache.get(assetExternalId);
    if (cached !== undefined) return cached;
    const promise = countAssetsInSubtree(sdk, assetExternalId, sampleCap);
    assetSubtreeCache.set(assetExternalId, promise);
    return promise;
  }

  function getFilterRootsEstimate(filterRoots: string[]) {
    const key = [...filterRoots].sort().join("|");
    const cached = filterRootsCache.get(key);
    if (cached !== undefined) return cached;
    const promise = countAssetsMatchingFilterRoots(sdk, filterRoots, sampleCap);
    filterRootsCache.set(key, promise);
    return promise;
  }

  function getDataSetAssets(dataSetId: number) {
    const cached = dataSetAssetsCache.get(dataSetId);
    if (cached !== undefined) return cached;
    const promise = countAssetsInDataSet(sdk, dataSetId, sampleCap);
    dataSetAssetsCache.set(dataSetId, promise);
    return promise;
  }

  function getDataSetTimeSeries(dataSetId: number) {
    const cached = dataSetTimeSeriesCache.get(dataSetId);
    if (cached !== undefined) return cached;
    const promise = countTimeSeriesInDataSet(sdk, dataSetId, sampleCap);
    dataSetTimeSeriesCache.set(dataSetId, promise);
    return promise;
  }

  const results: Record<string, CellLoadOutcome<Partial<LegacyLocationEstimates>>> = {};

  function publishOutcome(rowId: string, outcome: CellLoadOutcome<Partial<LegacyLocationEstimates>>) {
    results[rowId] = outcome;
    options?.onResult?.(rowId, outcome);
  }

  for (let index = 0; index < locations.length; index++) {
    const location = locations[index];
    const filterRoots = getLegacyFilterRoots(location);
    const detail = location.externalId || location.assetExternalId || location.rowId;
    options?.onProgress?.({ current: index, total: locations.length, detail });

    try {
      if (columnKey === "asset") {
        const assetSubtree =
          location.assetExternalId.length > 0
            ? await getAssetSubtree(location.assetExternalId)
            : null;
        publishOutcome(location.rowId, {
          ok: true,
          data: {
            assetSubtreeAssets:
              assetSubtree !== null
                ? { count: assetSubtree.count, capped: assetSubtree.capped, accurate: false }
                : null,
          },
        });
      } else if (columnKey === "appSpace") {
        const [appSpaceSample, appSpaceAccurate] = await Promise.all([
          location.appDataInstanceSpace.length > 0
            ? getSpaceSample(location.appDataInstanceSpace)
            : Promise.resolve(null),
          location.appDataInstanceSpace.length > 0
            ? getSpaceAccurate(location.appDataInstanceSpace)
            : Promise.resolve(null),
        ]);
        publishOutcome(location.rowId, {
          ok: true,
          data: {
            appSpaceNodes:
              appSpaceSample !== null ? toSampleEstimate(appSpaceSample, appSpaceAccurate) : null,
          },
        });
      } else if (columnKey === "sourceSpace") {
        const [sourceSpaceSample, sourceSpaceAccurate] = await Promise.all([
          location.sourceDataInstanceSpace.length > 0
            ? getSpaceSample(location.sourceDataInstanceSpace)
            : Promise.resolve(null),
          location.sourceDataInstanceSpace.length > 0
            ? getSpaceAccurate(location.sourceDataInstanceSpace)
            : Promise.resolve(null),
        ]);
        publishOutcome(location.rowId, {
          ok: true,
          data: {
            sourceSpaceNodes:
              sourceSpaceSample !== null ? toSampleEstimate(sourceSpaceSample, sourceSpaceAccurate) : null,
          },
        });
      } else if (columnKey === "filter") {
        const filterSubtree =
          filterRoots.length > 0 ? await getFilterRootsEstimate(filterRoots) : null;
        publishOutcome(location.rowId, {
          ok: true,
          data: {
            filterRootCount: filterRoots.length,
            filterSubtreeAssets:
              filterSubtree !== null
                ? { count: filterSubtree.count, capped: filterSubtree.capped, accurate: false }
                : null,
          },
        });
      } else if (columnKey === "dataSet") {
        const [dataSetAssets, dataSetTimeSeries] = await Promise.all([
          location.dataSetId !== null ? getDataSetAssets(location.dataSetId) : Promise.resolve(null),
          location.dataSetId !== null ? getDataSetTimeSeries(location.dataSetId) : Promise.resolve(null),
        ]);
        publishOutcome(location.rowId, {
          ok: true,
          data: {
            dataSetAssets:
              dataSetAssets !== null
                ? { count: dataSetAssets.count, capped: dataSetAssets.capped, accurate: false }
                : null,
            dataSetTimeSeries:
              dataSetTimeSeries !== null
                ? { count: dataSetTimeSeries.count, capped: dataSetTimeSeries.capped, accurate: false }
                : null,
          },
        });
      }
    } catch (error) {
      publishOutcome(location.rowId, { ok: false, error: toCellLoadError(error) });
    }
  }

  options?.onProgress?.({ current: locations.length, total: locations.length });
  return results;
}

export async function fetchLegacyLocationEstimates(
  sdk: CogniteClient,
  locations: LegacyConfigLocation[],
  sampleCap: SampleCapValue = DEFAULT_SAMPLE_CAP
): Promise<Record<string, LegacyLocationEstimates>> {
  const spaceSampleCache = new Map<string, Promise<{ count: number; capped: boolean }>>();
  const spaceAccurateCache = new Map<string, Promise<number | null>>();
  const assetSubtreeCache = new Map<string, Promise<{ count: number; capped: boolean } | null>>();
  const filterRootsCache = new Map<string, Promise<{ count: number; capped: boolean } | null>>();
  const dataSetAssetsCache = new Map<number, Promise<{ count: number; capped: boolean } | null>>();
  const dataSetTimeSeriesCache = new Map<number, Promise<{ count: number; capped: boolean } | null>>();

  function getSpaceSample(space: string) {
    if (space.length === 0) return Promise.resolve({ count: 0, capped: false });
    const cached = spaceSampleCache.get(space);
    if (cached !== undefined) return cached;
    const promise = countAllNodesForSpace(sdk, space, sampleCap);
    spaceSampleCache.set(space, promise);
    return promise;
  }

  function getSpaceAccurate(space: string) {
    if (space.length === 0) return Promise.resolve(null);
    const cached = spaceAccurateCache.get(space);
    if (cached !== undefined) return cached;
    const promise = aggregateCountAllNodesInSpace(sdk, space);
    spaceAccurateCache.set(space, promise);
    return promise;
  }

  function getAssetSubtree(assetExternalId: string) {
    const cached = assetSubtreeCache.get(assetExternalId);
    if (cached !== undefined) return cached;
    const promise = countAssetsInSubtree(sdk, assetExternalId, sampleCap);
    assetSubtreeCache.set(assetExternalId, promise);
    return promise;
  }

  function getFilterRootsEstimate(filterRoots: string[]) {
    const key = [...filterRoots].sort().join("|");
    const cached = filterRootsCache.get(key);
    if (cached !== undefined) return cached;
    const promise = countAssetsMatchingFilterRoots(sdk, filterRoots, sampleCap);
    filterRootsCache.set(key, promise);
    return promise;
  }

  function getDataSetAssets(dataSetId: number) {
    const cached = dataSetAssetsCache.get(dataSetId);
    if (cached !== undefined) return cached;
    const promise = countAssetsInDataSet(sdk, dataSetId, sampleCap);
    dataSetAssetsCache.set(dataSetId, promise);
    return promise;
  }

  function getDataSetTimeSeries(dataSetId: number) {
    const cached = dataSetTimeSeriesCache.get(dataSetId);
    if (cached !== undefined) return cached;
    const promise = countTimeSeriesInDataSet(sdk, dataSetId, sampleCap);
    dataSetTimeSeriesCache.set(dataSetId, promise);
    return promise;
  }

  const entries = await Promise.all(
    locations.map(async (location) => {
      const filterRoots = getLegacyFilterRoots(location);
      const filterRootCount = filterRoots.length;

      try {
        const [
          appSpaceSample,
          appSpaceAccurate,
          sourceSpaceSample,
          sourceSpaceAccurate,
          assetSubtree,
          filterSubtree,
          dataSetAssets,
          dataSetTimeSeries,
        ] = await Promise.all([
          location.appDataInstanceSpace.length > 0
            ? getSpaceSample(location.appDataInstanceSpace)
            : Promise.resolve(null),
          location.appDataInstanceSpace.length > 0
            ? getSpaceAccurate(location.appDataInstanceSpace)
            : Promise.resolve(null),
          location.sourceDataInstanceSpace.length > 0
            ? getSpaceSample(location.sourceDataInstanceSpace)
            : Promise.resolve(null),
          location.sourceDataInstanceSpace.length > 0
            ? getSpaceAccurate(location.sourceDataInstanceSpace)
            : Promise.resolve(null),
          location.assetExternalId.length > 0
            ? getAssetSubtree(location.assetExternalId)
            : Promise.resolve(null),
          filterRoots.length > 0 ? getFilterRootsEstimate(filterRoots) : Promise.resolve(null),
          location.dataSetId !== null ? getDataSetAssets(location.dataSetId) : Promise.resolve(null),
          location.dataSetId !== null ? getDataSetTimeSeries(location.dataSetId) : Promise.resolve(null),
        ]);

        const estimates: LegacyLocationEstimates = {
          appSpaceNodes:
            appSpaceSample !== null ? toSampleEstimate(appSpaceSample, appSpaceAccurate) : null,
          sourceSpaceNodes:
            sourceSpaceSample !== null ? toSampleEstimate(sourceSpaceSample, sourceSpaceAccurate) : null,
          assetSubtreeAssets:
            assetSubtree !== null
              ? { count: assetSubtree.count, capped: assetSubtree.capped, accurate: false }
              : null,
          filterRootCount,
          filterSubtreeAssets:
            filterSubtree !== null
              ? { count: filterSubtree.count, capped: filterSubtree.capped, accurate: false }
              : null,
          dataSetAssets:
            dataSetAssets !== null
              ? { count: dataSetAssets.count, capped: dataSetAssets.capped, accurate: false }
              : null,
          dataSetTimeSeries:
            dataSetTimeSeries !== null
              ? { count: dataSetTimeSeries.count, capped: dataSetTimeSeries.capped, accurate: false }
              : null,
        };

        return [location.rowId, estimates] as const;
      } catch {
        return [location.rowId, emptyLegacyLocationEstimates(filterRootCount)] as const;
      }
    })
  );

  return Object.fromEntries(entries);
}

export function formatLegacyEstimate(
  estimate: SampleCountEstimate | null,
  unit: string
): string | null {
  if (estimate === null || estimate.count === null) return null;
  const suffix = estimate.accurate ? "" : estimate.capped ? "*" : "";
  return unit.length > 0 ? `${estimate.count}${suffix} ${unit}` : `${estimate.count}${suffix}`;
}
