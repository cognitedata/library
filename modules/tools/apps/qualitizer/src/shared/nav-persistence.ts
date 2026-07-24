const STORAGE_KEY = "qualitizer.nav";

export type PersistedTransformationsSubView = "list" | "overlap" | "dataModelUsage";

export type PersistedVersioningSubView = "viewVersions" | "dataModelVersions";

export type PersistedDataCatalogSubView =
  | "overview"
  | "propertyExplorer"
  | "dataModelVersions"
  | "viewVersions"
  | "docLookup";

export type PersistedPermissionsSubView =
  | "groups"
  | "compare"
  | "crossProject"
  | "spaces"
  | "datasets";

/** Internal Assets area: catalog models with asset views vs all CogniteAsset-shaped views. */
export type PersistedAssetsSubView = "dataModels" | "standaloneViews";

export type PersistedInfieldSubView =
  | "legacyConfig"
  | "infield2Data"
  | "connectivityMap"
  | "migrationScripts";

export type PersistedInfieldCdmSubView = "cdmSetup" | "cdmDataExplorer";

export type PersistedInfieldSampleCap = 500 | 5000 | 25000 | 100000 | "all";

export type PersistedConsistencyPageSize = 25 | 50 | 100 | 250 | 500 | 1000;

export type PersistedNavState = {
  mode?: string;
  transformationsSubView?: PersistedTransformationsSubView;
  versioningSubView?: PersistedVersioningSubView;
  dataCatalogSubView?: PersistedDataCatalogSubView;
  permissionsSubView?: PersistedPermissionsSubView;
  assetsSubView?: PersistedAssetsSubView;
  infieldSubView?: PersistedInfieldSubView;
  infieldCdmSubView?: PersistedInfieldCdmSubView;
  infieldSampleCap?: PersistedInfieldSampleCap;
  infield2DataLocationKey?: string;
  infieldCdmDataLocationKey?: string;
  infieldConsistencyPageSize?: PersistedConsistencyPageSize;
};

export function loadNavState(): PersistedNavState {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as unknown;
    if (typeof parsed !== "object" || parsed === null) return {};
    const obj = parsed as Record<string, unknown>;
    return {
      mode: typeof obj.mode === "string" ? obj.mode : undefined,
      transformationsSubView:
        obj.transformationsSubView === "list" ||
        obj.transformationsSubView === "overlap" ||
        obj.transformationsSubView === "dataModelUsage"
          ? obj.transformationsSubView
          : undefined,
      versioningSubView:
        obj.versioningSubView === "viewVersions" || obj.versioningSubView === "dataModelVersions"
          ? obj.versioningSubView
          : undefined,
      dataCatalogSubView:
        obj.dataCatalogSubView === "overview" ||
        obj.dataCatalogSubView === "propertyExplorer" ||
        obj.dataCatalogSubView === "dataModelVersions" ||
        obj.dataCatalogSubView === "viewVersions" ||
        obj.dataCatalogSubView === "docLookup"
          ? obj.dataCatalogSubView
          : undefined,
      permissionsSubView:
        obj.permissionsSubView === "groups" ||
        obj.permissionsSubView === "compare" ||
        obj.permissionsSubView === "crossProject" ||
        obj.permissionsSubView === "spaces" ||
        obj.permissionsSubView === "datasets"
          ? obj.permissionsSubView
          : undefined,
      assetsSubView:
        obj.assetsSubView === "dataModels" || obj.assetsSubView === "standaloneViews"
          ? obj.assetsSubView
          : undefined,
      infieldSubView:
        obj.infieldSubView === "legacyConfig" ||
        obj.infieldSubView === "infield2Data" ||
        obj.infieldSubView === "connectivityMap" ||
        obj.infieldSubView === "migrationScripts"
          ? obj.infieldSubView
          : undefined,
      infieldCdmSubView:
        obj.infieldCdmSubView === "cdmSetup" || obj.infieldCdmSubView === "cdmDataExplorer"
          ? obj.infieldCdmSubView
          : obj.infieldSubView === "newConfig"
            ? "cdmSetup"
            : obj.infieldSubView === "infieldCdmData" || obj.infieldSubView === "infieldData"
              ? "cdmDataExplorer"
              : undefined,
      infieldSampleCap:
        obj.infieldSampleCap === 500 ||
        obj.infieldSampleCap === 5000 ||
        obj.infieldSampleCap === 25000 ||
        obj.infieldSampleCap === 100000 ||
        obj.infieldSampleCap === "all"
          ? obj.infieldSampleCap
          : undefined,
      infield2DataLocationKey:
        typeof obj.infield2DataLocationKey === "string" && obj.infield2DataLocationKey.length > 0
          ? obj.infield2DataLocationKey
          : undefined,
      infieldCdmDataLocationKey:
        typeof obj.infieldCdmDataLocationKey === "string" && obj.infieldCdmDataLocationKey.length > 0
          ? obj.infieldCdmDataLocationKey
          : undefined,
      infieldConsistencyPageSize:
        obj.infieldConsistencyPageSize === 25 ||
        obj.infieldConsistencyPageSize === 50 ||
        obj.infieldConsistencyPageSize === 100 ||
        obj.infieldConsistencyPageSize === 250 ||
        obj.infieldConsistencyPageSize === 500 ||
        obj.infieldConsistencyPageSize === 1000
          ? obj.infieldConsistencyPageSize
          : undefined,
    };
  } catch {
    return {};
  }
}

export function saveNavState(partial: Partial<PersistedNavState>): void {
  try {
    const current = loadNavState();
    const next: PersistedNavState = { ...current, ...partial };
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  } catch {
    // ignore
  }
}
