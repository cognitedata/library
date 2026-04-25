const STORAGE_KEY = "qualitizer.nav";

export type PersistedTransformationsSubView = "list" | "overlap" | "dataModelUsage";

export type PersistedVersioningSubView = "viewVersions" | "dataModelVersions";

export type PersistedDataCatalogSubView =
  | "overview"
  | "propertyExplorer"
  | "dataModelVersions"
  | "viewVersions";

export type PersistedPermissionsSubView =
  | "groups"
  | "compare"
  | "crossProject"
  | "spaces"
  | "datasets";

/** Internal Assets area: catalog models with asset views vs all CogniteAsset-shaped views. */
export type PersistedAssetsSubView = "dataModels" | "standaloneViews";

export type PersistedNavState = {
  mode?: string;
  transformationsSubView?: PersistedTransformationsSubView;
  versioningSubView?: PersistedVersioningSubView;
  dataCatalogSubView?: PersistedDataCatalogSubView;
  permissionsSubView?: PersistedPermissionsSubView;
  assetsSubView?: PersistedAssetsSubView;
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
        obj.dataCatalogSubView === "viewVersions"
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
