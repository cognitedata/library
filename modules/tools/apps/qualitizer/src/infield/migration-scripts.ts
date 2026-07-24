import type { LegacyConfigLocation, SpaceProbeApiCall, WaveLabel } from "./types";

const WAVE_1_PREFIXES = ["NA", "BC", "FO", "CL", "GE", "WW", "FM-"];
const WAVE_2_PREFIXES = ["CL-", "BI", "EN", "PN", "SI", "NP"];
const WAVE_3_PREFIXES = ["UE", "SA", "AT", "FE", "LU"];

const INSTANCE_SPACE_SITE_PATTERN = /^(?:SAP|APMA)-([A-Z0-9]{2,8})-ALL-DAT$/i;

/** Pilot sites handled separately — excluded from migration script generation for now. */
export const DEFERRED_MIGRATION_SITE_CODES = new Set(["CLK", "FRA"]);

const DEFERRED_MIGRATION_ASSET_PREFIXES = ["CL-", "FM-"];

const LINEAGE_SKIP_LABELS =
  "equipment-class-characteristics,bom-items,bom-header";

export type MigrationWaveFilter = WaveLabel;

export type MigrationResourceCheck = {
  resourceName: string;
  exists: boolean;
  statusDetail: string;
  apiCalls: SpaceProbeApiCall[];
};

export type MigrationValidationSnapshot = {
  spaces: Record<string, MigrationResourceCheck>;
  dataSets: Record<string, MigrationResourceCheck>;
};

export type LineageScriptEntry = {
  rowId: string;
  siteCode: string | null;
  resourceName: string | null;
  instanceSpace: string | null;
  assetExternalId: string;
  waveLabel: WaveLabel;
  spaceCheck: MigrationResourceCheck;
  dataSetCheck: MigrationResourceCheck;
  ready: boolean;
};

export function deriveLegacySiteCode(assetExternalId: string): string | null {
  const parts = assetExternalId.split("-").filter(Boolean);
  if (parts.length < 2) return null;

  const candidate = parts[1]?.toUpperCase() ?? "";
  if (/^[A-Z]{2,5}$/.test(candidate)) return candidate;
  return null;
}

export function deriveSiteCodeFromInstanceSpace(space: string): string | null {
  const trimmed = space.trim();
  if (trimmed.length === 0) return null;

  const strict = trimmed.match(INSTANCE_SPACE_SITE_PATTERN);
  if (strict?.[1] !== undefined) return strict[1].toUpperCase();

  const parts = trimmed.split("-").filter(Boolean);
  if (parts.length < 2) return null;

  const candidate = parts[1]?.toUpperCase() ?? "";
  if (/^[A-Z0-9]{2,8}$/.test(candidate)) return candidate;
  return null;
}

export function resolveLegacyInstanceSpace(location: LegacyConfigLocation): string | null {
  const appSpace = location.appDataInstanceSpace.trim();
  if (appSpace.length > 0) return appSpace;

  const sourceSpace = location.sourceDataInstanceSpace.trim();
  if (sourceSpace.length > 0) return sourceSpace;

  const site = deriveLegacySiteCode(location.assetExternalId);
  if (site !== null) return `APMA-${site}-ALL-DAT`;

  return null;
}

export function resolveMigrationSiteCode(location: LegacyConfigLocation): string | null {
  const instanceSpace = resolveLegacyInstanceSpace(location);
  if (instanceSpace !== null) {
    const fromSpace = deriveSiteCodeFromInstanceSpace(instanceSpace);
    if (fromSpace !== null) return fromSpace;
  }
  return deriveLegacySiteCode(location.assetExternalId);
}

export function isDeferredMigrationLocation(location: LegacyConfigLocation): boolean {
  const asset = location.assetExternalId.trim().toUpperCase();
  if (DEFERRED_MIGRATION_ASSET_PREFIXES.some((prefix) => asset.startsWith(prefix))) {
    return true;
  }
  const siteCode = resolveMigrationSiteCode(location);
  return siteCode !== null && DEFERRED_MIGRATION_SITE_CODES.has(siteCode);
}

export function filterMigrationScriptLocations(locations: LegacyConfigLocation[]): LegacyConfigLocation[] {
  return locations.filter((location) => !isDeferredMigrationLocation(location));
}

export function getWaveLabel(location: LegacyConfigLocation): WaveLabel {
  const candidate = location.assetExternalId.trim().toUpperCase();
  if (candidate.length === 0) return "Unassigned";
  if (WAVE_2_PREFIXES.some((prefix) => candidate.startsWith(prefix))) return "Wave 2";
  if (WAVE_1_PREFIXES.some((prefix) => candidate.startsWith(prefix))) return "Wave 1";
  if (WAVE_3_PREFIXES.some((prefix) => candidate.startsWith(prefix))) return "Wave 3";
  return "Unassigned";
}

export function getWaveSortRank(location: LegacyConfigLocation): number {
  const waveLabel = getWaveLabel(location);
  if (waveLabel === "Wave 1") return 0;
  if (waveLabel === "Wave 2") return 1;
  if (waveLabel === "Wave 3") return 2;
  return 3;
}

export function buildSapAllDatResourceName(siteCode: string): string {
  return `SAP-${siteCode.toUpperCase()}-ALL-DAT`;
}

export function buildLineageScript(resourceName: string): string {
  return `python migration/hybrid-asset-preparation.py process-dataset --asset-space ${resourceName} --data-set-name ${resourceName} --continue-on-error --skip-labels=${LINEAGE_SKIP_LABELS}`;
}

export function buildLineageScriptsShell(resourceNames: string[]): string {
  return resourceNames.map((name) => buildLineageScript(name)).join("\n\n");
}

export function filterLocationsByWave(
  locations: LegacyConfigLocation[],
  wave: MigrationWaveFilter
): LegacyConfigLocation[] {
  return locations.filter((location) => getWaveLabel(location) === wave);
}

function unresolvedSiteResourceCheck(
  instanceSpace: string | null,
  assetExternalId: string
): MigrationResourceCheck {
  const detail =
    instanceSpace !== null
      ? `Could not derive a site code from instance space "${instanceSpace}" or asset "${assetExternalId}".`
      : `No instance space configured and could not derive a site code from asset "${assetExternalId}".`;
  return {
    resourceName: "—",
    exists: false,
    statusDetail: detail,
    apiCalls: [],
  };
}

export function buildLineageScriptEntries(
  locations: LegacyConfigLocation[],
  options: {
    wave: MigrationWaveFilter;
    validation: MigrationValidationSnapshot;
  }
): LineageScriptEntry[] {
  const filtered = filterLocationsByWave(filterMigrationScriptLocations(locations), options.wave);

  return filtered
    .sort(
      (a, b) =>
        getWaveSortRank(a) - getWaveSortRank(b) ||
        a.assetExternalId.localeCompare(b.assetExternalId) ||
        a.rowId.localeCompare(b.rowId)
    )
    .map((location) => {
      const instanceSpace = resolveLegacyInstanceSpace(location);
      const siteCode = resolveMigrationSiteCode(location);
      const resourceName = siteCode !== null ? buildSapAllDatResourceName(siteCode) : null;
      const unresolved = unresolvedSiteResourceCheck(instanceSpace, location.assetExternalId);
      const spaceCheck =
        resourceName === null
          ? unresolved
          : (options.validation.spaces[resourceName] ??
            missingMigrationResourceCheck(resourceName, "space"));
      const dataSetCheck =
        resourceName === null
          ? unresolved
          : (options.validation.dataSets[resourceName] ??
            missingMigrationResourceCheck(resourceName, "data set"));
      return {
        rowId: location.rowId,
        siteCode,
        resourceName,
        instanceSpace,
        assetExternalId: location.assetExternalId,
        waveLabel: getWaveLabel(location),
        spaceCheck,
        dataSetCheck,
        ready: resourceName !== null && spaceCheck.exists && dataSetCheck.exists,
      };
    });
}

function missingMigrationResourceCheck(
  resourceName: string,
  kind: "space" | "data set"
): MigrationResourceCheck {
  const label = kind === "space" ? "DMS space" : "Data set";
  return {
    resourceName,
    exists: false,
    statusDetail: `${label} "${resourceName}" was not checked.`,
    apiCalls: [],
  };
}

export function collectSapAllDatResourceNames(locations: LegacyConfigLocation[]): string[] {
  const names = new Set<string>();
  for (const location of filterMigrationScriptLocations(locations)) {
    const siteCode = resolveMigrationSiteCode(location);
    if (siteCode === null) continue;
    names.add(buildSapAllDatResourceName(siteCode));
  }
  return [...names];
}
