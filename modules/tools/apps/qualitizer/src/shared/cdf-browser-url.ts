const DEFAULT_CDF_BROWSER_URL = "https://fusion.cognite.com";
const DEFAULT_CDF_CLUSTER = "api.cognitedata.com";

const raw = (import.meta.env.CDF_BROWSER_URL as string | undefined) ?? "";
const isMissing = !raw || String(raw).trim() === "";

/** Fusion UI base URL (no trailing slash). Used for transformation, data model, view, and other Fusion links. */
export const CDF_BROWSER_URL = isMissing
  ? DEFAULT_CDF_BROWSER_URL
  : String(raw).replace(/\/$/, "") || DEFAULT_CDF_BROWSER_URL;

/** CDF cluster host derived from CDF_URL (e.g. az-eastus-1.cognitedata.com). */
const cdfUrlRaw = (import.meta.env.CDF_URL as string | undefined) ?? "";
const cdfUrl =
  cdfUrlRaw && cdfUrlRaw !== "undefined" ? cdfUrlRaw : "https://api.cognitedata.com";
export const CDF_CLUSTER =
  cdfUrl.replace(/^https?:\/\//, "").replace(/\/.*$/, "").split(":")[0] || DEFAULT_CDF_CLUSTER;

function fusionWorkspaceQuery(): string {
  return `cluster=${encodeURIComponent(CDF_CLUSTER)}&workspace=data-management`;
}

function fusionPathSeg(s: string): string {
  return encodeURIComponent(s);
}

export function getTransformationPreviewUrl(
  project: string,
  id: string | number
): string {
  return `${CDF_BROWSER_URL}/${project}/transformations/${id}/preview?cluster=${CDF_CLUSTER}&workspace=data-management`;
}

export function getTransformationRunHistoryUrl(
  project: string,
  id: string | number
): string {
  return `${CDF_BROWSER_URL}/${project}/transformations/${id}/run-history?cluster=${CDF_CLUSTER}&workspace=data-management`;
}

export function getAssetUrl(
  project: string,
  instanceSpace: string,
  instanceExternalId: string,
  viewSpace = "cdf_cdm",
  viewVersion = "v1",
  viewExternalId = "CogniteAsset"
): string {
  const encoded = encodeURIComponent(instanceExternalId);
  return `${CDF_BROWSER_URL}/${project}/search/asset/${viewSpace}/${viewVersion}/${viewExternalId}/${instanceSpace}/${encoded}?cluster=${CDF_CLUSTER}`;
}

export function getFunctionsPageUrl(project: string): string {
  return `${CDF_BROWSER_URL}/${project}/functions?cluster=${CDF_CLUSTER}&workspace=data-management`;
}

export function getWorkflowEditorUrl(
  project: string,
  workflowExternalId: string
): string {
  const encoded = encodeURIComponent(workflowExternalId);
  return `${CDF_BROWSER_URL}/${project}/flows/${encoded}/editor?cluster=${CDF_CLUSTER}&workspace=data-management`;
}

/** Fusion: /{project}/data-models/{space}/{externalId}/{version} — use version `latest` when omitted. */
export function getDataModelUrl(
  project: string,
  space: string,
  externalId: string,
  version?: string
): string {
  const ver =
    version != null && String(version).trim() !== "" ? String(version).trim() : "latest";
  return `${CDF_BROWSER_URL}/${fusionPathSeg(project)}/data-models/${fusionPathSeg(space)}/${fusionPathSeg(externalId)}/${fusionPathSeg(ver)}?${fusionWorkspaceQuery()}`;
}

/** Fusion: /{project}/views/{space}/{externalId}/{version} — use version `latest` when omitted. */
export function getViewUrl(
  project: string,
  space: string,
  externalId: string,
  version?: string
): string {
  const ver =
    version != null && String(version).trim() !== "" ? String(version).trim() : "latest";
  return `${CDF_BROWSER_URL}/${fusionPathSeg(project)}/views/${fusionPathSeg(space)}/${fusionPathSeg(externalId)}/${fusionPathSeg(ver)}?${fusionWorkspaceQuery()}`;
}

/**
 * Fusion data management preview for a view type on a specific data model revision.
 * Example: …/data-models/{space}/{modelId}/{revision}/data-management/preview?type=Asset&cluster=…&workspace=data-management
 * `viewTypeExternalId` is the view external id (Fusion `type` query param).
 */
export function getDataModelViewPreviewUrl(
  project: string,
  dataModelSpace: string,
  dataModelExternalId: string,
  dataModelVersion: string,
  viewTypeExternalId: string
): string {
  const ver =
    dataModelVersion != null && String(dataModelVersion).trim() !== ""
      ? String(dataModelVersion).trim()
      : "latest";
  const base = `${CDF_BROWSER_URL}/${fusionPathSeg(project)}/data-models/${fusionPathSeg(dataModelSpace)}/${fusionPathSeg(dataModelExternalId)}/${fusionPathSeg(ver)}/data-management/preview`;
  return `${base}?type=${encodeURIComponent(viewTypeExternalId)}&${fusionWorkspaceQuery()}`;
}

export function warnIfCdfBrowserUrlMissing(): void {
  if (isMissing) {
    console.warn(
      "[Qualitizer] CDF_BROWSER_URL is not set. Using default:",
      DEFAULT_CDF_BROWSER_URL,
      "- Add CDF_BROWSER_URL to your .env (e.g. https://your-cluster.fusion.cognite.com/) for Fusion links (transformations, data models, views)."
    );
  }
}
