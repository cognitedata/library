const DEFAULT_CDF_BROWSER_URL = "https://fusion.cognite.com";
const DEFAULT_CDF_CLUSTER = "api.cognitedata.com";

const raw = (import.meta.env.CDF_BROWSER_URL as string | undefined) ?? "";
const isMissing = !raw || String(raw).trim() === "";

/** Fusion UI base URL (no trailing slash). Used for transformation preview/run-history links. */
export const CDF_BROWSER_URL = isMissing
  ? DEFAULT_CDF_BROWSER_URL
  : String(raw).replace(/\/$/, "") || DEFAULT_CDF_BROWSER_URL;

/** CDF cluster host derived from CDF_URL (e.g. az-eastus-1.cognitedata.com). */
const cdfUrlRaw = (import.meta.env.CDF_URL as string | undefined) ?? "";
const cdfUrl =
  cdfUrlRaw && cdfUrlRaw !== "undefined" ? cdfUrlRaw : "https://api.cognitedata.com";
export const CDF_CLUSTER =
  cdfUrl.replace(/^https?:\/\//, "").replace(/\/.*$/, "").split(":")[0] || DEFAULT_CDF_CLUSTER;

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

export function warnIfCdfBrowserUrlMissing(): void {
  if (isMissing) {
    console.warn(
      "[Qualitizer] CDF_BROWSER_URL is not set. Using default:",
      DEFAULT_CDF_BROWSER_URL,
      "- Add CDF_BROWSER_URL to your .env (e.g. https://your-cluster.fusion.cognite.com/) for transformation preview links."
    );
  }
}
