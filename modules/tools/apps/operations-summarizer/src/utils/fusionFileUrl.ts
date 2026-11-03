import appConfig from "../../app.json";

export interface FusionFileLinkClient {
  project: string;
  getBaseUrl: () => string;
  files: {
    retrieve: (
      ids: Array<{ instanceId: { space: string; externalId: string } }>,
    ) => Promise<Array<{ id?: number }>>;
  };
}

function encodeHashValue(value: string): string {
  return encodeURIComponent(value).replace(/%20/g, "+");
}

export function clusterHostFromBaseUrl(baseUrl: string): string {
  try {
    return new URL(baseUrl).host;
  } catch {
    return baseUrl.replace(/^https?:\/\//, "").split("/")[0] ?? baseUrl;
  }
}

export function isLocalOrFlowsAppOrigin(origin: string): boolean {
  try {
    const { hostname } = new URL(origin);
    return (
      hostname === "localhost" ||
      hostname === "127.0.0.1" ||
      hostname.endsWith(".cogniteappsdata.com")
    );
  } catch {
    return false;
  }
}

/** Fusion web origin. Uses the current host when the app runs inside Fusion. */
export function resolveFusionOrigin(
  windowOrigin: string = window.location.origin,
): string {
  if (!isLocalOrFlowsAppOrigin(windowOrigin)) {
    return windowOrigin.replace(/\/$/, "");
  }
  const org = appConfig.deployments[0]?.org;
  if (org) return `https://${org}.fusion.cognite.com`;
  return windowOrigin.replace(/\/$/, "");
}

export function buildFusionFileSearchUrl(input: {
  fusionOrigin: string;
  project: string;
  clusterHost: string;
  fileId: number;
  searchQuery: string;
}): string {
  const origin = input.fusionOrigin.replace(/\/$/, "");
  const query = new URLSearchParams({
    cluster: input.clusterHost,
    workspace: "flows",
  });
  const preview = JSON.stringify({ id: input.fileId, type: "file" });
  const hash = [
    `searchCategory=${encodeHashValue('"file"')}`,
    `searchQuery=${encodeHashValue(`"${input.searchQuery}"`)}`,
    `preview_instance=${encodeHashValue(preview)}`,
  ].join("&");
  return `${origin}/${encodeURIComponent(input.project)}/search/file/${input.fileId}?${query.toString()}#${hash}`;
}

export async function resolveFusionFileUrl(
  client: FusionFileLinkClient,
  input: { space: string; externalId: string; searchQuery: string },
): Promise<string | undefined> {
  const files = await client.files.retrieve([
    { instanceId: { space: input.space, externalId: input.externalId } },
  ]);
  const fileId = files[0]?.id;
  if (typeof fileId !== "number") return undefined;
  return buildFusionFileSearchUrl({
    fusionOrigin: resolveFusionOrigin(),
    project: client.project,
    clusterHost: clusterHostFromBaseUrl(client.getBaseUrl()),
    fileId,
    searchQuery: input.searchQuery,
  });
}
