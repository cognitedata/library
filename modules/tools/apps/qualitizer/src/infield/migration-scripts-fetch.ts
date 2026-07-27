import type { CogniteClient } from "@cognite/sdk";
import { withTransientRetries } from "@/shared/transient-http-retry";
import type { LegacyConfigLocation, SpaceProbeApiCall } from "./types";
import type { MigrationResourceCheck, MigrationValidationSnapshot } from "./migration-scripts";

function buildSpaceCheck(space: string, exists: boolean, apiCalls: SpaceProbeApiCall[]): MigrationResourceCheck {
  return {
    resourceName: space,
    exists,
    apiCalls,
    statusDetail: exists
      ? `DMS space "${space}" exists.`
      : `DMS space "${space}" was not found.`,
  };
}

function buildDataSetCheck(
  name: string,
  exists: boolean,
  apiCalls: SpaceProbeApiCall[],
  dataSetCount: number
): MigrationResourceCheck {
  return {
    resourceName: name,
    exists,
    apiCalls,
    statusDetail: exists
      ? `Data set "${name}" exists.`
      : `Data set "${name}" was not found among ${dataSetCount} data sets in the project.`,
  };
}

export async function fetchMigrationValidation(
  sdk: CogniteClient,
  resourceNames: string[]
): Promise<MigrationValidationSnapshot> {
  const uniqueNames = [...new Set(resourceNames.map((name) => name.trim()).filter((name) => name.length > 0))];
  const spaces: Record<string, MigrationResourceCheck> = {};
  const dataSets: Record<string, MigrationResourceCheck> = {};

  for (const name of uniqueNames) {
    spaces[name] = buildSpaceCheck(name, false, []);
    dataSets[name] = buildDataSetCheck(name, false, [], 0);
  }

  for (let index = 0; index < uniqueNames.length; index += 100) {
    const batch = uniqueNames.slice(index, index + 100);
    const response = await withTransientRetries(() => sdk.spaces.retrieve(batch));
    const retrieveCall: SpaceProbeApiCall = {
      api: "POST /models/spaces/byids",
      request: batch,
      response: { items: response.items ?? [] },
    };
    const found = new Set((response.items ?? []).map((space) => space.space));
    for (const space of batch) {
      spaces[space] = buildSpaceCheck(space, found.has(space), [retrieveCall]);
    }
  }

  const listApiCalls: SpaceProbeApiCall[] = [];
  const foundDataSetNames = new Set<string>();
  let cursor: string | undefined;

  do {
    const request = { limit: 1000, cursor };
    const response = await sdk.post<{
      items?: Array<{ id: number; externalId?: string; name?: string }>;
      nextCursor?: string | null;
    }>(`/api/v1/projects/${sdk.project}/datasets/list`, {
      data: request,
    });
    const items = response.data?.items ?? [];
    listApiCalls.push({
      api: `POST /api/v1/projects/${sdk.project}/datasets/list`,
      request,
      response: {
        items: items.map((item) => ({
          id: item.id,
          externalId: item.externalId,
          name: item.name,
        })),
        nextCursor: response.data?.nextCursor ?? null,
      },
    });
    for (const item of items) {
      if (typeof item.name === "string" && item.name.length > 0) {
        foundDataSetNames.add(item.name);
      }
    }
    cursor = response.data?.nextCursor ?? undefined;
  } while (cursor);

  for (const name of uniqueNames) {
    dataSets[name] = buildDataSetCheck(name, foundDataSetNames.has(name), listApiCalls, foundDataSetNames.size);
  }

  return { spaces, dataSets };
}
