import type { CogniteClient } from "@cognite/sdk";
import { evaluateCfihosOilAndGasDerivativeFromModels } from "./cfihos-oil-and-gas-derivative";
import {
  evaluateIsaManufacturingDerivativeFromModels,
  extractViewExternalIdsFromDataModelViews,
  type IsaDataModelSummary,
} from "./isa-manufacturing-derivative";
import type { DataModelRef, DeploymentPackProbeContext } from "./types";

function dataModelExactKey(ref: DataModelRef): string {
  return `${ref.space}:${ref.externalId}:${ref.version ?? ""}`;
}

async function loadFunctionExternalIds(sdk: CogniteClient, project: string): Promise<Set<string>> {
  const keys = new Set<string>();
  let cursor: string | undefined;
  do {
    const response = await sdk.post<{
      items?: Array<{ id?: string; name?: string; externalId?: string }>;
      nextCursor?: string | null;
    }>(`/api/v1/projects/${encodeURIComponent(project)}/functions/list`, {
      data: JSON.stringify({ limit: 100, cursor }),
    });
    for (const item of response.data?.items ?? []) {
      if (item.externalId) keys.add(item.externalId);
      if (item.name) keys.add(item.name);
      if (item.id) keys.add(item.id);
    }
    cursor = response.data?.nextCursor ?? undefined;
  } while (cursor);
  return keys;
}

async function loadDataModelInventory(sdk: CogniteClient): Promise<{
  exact: Set<string>;
  anyVersion: Set<string>;
  models: IsaDataModelSummary[];
}> {
  const exact = new Set<string>();
  const anyVersion = new Set<string>();
  const models: IsaDataModelSummary[] = [];
  let cursor: string | undefined;
  do {
    const page = await sdk.dataModels.list({
      includeGlobal: true,
      allVersions: true,
      limit: 1000,
      cursor,
    });
    for (const m of page.items ?? []) {
      const space = m.space;
      const ext = m.externalId;
      if (!space || !ext) continue;
      anyVersion.add(`${space}:${ext}`);
      const ver = m.version != null ? String(m.version) : "";
      if (ver) exact.add(`${space}:${ext}:${ver}`);
      models.push({
        space,
        externalId: ext,
        version: ver || undefined,
        name: m.name,
        description: m.description,
        viewExternalIds: extractViewExternalIdsFromDataModelViews(m.views),
      });
    }
    cursor = page.nextCursor ?? undefined;
  } while (cursor);
  return { exact, anyVersion, models };
}

async function loadTransformationExternalIds(
  sdk: CogniteClient,
  project: string
): Promise<Set<string>> {
  const keys = new Set<string>();
  const response = (await sdk.get(
    `/api/v1/projects/${encodeURIComponent(project)}/transformations`,
    {
      params: { includePublic: "true", limit: "1000" },
    }
  )) as {
    data?: {
      items?: Array<{ id?: number | string; externalId?: string; name?: string }>;
    };
  };
  for (const item of response.data?.items ?? []) {
    if (item.externalId) keys.add(item.externalId);
    if (item.name) keys.add(item.name);
    keys.add(String(item.id));
  }
  return keys;
}

async function loadLocationFilterExternalIds(
  sdk: CogniteClient,
  project: string
): Promise<Set<string>> {
  const keys = new Set<string>();
  try {
    let cursor: string | undefined;
    do {
      const response = (await sdk.get(
        `/api/v1/projects/${encodeURIComponent(project)}/locations`,
        {
          params: {
            limit: "1000",
            ...(cursor ? { cursor } : {}),
          } as Record<string, string>,
        }
      )) as {
        data?: {
          items?: Array<{ externalId?: string }>;
          nextCursor?: string | null;
        };
      };
      for (const item of response.data?.items ?? []) {
        if (item.externalId) keys.add(item.externalId);
      }
      cursor = response.data?.nextCursor ?? undefined;
    } while (cursor);
  } catch {
    /* no Locations READ or endpoint differs */
  }
  return keys;
}

export async function fetchLiveDeploymentPackProbeContext(
  sdk: CogniteClient,
  project: string
): Promise<DeploymentPackProbeContext> {
  const [functions, dm, transformations, locationFilters] = await Promise.all([
    loadFunctionExternalIds(sdk, project),
    loadDataModelInventory(sdk),
    loadTransformationExternalIds(sdk, project),
    loadLocationFilterExternalIds(sdk, project),
  ]);

  return {
    async hasFunction(externalId) {
      return functions.has(externalId);
    },
    async hasDataModel(ref) {
      if (ref.version) {
        return dm.exact.has(dataModelExactKey(ref));
      }
      return dm.anyVersion.has(`${ref.space}:${ref.externalId}`);
    },
    async hasTransformation(externalId) {
      return transformations.has(externalId);
    },
    async hasLocationFilter(externalId) {
      return locationFilters.has(externalId);
    },
    async evaluateIsaManufacturingDerivative(rule) {
      return evaluateIsaManufacturingDerivativeFromModels(dm.models, rule);
    },
    async evaluateCfihosOilAndGasDerivative(rule) {
      return evaluateCfihosOilAndGasDerivativeFromModels(dm.models, rule);
    },
  };
}
