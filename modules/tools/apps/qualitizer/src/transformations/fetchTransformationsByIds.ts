import type { CogniteClient } from "@cognite/sdk";

const BATCH_SIZE = 100;

export type TransformationByIdsRow = {
  id?: number | string;
  name?: string;
  query?: string;
  destination?: {
    view?: { space?: string; externalId?: string; version?: string };
    dataModel?: {
      space?: string;
      externalId?: string;
      version?: string;
      destinationType?: string;
    };
  };
};

function buildByIdsPayload(ids: string[]): Array<{ id: number } | { id: string }> {
  const items: Array<{ id: number } | { id: string }> = [];
  for (const raw of ids) {
    if (!raw) continue;
    const n = Number(raw);
    if (Number.isFinite(n) && String(n) === raw) items.push({ id: n });
    else items.push({ id: raw });
  }
  return items;
}

/** Load full transformation rows (query, destination, …) via POST …/transformations/byids. */
export async function fetchTransformationsByIds(
  sdk: CogniteClient,
  project: string,
  ids: string[]
): Promise<Map<string, TransformationByIdsRow>> {
  const out = new Map<string, TransformationByIdsRow>();
  const unique = [...new Set(ids.filter(Boolean))];
  for (let i = 0; i < unique.length; i += BATCH_SIZE) {
    const chunk = unique.slice(i, i + BATCH_SIZE);
    const items = buildByIdsPayload(chunk);
    if (items.length === 0) continue;
    try {
      const response = (await sdk.post(
        `/api/v1/projects/${project}/transformations/byids`,
        { data: { items } }
      )) as { data?: { items?: TransformationByIdsRow[] } };
      for (const row of response.data?.items ?? []) {
        if (row.id != null) out.set(String(row.id), row);
      }
    } catch {
      /* skip batch */
    }
  }
  return out;
}
