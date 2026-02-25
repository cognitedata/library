import { useQuery } from "@tanstack/react-query";
import type { DataModel } from "@cognite/sdk";
import { cdfDataModelToDocModel } from "@/lib/cdfToDocModel";
import type { DocModel } from "@/types/dataModel";

export interface DataModelSelection {
  space: string;
  externalId: string;
  version: string;
}

/** Collect unique container refs from inline views. Use data model space when container.space is missing. */
function getContainerRefsFromModel(dm: DataModel): { space: string; externalId: string }[] {
  const refs = new Map<string, { space: string; externalId: string }>();
  const modelSpace = dm.space ?? "";
  const views = (dm.views ?? []) as Array<{ properties?: Record<string, { container?: { space?: string; externalId?: string } }> }>;
  for (const v of views) {
    if (!v?.properties || typeof v.properties !== "object") continue;
    for (const prop of Object.values(v.properties)) {
      const c = prop?.container;
      if (!c?.externalId) continue;
      const space = c.space ?? modelSpace;
      const key = `${space}|${c.externalId}`;
      if (!refs.has(key)) refs.set(key, { space, externalId: c.externalId });
    }
  }
  return Array.from(refs.values());
}

/** Minimal shape we read from a container definition (SDK returns ContainerDefinition). */
interface ContainerShape {
  space?: string;
  externalId?: string;
  properties?: Record<string, { type?: string }>;
}

async function fetchAndTransform(
  client: {
    dataModels: { retrieve: (ids: DataModelSelection[], opts?: { inlineViews?: boolean }) => Promise<{ items: DataModel[] }> };
    containers?: { retrieve: (items: { space: string; externalId: string }[]) => Promise<unknown> };
  },
  selection: DataModelSelection
): Promise<DocModel> {
  const res = await client.dataModels.retrieve([selection], { inlineViews: true });
  const dm = res.items?.[0];
  if (!dm) throw new Error("Data model not found");

  let containersByKey: Record<string, { properties: Record<string, { type?: string }> }> = {};
  const refs = getContainerRefsFromModel(dm);
  if (refs.length > 0 && client.containers?.retrieve) {
    try {
      const containerRes = (await client.containers.retrieve(refs)) as { items?: ContainerShape[] };
      const items = containerRes.items ?? [];
      const modelSpace = dm.space ?? "";
      for (const c of items) {
        const externalId = c.externalId ?? "";
        if (externalId) {
          const space = c.space ?? modelSpace;
          const key = `${space}|${externalId}`;
          containersByKey[key] = { properties: c.properties ?? {} };
        }
      }
    } catch {
      // If container fetch fails (e.g. no permission), continue without container types
    }
  }

  return cdfDataModelToDocModel(dm, containersByKey);
}

export function useDataModelDoc(
  sdk: ReturnType<typeof import("@cognite/dune").useDune>["sdk"] | null,
  selection: DataModelSelection | null
) {
  return useQuery({
    queryKey: ["dataModelDoc", selection?.space, selection?.externalId, selection?.version],
    queryFn: () => (sdk && selection ? fetchAndTransform(sdk, selection) : Promise.reject(new Error("No selection"))),
    enabled: !!sdk && !!selection?.space && !!selection?.externalId && !!selection?.version,
  });
}
