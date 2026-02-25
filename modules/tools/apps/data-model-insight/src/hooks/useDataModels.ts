import { useQuery } from "@tanstack/react-query";
import type { DataModel } from "@cognite/sdk";

export interface DataModelRef {
  space: string;
  externalId: string;
  version: string;
  name?: string;
}

function isDataModelCore(v: unknown): v is { space: string; externalId: string; version: string; name?: string } {
  return (
    v != null &&
    typeof v === "object" &&
    "space" in v &&
    "externalId" in v &&
    "version" in v &&
    typeof (v as { space: unknown }).space === "string" &&
    typeof (v as { externalId: unknown }).externalId === "string" &&
    typeof (v as { version: unknown }).version === "string"
  );
}

async function listAll(client: { dataModels: { list: (opts?: { space?: string; limit?: number }) => AsyncIterable<DataModel> } }, space?: string): Promise<DataModelRef[]> {
  const out: DataModelRef[] = [];
  const it = client.dataModels.list({ space, limit: 500 });
  for await (const dm of it) {
    if (isDataModelCore(dm)) {
      out.push({
        space: dm.space,
        externalId: dm.externalId,
        version: dm.version,
        name: dm.name,
      });
    }
  }
  return out;
}

export function useDataModels(sdk: ReturnType<typeof import("@cognite/dune").useDune>["sdk"] | null, space?: string) {
  return useQuery({
    queryKey: ["dataModels", space ?? "all"],
    queryFn: () => (sdk ? listAll(sdk, space) : Promise.resolve([])),
    enabled: !!sdk,
  });
}
