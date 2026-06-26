import { readFetchError } from "./queryApi";

export type ViewSchemaField = {
  name: string;
  kind?: string;
  type?: string;
  list?: boolean;
  nullable?: boolean;
  target?: string;
  connectionType?: string;
};

export async function fetchViewSchema(
  space: string,
  externalId: string,
  version: string
): Promise<{ properties: string[]; fields: ViewSchemaField[] }> {
  const params = new URLSearchParams({ space, external_id: externalId, version });
  const r = await fetch(`/api/transform/data-modeling/view/properties?${params.toString()}`);
  if (!r.ok) throw new Error(await readFetchError(r));
  const body = (await r.json()) as { properties?: string[]; fields?: ViewSchemaField[] };
  return {
    properties: Array.isArray(body.properties) ? body.properties : [],
    fields: Array.isArray(body.fields) ? body.fields : [],
  };
}

export async function fetchViewProperties(
  space: string,
  externalId: string,
  version: string
): Promise<string[]> {
  const { properties } = await fetchViewSchema(space, externalId, version);
  return properties;
}
