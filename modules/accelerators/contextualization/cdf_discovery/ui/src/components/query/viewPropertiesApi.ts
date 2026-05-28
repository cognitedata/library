async function readFetchError(r: Response): Promise<string> {
  let msg = r.statusText;
  try {
    const j = (await r.json()) as { detail?: unknown };
    const d = j?.detail;
    if (typeof d === "string") msg = d;
    else if (Array.isArray(d)) {
      msg = d
        .map((x) => (x && typeof x === "object" && "msg" in x ? String((x as { msg?: unknown }).msg) : ""))
        .filter(Boolean)
        .join("; ");
    }
  } catch {
    /* ignore */
  }
  return msg;
}

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
