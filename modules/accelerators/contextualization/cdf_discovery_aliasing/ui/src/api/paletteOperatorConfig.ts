export type PaletteOperatorConfig = {
  stars: { node_ids: string[] };
};

export async function fetchPaletteOperatorConfig(): Promise<PaletteOperatorConfig> {
  const r = await fetch("/api/cdf/palette/operator-config");
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<PaletteOperatorConfig>;
}

export async function savePaletteStars(
  nodeIds: string[]
): Promise<{ stars: { node_ids: string[] } }> {
  const r = await fetch("/api/cdf/palette/config/stars", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ node_ids: nodeIds }),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ stars: { node_ids: string[] } }>;
}
