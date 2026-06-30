export type IndexHitRow = {
  normalized_term?: string;
  term?: string;
  source_type?: string;
  reference_external_id?: string;
  confidence?: number | string;
  scope_key?: string;
};

export function asHitRows(hits: unknown[]): IndexHitRow[] {
  return hits.map((h) => (typeof h === "object" && h != null ? (h as IndexHitRow) : {}));
}

export function queryHits(result: unknown): IndexHitRow[] {
  if (Array.isArray(result)) return asHitRows(result);
  if (result && typeof result === "object" && "hits" in result) {
    const hits = (result as { hits: unknown }).hits;
    return Array.isArray(hits) ? asHitRows(hits) : [];
  }
  return [];
}

export function queryReuseMetrics(result: unknown): Record<string, unknown> | null {
  if (!result || typeof result !== "object" || Array.isArray(result)) return null;
  const reuse = (result as { reuse_metrics?: unknown }).reuse_metrics;
  if (!reuse || typeof reuse !== "object" || Array.isArray(reuse)) return null;
  return reuse as Record<string, unknown>;
}

export function queryByTermRows(result: unknown): Record<string, unknown>[] {
  const metrics = queryReuseMetrics(result);
  if (!metrics) return [];
  const byTerm = metrics.by_term;
  return Array.isArray(byTerm) ? (byTerm as Record<string, unknown>[]) : [];
}

export function buildDryRunRows(result: unknown): Record<string, unknown>[] {
  if (!result || typeof result !== "object" || Array.isArray(result)) return [];
  const results = (result as { results?: unknown }).results;
  return Array.isArray(results) ? (results as Record<string, unknown>[]) : [];
}

export function rowSummaryFields(detail: unknown): { labelKey: string; value: string }[] {
  if (!detail || typeof detail !== "object") return [];
  const row = detail as Record<string, unknown>;
  const fields: { labelKey: string; value: string }[] = [];
  const term = row.normalized_term ?? row.term;
  if (term != null) fields.push({ labelKey: "properties.summary.term", value: String(term) });
  if (row.source_type != null) {
    fields.push({ labelKey: "properties.summary.sourceType", value: String(row.source_type) });
  }
  if (row.reference_external_id != null) {
    fields.push({ labelKey: "properties.summary.reference", value: String(row.reference_external_id) });
  }
  if (row.confidence != null) {
    fields.push({ labelKey: "properties.summary.confidence", value: String(row.confidence) });
  }
  if (row.scope_key != null) {
    fields.push({ labelKey: "properties.summary.scopeKey", value: String(row.scope_key) });
  }
  const scopes = row.scope_keys ?? row.scopes ?? row.match_scope_keys;
  if (scopes != null) {
    fields.push({
      labelKey: "properties.summary.scopes",
      value: Array.isArray(scopes) ? scopes.join(", ") : String(scopes),
    });
  }
  return fields;
}
