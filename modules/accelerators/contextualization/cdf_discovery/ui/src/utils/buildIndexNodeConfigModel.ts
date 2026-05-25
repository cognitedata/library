/** Helpers for build_index node ``data.config.index_kinds``. */

import type { JsonObject } from "../types/jsonConfig";

export type IndexKindRow = {
  kind: string;
  properties: string[];
};

export function defaultIndexKindRow(): IndexKindRow {
  return { kind: "", properties: [""] };
}

export function metadataIndexKeyPreset(): IndexKindRow {
  return { kind: "metadata", properties: ["indexKey"] };
}

export function indexKindsStructuredEditable(raw: unknown): raw is Record<string, unknown> {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return false;
  for (const value of Object.values(raw as Record<string, unknown>)) {
    if (!Array.isArray(value)) return false;
    for (const item of value) {
      if (item !== null && typeof item !== "string" && typeof item !== "number") return false;
    }
  }
  return true;
}

export function rowsFromIndexKinds(raw: unknown): IndexKindRow[] {
  if (!indexKindsStructuredEditable(raw)) return [];
  const out: IndexKindRow[] = [];
  for (const [kind, props] of Object.entries(raw)) {
    const kindName = String(kind ?? "").trim();
    if (!kindName) continue;
    const properties: string[] = [];
    if (Array.isArray(props)) {
      for (const prop of props) {
        const name = String(prop ?? "").trim();
        if (name) properties.push(name);
      }
    }
    out.push({ kind: kindName, properties: properties.length > 0 ? properties : [""] });
  }
  return out;
}

export function indexKindsToConfig(rows: IndexKindRow[]): JsonObject | undefined {
  const out: Record<string, string[]> = {};
  for (const row of rows) {
    const kind = row.kind.trim();
    if (!kind) continue;
    const props = row.properties.map((p) => p.trim()).filter(Boolean);
    if (props.length === 0) continue;
    out[kind] = props;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

export function parseIndexKindsJson(text: string): IndexKindRow[] | null {
  const trimmed = text.trim();
  if (!trimmed) return [];
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (!indexKindsStructuredEditable(parsed)) return null;
    return rowsFromIndexKinds(parsed);
  } catch {
    return null;
  }
}

export function readIndexKinds(config: Record<string, unknown>): unknown {
  return config.index_kinds;
}

export function indexKindRowCount(config: Record<string, unknown>): number {
  return rowsFromIndexKinds(readIndexKinds(config)).length;
}

export function indexKindPairCount(config: Record<string, unknown>): number {
  let n = 0;
  for (const row of rowsFromIndexKinds(readIndexKinds(config))) {
    n += row.properties.map((p) => p.trim()).filter(Boolean).length;
  }
  return n;
}

export function buildIndexSummary(config: Record<string, unknown>): string {
  const rows = rowsFromIndexKinds(readIndexKinds(config));
  const handler = String(config.handler_id ?? config.handler ?? "").trim();
  const parts: string[] = [];
  if (handler) parts.push(handler);
  if (rows.length > 0) {
    parts.push(
      ...rows.map((row) => {
        const props = row.properties.map((p) => p.trim()).filter(Boolean);
        if (props.length === 0) return row.kind.trim();
        return `${row.kind.trim()}:${props.join(",")}`;
      })
    );
  }
  return parts.filter(Boolean).join(" · ");
}
