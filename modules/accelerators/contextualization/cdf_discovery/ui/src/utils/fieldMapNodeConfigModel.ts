import type { JsonObject } from "../types/jsonConfig";
import {
  type FieldMappingRow,
  readFieldMappings,
  inferSourceSqlHint,
  type CanvasFieldGraphContext,
  resolveAvailableInputFields,
  resolveSuggestedOutputFields,
} from "./canvasFieldGraph";

export type { FieldMappingRow };

function quoteSqlIdent(name: string): string {
  const s = name.trim();
  if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s)) return s;
  return `\`${s.replace(/`/g, "``")}\``;
}

export function parseFieldMappings(cfg: Record<string, unknown>): FieldMappingRow[] {
  return readFieldMappings(cfg);
}

export function mappingsToConfig(mappings: FieldMappingRow[]): JsonObject[] {
  return mappings
    .filter((m) => m.input_field.trim() || m.output_field.trim())
    .map((m) => ({
      input_field: m.input_field.trim(),
      output_field: m.output_field.trim(),
    }));
}

export function defaultFieldMapNodeConfig(): Record<string, unknown> {
  return {
    description: "Field map",
    enabled: true,
    mappings: [],
  };
}

/** Merge graph suggestions with saved mappings without overwriting existing rows. */
export function mergeMappingsWithSuggestions(
  existing: FieldMappingRow[],
  suggestedInputs: string[],
  suggestedOutputs: string[]
): FieldMappingRow[] {
  const rows = existing.map((r) => ({ ...r }));
  const usedOutputs = new Set(rows.map((r) => r.output_field.trim()).filter(Boolean));
  const usedInputs = new Set(rows.map((r) => r.input_field.trim()).filter(Boolean));

  for (const out of suggestedOutputs) {
    if (usedOutputs.has(out)) continue;
    const match = suggestedInputs.find((inp) => inp === out || !usedInputs.has(inp));
    rows.push({
      input_field: match && !usedInputs.has(match) ? match : "",
      output_field: out,
    });
    usedOutputs.add(out);
    if (match) usedInputs.add(match);
  }

  for (const inp of suggestedInputs) {
    if (usedInputs.has(inp)) continue;
    rows.push({ input_field: inp, output_field: "" });
    usedInputs.add(inp);
  }

  return rows;
}

export function buildConceptualSqlPreview(
  mappings: FieldMappingRow[],
  sourceHint: string
): string {
  const complete = mappings.filter((m) => m.input_field.trim() && m.output_field.trim());
  if (complete.length === 0) {
    return `-- SELECT input_field AS output_field\n-- FROM ${sourceHint}`;
  }
  const selectParts = complete.map(
    (m) => `${quoteSqlIdent(m.input_field)} AS ${quoteSqlIdent(m.output_field)}`
  );
  const fromClause = sourceHint.includes("SELECT") ? `(\n  ${sourceHint.replace(/\n/g, "\n  ")}\n) AS _source` : sourceHint;
  return `SELECT\n  ${selectParts.join(",\n  ")}\nFROM ${fromClause}`;
}

export function fieldMapSummary(config: Record<string, unknown>): string {
  const mappings = parseFieldMappings(config);
  const complete = mappings.filter((m) => m.input_field.trim() && m.output_field.trim()).length;
  return `${complete}/${mappings.length}`;
}

export function syncMappingsFromGraph(
  config: Record<string, unknown>,
  ctx: CanvasFieldGraphContext
): Record<string, unknown> {
  const existing = parseFieldMappings(config);
  const inputs = resolveAvailableInputFields(ctx);
  const outputs = resolveSuggestedOutputFields(ctx);
  const merged = mergeMappingsWithSuggestions(existing, inputs, outputs);
  return { ...config, mappings: mappingsToConfig(merged) };
}

export function buildSqlPreviewForConfig(
  config: Record<string, unknown>,
  ctx: CanvasFieldGraphContext
): string {
  const mappings = parseFieldMappings(config);
  const source = inferSourceSqlHint(ctx);
  return buildConceptualSqlPreview(mappings, source);
}
