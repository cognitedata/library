import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/jsonConfig";

export type StreamSaveValidation = {
  ok: boolean;
  issues: MessageKey[];
};

export function hasStreamDefinitionOverride(cfg: Record<string, unknown>): boolean {
  const raw = cfg.stream_definition;
  if (raw && typeof raw === "object" && !Array.isArray(raw)) return true;
  if (typeof raw === "string" && raw.trim()) {
    try {
      const parsed = JSON.parse(raw);
      return Boolean(parsed && typeof parsed === "object" && !Array.isArray(parsed));
    } catch {
      return true;
    }
  }
  return false;
}

export function validateStreamSaveConfig(cfg: Record<string, unknown>): StreamSaveValidation {
  const issues: MessageKey[] = [];
  if (hasStreamDefinitionOverride(cfg)) {
    if (typeof cfg.stream_definition === "string" && cfg.stream_definition.trim()) {
      try {
        JSON.parse(cfg.stream_definition);
      } catch {
        issues.push("transform.save.streamErrorDefinitionJson");
      }
    }
    return { ok: issues.length === 0, issues };
  }
  const ext = String(cfg.stream_external_id ?? cfg.externalId ?? "").trim();
  if (!ext) issues.push("transform.save.streamErrorExternalId");
  return { ok: issues.length === 0, issues };
}

export function streamSaveSummary(cfg: Record<string, unknown>): string {
  const ext = String(cfg.stream_external_id ?? cfg.externalId ?? "").trim();
  if (!ext) return String(cfg.description ?? "").trim();
  const space = String(cfg.stream_space ?? cfg.space ?? "").trim();
  return space ? `${space}/${ext}` : ext;
}

export function readStreamSources(cfg: Record<string, unknown>): JsonObject[] {
  const raw = cfg.sources;
  if (!Array.isArray(raw)) return [];
  return raw.filter((x) => x && typeof x === "object" && !Array.isArray(x)) as JsonObject[];
}
