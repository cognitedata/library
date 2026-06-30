import type { JsonObject } from "../types/jsonConfig";

export type ResolveNodePath = {
  space: string;
  externalId: string;
};

export type ResolveRule = {
  whenReferenceTypes: string[];
  space: string;
  externalId: string;
  fallback?: ResolveNodePath;
};

export type ResolveSideMode = "incoming_instance" | "rules";

export type ResolveSideConfig =
  | { mode: "incoming_instance" }
  | { mode: "rules"; rules: ResolveRule[] };

export type ResolveIncomingViewEntry = {
  forward: ResolveSideConfig;
  target: ResolveSideConfig;
};

export type ResolveByIncomingView = Record<string, ResolveIncomingViewEntry>;

function asRecord(value: unknown): Record<string, unknown> {
  return value != null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function stringList(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((v) => String(v)).filter((v) => v.length > 0);
}

export function emptyResolveRule(): ResolveRule {
  return {
    whenReferenceTypes: [],
    space: "reference_space",
    externalId: "reference_external_id",
  };
}

export function emptyResolveIncomingViewEntry(): ResolveIncomingViewEntry {
  return {
    forward: { mode: "incoming_instance" },
    target: { mode: "incoming_instance" },
  };
}

function parseResolveNodePath(raw: unknown): ResolveNodePath | undefined {
  const o = asRecord(raw);
  const space = o.space != null ? String(o.space) : "";
  const externalId = o.external_id != null ? String(o.external_id) : "";
  if (!space && !externalId) return undefined;
  return { space, externalId };
}

function parseResolveRule(raw: unknown): ResolveRule {
  const o = asRecord(raw);
  const fallback = parseResolveNodePath(o.fallback);
  return {
    whenReferenceTypes: stringList(o.when_reference_types),
    space: o.space != null ? String(o.space) : "",
    externalId: o.external_id != null ? String(o.external_id) : "",
    ...(fallback ? { fallback } : {}),
  };
}

export function parseResolveSide(raw: unknown): ResolveSideConfig {
  const o = asRecord(raw);
  if (o.source === "incoming_instance") {
    return { mode: "incoming_instance" };
  }
  const rulesRaw = o.rules;
  if (Array.isArray(rulesRaw) && rulesRaw.length > 0) {
    return { mode: "rules", rules: rulesRaw.map(parseResolveRule) };
  }
  return { mode: "rules", rules: [emptyResolveRule()] };
}

function parseResolveIncomingViewEntry(raw: unknown): ResolveIncomingViewEntry {
  const o = asRecord(raw);
  return {
    forward: parseResolveSide(o.forward),
    target: parseResolveSide(o.target),
  };
}

export function parseResolveByIncomingView(raw: unknown): ResolveByIncomingView {
  const o = asRecord(raw);
  const out: ResolveByIncomingView = {};
  for (const [key, entry] of Object.entries(o)) {
    out[key] = parseResolveIncomingViewEntry(entry);
  }
  return out;
}

function serializeResolveNodePath(path: ResolveNodePath): Record<string, string> {
  return {
    space: path.space,
    external_id: path.externalId,
  };
}

function serializeResolveRule(rule: ResolveRule): Record<string, unknown> {
  const out: Record<string, unknown> = {
    when_reference_types: rule.whenReferenceTypes,
    space: rule.space,
    external_id: rule.externalId,
  };
  if (rule.fallback?.space || rule.fallback?.externalId) {
    out.fallback = serializeResolveNodePath(rule.fallback);
  }
  return out;
}

export function serializeResolveSide(side: ResolveSideConfig): Record<string, unknown> {
  if (side.mode === "incoming_instance") {
    return { source: "incoming_instance" };
  }
  return {
    rules: side.rules.map(serializeResolveRule),
  };
}

export function serializeResolveIncomingViewEntry(
  entry: ResolveIncomingViewEntry
): Record<string, unknown> {
  return {
    forward: serializeResolveSide(entry.forward),
    target: serializeResolveSide(entry.target),
  };
}

export function serializeResolveByIncomingView(map: ResolveByIncomingView): JsonObject {
  const out: JsonObject = {};
  for (const [key, entry] of Object.entries(map)) {
    out[key] = serializeResolveIncomingViewEntry(entry);
  }
  return out;
}

export function resolveEntryForView(
  map: ResolveByIncomingView,
  viewKey: string
): ResolveIncomingViewEntry {
  return map[viewKey] ?? emptyResolveIncomingViewEntry();
}

export function syncResolveMapWithIncomingViews(
  map: ResolveByIncomingView,
  incomingViews: string[]
): ResolveByIncomingView {
  const next: ResolveByIncomingView = {};
  for (const key of incomingViews) {
    next[key] = map[key] ?? emptyResolveIncomingViewEntry();
  }
  return next;
}
