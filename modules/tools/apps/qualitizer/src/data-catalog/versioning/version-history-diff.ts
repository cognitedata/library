import type { DmVersionSnapshot, PropChange, TransitionDiff, ViewRef, ViewVersionDiff } from "./version-history-types";
import { truncJson } from "./version-history-utils";

export function parseViewsArray(views: unknown): ViewRef[] {
  if (!Array.isArray(views)) return [];
  const out: ViewRef[] = [];
  for (const v of views) {
    if (v && typeof v === "object" && "space" in v && "externalId" in v) {
      const o = v as { space: string; externalId: string; version?: string; name?: string };
      out.push({
        key: `${o.space}:${o.externalId}`,
        version: String(o.version ?? ""),
        raw: v,
      });
    }
  }
  return out;
}

export function isLikelyFullViewDef(v: unknown): v is Record<string, unknown> {
  if (!v || typeof v !== "object") return false;
  const o = v as Record<string, unknown>;
  return "properties" in o && typeof o.properties === "object" && o.properties !== null;
}

function implementsSignature(def: Record<string, unknown>): string {
  const imp = def.implements;
  if (!Array.isArray(imp)) return "";
  const parts: string[] = [];
  for (const x of imp) {
    if (x && typeof x === "object" && "space" in x && "externalId" in x) {
      const r = x as { space: string; externalId: string; version?: string };
      parts.push(`${r.space}:${r.externalId}@${r.version ?? "?"}`);
    }
  }
  return parts.join(" | ");
}

function isViewRefNode(o: unknown): o is { type: string; space: string; externalId: string; version?: string } {
  if (!o || typeof o !== "object") return false;
  const x = o as Record<string, unknown>;
  return x.type === "view" && typeof x.space === "string" && typeof x.externalId === "string";
}

function isTypeRefNode(o: unknown): o is { space: string; externalId: string } {
  if (!o || typeof o !== "object") return false;
  const x = o as Record<string, unknown>;
  return typeof x.space === "string" && typeof x.externalId === "string";
}

function fmtScalar(v: unknown): string {
  if (v === null || v === undefined) return String(v);
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") return String(v);
  return truncJson(v, 120);
}

function explainViewRefDelta(before: unknown, after: unknown): string[] {
  if (!isViewRefNode(before) || !isViewRefNode(after)) return [];
  if (before.space === after.space && before.externalId === after.externalId) {
    if (before.version !== after.version) {
      return [
        `${before.externalId} (${before.space}): ${before.version ?? "?"} → ${after.version ?? "?"}`,
      ];
    }
    return [];
  }
  const a = `${before.externalId} (${before.space})${before.version != null ? ` ${before.version}` : ""}`;
  const b = `${after.externalId} (${after.space})${after.version != null ? ` ${after.version}` : ""}`;
  return [`View reference: ${a} → ${b}`];
}

function explainTypeRefDelta(before: unknown, after: unknown): string[] {
  if (!isTypeRefNode(before) || !isTypeRefNode(after)) return [];
  if (before.space === after.space && before.externalId === after.externalId) return [];
  return [
    `Type node: ${before.externalId} (${before.space}) → ${after.externalId} (${after.space})`,
  ];
}

function collectLeafDiffs(before: unknown, after: unknown, path: string): string[] {
  if (before === after) return [];
  const vr = explainViewRefDelta(before, after);
  if (vr.length && isViewRefNode(before) && isViewRefNode(after)) {
    return vr;
  }
  if (
    before == null ||
    after == null ||
    typeof before !== "object" ||
    typeof after !== "object" ||
    Array.isArray(before) ||
    Array.isArray(after)
  ) {
    return [`${path}: ${fmtScalar(before)} → ${fmtScalar(after)}`];
  }
  const oa = before as Record<string, unknown>;
  const ob = after as Record<string, unknown>;
  const keys = new Set([...Object.keys(oa), ...Object.keys(ob)]);
  const out: string[] = [];
  for (const k of [...keys].sort()) {
    if (JSON.stringify(oa[k]) === JSON.stringify(ob[k])) continue;
    out.push(...collectLeafDiffs(oa[k], ob[k], path ? `${path}.${k}` : k));
  }
  return out;
}

function humanizeViewPropertyChange(propName: string, before: unknown, after: unknown): string[] {
  if (before == null || after == null) return [];
  if (typeof before !== "object" || typeof after !== "object") {
    return before === after ? [] : [`${propName}: ${fmtScalar(before)} → ${fmtScalar(after)}`];
  }
  const ba = before as Record<string, unknown>;
  const bb = after as Record<string, unknown>;

  if (
    "container" in ba &&
    "containerPropertyIdentifier" in ba &&
    "container" in bb &&
    "containerPropertyIdentifier" in bb
  ) {
    const lines: string[] = [];
    const cId = (o: Record<string, unknown>) =>
      `${truncJson(o.container, 80)} · ${String(o.containerPropertyIdentifier)}`;
    if (JSON.stringify(ba.container) !== JSON.stringify(bb.container)) {
      lines.push(`${propName} mapping: container ${cId(ba)} → ${cId(bb)}`);
    } else if (ba.containerPropertyIdentifier !== bb.containerPropertyIdentifier) {
      lines.push(
        `${propName}: container property ${String(ba.containerPropertyIdentifier)} → ${String(bb.containerPropertyIdentifier)}`
      );
    }
    lines.push(...collectLeafDiffs(ba.type, bb.type, `${propName}.type`));
    if (ba.name !== bb.name || ba.description !== bb.description) {
      if (ba.name !== bb.name) {
        lines.push(`${propName}.name: ${fmtScalar(ba.name)} → ${fmtScalar(bb.name)}`);
      }
      if (ba.description !== bb.description) {
        lines.push(`${propName}.description: changed`);
      }
    }
    return lines.filter(Boolean);
  }

  const lines: string[] = [];
  lines.push(...explainViewRefDelta(ba.source, bb.source).map((s) => `${propName}: ${s}`));
  lines.push(...explainTypeRefDelta(ba.type, bb.type).map((s) => `${propName}: ${s}`));
  if (ba.connectionType !== bb.connectionType) {
    lines.push(`${propName}.connectionType: ${fmtScalar(ba.connectionType)} → ${fmtScalar(bb.connectionType)}`);
  }

  const skip = new Set(["source", "type", "connectionType"]);
  const keys = new Set([...Object.keys(ba), ...Object.keys(bb)]);
  for (const k of [...keys].sort()) {
    if (skip.has(k)) {
      if (k === "source" && isViewRefNode(ba.source) && isViewRefNode(bb.source)) {
        if (explainViewRefDelta(ba.source, bb.source).length) continue;
      }
      if (k === "type" && isTypeRefNode(ba.type) && isTypeRefNode(bb.type)) {
        if (explainTypeRefDelta(ba.type, bb.type).length) continue;
      }
      if (k === "connectionType") continue;
    }
    if (JSON.stringify(ba[k]) === JSON.stringify(bb[k])) continue;
    lines.push(...collectLeafDiffs(ba[k], bb[k], `${propName}.${k}`));
  }

  const seen = new Set<string>();
  return lines.filter((line) => (seen.has(line) ? false : (seen.add(line), true)));
}

function summarizeViewProperty(name: string, p: unknown): string {
  if (!p || typeof p !== "object") return `${name}: ${truncJson(p, 120)}`;
  const o = p as Record<string, unknown>;
  if ("container" in o && "containerPropertyIdentifier" in o) {
    return `${name}: ${truncJson(
      {
        container: o.container,
        containerPropertyIdentifier: o.containerPropertyIdentifier,
        type: o.type,
        name: o.name,
        description: o.description,
      },
      220
    )}`;
  }
  if (isViewRefNode(p)) {
    return `${name}: view ${p.externalId} (${p.space})${p.version != null ? ` ${p.version}` : ""}`;
  }
  if ("connectionType" in o && (o.source != null || o.type != null)) {
    const bits: string[] = [];
    if (typeof o.connectionType === "string") bits.push(o.connectionType);
    if (isViewRefNode(o.source)) {
      bits.push(`source ${o.source.externalId}@${o.source.version ?? "?"}`);
    }
    if (isTypeRefNode(o.type)) {
      bits.push(`type ${o.type.externalId}`);
    }
    return `${name}: ${bits.join(" · ")}`;
  }
  return `${name}: ${truncJson(p, 200)}`;
}

function diffViewDefinitions(
  prev: Record<string, unknown>,
  next: Record<string, unknown>,
  viewLabel: string,
  fromVersion: string,
  toVersion: string
): ViewVersionDiff {
  const metaChanges: string[] = [];
  const metaPairs: Array<[string, unknown, unknown]> = [
    ["name", prev.name, next.name],
    ["description", prev.description, next.description],
    ["usedFor", prev.usedFor, next.usedFor],
    ["writable", prev.writable, next.writable],
    ["queryable", prev.queryable, next.queryable],
  ];
  for (const [k, a, b] of metaPairs) {
    if (JSON.stringify(a) !== JSON.stringify(b)) {
      metaChanges.push(`${k}: ${truncJson(a, 80)} → ${truncJson(b, 80)}`);
    }
  }

  const fPrev = prev.filter;
  const fNext = next.filter;
  const filterChanged = JSON.stringify(fPrev) !== JSON.stringify(fNext);

  const impPrev = implementsSignature(prev);
  const impNext = implementsSignature(next);
  if (impPrev !== impNext) {
    metaChanges.push(`implements: ${impPrev || "(none)"} → ${impNext || "(none)"}`);
  }

  const pPrev = (prev.properties as Record<string, unknown>) ?? {};
  const pNext = (next.properties as Record<string, unknown>) ?? {};
  const keys = new Set([...Object.keys(pPrev), ...Object.keys(pNext)]);
  const propChanges: PropChange[] = [];
  for (const name of [...keys].sort()) {
    const a = pPrev[name];
    const b = pNext[name];
    if (a === undefined && b !== undefined) {
      propChanges.push({ kind: "add", name, after: summarizeViewProperty(name, b) });
    } else if (a !== undefined && b === undefined) {
      propChanges.push({ kind: "remove", name, before: summarizeViewProperty(name, a) });
    } else if (JSON.stringify(a) !== JSON.stringify(b)) {
      const semanticLines = humanizeViewPropertyChange(name, a, b);
      if (semanticLines.length > 0) {
        propChanges.push({ kind: "modify", name, semanticLines });
      } else {
        propChanges.push({
          kind: "modify",
          name,
          before: summarizeViewProperty(name, a),
          after: summarizeViewProperty(name, b),
        });
      }
    }
  }

  return {
    viewLabel,
    fromVersion,
    toVersion,
    metaChanges,
    propChanges,
    filterChanged,
  };
}

export function buildTransitionDiff(prev: DmVersionSnapshot, next: DmVersionSnapshot): TransitionDiff {
  const modelMetaChanges: string[] = [];
  if ((prev.name ?? "") !== (next.name ?? "")) {
    modelMetaChanges.push(`name: ${prev.name ?? ""} → ${next.name ?? ""}`);
  }
  if ((prev.description ?? "") !== (next.description ?? "")) {
    modelMetaChanges.push(
      `description: ${truncJson(prev.description, 60)} → ${truncJson(next.description, 60)}`
    );
  }

  const a = parseViewsArray(prev.views);
  const b = parseViewsArray(next.views);
  const mapA = new Map(a.map((x) => [x.key, x]));
  const mapB = new Map(b.map((x) => [x.key, x]));

  const viewsRemoved: ViewRef[] = [];
  const viewsAdded: ViewRef[] = [];
  const viewVersionChanges: TransitionDiff["viewVersionChanges"] = [];

  for (const [k, ref] of mapA) {
    if (!mapB.has(k)) viewsRemoved.push(ref);
  }
  for (const [k, ref] of mapB) {
    if (!mapA.has(k)) viewsAdded.push(ref);
  }

  for (const [k, br] of mapB) {
    const ar = mapA.get(k);
    if (!ar) continue;
    if (ar.version === br.version) continue;

    const colon = k.indexOf(":");
    const space = colon >= 0 ? k.slice(0, colon) : "";
    const externalId = colon >= 0 ? k.slice(colon + 1) : k;
    const rawA = ar.raw;
    const rawB = br.raw;
    const label =
      isLikelyFullViewDef(rawB) && typeof rawB.name === "string"
        ? `${rawB.name} (${space}/${externalId})`
        : `${space}/${externalId}`;

    let viewDiff: ViewVersionDiff | null = null;
    if (isLikelyFullViewDef(rawA) && isLikelyFullViewDef(rawB)) {
      viewDiff = diffViewDefinitions(rawA, rawB, label, ar.version, br.version);
    }

    viewVersionChanges.push({ ref: br, prevRef: ar, viewDiff });
  }

  return {
    fromVersion: prev.version,
    toVersion: next.version,
    fromSnap: prev,
    toSnap: next,
    modelMetaChanges,
    viewsAdded,
    viewsRemoved,
    viewVersionChanges,
  };
}

export function snapshotForKey(
  dmKey: string,
  version: string,
  detailsMap: ReadonlyMap<string, DmVersionSnapshot>,
  fallback: DmVersionSnapshot | undefined
): DmVersionSnapshot | undefined {
  const k = `${dmKey}:${version}`;
  return detailsMap.get(k) ?? fallback;
}
