import type { MouseEvent as ReactMouseEvent } from "react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useI18n } from "@/shared/i18n";
import { getDataModelUrl } from "@/shared/cdf-browser-url";
import { useAppSdk } from "@/shared/auth";
import { compareVersionStrings } from "./versioning-utils";

export type DmVersionSnapshot = {
  space: string;
  externalId: string;
  version: string;
  name?: string;
  description?: string;
  createdTime?: number;
  lastUpdatedTime?: number;
  views?: unknown;
};

type ViewRef = {
  key: string;
  version: string;
  raw: unknown;
};

type PropChange = {
  kind: "add" | "remove" | "modify";
  name: string;
  before?: string;
  after?: string;
  /** When set, UI shows these lines instead of raw before/after JSON */
  semanticLines?: string[];
};

type ViewVersionDiff = {
  viewLabel: string;
  fromVersion: string;
  toVersion: string;
  metaChanges: string[];
  propChanges: PropChange[];
  filterChanged: boolean;
};

type TransitionDiff = {
  fromVersion: string;
  toVersion: string;
  fromSnap: DmVersionSnapshot;
  toSnap: DmVersionSnapshot;
  modelMetaChanges: string[];
  viewsAdded: ViewRef[];
  viewsRemoved: ViewRef[];
  viewVersionChanges: Array<{ ref: ViewRef; prevRef: ViewRef; viewDiff: ViewVersionDiff | null }>;
};

function parseViewsArray(views: unknown): ViewRef[] {
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

function isLikelyFullViewDef(v: unknown): v is Record<string, unknown> {
  if (!v || typeof v !== "object") return false;
  const o = v as Record<string, unknown>;
  return "properties" in o && typeof o.properties === "object" && o.properties !== null;
}

function formatTs(ms?: number): string | null {
  if (ms == null || Number.isNaN(ms)) return null;
  try {
    return new Date(ms).toLocaleString();
  } catch {
    return null;
  }
}

function shouldShowUpdatedSeparate(created?: number, updated?: number): boolean {
  if (created == null || updated == null) return false;
  return updated !== created;
}

function truncJson(v: unknown, max = 280): string {
  try {
    const s = JSON.stringify(v);
    if (s.length <= max) return s;
    return `${s.slice(0, max)}…`;
  } catch {
    return String(v);
  }
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

/** Human-readable lines when a view reference (e.g. connection source) changed. */
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

function buildTransitionDiff(prev: DmVersionSnapshot, next: DmVersionSnapshot): TransitionDiff {
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

function snapshotForKey(
  dmKey: string,
  version: string,
  detailsMap: ReadonlyMap<string, DmVersionSnapshot>,
  fallback: DmVersionSnapshot | undefined
): DmVersionSnapshot | undefined {
  const k = `${dmKey}:${version}`;
  return detailsMap.get(k) ?? fallback;
}

const HEATMAP_VER_PROP_SEP = "\x1f";

function heatmapCellKey(version: string, propName: string): string {
  return `${version}${HEATMAP_VER_PROP_SEP}${propName}`;
}

function dmViewKey(space: string, externalId: string): string {
  return `${space}:${externalId}`;
}

function formatDmViewKey(key: string): string {
  const i = key.indexOf(":");
  if (i < 0) return key;
  return `${key.slice(0, i)}/${key.slice(i + 1)}`;
}

type EffectivePropEntry = {
  winnerKey: string;
  shadowed: string[];
};

type HeatmapRootContrib = {
  rootKey: string;
  entry: EffectivePropEntry;
};

function buildFullViewDefMapFromSnapshot(snap: DmVersionSnapshot): Map<string, Record<string, unknown>> {
  const m = new Map<string, Record<string, unknown>>();
  const raw = snap.views;
  if (!Array.isArray(raw)) return m;
  for (const v of raw) {
    if (!v || typeof v !== "object") continue;
    const o = v as Record<string, unknown>;
    if (!isLikelyFullViewDef(v)) continue;
    const space = typeof o.space === "string" ? o.space : "";
    const externalId = typeof o.externalId === "string" ? o.externalId : "";
    if (!space || !externalId) continue;
    m.set(dmViewKey(space, externalId), o);
  }
  return m;
}

function absorbImplementsLaterWins(
  target: Map<string, EffectivePropEntry>,
  sub: Map<string, EffectivePropEntry>
): void {
  for (const [prop, e] of sub) {
    const cur = target.get(prop);
    if (!cur) {
      target.set(prop, { winnerKey: e.winnerKey, shadowed: [...e.shadowed] });
    } else {
      target.set(prop, {
        winnerKey: e.winnerKey,
        shadowed: uniqueViewKeyChain([...cur.shadowed, cur.winnerKey, ...e.shadowed]),
      });
    }
  }
}

function uniqueViewKeyChain(keys: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const k of keys) {
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(k);
  }
  return out;
}

/** Resolves effective properties for one view using DMS rules: process `implements` in order (later overrides earlier); own properties override inherited. */
function resolveEffectivePropsForViewKey(
  viewKey: string,
  defs: ReadonlyMap<string, Record<string, unknown>>,
  visiting: Set<string>
): Map<string, EffectivePropEntry> {
  if (visiting.has(viewKey)) return new Map();
  const def = defs.get(viewKey);
  if (!def || !isLikelyFullViewDef(def)) return new Map();

  visiting.add(viewKey);
  const merged = new Map<string, EffectivePropEntry>();
  try {
    const imp = Array.isArray(def.implements) ? def.implements : [];
    for (const ref of imp) {
      if (!ref || typeof ref !== "object") continue;
      const r = ref as { space?: string; externalId?: string };
      if (typeof r.space !== "string" || typeof r.externalId !== "string") continue;
      const pk = dmViewKey(r.space, r.externalId);
      const sub = resolveEffectivePropsForViewKey(pk, defs, visiting);
      absorbImplementsLaterWins(merged, sub);
    }

    const own = (def.properties as Record<string, unknown>) ?? {};
    for (const prop of Object.keys(own)) {
      const cur = merged.get(prop);
      if (!cur) {
        merged.set(prop, { winnerKey: viewKey, shadowed: [] });
      } else {
        merged.set(prop, {
          winnerKey: viewKey,
          shadowed: uniqueViewKeyChain([...cur.shadowed, cur.winnerKey]),
        });
      }
    }
    return merged;
  } finally {
    visiting.delete(viewKey);
  }
}

function fallbackPropsFromRawView(raw: unknown): Map<string, EffectivePropEntry> | null {
  if (!raw || typeof raw !== "object") return null;
  const o = raw as Record<string, unknown>;
  const props = o.properties;
  if (typeof props !== "object" || props === null) return null;
  const space = typeof o.space === "string" ? o.space : "";
  const externalId = typeof o.externalId === "string" ? o.externalId : "";
  if (!space || !externalId) return null;
  const rk = dmViewKey(space, externalId);
  const m = new Map<string, EffectivePropEntry>();
  for (const propName of Object.keys(props as Record<string, unknown>)) {
    m.set(propName, { winnerKey: rk, shadowed: [] });
  }
  return m.size > 0 ? m : null;
}

/** One entry per view external id; prefer the array item that carries a full inline definition. */
function preferredRawByModelViewKey(snap: DmVersionSnapshot): Map<string, unknown> {
  const m = new Map<string, unknown>();
  for (const ref of parseViewsArray(snap.views)) {
    const prev = m.get(ref.key);
    if (prev == null) {
      m.set(ref.key, ref.raw);
    } else if (!isLikelyFullViewDef(prev) && isLikelyFullViewDef(ref.raw)) {
      m.set(ref.key, ref.raw);
    }
  }
  return m;
}

/** Per property name: contributing model member views and effective winner / shadowed chain (within each root). */
function effectivePropertyContributorsByProp(snap: DmVersionSnapshot): Map<string, HeatmapRootContrib[]> {
  const byProp = new Map<string, HeatmapRootContrib[]>();
  const defMap = buildFullViewDefMapFromSnapshot(snap);

  for (const [viewKey, raw] of preferredRawByModelViewKey(snap)) {
    const resolved = resolveEffectivePropsForViewKey(viewKey, defMap, new Set());
    const eff =
      resolved.size > 0 ? resolved : fallbackPropsFromRawView(raw) ?? new Map<string, EffectivePropEntry>();

    for (const [propName, entry] of eff) {
      const list = byProp.get(propName) ?? [];
      list.push({ rootKey: viewKey, entry });
      byProp.set(propName, list);
    }
  }
  return byProp;
}

function formatHeatmapResolutionNote(
  contributors: ReadonlyArray<HeatmapRootContrib>,
  t: (key: string, params?: Record<string, string | number>) => string
): string {
  if (contributors.length === 0) return "";
  const lines: string[] = [t("dataCatalog.versionHistory.fieldHeatmapResolutionRule")];
  if (contributors.length > 1) {
    lines.push(t("dataCatalog.versionHistory.fieldHeatmapResolutionMultiRoot"));
  }
  for (const { rootKey, entry } of contributors) {
    const rootLbl = formatDmViewKey(rootKey);
    const winLbl = formatDmViewKey(entry.winnerKey);
    lines.push(
      t("dataCatalog.versionHistory.fieldHeatmapResolutionModelMember", {
        root: rootLbl,
        utilized: winLbl,
      })
    );
    if (entry.shadowed.length > 0) {
      lines.push(
        t("dataCatalog.versionHistory.fieldHeatmapResolutionShadowed", {
          views: entry.shadowed.map(formatDmViewKey).join(", "),
        })
      );
    }
  }
  return lines.join("\n");
}

type FieldPresenceHeatmapModel = {
  columns: string[];
  rowVersions: string[];
  matrix: boolean[][];
  resolutionByCellKey: ReadonlyMap<string, string>;
};

function buildFieldPresenceHeatmap(
  versions: string[],
  dmKey: string,
  detailsMap: ReadonlyMap<string, DmVersionSnapshot>,
  rowVersions: ReadonlyMap<string, DmVersionSnapshot>,
  t: (key: string, params?: Record<string, string | number>) => string
): FieldPresenceHeatmapModel | null {
  if (versions.length === 0) return null;
  const perVersion: Map<string, HeatmapRootContrib[]>[] = [];
  const union = new Set<string>();
  for (const ver of versions) {
    const snap = snapshotForKey(dmKey, ver, detailsMap, rowVersions.get(ver));
    const byProp = snap ? effectivePropertyContributorsByProp(snap) : new Map<string, HeatmapRootContrib[]>();
    perVersion.push(byProp);
    for (const p of byProp.keys()) union.add(p);
  }
  if (union.size === 0) return null;
  const columns = [...union].sort((a, b) => a.localeCompare(b));
  const matrix = perVersion.map((byProp) => columns.map((c) => (byProp.get(c)?.length ?? 0) > 0));

  const resolutionByCellKey = new Map<string, string>();
  for (let vi = 0; vi < versions.length; vi++) {
    const ver = versions[vi];
    const byProp = perVersion[vi];
    if (ver == null || !byProp) continue;
    for (const propName of columns) {
      const contribs = byProp.get(propName);
      if (contribs == null || contribs.length === 0) continue;
      resolutionByCellKey.set(heatmapCellKey(ver, propName), formatHeatmapResolutionNote(contribs, t));
    }
  }

  return { columns, rowVersions: versions, matrix, resolutionByCellKey };
}

/** Matches `DEST_DM_VERSION_UNSPECIFIED` in DataModelVersions (destination.dataModel without version). */
const DM_TX_DEST_LATEST = "__dest_dm_latest__";

const HEAT_CELL_W = 5;
const HEAT_CELL_H = 10;
const HEAT_ROW_H = 22;
const HEAT_ROW_LABEL_W = 200;

/** Rows are newest-first (index 0 = latest). True if field is absent here but present in some newer row. */
function heatmapFieldMissingBeforeLaterRevision(matrix: boolean[][], ri: number, ci: number): boolean {
  if (matrix[ri]?.[ci]) return false;
  for (let j = 0; j < ri; j++) {
    if (matrix[j]?.[ci]) return true;
  }
  return false;
}

/**
 * When hovering a row label (newest-first index h), highlight orange on rows 1..h (up to but not including latest)
 * for cells where the field is missing at that revision but appears in a newer one.
 */
function heatmapAddedFieldOrangeCell(
  matrix: boolean[][],
  ri: number,
  ci: number,
  labelHoverRow: number | null
): boolean {
  if (labelHoverRow == null || labelHoverRow < 1) return false;
  if (ri < 1 || ri > labelHoverRow) return false;
  return heatmapFieldMissingBeforeLaterRevision(matrix, ri, ci);
}

/** Map pointer X on the row data strip to a column index (avoids dead zones between tiny cell rects). */
function heatmapPickColumnFromStripEvent(
  e: ReactMouseEvent<SVGRectElement>,
  columnCount: number
): number | null {
  if (columnCount <= 0) return null;
  const el = e.currentTarget;
  const r = el.getBoundingClientRect();
  const x = e.clientX - r.left;
  const w = r.width;
  if (w <= 0) return null;
  const ci = Math.floor((x / w) * columnCount);
  if (ci < 0 || ci >= columnCount) return null;
  return ci;
}

type HeatmapRowVisual = {
  usageLine: string;
  olderWriteDestination: boolean;
  rowTooltip: string;
};

type FieldPresenceHeatmapProps = {
  columns: string[];
  rows: string[];
  matrix: boolean[][];
  resolutionByCellKey: ReadonlyMap<string, string>;
  rowsMeta: HeatmapRowVisual[];
  showTxSqlLegend: boolean;
  t: (key: string, params?: Record<string, string | number>) => string;
};

function FieldPresenceHeatmap({
  columns,
  rows,
  matrix,
  resolutionByCellKey,
  rowsMeta,
  showTxSqlLegend,
  t,
}: FieldPresenceHeatmapProps) {
  const [hover, setHover] = useState<{ ri: number; ci: number } | null>(null);
  const [pinned, setPinned] = useState<{ ri: number; ci: number } | null>(null);
  const [labelHoverRow, setLabelHoverRow] = useState<number | null>(null);

  useEffect(() => {
    setHover(null);
    setPinned(null);
    setLabelHoverRow(null);
  }, [columns, rows, resolutionByCellKey]);

  const svgW = HEAT_ROW_LABEL_W + columns.length * HEAT_CELL_W;
  const svgH = rows.length * HEAT_ROW_H;
  const cellY = (ri: number) => ri * HEAT_ROW_H + (HEAT_ROW_H - HEAT_CELL_H) / 2;

  const verLabel = (v: string) => (v.length > 26 ? `${v.slice(0, 25)}…` : v);

  const cellAt = useCallback(
    (ri: number, ci: number) => {
      const ver = rows[ri];
      const propName = columns[ci];
      if (ver == null || propName == null) return null;
      const exists = matrix[ri]?.[ci] ?? false;
      const resolutionNote = resolutionByCellKey.get(heatmapCellKey(ver, propName)) ?? "";
      return {
        version: ver,
        propName,
        exists,
        resolutionNote,
      };
    },
    [rows, columns, matrix, resolutionByCellKey]
  );

  const primary = pinned ?? hover;
  const primaryDetail = primary ? cellAt(primary.ri, primary.ci) : null;
  const showPinned = pinned != null;
  const showHoverOnly = pinned == null && hover != null;

  const alsoHovering =
    pinned != null &&
    hover != null &&
    (hover.ri !== pinned.ri || hover.ci !== pinned.ci)
      ? cellAt(hover.ri, hover.ci)
      : null;

  const alsoHoveringLine =
    alsoHovering != null
      ? [
          t("dataCatalog.versionHistory.fieldHeatmapTooltip", {
            version: alsoHovering.version,
            field: alsoHovering.propName,
          }),
          alsoHovering.resolutionNote,
        ]
          .filter(Boolean)
          .join("\n")
      : "";

  return (
    <div className="mb-3 rounded-md border border-slate-200 bg-white p-2">
      <p className="mb-1 text-[11px] leading-snug text-slate-600">
        {t("dataCatalog.versionHistory.fieldHeatmapCaption")}
      </p>
      {showTxSqlLegend ? (
        <p className="mb-1 text-[10px] leading-snug text-slate-600">
          {t("dataCatalog.versionHistory.fieldHeatmapLegendTxSql")}
        </p>
      ) : null}
      <p className="mb-2 text-[10px] leading-snug text-amber-900/90">
        {t("dataCatalog.versionHistory.fieldHeatmapLegendOrange")}
      </p>
      {labelHoverRow != null && labelHoverRow >= 1 ? (
        <p className="mb-2 text-[10px] leading-snug text-orange-800">
          {t("dataCatalog.versionHistory.fieldHeatmapLegendAddedFieldHover", {
            version: rows[labelHoverRow] ?? "",
          })}
        </p>
      ) : null}

      <div className="mb-2 rounded-md border border-slate-200 bg-slate-50 px-2 py-2">
        <div className="flex min-h-[26px] flex-wrap items-center justify-between gap-2">
          <span className="text-[11px] font-medium text-slate-800">
            {t("dataCatalog.versionHistory.fieldHeatmapDetailTitle")}
          </span>
          <div className="flex min-h-[22px] items-center gap-2">
            {showPinned ? (
              <span className="rounded bg-blue-100 px-1.5 py-0.5 text-[10px] font-medium text-blue-900">
                {t("dataCatalog.versionHistory.fieldHeatmapDetailPinned")}
              </span>
            ) : null}
            {showHoverOnly ? (
              <span className="rounded bg-slate-200/80 px-1.5 py-0.5 text-[10px] font-medium text-slate-700">
                {t("dataCatalog.versionHistory.fieldHeatmapDetailHover")}
              </span>
            ) : null}
            {pinned ? (
              <button
                type="button"
                className="rounded border border-slate-300 bg-white px-2 py-0.5 text-[10px] text-slate-700 hover:bg-slate-100"
                onClick={() => setPinned(null)}
              >
                {t("dataCatalog.versionHistory.fieldHeatmapDetailClearPin")}
              </button>
            ) : null}
          </div>
        </div>
        <div className="h-36 shrink-0 overflow-y-auto overflow-x-hidden overscroll-y-contain pr-1 [scrollbar-gutter:stable]">
          {primaryDetail ? (
            <dl className="mt-2 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-[11px] leading-snug">
              <dt className="text-slate-500">{t("dataCatalog.versionHistory.fieldHeatmapDetailVersion")}</dt>
              <dd className="min-w-0 break-all font-mono text-slate-900">{primaryDetail.version}</dd>
              <dt className="text-slate-500">{t("dataCatalog.versionHistory.fieldHeatmapDetailField")}</dt>
              <dd className="min-w-0 break-all font-mono text-slate-900">{primaryDetail.propName}</dd>
              <dt className="text-slate-500">{t("dataCatalog.versionHistory.fieldHeatmapDetailPresence")}</dt>
              <dd className="font-medium text-slate-900">
                {primaryDetail.exists
                  ? t("dataCatalog.versionHistory.fieldHeatmapDetailYes")
                  : t("dataCatalog.versionHistory.fieldHeatmapDetailNo")}
              </dd>
              {primaryDetail.resolutionNote ? (
                <>
                  <dt className="self-start text-slate-500">
                    {t("dataCatalog.versionHistory.fieldHeatmapDetailResolution")}
                  </dt>
                  <dd className="min-w-0 whitespace-pre-wrap break-words text-slate-700">
                    {primaryDetail.resolutionNote}
                  </dd>
                </>
              ) : null}
            </dl>
          ) : (
            <p className="mt-2 text-[11px] leading-snug text-slate-600">
              {t("dataCatalog.versionHistory.fieldHeatmapDetailEmpty")}
            </p>
          )}
          {alsoHovering != null ? (
            <p className="mt-2 border-t border-slate-200 pt-2 text-[10px] leading-snug text-slate-600 whitespace-pre-wrap break-words">
              <span className="font-medium text-slate-700">
                {t("dataCatalog.versionHistory.fieldHeatmapDetailHover")}
              </span>
              {": "}
              {alsoHoveringLine}
            </p>
          ) : null}
        </div>
      </div>

      <div
        className="max-w-full overflow-x-auto rounded border border-slate-100 bg-slate-50/50"
        onMouseLeave={() => {
          setHover(null);
          setLabelHoverRow(null);
        }}
      >
        <svg width={svgW} height={svgH} className="block min-w-0 shrink-0">
          {rows.map((ver, ri) => {
            const meta = rowsMeta[ri];
            const usage = meta?.usageLine ?? "";
            const olderDest = meta?.olderWriteDestination ?? false;
            const rowTip = meta?.rowTooltip ?? ver;
            const rowY = ri * HEAT_ROW_H;
            return (
              <g key={ver}>
                {olderDest ? (
                  <rect
                    x={0.5}
                    y={rowY + 0.5}
                    width={svgW - 1}
                    height={HEAT_ROW_H - 1}
                    rx={3}
                    fill="none"
                    stroke="#ea580c"
                    strokeWidth={1.75}
                    pointerEvents="none"
                  />
                ) : null}
                <rect
                  x={0}
                  y={rowY}
                  width={HEAT_ROW_LABEL_W}
                  height={HEAT_ROW_H}
                  fill="transparent"
                  pointerEvents="all"
                  onMouseEnter={() => {
                    setHover(null);
                    setLabelHoverRow(ri);
                  }}
                  onMouseLeave={() => setLabelHoverRow(null)}
                >
                  <title>{rowTip}</title>
                </rect>
                <text
                  x={4}
                  y={rowY + 12}
                  fontSize={10}
                  fill="#0f172a"
                  className="font-mono"
                  style={{ pointerEvents: "none" }}
                >
                  {verLabel(ver)}
                </text>
                <text
                  x={4}
                  y={rowY + 20}
                  fontSize={7}
                  fill="#64748b"
                  className="font-sans"
                  style={{ pointerEvents: "none" }}
                >
                  {usage.length > 42 ? `${usage.slice(0, 41)}…` : usage}
                </text>
                {columns.length > 0 ? (
                  <rect
                    x={HEAT_ROW_LABEL_W}
                    y={rowY}
                    width={columns.length * HEAT_CELL_W}
                    height={HEAT_ROW_H}
                    fill="transparent"
                    pointerEvents="all"
                    style={{ cursor: "pointer" }}
                    onMouseEnter={() => setLabelHoverRow(null)}
                    onMouseMove={(e) => {
                      setLabelHoverRow(null);
                      const ci = heatmapPickColumnFromStripEvent(e, columns.length);
                      if (ci != null) setHover({ ri, ci });
                    }}
                    onClick={(e) => {
                      const ci = heatmapPickColumnFromStripEvent(e, columns.length);
                      if (ci == null) return;
                      setPinned((p) => (p?.ri === ri && p?.ci === ci ? null : { ri, ci }));
                    }}
                  />
                ) : null}
                {columns.map((colKey, ci) => {
                  const exists = matrix[ri]?.[ci] ?? false;
                  const cx = HEAT_ROW_LABEL_W + ci * HEAT_CELL_W;
                  const cy = cellY(ri);
                  const isPinned = pinned?.ri === ri && pinned?.ci === ci;
                  const isHover = hover?.ri === ri && hover?.ci === ci;
                  const addedGapOrange = heatmapAddedFieldOrangeCell(matrix, ri, ci, labelHoverRow);
                  const stroke = isPinned ? "#1d4ed8" : isHover ? "#64748b" : "#e2e8f0";
                  const strokeW = isPinned ? 1.25 : isHover ? 0.9 : 0.35;
                  const fill = exists ? "#2563eb" : addedGapOrange ? "#fb923c" : "#ffffff";
                  return (
                    <rect
                      key={colKey}
                      x={cx}
                      y={cy}
                      width={HEAT_CELL_W}
                      height={HEAT_CELL_H}
                      fill={fill}
                      stroke={stroke}
                      strokeWidth={strokeW}
                      pointerEvents="none"
                    />
                  );
                })}
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
}

const EMPTY_TX_MAP = new Map<string, Array<{ id: string; name: string }>>();
const EMPTY_STR_SET = new Set<string>();

type VersionSnapshotMetaProps = {
  roleLabel: string;
  version: string;
  snap: DmVersionSnapshot;
  fusionUrl: (v: string) => string;
  t: (key: string, params?: Record<string, string | number>) => string;
};

function VersionSnapshotMeta({ roleLabel, version, snap, fusionUrl, t }: VersionSnapshotMetaProps) {
  const ct = formatTs(snap.createdTime);
  const lut = formatTs(snap.lastUpdatedTime);
  return (
    <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5 text-[11px] leading-snug">
      <span className="shrink-0 font-medium text-slate-600">{roleLabel}</span>
      <a
        href={fusionUrl(version)}
        target="_blank"
        rel="noreferrer"
        className="font-mono font-semibold text-blue-600 hover:underline"
      >
        {version}
      </a>
      {ct ? (
        <span className="text-slate-500">
          {t("dataCatalog.versionHistory.created")}: {ct}
        </span>
      ) : null}
      {lut && shouldShowUpdatedSeparate(snap.createdTime, snap.lastUpdatedTime) ? (
        <span className="text-slate-500">{t("dataCatalog.versionHistory.updated")}: {lut}</span>
      ) : null}
    </div>
  );
}

type DataCatalogVersionHistoryProps = {
  label: string;
  dmKey: string;
  versionsOrdered: string[];
  detailsMap: ReadonlyMap<string, DmVersionSnapshot>;
  rowVersions: ReadonlyMap<string, DmVersionSnapshot>;
  dmTxByCell?: ReadonlyMap<string, Array<{ id: string; name: string }>>;
  dmKeysInCatalog?: ReadonlySet<string>;
  dmKeysInTransformation?: ReadonlySet<string>;
};

export function DataCatalogVersionHistory({
  label,
  dmKey,
  versionsOrdered,
  detailsMap,
  rowVersions,
  dmTxByCell = EMPTY_TX_MAP,
  dmKeysInCatalog = EMPTY_STR_SET,
  dmKeysInTransformation = EMPTY_STR_SET,
}: DataCatalogVersionHistoryProps) {
  const { t } = useI18n();
  const { sdk } = useAppSdk();
  const [openSteps, setOpenSteps] = useState<Set<number>>(() => new Set([0]));

  useEffect(() => {
    setOpenSteps(new Set([0]));
  }, [dmKey]);

  const versionsAscending = useMemo(
    () => [...versionsOrdered].sort(compareVersionStrings),
    [versionsOrdered]
  );

  const transitions = useMemo(() => {
    const out: TransitionDiff[] = [];
    for (let i = 1; i < versionsAscending.length; i++) {
      const vPrev = versionsAscending[i - 1];
      const vNext = versionsAscending[i];
      if (vPrev == null || vNext == null) continue;
      const sPrev = snapshotForKey(dmKey, vPrev, detailsMap, rowVersions.get(vPrev));
      const sNext = snapshotForKey(dmKey, vNext, detailsMap, rowVersions.get(vNext));
      if (!sPrev || !sNext) continue;
      out.push(buildTransitionDiff(sPrev, sNext));
    }
    return out.reverse();
  }, [dmKey, versionsAscending, detailsMap, rowVersions]);

  const versionsNewestFirst = useMemo(
    () => [...versionsAscending].reverse(),
    [versionsAscending]
  );

  const fieldHeatmap = useMemo(
    () => buildFieldPresenceHeatmap(versionsNewestFirst, dmKey, detailsMap, rowVersions, t),
    [versionsNewestFirst, dmKey, detailsMap, rowVersions, t]
  );

  const heatmapRowsMeta = useMemo((): HeatmapRowVisual[] => {
    const latestVer = versionsNewestFirst[0];
    return versionsNewestFirst.map((ver) => {
      const isLatest = ver === latestVer;
      const cellKey = `${dmKey}:${ver}`;
      const destLatestKey = `${dmKey}:${DM_TX_DEST_LATEST}`;
      const txAtVer = dmTxByCell.has(cellKey);
      const txAtLatestUnspec = isLatest && dmTxByCell.has(destLatestKey);
      const txDest = txAtVer || txAtLatestUnspec;

      const seen = new Set<string>();
      const nameList: string[] = [];
      for (const list of [dmTxByCell.get(cellKey), isLatest ? dmTxByCell.get(destLatestKey) : undefined]) {
        for (const x of list ?? []) {
          if (seen.has(x.id)) continue;
          seen.add(x.id);
          nameList.push(x.name);
        }
      }

      const parts: string[] = [];
      if (isLatest) parts.push(t("dataCatalog.versionHistory.fieldHeatmapRowLatest"));
      if (dmKeysInCatalog.has(dmKey) && isLatest) {
        parts.push(t("dataCatalog.versionHistory.fieldHeatmapRowCatalog"));
      }
      if (dmKeysInTransformation.has(dmKey)) {
        parts.push(t("dataCatalog.versionHistory.fieldHeatmapRowTxRefs"));
      }
      if (txDest) {
        parts.push(
          isLatest
            ? t("dataCatalog.versionHistory.fieldHeatmapRowWriteDest")
            : t("dataCatalog.versionHistory.fieldHeatmapRowWriteDestOlder")
        );
      }
      const usageLine = parts.join(" · ");

      let rowTooltip = ver;
      if (usageLine) rowTooltip += `\n${usageLine}`;
      if (nameList.length > 0) {
        rowTooltip += `\n${t("dataCatalog.versionHistory.fieldHeatmapRowTooltipTx", { names: nameList.join(", ") })}`;
      }

      return {
        usageLine,
        olderWriteDestination: txDest && !isLatest,
        rowTooltip,
      };
    });
  }, [versionsNewestFirst, dmKey, dmTxByCell, dmKeysInCatalog, dmKeysInTransformation, t]);

  const toggleStep = (i: number) => {
    setOpenSteps((prev) => {
      const n = new Set(prev);
      if (n.has(i)) n.delete(i);
      else n.add(i);
      return n;
    });
  };

  const fusionUrl = (version: string) => {
    const colonIdx = dmKey.indexOf(":");
    const space = colonIdx >= 0 ? dmKey.slice(0, colonIdx) : "";
    const externalId = colonIdx >= 0 ? dmKey.slice(colonIdx + 1) : dmKey;
    return getDataModelUrl(sdk.project, space, externalId, version);
  };

  return (
    <div className="min-w-0 w-full rounded-lg border border-slate-200 bg-white shadow-sm">
      <div className="border-b border-slate-200 px-5 py-4">
        <h2 className="text-xl font-semibold text-slate-900">{t("dataCatalog.versionHistory.title")}</h2>
        <p className="mt-1 text-sm text-slate-500">
          {label} · {versionsAscending.length} {t("dataCatalog.versionHistory.versions")}
        </p>
      </div>

      <div className="px-5 py-4">
          <div className="mb-3 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
            {t("dataCatalog.versionHistory.hint")}
          </div>

          {fieldHeatmap ? (
            <FieldPresenceHeatmap
              columns={fieldHeatmap.columns}
              rows={fieldHeatmap.rowVersions}
              matrix={fieldHeatmap.matrix}
              resolutionByCellKey={fieldHeatmap.resolutionByCellKey}
              rowsMeta={heatmapRowsMeta}
              showTxSqlLegend={dmKeysInTransformation.has(dmKey)}
              t={t}
            />
          ) : versionsAscending.length > 0 ? (
            <p className="mb-3 text-[11px] text-slate-500">
              {t("dataCatalog.versionHistory.fieldHeatmapEmpty")}
            </p>
          ) : null}

          {transitions.length === 0 && versionsAscending.length === 1 ? (
            (() => {
              const v = versionsAscending[0];
              if (v == null) {
                return (
                  <p className="text-sm text-slate-500">{t("dataCatalog.versionHistory.noTransitions")}</p>
                );
              }
              const snap = snapshotForKey(dmKey, v, detailsMap, rowVersions.get(v));
              return (
                <div className="rounded-lg border border-slate-200 bg-white px-3 py-3">
                  {snap ? (
                    <VersionSnapshotMeta
                      roleLabel={t("dataCatalog.versionHistory.stepSingle")}
                      version={v}
                      snap={snap}
                      fusionUrl={fusionUrl}
                      t={t}
                    />
                  ) : (
                    <span className="font-mono text-sm text-slate-800">{v}</span>
                  )}
                  <p className="mt-2 text-xs text-slate-500">
                    {t("dataCatalog.versionHistory.noTransitions")}
                  </p>
                </div>
              );
            })()
          ) : null}

          {transitions.length === 0 && versionsAscending.length !== 1 ? (
            <p className="text-sm text-slate-500">{t("dataCatalog.versionHistory.noTransitions")}</p>
          ) : null}

          {transitions.length > 0 ? (
            <ul className="space-y-3">
              {transitions.map((tr, idx) => {
                const expanded = openSteps.has(idx);
                const hasChanges =
                  tr.modelMetaChanges.length > 0 ||
                  tr.viewsAdded.length > 0 ||
                  tr.viewsRemoved.length > 0 ||
                  tr.viewVersionChanges.length > 0;

                return (
                  <li
                    key={`${tr.fromVersion}→${tr.toVersion}`}
                    className="overflow-hidden rounded-lg border border-slate-200 bg-white"
                  >
                    <button
                      type="button"
                      onClick={() => toggleStep(idx)}
                      className="flex w-full items-center justify-between gap-2 bg-slate-50 px-3 py-2 text-left text-sm font-medium text-slate-800 hover:bg-slate-100"
                    >
                      <span>
                        {t("dataCatalog.versionHistory.transitionLabel", {
                          from: tr.fromVersion,
                          to: tr.toVersion,
                        })}
                      </span>
                      <span className="shrink-0 text-xs text-slate-500">
                        {hasChanges ? t("dataCatalog.versionHistory.hasChanges") : t("dataCatalog.versionHistory.noStructural")}
                        {expanded ? " ▲" : " ▼"}
                      </span>
                    </button>
                    {expanded ? (
                      <div className="space-y-3 px-3 py-3 text-sm text-slate-700">
                        <div className="space-y-1.5 rounded-md border border-slate-100 bg-slate-50/90 px-2.5 py-2">
                          <VersionSnapshotMeta
                            roleLabel={t("dataCatalog.versionHistory.stepFrom")}
                            version={tr.fromVersion}
                            snap={tr.fromSnap}
                            fusionUrl={fusionUrl}
                            t={t}
                          />
                          <VersionSnapshotMeta
                            roleLabel={t("dataCatalog.versionHistory.stepTo")}
                            version={tr.toVersion}
                            snap={tr.toSnap}
                            fusionUrl={fusionUrl}
                            t={t}
                          />
                        </div>
                        {tr.modelMetaChanges.length > 0 ? (
                          <div>
                            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                              {t("dataCatalog.versionHistory.modelFields")}
                            </div>
                            <ul className="mt-1 list-disc space-y-1 pl-5 text-xs">
                              {tr.modelMetaChanges.map((line, i) => (
                                <li key={i}>{line}</li>
                              ))}
                            </ul>
                          </div>
                        ) : null}

                        {tr.viewsRemoved.length > 0 ? (
                          <div>
                            <div className="text-xs font-semibold text-red-700">
                              {t("dataCatalog.versionHistory.viewsRemoved")}
                            </div>
                            <ul className="mt-1 space-y-1 font-mono text-xs">
                              {tr.viewsRemoved.map((v) => (
                                <li key={v.key} className="rounded bg-red-50 px-2 py-1 text-red-900">
                                  {v.key}
                                  {v.version ? ` @ ${v.version}` : ""}
                                </li>
                              ))}
                            </ul>
                          </div>
                        ) : null}

                        {tr.viewsAdded.length > 0 ? (
                          <div>
                            <div className="text-xs font-semibold text-emerald-700">
                              {t("dataCatalog.versionHistory.viewsAdded")}
                            </div>
                            <ul className="mt-1 space-y-1 font-mono text-xs">
                              {tr.viewsAdded.map((v) => (
                                <li
                                  key={v.key}
                                  className="rounded bg-emerald-50 px-2 py-1 text-emerald-900"
                                >
                                  {v.key}
                                  {v.version ? ` @ ${v.version}` : ""}
                                </li>
                              ))}
                            </ul>
                          </div>
                        ) : null}

                        {tr.viewVersionChanges.length > 0 ? (
                          <div>
                            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                              {t("dataCatalog.versionHistory.viewVersionBumps")}
                            </div>
                            <ul className="mt-2 space-y-2">
                              {tr.viewVersionChanges.map(({ ref, prevRef, viewDiff }) => {
                                const rawPrev = prevRef.raw;
                                const rawNext = ref.raw;
                                const prevCt =
                                  isLikelyFullViewDef(rawPrev) && typeof rawPrev.createdTime === "number"
                                    ? formatTs(rawPrev.createdTime)
                                    : null;
                                const nextCt =
                                  isLikelyFullViewDef(rawNext) && typeof rawNext.createdTime === "number"
                                    ? formatTs(rawNext.createdTime)
                                    : null;
                                const prevLut =
                                  isLikelyFullViewDef(rawPrev) &&
                                  typeof rawPrev.lastUpdatedTime === "number" &&
                                  shouldShowUpdatedSeparate(
                                    typeof rawPrev.createdTime === "number"
                                      ? rawPrev.createdTime
                                      : undefined,
                                    rawPrev.lastUpdatedTime
                                  )
                                    ? formatTs(rawPrev.lastUpdatedTime)
                                    : null;
                                const nextLut =
                                  isLikelyFullViewDef(rawNext) &&
                                  typeof rawNext.lastUpdatedTime === "number" &&
                                  shouldShowUpdatedSeparate(
                                    typeof rawNext.createdTime === "number"
                                      ? rawNext.createdTime
                                      : undefined,
                                    rawNext.lastUpdatedTime
                                  )
                                    ? formatTs(rawNext.lastUpdatedTime)
                                    : null;
                                return (
                                <li
                                  key={`${ref.key}:${ref.version}`}
                                  className="rounded-md border border-amber-200 bg-amber-50/80 px-2 py-2 text-xs"
                                >
                                  <div className="font-mono font-semibold text-amber-950">
                                    {ref.key}
                                  </div>
                                  <div className="mt-1 text-amber-900">
                                    {prevRef.version} → {ref.version}
                                  </div>
                                  {prevCt || nextCt || prevLut || nextLut ? (
                                    <div className="mt-1 flex flex-wrap gap-x-2 gap-y-0.5 text-[10px] text-slate-600">
                                      {prevCt ? (
                                        <span>
                                          {t("dataCatalog.versionHistory.viewPrevCreated")}: {prevCt}
                                        </span>
                                      ) : null}
                                      {nextCt ? (
                                        <span>
                                          {t("dataCatalog.versionHistory.viewNextCreated")}: {nextCt}
                                        </span>
                                      ) : null}
                                      {prevLut ? (
                                        <span>
                                          {t("dataCatalog.versionHistory.viewPrevUpdated")}: {prevLut}
                                        </span>
                                      ) : null}
                                      {nextLut ? (
                                        <span>
                                          {t("dataCatalog.versionHistory.viewNextUpdated")}: {nextLut}
                                        </span>
                                      ) : null}
                                    </div>
                                  ) : null}
                                  {viewDiff &&
                                  (viewDiff.metaChanges.length > 0 ||
                                    viewDiff.propChanges.length > 0 ||
                                    viewDiff.filterChanged) ? (
                                    <div className="mt-2 space-y-2 border-t border-amber-200/80 pt-2">
                                      {viewDiff.metaChanges.length > 0 ? (
                                        <ul className="list-disc space-y-0.5 pl-4 text-[11px] text-slate-700">
                                          {viewDiff.metaChanges.map((m, mi) => (
                                            <li key={mi}>{m}</li>
                                          ))}
                                        </ul>
                                      ) : null}
                                      {viewDiff.filterChanged ? (
                                        <p className="text-[11px] text-slate-600">
                                          {t("dataCatalog.versionHistory.filterChanged")}
                                        </p>
                                      ) : null}
                                      {viewDiff.propChanges.length > 0 ? (
                                        <div className="space-y-1.5">
                                          {viewDiff.propChanges.map((pc) => (
                                            <div
                                              key={pc.name}
                                              className={`rounded border px-2 py-1 font-mono text-[10px] leading-snug ${
                                                pc.kind === "add"
                                                  ? "border-emerald-200 bg-emerald-50/90"
                                                  : pc.kind === "remove"
                                                    ? "border-red-200 bg-red-50/90"
                                                    : "border-slate-200 bg-white"
                                              }`}
                                            >
                                              <span className="font-sans text-[11px] font-semibold text-slate-800">
                                                {pc.kind === "add"
                                                  ? "+ "
                                                  : pc.kind === "remove"
                                                    ? "− "
                                                    : "~ "}
                                                {pc.name}
                                              </span>
                                              {pc.semanticLines && pc.semanticLines.length > 0 ? (
                                                <ul className="mt-1 list-disc space-y-0.5 pl-4 font-sans text-[11px] text-slate-800">
                                                  {pc.semanticLines.map((line, li) => (
                                                    <li key={li} className="break-words">
                                                      {line}
                                                    </li>
                                                  ))}
                                                </ul>
                                              ) : null}
                                              {!pc.semanticLines?.length && pc.before ? (
                                                <pre className="mt-1 whitespace-pre-wrap break-all text-slate-600">
                                                  {pc.before}
                                                </pre>
                                              ) : null}
                                              {!pc.semanticLines?.length && pc.after ? (
                                                <pre className="mt-1 whitespace-pre-wrap break-all text-slate-800">
                                                  {pc.after}
                                                </pre>
                                              ) : null}
                                            </div>
                                          ))}
                                        </div>
                                      ) : null}
                                    </div>
                                  ) : viewDiff ? (
                                    <p className="mt-1 text-[11px] text-slate-600">
                                      {t("dataCatalog.versionHistory.viewSchemaUnchanged")}
                                    </p>
                                  ) : (
                                    <p className="mt-1 text-[11px] text-slate-600">
                                      {t("dataCatalog.versionHistory.inlineViewMissing")}
                                    </p>
                                  )}
                                </li>
                              );
                              })}
                            </ul>
                          </div>
                        ) : null}

                        {!hasChanges ? (
                          <p className="text-xs text-slate-500">{t("dataCatalog.versionHistory.identicalFingerprint")}</p>
                        ) : null}
                      </div>
                    ) : null}
                  </li>
                );
              })}
            </ul>
          ) : null}
      </div>
    </div>
  );
}
