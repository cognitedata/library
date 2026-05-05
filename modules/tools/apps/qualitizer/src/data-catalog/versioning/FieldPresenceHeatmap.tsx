import type { MouseEvent as ReactMouseEvent } from "react";
import { useCallback, useEffect, useState } from "react";
import type { DmVersionSnapshot } from "./version-history-types";
import { isLikelyFullViewDef, parseViewsArray, snapshotForKey } from "./version-history-diff";

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

/** Stable signature of which view supplies each property per model member root (for comparing versions). */
function heatmapWinnerSignatureForProp(
  byProp: Map<string, HeatmapRootContrib[]>,
  propName: string
): string {
  const list = byProp.get(propName);
  if (!list || list.length === 0) return "";
  return [...list]
    .sort((a, b) => a.rootKey.localeCompare(b.rootKey))
    .map((x) => {
      if (!x.entry.winnerKey.trim()) {
        console.warn("[fieldHeatmap] winner signature has empty winnerKey (root still keyed)", {
          propName,
          rootKey: x.rootKey,
        });
      }
      return `${x.rootKey}\x1f${x.entry.winnerKey}`;
    })
    .join("|");
}

/** Newest-first row index `ri`: property present here and on an adjacent version row, but resolved winner signature differs. */
function heatmapWinnerDriftVsNeighbor(
  matrix: boolean[][],
  rows: string[],
  columns: string[],
  winnerSigByCellKey: ReadonlyMap<string, string>,
  ri: number,
  ci: number
): boolean {
  if (!matrix[ri]?.[ci]) return false;
  const prop = columns[ci];
  const ver = rows[ri];
  if (prop == null || ver == null) return false;
  const sig = winnerSigByCellKey.get(heatmapCellKey(ver, prop)) ?? "";
  if (!sig) return false;

  const olderRi = ri + 1;
  if (olderRi < rows.length) {
    const vOld = rows[olderRi];
    if (vOld != null && matrix[olderRi]?.[ci]) {
      const sOld = winnerSigByCellKey.get(heatmapCellKey(vOld, prop)) ?? "";
      if (sOld && sig !== sOld) return true;
    }
  }
  const newerRi = ri - 1;
  if (newerRi >= 0) {
    const vNew = rows[newerRi];
    if (vNew != null && matrix[newerRi]?.[ci]) {
      const sNew = winnerSigByCellKey.get(heatmapCellKey(vNew, prop)) ?? "";
      if (sNew && sig !== sNew) return true;
    }
  }
  return false;
}

type HeatmapResolutionRowData = {
  rootKey: string;
  winnerKey: string;
  shadowed: string[];
};

type HeatmapWinnerDriftChange = {
  rootKey: string;
  fromWinner: string;
  toWinner: string;
  /** When `fromWinner` is empty: this model version has no winner entry for this root (expected asymmetry vs neighbor). */
  fromAbsentInVersion?: string;
  /** When `toWinner` is empty: same for the sig-after side of the diff. */
  toAbsentInVersion?: string;
  /** How to present this row: root only on one side vs supplier swap vs malformed empty in sig map. */
  driftRowKind?: "root_added" | "root_removed" | "supplier_changed";
};

type HeatmapWinnerDriftBlock = {
  neighborVersion: string;
  relation: "older" | "newer";
  changes: HeatmapWinnerDriftChange[];
  sigBefore: string;
  sigAfter: string;
};

type HeatmapDriftMissingWinnerDebugPayload = {
  cell: { version: string; field: string };
  driftBlock: {
    relation: "older" | "newer";
    neighborVersion: string;
    sigBefore: string;
    sigAfter: string;
  };
  change: HeatmapWinnerDriftChange;
  missingSide: "from" | "to";
};

function contributorsToResolutionRows(
  contribs: ReadonlyArray<HeatmapRootContrib>
): HeatmapResolutionRowData[] {
  return contribs.map((c) => ({
    rootKey: c.rootKey,
    winnerKey: c.entry.winnerKey,
    shadowed: [...c.entry.shadowed],
  }));
}

function isTrivialResolutionRow(row: HeatmapResolutionRowData): boolean {
  return row.shadowed.length === 0 && row.rootKey === row.winnerKey;
}

/** Heatmap cells merge a property name across all model member views; details only list meaningful chains. */
function filterResolutionRowsForDetails(rows: HeatmapResolutionRowData[]): {
  displayRows: HeatmapResolutionRowData[];
  trivialMultiRootSummary: boolean;
  omittedTrivialCount: number;
} {
  if (rows.length <= 1) {
    return { displayRows: rows, trivialMultiRootSummary: false, omittedTrivialCount: 0 };
  }
  const nonTrivial = rows.filter((r) => !isTrivialResolutionRow(r));
  if (nonTrivial.length > 0) {
    return {
      displayRows: nonTrivial,
      trivialMultiRootSummary: false,
      omittedTrivialCount: rows.length - nonTrivial.length,
    };
  }
  return {
    displayRows: [],
    trivialMultiRootSummary: true,
    omittedTrivialCount: rows.length,
  };
}

function parseHeatmapWinnerSig(sig: string): Map<string, string> {
  const m = new Map<string, string>();
  for (const part of sig.split("|")) {
    if (!part) continue;
    const i = part.indexOf("\x1f");
    if (i < 0) continue;
    m.set(part.slice(0, i), part.slice(i + 1));
  }
  return m;
}

function diffHeatmapWinnerSigs(sigFrom: string, sigTo: string): HeatmapWinnerDriftChange[] {
  const a = parseHeatmapWinnerSig(sigFrom);
  const b = parseHeatmapWinnerSig(sigTo);
  const roots = new Set([...a.keys(), ...b.keys()]);
  const out: HeatmapWinnerDriftChange[] = [];
  for (const r of [...roots].sort()) {
    const from = a.get(r) ?? "";
    const to = b.get(r) ?? "";
    if (from !== to) out.push({ rootKey: r, fromWinner: from, toWinner: to });
  }
  return out;
}

function enrichAndLogDriftChanges(
  raw: HeatmapWinnerDriftChange[],
  relation: "older" | "newer",
  cellVersion: string,
  neighborVersion: string,
  prop: string,
  sigBefore: string,
  sigAfter: string
): HeatmapWinnerDriftChange[] {
  const mapBefore = parseHeatmapWinnerSig(sigBefore);
  const mapAfter = parseHeatmapWinnerSig(sigAfter);
  return raw.map((ch) => {
    let fromAbsentInVersion: string | undefined;
    let toAbsentInVersion: string | undefined;
    let driftRowKind: HeatmapWinnerDriftChange["driftRowKind"] = "supplier_changed";

    if (!ch.fromWinner.trim()) {
      if (!mapBefore.has(ch.rootKey)) {
        fromAbsentInVersion = relation === "older" ? neighborVersion : cellVersion;
        if (ch.toWinner.trim()) driftRowKind = "root_added";
      } else {
        const v = mapBefore.get(ch.rootKey) ?? "";
        console.warn("[fieldHeatmap drift] empty fromWinner but root exists in before-sig map", {
          prop,
          cellVersion,
          neighborVersion,
          relation,
          rootKey: ch.rootKey,
          fromWinnerRaw: v,
          sigBefore,
          sigAfter,
        });
      }
    }
    if (!ch.toWinner.trim()) {
      if (!mapAfter.has(ch.rootKey)) {
        toAbsentInVersion = relation === "older" ? cellVersion : neighborVersion;
        if (ch.fromWinner.trim()) driftRowKind = "root_removed";
      } else {
        const v = mapAfter.get(ch.rootKey) ?? "";
        console.warn("[fieldHeatmap drift] empty toWinner but root exists in after-sig map", {
          prop,
          cellVersion,
          neighborVersion,
          relation,
          rootKey: ch.rootKey,
          toWinnerRaw: v,
          sigBefore,
          sigAfter,
        });
      }
    }
    return { ...ch, fromAbsentInVersion, toAbsentInVersion, driftRowKind };
  });
}

/** Version row where the contributing-root set change is attributed (this row vs neighbor row). */
function driftRootAddRemoveTargetVersion(
  relation: "older" | "newer",
  cellVersion: string,
  neighborVersion: string
): string {
  return relation === "older" ? cellVersion : neighborVersion;
}

function buildWinnerDriftBlocksForCell(
  matrix: boolean[][],
  rows: string[],
  columns: string[],
  winnerSigByCellKey: ReadonlyMap<string, string>,
  ri: number,
  ci: number
): HeatmapWinnerDriftBlock[] {
  const blocks: HeatmapWinnerDriftBlock[] = [];
  if (!matrix[ri]?.[ci]) return blocks;
  const prop = columns[ci];
  const ver = rows[ri];
  if (prop == null || ver == null) return blocks;
  const sigHere = winnerSigByCellKey.get(heatmapCellKey(ver, prop)) ?? "";
  if (!sigHere) return blocks;

  const olderRi = ri + 1;
  if (olderRi < rows.length) {
    const vOld = rows[olderRi];
    if (vOld != null && matrix[olderRi]?.[ci]) {
      const sigOld = winnerSigByCellKey.get(heatmapCellKey(vOld, prop)) ?? "";
      const changes = enrichAndLogDriftChanges(
        diffHeatmapWinnerSigs(sigOld, sigHere).filter((ch) => ch.fromWinner !== ch.toWinner),
        "older",
        ver,
        vOld,
        prop,
        sigOld,
        sigHere
      );
      if (changes.length > 0) {
        blocks.push({
          neighborVersion: vOld,
          relation: "older",
          changes,
          sigBefore: sigOld,
          sigAfter: sigHere,
        });
      }
    }
  }
  const newerRi = ri - 1;
  if (newerRi >= 0) {
    const vNew = rows[newerRi];
    if (vNew != null && matrix[newerRi]?.[ci]) {
      const sigNew = winnerSigByCellKey.get(heatmapCellKey(vNew, prop)) ?? "";
      const changes = enrichAndLogDriftChanges(
        diffHeatmapWinnerSigs(sigHere, sigNew).filter((ch) => ch.fromWinner !== ch.toWinner),
        "newer",
        ver,
        vNew,
        prop,
        sigHere,
        sigNew
      );
      if (changes.length > 0) {
        blocks.push({
          neighborVersion: vNew,
          relation: "newer",
          changes,
          sigBefore: sigHere,
          sigAfter: sigNew,
        });
      }
    }
  }
  return blocks;
}

function HeatmapResolutionChips({
  rows,
  emphasizeSupplyingWinner = false,
}: {
  rows: HeatmapResolutionRowData[];
  emphasizeSupplyingWinner?: boolean;
}) {
  if (rows.length === 0) return null;
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      {rows.map((row) => (
        <div key={row.rootKey} className="flex min-w-0 flex-wrap items-center gap-1 text-[10px] leading-tight">
          <span className="rounded border border-slate-300 bg-white px-1 py-px font-mono text-slate-600 break-all">
            {formatDmViewKey(row.rootKey)}
          </span>
          {row.shadowed.length > 0 ? (
            <>
              <span className="text-slate-400">·</span>
              {row.shadowed.map((s) => (
                <span
                  key={`${row.rootKey}:${s}`}
                  className="rounded bg-slate-200/90 px-1 py-px font-mono text-slate-500 line-through break-all"
                >
                  {formatDmViewKey(s)}
                </span>
              ))}
            </>
          ) : null}
          <span className="text-slate-500">→</span>
          <span
            className={`rounded bg-blue-100 px-1 py-px font-mono font-medium text-blue-900 break-all ${
              emphasizeSupplyingWinner ? "border-2 border-sky-600" : ""
            }`}
          >
            {formatDmViewKey(row.winnerKey)}
          </span>
        </div>
      ))}
    </div>
  );
}

function HeatmapWinnerDriftChips({
  blocks,
  t,
  withTopRule = true,
  driftCell,
  onDriftMissingWinnerClick,
}: {
  blocks: HeatmapWinnerDriftBlock[];
  t: (key: string, params?: Record<string, string | number>) => string;
  withTopRule?: boolean;
  driftCell?: { version: string; field: string };
  onDriftMissingWinnerClick?: (payload: HeatmapDriftMissingWinnerDebugPayload) => void;
}) {
  const nonEmpty = blocks.filter((b) => b.changes.length > 0);
  if (nonEmpty.length === 0) return null;
  return (
    <div
      className={
        withTopRule
          ? "mt-2 flex min-w-0 flex-col gap-2 border-t border-slate-200 pt-2"
          : "flex min-w-0 flex-col gap-2"
      }
    >
      {nonEmpty.map((b) => (
        <div key={`${b.relation}:${b.neighborVersion}`} className="min-w-0">
          <p className="mb-1 text-[10px] font-medium text-slate-600">
            {b.relation === "older"
              ? t("dataCatalog.versionHistory.fieldHeatmapDriftVsOlder", { version: b.neighborVersion })
              : t("dataCatalog.versionHistory.fieldHeatmapDriftVsNewer", { version: b.neighborVersion })}
          </p>
          <div className="flex flex-col gap-1.5">
            {b.changes.map((ch) => {
              const cellVersion = driftCell?.version ?? "";
              const targetVer = driftRootAddRemoveTargetVersion(b.relation, cellVersion, b.neighborVersion);
              const rk = formatDmViewKey(ch.rootKey);

              if (ch.driftRowKind === "root_added") {
                return (
                  <p
                    key={ch.rootKey}
                    className="text-[10px] leading-snug text-emerald-900"
                  >
                    {t("dataCatalog.versionHistory.fieldHeatmapDriftRootAdded", {
                      version: targetVer,
                      root: rk,
                      supplier: formatDmViewKey(ch.toWinner),
                    })}
                  </p>
                );
              }
              if (ch.driftRowKind === "root_removed") {
                return (
                  <p
                    key={ch.rootKey}
                    className="text-[10px] leading-snug text-rose-900"
                  >
                    {t("dataCatalog.versionHistory.fieldHeatmapDriftRootRemoved", {
                      version: targetVer,
                      root: rk,
                      supplier: formatDmViewKey(ch.fromWinner),
                    })}
                  </p>
                );
              }

              return (
                <div key={ch.rootKey} className="flex min-w-0 flex-wrap items-center gap-1.5 text-[10px]">
                  <span className="rounded border border-slate-200 bg-white px-1 py-px font-mono text-slate-600 break-all">
                    {rk}
                  </span>
                  <div className="inline-flex min-w-0 max-w-full flex-wrap items-center gap-1 rounded-md border-2 border-sky-600 bg-sky-50 px-1.5 py-1">
                    {ch.fromWinner.trim() === "" ? (
                      <button
                        type="button"
                        className="rounded border border-amber-600 bg-amber-50 px-1 py-px text-left font-mono text-[9px] leading-tight text-amber-950 hover:bg-amber-100 break-all"
                        onClick={() =>
                          onDriftMissingWinnerClick?.({
                            cell: driftCell ?? { version: "", field: "" },
                            driftBlock: {
                              relation: b.relation,
                              neighborVersion: b.neighborVersion,
                              sigBefore: b.sigBefore,
                              sigAfter: b.sigAfter,
                            },
                            change: ch,
                            missingSide: "from",
                          })
                        }
                      >
                        {ch.fromAbsentInVersion != null
                          ? t("dataCatalog.versionHistory.fieldHeatmapDriftWinnerAbsentInVersion", {
                              version: ch.fromAbsentInVersion,
                            })
                          : t("dataCatalog.versionHistory.fieldHeatmapDriftWinnerMalformedSig")}
                      </button>
                    ) : (
                      <span className="rounded border border-slate-200 bg-white px-1 py-px font-mono text-slate-800 break-all">
                        {formatDmViewKey(ch.fromWinner)}
                      </span>
                    )}
                    <span className="shrink-0 font-medium text-sky-800">→</span>
                    {ch.toWinner.trim() === "" ? (
                      <button
                        type="button"
                        className="rounded border border-amber-600 bg-amber-50 px-1 py-px text-left font-mono text-[9px] leading-tight text-amber-950 hover:bg-amber-100 break-all"
                        onClick={() =>
                          onDriftMissingWinnerClick?.({
                            cell: driftCell ?? { version: "", field: "" },
                            driftBlock: {
                              relation: b.relation,
                              neighborVersion: b.neighborVersion,
                              sigBefore: b.sigBefore,
                              sigAfter: b.sigAfter,
                            },
                            change: ch,
                            missingSide: "to",
                          })
                        }
                      >
                        {ch.toAbsentInVersion != null
                          ? t("dataCatalog.versionHistory.fieldHeatmapDriftWinnerAbsentInVersion", {
                              version: ch.toAbsentInVersion,
                            })
                          : t("dataCatalog.versionHistory.fieldHeatmapDriftWinnerMalformedSig")}
                      </button>
                    ) : (
                      <span className="rounded border border-sky-500 bg-sky-100 px-1 py-px font-mono text-slate-900 break-all">
                        {formatDmViewKey(ch.toWinner)}
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}

export type FieldPresenceHeatmapModel = {
  columns: string[];
  rowVersions: string[];
  matrix: boolean[][];
  resolutionRowsByCellKey: ReadonlyMap<string, HeatmapResolutionRowData[]>;
  winnerSigByCellKey: ReadonlyMap<string, string>;
};

export function buildFieldPresenceHeatmap(
  versions: string[],
  dmKey: string,
  detailsMap: ReadonlyMap<string, DmVersionSnapshot>,
  rowVersions: ReadonlyMap<string, DmVersionSnapshot>
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

  const resolutionRowsByCellKey = new Map<string, HeatmapResolutionRowData[]>();
  const winnerSigByCellKey = new Map<string, string>();
  for (let vi = 0; vi < versions.length; vi++) {
    const ver = versions[vi];
    const byProp = perVersion[vi];
    if (ver == null || !byProp) continue;
    for (const propName of columns) {
      const contribs = byProp.get(propName);
      if (contribs == null || contribs.length === 0) continue;
      resolutionRowsByCellKey.set(heatmapCellKey(ver, propName), contributorsToResolutionRows(contribs));
      const sig = heatmapWinnerSignatureForProp(byProp, propName);
      if (sig) winnerSigByCellKey.set(heatmapCellKey(ver, propName), sig);
    }
  }

  return { columns, rowVersions: versions, matrix, resolutionRowsByCellKey, winnerSigByCellKey };
}

/** Matches `DEST_DM_VERSION_UNSPECIFIED` in DataModelVersions (destination.dataModel without version). */
export const DM_TX_DEST_LATEST = "__dest_dm_latest__";

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

export type HeatmapRowVisual = {
  usageLine: string;
  olderWriteDestination: boolean;
  rowTooltip: string;
};

type FieldPresenceHeatmapProps = {
  columns: string[];
  rows: string[];
  matrix: boolean[][];
  resolutionRowsByCellKey: ReadonlyMap<string, HeatmapResolutionRowData[]>;
  winnerSigByCellKey: ReadonlyMap<string, string>;
  rowsMeta: HeatmapRowVisual[];
  showTxSqlLegend: boolean;
  t: (key: string, params?: Record<string, string | number>) => string;
};

const HEAT_PRESENT = "#2563eb";
const HEAT_PRESENT_WINNER_DRIFT = "#93c5fd";

export function FieldPresenceHeatmap({
  columns,
  rows,
  matrix,
  resolutionRowsByCellKey,
  winnerSigByCellKey,
  rowsMeta,
  showTxSqlLegend,
  t,
}: FieldPresenceHeatmapProps) {
  const [hover, setHover] = useState<{ ri: number; ci: number } | null>(null);
  const [pinned, setPinned] = useState<{ ri: number; ci: number } | null>(null);
  const [labelHoverRow, setLabelHoverRow] = useState<number | null>(null);
  const [heatmapDriftDebugJson, setHeatmapDriftDebugJson] = useState<string | null>(null);

  useEffect(() => {
    setHover(null);
    setPinned(null);
    setLabelHoverRow(null);
  }, [columns, rows, resolutionRowsByCellKey, winnerSigByCellKey]);

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
      const resolutionRows =
        resolutionRowsByCellKey.get(heatmapCellKey(ver, propName)) ?? [];
      const hasWinnerDriftColor =
        exists && heatmapWinnerDriftVsNeighbor(matrix, rows, columns, winnerSigByCellKey, ri, ci);
      const winnerDriftBlocks = hasWinnerDriftColor
        ? buildWinnerDriftBlocksForCell(matrix, rows, columns, winnerSigByCellKey, ri, ci)
        : [];
      return {
        version: ver,
        propName,
        exists,
        resolutionRows,
        winnerDriftBlocks,
      };
    },
    [rows, columns, matrix, resolutionRowsByCellKey, winnerSigByCellKey]
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

  const primaryResolutionDetail =
    primaryDetail != null && primaryDetail.resolutionRows.length > 0
      ? filterResolutionRowsForDetails(primaryDetail.resolutionRows)
      : null;
  const alsoHoverResolutionDetail =
    alsoHovering != null && alsoHovering.resolutionRows.length > 0
      ? filterResolutionRowsForDetails(alsoHovering.resolutionRows)
      : null;

  useEffect(() => {
    setHeatmapDriftDebugJson(null);
  }, [primary?.ri, primary?.ci]);

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
      <p className="mb-2 text-[10px] leading-snug text-slate-600">
        {t("dataCatalog.versionHistory.fieldHeatmapLegendLightBlue")}
      </p>

      <div className="mb-2 rounded-md border border-slate-200 bg-slate-50 px-2 py-2">
        <div className="flex min-h-[26px] flex-wrap items-center justify-between gap-2">
          <span className="text-[11px] font-medium text-slate-800">
            {t("dataCatalog.versionHistory.fieldHeatmapDetailTitle")}
          </span>
          <div className="flex h-[22px] shrink-0 items-center gap-2">
            <span
              className={`rounded bg-blue-100 px-1.5 py-0.5 text-[10px] font-medium text-blue-900 ${
                showPinned ? "" : "invisible pointer-events-none"
              }`}
            >
              {t("dataCatalog.versionHistory.fieldHeatmapDetailPinned")}
            </span>
            <span
              className={`rounded bg-slate-200/80 px-1.5 py-0.5 text-[10px] font-medium text-slate-700 ${
                showHoverOnly ? "" : "invisible pointer-events-none"
              }`}
            >
              {t("dataCatalog.versionHistory.fieldHeatmapDetailHover")}
            </span>
            <button
              type="button"
              className={`rounded border border-slate-300 bg-white px-2 py-0.5 text-[10px] text-slate-700 hover:bg-slate-100 ${
                pinned ? "" : "invisible pointer-events-none"
              }`}
              onClick={() => setPinned(null)}
            >
              {t("dataCatalog.versionHistory.fieldHeatmapDetailClearPin")}
            </button>
          </div>
        </div>
        <div className="h-36 shrink-0 overflow-y-auto overflow-x-hidden overscroll-y-contain pr-1 [scrollbar-gutter:stable]">
          {labelHoverRow != null && labelHoverRow >= 1 ? (
            <p className="pt-1 text-[10px] leading-snug text-orange-800">
              {t("dataCatalog.versionHistory.fieldHeatmapLegendAddedFieldHover", {
                version: rows[labelHoverRow] ?? "",
              })}
            </p>
          ) : primaryDetail ? (
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
              {primaryDetail.resolutionRows.length > 0 ? (
                <>
                  <dt className="self-start text-slate-500">
                    {t("dataCatalog.versionHistory.fieldHeatmapDetailResolution")}
                  </dt>
                  <dd className="min-w-0 text-slate-700">
                    {primaryResolutionDetail?.trivialMultiRootSummary ? (
                      <p className="text-[10px] leading-snug text-slate-600">
                        {t("dataCatalog.versionHistory.fieldHeatmapResolutionTrivialManyRoots", {
                          field: primaryDetail.propName,
                          count: primaryResolutionDetail.omittedTrivialCount,
                        })}
                      </p>
                    ) : (
                      <>
                        <p className="mb-1.5 text-[10px] leading-snug text-slate-500">
                          {t("dataCatalog.versionHistory.fieldHeatmapResolutionRuleShort")}
                        </p>
                        {primaryResolutionDetail != null && primaryResolutionDetail.displayRows.length > 0 ? (
                          <HeatmapResolutionChips
                            rows={primaryResolutionDetail.displayRows}
                            emphasizeSupplyingWinner={
                              primaryDetail.winnerDriftBlocks.length > 0 &&
                              primaryResolutionDetail.displayRows.length > 0
                            }
                          />
                        ) : null}
                        {primaryResolutionDetail != null && primaryResolutionDetail.omittedTrivialCount > 0 ? (
                          <p className="mt-1.5 text-[10px] leading-snug text-slate-500">
                            {t("dataCatalog.versionHistory.fieldHeatmapResolutionOmittedTrivialRoots", {
                              count: primaryResolutionDetail.omittedTrivialCount,
                            })}
                          </p>
                        ) : null}
                      </>
                    )}
                    <HeatmapWinnerDriftChips
                      blocks={primaryDetail.winnerDriftBlocks}
                      t={t}
                      withTopRule
                      driftCell={{ version: primaryDetail.version, field: primaryDetail.propName }}
                      onDriftMissingWinnerClick={(p) =>
                        setHeatmapDriftDebugJson(JSON.stringify(p, null, 2))
                      }
                    />
                  </dd>
                </>
              ) : primaryDetail.winnerDriftBlocks.length > 0 ? (
                <>
                  <dt className="self-start text-slate-500">
                    {t("dataCatalog.versionHistory.fieldHeatmapDetailWinnerDrift")}
                  </dt>
                  <dd className="min-w-0 text-slate-700">
                    <HeatmapWinnerDriftChips
                      blocks={primaryDetail.winnerDriftBlocks}
                      t={t}
                      withTopRule={false}
                      driftCell={{ version: primaryDetail.version, field: primaryDetail.propName }}
                      onDriftMissingWinnerClick={(p) =>
                        setHeatmapDriftDebugJson(JSON.stringify(p, null, 2))
                      }
                    />
                  </dd>
                </>
              ) : null}
            </dl>
          ) : (
            <p className="mt-2 text-[11px] leading-snug text-slate-600">
              {t("dataCatalog.versionHistory.fieldHeatmapDetailEmpty")}
            </p>
          )}
          {labelHoverRow == null && alsoHovering != null ? (
            <div className="mt-2 border-t border-slate-200 pt-2 text-[10px] leading-snug text-slate-600">
              <p className="font-medium text-slate-700">
                {t("dataCatalog.versionHistory.fieldHeatmapDetailHover")}
                {": "}
                {t("dataCatalog.versionHistory.fieldHeatmapTooltip", {
                  version: alsoHovering.version,
                  field: alsoHovering.propName,
                })}
              </p>
              {alsoHovering.resolutionRows.length > 0 && alsoHoverResolutionDetail != null ? (
                <div className="mt-1.5">
                  {alsoHoverResolutionDetail.trivialMultiRootSummary ? (
                    <p className="text-[10px] leading-snug text-slate-600">
                      {t("dataCatalog.versionHistory.fieldHeatmapResolutionTrivialManyRoots", {
                        field: alsoHovering.propName,
                        count: alsoHoverResolutionDetail.omittedTrivialCount,
                      })}
                    </p>
                  ) : (
                    <>
                      <HeatmapResolutionChips
                        rows={alsoHoverResolutionDetail.displayRows}
                        emphasizeSupplyingWinner={
                          alsoHovering.winnerDriftBlocks.length > 0 &&
                          alsoHoverResolutionDetail.displayRows.length > 0
                        }
                      />
                      {alsoHoverResolutionDetail.omittedTrivialCount > 0 ? (
                        <p className="mt-1.5 text-[10px] leading-snug text-slate-500">
                          {t("dataCatalog.versionHistory.fieldHeatmapResolutionOmittedTrivialRoots", {
                            count: alsoHoverResolutionDetail.omittedTrivialCount,
                          })}
                        </p>
                      ) : null}
                    </>
                  )}
                </div>
              ) : null}
              <HeatmapWinnerDriftChips
                blocks={alsoHovering.winnerDriftBlocks}
                t={t}
                withTopRule={alsoHovering.resolutionRows.length > 0}
                driftCell={{ version: alsoHovering.version, field: alsoHovering.propName }}
                onDriftMissingWinnerClick={(p) => setHeatmapDriftDebugJson(JSON.stringify(p, null, 2))}
              />
            </div>
          ) : null}
        </div>
        {heatmapDriftDebugJson != null ? (
          <div className="mt-2 border-t border-slate-200 pt-2">
            <div className="mb-1 flex flex-wrap items-center justify-between gap-2">
              <span className="text-[10px] font-medium text-slate-600">
                {t("dataCatalog.versionHistory.fieldHeatmapDriftDebugTitle")}
              </span>
              <button
                type="button"
                className="shrink-0 rounded border border-slate-300 bg-white px-2 py-0.5 text-[10px] text-slate-700 hover:bg-slate-100"
                onClick={() => setHeatmapDriftDebugJson(null)}
              >
                {t("dataCatalog.versionHistory.fieldHeatmapDriftDebugDismiss")}
              </button>
            </div>
            <pre className="max-h-40 overflow-auto rounded border border-slate-300 bg-slate-950 p-2 text-[9px] leading-snug text-amber-100">
              {heatmapDriftDebugJson}
            </pre>
          </div>
        ) : null}
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
                  const winnerDrift =
                    exists &&
                    heatmapWinnerDriftVsNeighbor(matrix, rows, columns, winnerSigByCellKey, ri, ci);
                  const stroke = isPinned ? "#1d4ed8" : isHover ? "#64748b" : "#e2e8f0";
                  const strokeW = isPinned ? 1.25 : isHover ? 0.9 : 0.35;
                  const fill = exists
                    ? winnerDrift
                      ? HEAT_PRESENT_WINNER_DRIFT
                      : HEAT_PRESENT
                    : addedGapOrange
                      ? "#fb923c"
                      : "#ffffff";
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
