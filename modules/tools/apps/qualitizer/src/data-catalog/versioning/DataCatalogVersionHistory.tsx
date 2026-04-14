import { useEffect, useMemo, useState } from "react";
import { useI18n } from "@/shared/i18n";
import { getDataModelUrl } from "@/shared/cdf-browser-url";
import { useAppSdk } from "@/shared/auth";
import { compareVersionStrings } from "./versioning-utils";
import type { DmVersionSnapshot, TransitionDiff } from "./version-history-types";
import { formatTs, shouldShowUpdatedSeparate } from "./version-history-utils";
import { buildTransitionDiff, isLikelyFullViewDef, snapshotForKey } from "./version-history-diff";
import { DM_TX_DEST_LATEST, FieldPresenceHeatmap, buildFieldPresenceHeatmap, type HeatmapRowVisual } from "./FieldPresenceHeatmap";
import { VersionSnapshotMeta } from "./VersionSnapshotMeta";

export type { DmVersionSnapshot } from "./version-history-types";

const EMPTY_TX_MAP = new Map<string, Array<{ id: string; name: string }>>();
const EMPTY_STR_SET = new Set<string>();

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
    () => buildFieldPresenceHeatmap(versionsNewestFirst, dmKey, detailsMap, rowVersions),
    [versionsNewestFirst, dmKey, detailsMap, rowVersions]
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
              resolutionRowsByCellKey={fieldHeatmap.resolutionRowsByCellKey}
              winnerSigByCellKey={fieldHeatmap.winnerSigByCellKey}
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
