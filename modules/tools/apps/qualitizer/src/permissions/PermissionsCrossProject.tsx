import { ApiError } from "@/shared/ApiError";
import { capabilityActionBand, getActionDisplay } from "@/shared/permissions-utils";
import { useI18n } from "@/shared/i18n";
import type { KeyboardEvent as ReactKeyboardEvent } from "react";
import { useState } from "react";
import type { NormalizedCapability } from "./types";
import { PermissionsCrossProjectDriftModal } from "./PermissionsCrossProjectDriftModal";
import type {
  CrossProjectCapabilityCell,
  CrossProjectMatrixMetric,
  CrossProjectMembershipState,
} from "./useCrossProjectMembershipCheck";

type PermissionsCrossProjectProps = {
  state: CrossProjectMembershipState;
  privateMaskClass?: string;
};

function cellText(
  t: (key: string, vars?: Record<string, string | number>) => string,
  metric: CrossProjectMatrixMetric,
  member: boolean,
  groupId?: number,
  name?: string,
  sourceId?: string
): string {
  if (!member) return "—";
  if (metric === "status") return "✓";
  if (metric === "name") return name?.trim() || t("permissions.crossProject.cellUnknown");
  if (metric === "sourceId") return sourceId?.trim() || "—";
  return groupId != null ? String(groupId) : "—";
}

function parseNormFromDriftJson(json: string): NormalizedCapability | null {
  try {
    const o = JSON.parse(json) as {
      name?: unknown;
      actions?: string[];
      scope?: Record<string, unknown>;
    };
    if (typeof o.name !== "string") return null;
    return { name: o.name, actions: o.actions, scope: o.scope };
  } catch {
    return null;
  }
}

type DriftModalState = {
  capabilityName: string;
  projectUrlName: string;
  compareLabel: string;
  leftJson: string;
  rightJson: string;
};

function CapabilityDriftMarker(props: {
  c: CrossProjectCapabilityCell;
  capabilityName: string;
  projectUrlName: string;
  t: (key: string, vars?: Record<string, string | number>) => string;
  setDriftModal: (s: DriftModalState) => void;
}) {
  const { c, capabilityName, projectUrlName, t, setDriftModal } = props;
  if (!c.driftLeftJson || !c.driftRightJson || c.driftCompareLabel == null) return null;

  const openModal = () => {
    setDriftModal({
      capabilityName,
      projectUrlName,
      compareLabel: c.driftCompareLabel!,
      leftJson: c.driftLeftJson!,
      rightJson: c.driftRightJson!,
    });
  };

  const onKeyDown = (event: ReactKeyboardEvent) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    event.stopPropagation();
    openModal();
  };

  if (c.driftReadWriteTierOnly) {
    const n = parseNormFromDriftJson(c.driftLeftJson);
    const band = n ? capabilityActionBand(n) : "empty";
    if (band === "read" || band === "write") {
      const display = getActionDisplay(n!, t);
      const letter = band === "read" ? "R" : "W";
      const badgeTitle = t("permissions.crossProject.readWriteDriftBadgeTitle");
      return (
        <span
          role="button"
          tabIndex={0}
          className="inline-flex cursor-pointer items-center rounded-sm ring-1 ring-slate-300/80 hover:ring-slate-400"
          title={`${badgeTitle} — ${display.titleText}`}
          aria-label={`${badgeTitle}. ${display.titleText}`}
          onClick={(event) => {
            event.stopPropagation();
            openModal();
          }}
          onKeyDown={(event) => {
            event.stopPropagation();
            onKeyDown(event);
          }}
        >
          <span
            className="flex h-4 min-w-[1rem] items-center justify-center rounded px-0.5 text-[10px] font-bold leading-none text-slate-900"
            style={{ backgroundColor: display.color }}
          >
            {letter}
          </span>
        </span>
      );
    }
  }

  return (
    <span
      role="button"
      tabIndex={0}
      className="inline-block h-2.5 w-2.5 shrink-0 cursor-pointer rounded-full bg-orange-500 ring-1 ring-orange-600/30 hover:bg-orange-400"
      title={t("permissions.crossProject.scopeDriftDotTitle")}
      aria-label={t("permissions.crossProject.scopeDriftDotTitle")}
      onClick={(event) => {
        event.stopPropagation();
        openModal();
      }}
      onKeyDown={(event) => {
        event.stopPropagation();
        onKeyDown(event);
      }}
    />
  );
}

export function PermissionsCrossProject({ state, privateMaskClass = "" }: PermissionsCrossProjectProps) {
  const { t } = useI18n();
  const [metric, setMetric] = useState<CrossProjectMatrixMetric>("status");
  const [driftModal, setDriftModal] = useState<DriftModalState | null>(null);

  if (state.status === "idle" || state.status === "loading") {
    return <div className="text-sm text-slate-600">{t("permissions.crossProject.loading")}</div>;
  }

  if (state.status === "error") {
    return (
      <ApiError
        message={state.message}
        api={state.api}
        details={
          state.detailPayload != null ? (
            <pre className="mt-1 max-h-64 overflow-auto whitespace-pre-wrap font-mono text-[11px]">
              {JSON.stringify(state.detailPayload, null, 2)}
            </pre>
          ) : undefined
        }
      />
    );
  }

  const {
    projects,
    rows,
    allProjectsMatch,
    projectTokenGroupIdCounts,
    projectLogicalMemberCounts,
    capabilityRows,
    groupListAccessDeniedProjects,
  } = state;
  const hasIdOnlyRows = rows.some((r) => r.idOnlyMatch);
  const deniedProjectSet = new Set(groupListAccessDeniedProjects);

  return (
    <div className="space-y-4">
      <PermissionsCrossProjectDriftModal
        open={driftModal != null}
        capabilityName={driftModal?.capabilityName ?? ""}
        projectUrlName={driftModal?.projectUrlName ?? ""}
        compareLabel={driftModal?.compareLabel ?? ""}
        leftJson={driftModal?.leftJson ?? ""}
        rightJson={driftModal?.rightJson ?? ""}
        onClose={() => setDriftModal(null)}
      />
      {projects.length === 0 ? (
        <p className="text-sm text-slate-600">{t("permissions.crossProject.noProjects")}</p>
      ) : (
        <>
          {groupListAccessDeniedProjects.length > 0 ? (
            <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-950">
              {t("permissions.crossProject.groupDefinitionsForbiddenSummary", {
                projects: groupListAccessDeniedProjects.join(", "),
              })}
            </div>
          ) : null}
          <div
            className={`rounded-md border px-3 py-2 text-sm ${
              rows.length === 0
                ? "border-slate-200 bg-slate-50/90 text-slate-800"
                : allProjectsMatch
                  ? "border-emerald-200 bg-emerald-50/90 text-emerald-950"
                  : "border-amber-200 bg-amber-50/90 text-amber-950"
            }`}
          >
            {rows.length === 0
              ? t("permissions.crossProject.summaryEmpty")
              : allProjectsMatch
                ? t("permissions.crossProject.summaryMatch")
                : t("permissions.crossProject.summaryMismatch")}
          </div>

          {hasIdOnlyRows ? (
            <p className="text-xs text-slate-600">{t("permissions.crossProject.idOnlyNote")}</p>
          ) : null}

          {rows.length === 0 ? (
            <p className="text-sm text-slate-600">{t("permissions.crossProject.noMemberships")}</p>
          ) : (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-xs font-medium text-slate-700">
                  {t("permissions.crossProject.metricLabel")}
                </span>
                {(
                  [
                    ["status", "permissions.crossProject.metricStatus"],
                    ["name", "permissions.crossProject.metricName"],
                    ["sourceId", "permissions.crossProject.metricSourceId"],
                    ["id", "permissions.crossProject.metricId"],
                  ] as const
                ).map(([value, labelKey]) => (
                  <button
                    key={value}
                    type="button"
                    onClick={() => setMetric(value)}
                    className={`rounded-md px-3 py-1 text-xs font-medium transition ${
                      metric === value
                        ? "bg-slate-900 text-white"
                        : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
                    }`}
                  >
                    {t(labelKey)}
                  </button>
                ))}
              </div>

              <div className="overflow-auto rounded-md border border-slate-200">
                <table className="w-full min-w-[480px] border-collapse text-left text-xs">
                  <thead className="bg-slate-50 text-slate-600">
                    <tr>
                      <th className="sticky left-0 z-10 min-w-[200px] border-b border-r border-slate-200 bg-slate-50 px-2 py-2 font-medium">
                        {t("permissions.crossProject.colGroup")}
                      </th>
                      {projects.map((p) => {
                        const tokenN = projectTokenGroupIdCounts[p] ?? 0;
                        const logicalN = projectLogicalMemberCounts[p] ?? 0;
                        const title =
                          tokenN === logicalN
                            ? t("permissions.crossProject.columnCountTitleMatch", {
                                n: logicalN,
                              })
                            : t("permissions.crossProject.columnCountTitleMerged", {
                                logical: logicalN,
                                token: tokenN,
                                merged: tokenN - logicalN,
                              });
                        const deniedCol = deniedProjectSet.has(p);
                        return (
                          <th
                            key={p}
                            className={`border-b border-slate-200 px-2 py-2 text-center font-medium ${
                              deniedCol ? "bg-amber-100 ring-1 ring-inset ring-amber-300/90" : ""
                            }`}
                          >
                            <div className="flex flex-col items-center gap-0.5">
                              <span
                                className="max-w-[140px] truncate"
                                title={
                                  deniedCol
                                    ? `${p} — ${t("permissions.crossProject.columnDefinitionsForbiddenTitle")}`
                                    : p
                                }
                              >
                                {p}
                              </span>
                              <span
                                className="cursor-default rounded bg-slate-200/80 px-1.5 py-0.5 text-[10px] font-normal text-slate-700"
                                title={title}
                              >
                                {t("permissions.crossProject.memberCount", {
                                  n: logicalN,
                                })}
                              </span>
                            </div>
                          </th>
                        );
                      })}
                    </tr>
                  </thead>
                  <tbody className={`divide-y divide-slate-100${privateMaskClass}`}>
                    {rows.map((row) => {
                      const memberProjects = projects.filter((p) => row.cells[p]?.member);
                      const partial =
                        memberProjects.length > 0 && memberProjects.length < projects.length;
                      return (
                        <tr key={row.canonicalKey}>
                          <td className="sticky left-0 z-10 border-r border-slate-100 bg-white px-2 py-1.5 font-medium text-slate-800">
                            <span className="flex items-center gap-1.5">
                              {row.rowLabel}
                              {row.idOnlyMatch ? (
                                <span
                                  className="rounded bg-slate-100 px-1 py-0.5 text-[10px] font-normal text-slate-600"
                                  title={t("permissions.crossProject.idOnlyBadgeTitle")}
                                >
                                  {t("permissions.crossProject.idOnlyBadge")}
                                </span>
                              ) : null}
                            </span>
                          </td>
                          {projects.map((p) => {
                            const c = row.cells[p] ?? { member: false };
                            const text = cellText(t, metric, c.member, c.groupId, c.name, c.sourceId);
                            const gap = partial && !c.member;
                            const deniedCol = deniedProjectSet.has(p);
                            const memberTitle = [
                              c.name,
                              c.sourceId,
                              c.groupId != null ? `ID ${c.groupId}` : "",
                            ]
                              .filter(Boolean)
                              .join(" · ");
                            return (
                              <td
                                key={p}
                                className={`px-2 py-1.5 text-center font-mono ${
                                  c.member
                                    ? deniedCol
                                      ? "bg-emerald-100 text-emerald-950 ring-1 ring-inset ring-amber-400"
                                      : "bg-emerald-100 text-emerald-950"
                                    : gap
                                      ? "bg-rose-100 text-rose-950"
                                      : "bg-slate-100 text-slate-600"
                                }`}
                                title={
                                  c.member
                                    ? deniedCol
                                      ? [memberTitle, t("permissions.crossProject.membershipForbiddenCellTitle")]
                                          .filter(Boolean)
                                          .join(" — ")
                                      : memberTitle
                                    : gap
                                      ? t("permissions.crossProject.cellGapTitle")
                                      : ""
                                }
                              >
                                {metric === "status" ? (
                                  <span className="text-base">{text}</span>
                                ) : (
                                  <span className="text-[11px]">{text}</span>
                                )}
                              </td>
                            );
                          })}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              <div className="flex flex-wrap gap-3 text-[11px] text-slate-600">
                <span className="flex items-center gap-1.5">
                  <span className="inline-block h-3 w-6 rounded-sm bg-emerald-100 ring-1 ring-emerald-300/90" />
                  {t("permissions.crossProject.legendMember")}
                </span>
                <span className="flex items-center gap-1.5">
                  <span className="inline-block h-3 w-6 rounded-sm bg-rose-100 ring-1 ring-rose-300/90" />
                  {t("permissions.crossProject.legendGap")}
                </span>
                      <span className="flex items-center gap-1.5">
                        <span className="inline-block h-3 w-6 rounded-sm bg-slate-100 ring-1 ring-slate-300/90" />
                        {t("permissions.crossProject.legendOther")}
                      </span>
                <span className="flex items-center gap-1.5">
                  <span className="inline-flex h-3 w-6 items-center justify-center rounded-sm bg-amber-100 text-[10px] font-bold text-amber-950 ring-1 ring-amber-300/90">
                    ?
                  </span>
                  {t("permissions.crossProject.legendDefinitionsForbidden")}
                </span>
              </div>
            </>
          )}

          <div className="space-y-2 border-t border-slate-200 pt-4">
                <h3 className="text-sm font-semibold text-slate-900">
                  {t("permissions.crossProject.capabilitiesTitle")}
                </h3>
                <p className="text-xs text-slate-600">{t("permissions.crossProject.capabilitiesDescription")}</p>
                {capabilityRows.length === 0 ? (
                  <p className="text-sm text-slate-600">{t("permissions.crossProject.capabilitiesNone")}</p>
                ) : (
                  <>
                    <div className="overflow-auto rounded-md border border-slate-200">
                      <table className="w-full min-w-[480px] border-collapse text-left text-xs">
                        <thead className="bg-slate-50 text-slate-600">
                          <tr>
                            <th className="sticky left-0 z-10 min-w-[200px] border-b border-r border-slate-200 bg-slate-50 px-2 py-2 font-medium">
                              {t("permissions.crossProject.colCapability")}
                            </th>
                            {projects.map((p) => {
                              const deniedCol = deniedProjectSet.has(p);
                              return (
                                <th
                                  key={`cap-${p}`}
                                  className={`border-b border-slate-200 px-2 py-2 text-center font-medium ${
                                    deniedCol ? "bg-amber-100 ring-1 ring-inset ring-amber-300/90" : ""
                                  }`}
                                >
                                  <span
                                    className="max-w-[140px] truncate"
                                    title={
                                      deniedCol
                                        ? `${p} — ${t("permissions.crossProject.columnDefinitionsForbiddenTitle")}`
                                        : p
                                    }
                                  >
                                    {p}
                                  </span>
                                </th>
                              );
                            })}
                          </tr>
                        </thead>
                        <tbody className={`divide-y divide-slate-100${privateMaskClass}`}>
                          {capabilityRows.map((crow) => {
                            const projectsWithCapData = projects.filter(
                              (p) => !crow.cells[p]?.definitionsUnavailable
                            );
                            const presentProjects = projects.filter((p) => crow.cells[p]?.present);
                            const partial =
                              projectsWithCapData.length > 0 &&
                              presentProjects.length > 0 &&
                              presentProjects.length < projectsWithCapData.length;
                            return (
                              <tr key={crow.capabilityName}>
                                <td className="sticky left-0 z-10 border-r border-slate-100 bg-white px-2 py-1.5 font-medium text-slate-800">
                                  {crow.capabilityName}
                                </td>
                                {projects.map((p) => {
                                  const c = crow.cells[p] ?? {
                                    present: false,
                                    gap: false,
                                    showScopeDrift: false,
                                  };
                                  const gap =
                                    partial && !c.present && !c.definitionsUnavailable;
                                  return (
                                    <td
                                      key={p}
                                      className={`px-2 py-1.5 text-center ${
                                        c.definitionsUnavailable
                                          ? "bg-amber-100 text-amber-950 ring-1 ring-inset ring-amber-300/80"
                                          : c.present
                                            ? "bg-emerald-100 text-emerald-950"
                                            : gap
                                              ? "bg-rose-100 text-rose-950"
                                              : "bg-slate-100 text-slate-600"
                                      }`}
                                      title={
                                        c.definitionsUnavailable
                                          ? t("permissions.crossProject.capCellDefinitionsForbiddenTitle")
                                          : c.present
                                            ? c.showScopeDrift
                                              ? c.driftReadWriteTierOnly
                                                ? t("permissions.crossProject.capCellReadWriteDriftTitle")
                                                : t("permissions.crossProject.capCellDriftTitle")
                                              : t("permissions.crossProject.capCellPresentTitle")
                                            : gap
                                              ? t("permissions.crossProject.capCellGapTitle")
                                              : ""
                                      }
                                    >
                                      <span className="inline-flex items-center justify-center gap-1.5">
                                        {c.definitionsUnavailable ? (
                                          <span className="text-sm font-semibold">?</span>
                                        ) : c.present ? (
                                          <span className="text-base">✓</span>
                                        ) : gap ? (
                                          <span className="text-base">—</span>
                                        ) : (
                                          <span className="text-base">—</span>
                                        )}
                                        {c.showScopeDrift ? (
                                          <CapabilityDriftMarker
                                            c={c}
                                            capabilityName={crow.capabilityName}
                                            projectUrlName={p}
                                            t={t}
                                            setDriftModal={setDriftModal}
                                          />
                                        ) : null}
                                      </span>
                                    </td>
                                  );
                                })}
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                    <div className="flex flex-wrap gap-3 text-[11px] text-slate-600">
                      <span className="flex items-center gap-1.5">
                        <span className="inline-block h-3 w-6 rounded-sm bg-emerald-100 ring-1 ring-emerald-300/90" />
                        {t("permissions.crossProject.legendCapPresent")}
                      </span>
                      <span className="flex items-center gap-1.5">
                        <span className="inline-block h-3 w-6 rounded-sm bg-rose-100 ring-1 ring-rose-300/90" />
                        {t("permissions.crossProject.legendCapGap")}
                      </span>
                      <span className="flex items-center gap-1.5">
                        <span className="inline-flex h-3 w-6 items-center justify-center rounded-sm bg-amber-100 text-[10px] font-bold text-amber-950 ring-1 ring-amber-300/90">
                          ?
                        </span>
                        {t("permissions.crossProject.legendCapDefinitionsForbidden")}
                      </span>
                      <span className="flex items-center gap-1.5">
                        <span className="inline-block h-2.5 w-2.5 rounded-full bg-orange-500 ring-1 ring-orange-600/30" />
                        {t("permissions.crossProject.legendScopeDrift")}
                      </span>
                      <span className="flex items-center gap-1.5">
                        <span className="inline-flex items-center gap-1" aria-hidden>
                          <span
                            className="flex h-3 w-3 items-center justify-center rounded-sm text-[8px] font-bold text-slate-900 ring-1 ring-slate-300/80"
                            style={{ backgroundColor: "#bae6fd" }}
                          >
                            R
                          </span>
                          <span
                            className="flex h-3 w-3 items-center justify-center rounded-sm text-[8px] font-bold text-slate-900 ring-1 ring-slate-300/80"
                            style={{ backgroundColor: "#bbf7d0" }}
                          >
                            W
                          </span>
                        </span>
                        {t("permissions.crossProject.legendReadWriteDrift")}
                      </span>
                    </div>
                  </>
                )}
          </div>
        </>
      )}
    </div>
  );
}
