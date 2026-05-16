import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { useState } from "react";
import { useI18n } from "@/shared/i18n";
import { usePrivateMode } from "@/shared/PrivateModeContext";
import type {
  CompliantGroupEntry,
  LoadState,
  PermissionScopeDriftEntry,
  PermissionsHealthOverviewStats,
} from "./types";

type ChecksLoadingPhase = "listing" | "analyzing" | null;

type PermissionsHealthPanelProps = {
  permissionsStatus: LoadState;
  permissionsError: string | null;
  permissionScopeDrift: PermissionScopeDriftEntry[];
  compliantGroups?: CompliantGroupEntry[];
  permissionsStats?: PermissionsHealthOverviewStats | null;
  checksLoadingPhase?: ChecksLoadingPhase;
};

const REASON_ICONS: Record<string, string> = {
  all_scope: "🌐",
  no_capabilities: "📭",
  no_scope_entries: "📦",
  unique_scoping: "🔒",
  below_threshold: "📏",
  identical_scoping: "🤝",
};

export function PermissionsHealthPanel({
  permissionsStatus,
  permissionsError,
  permissionScopeDrift,
  compliantGroups,
  permissionsStats = null,
  checksLoadingPhase = null,
}: PermissionsHealthPanelProps) {
  const { t } = useI18n();
  const { isPrivateMode } = usePrivateMode();
  const pc = isPrivateMode ? " private-mask" : "";
  const [openId, setOpenId] = useState<string | null>(null);
  const [showCompliant, setShowCompliant] = useState(false);

  const checksLoading = permissionsStatus === "loading";

  return (
    <>
      {checksLoading ? (
        <div className="rounded-md border border-slate-200 bg-sky-50 px-3 py-3 text-sm text-slate-700">
          <p className="font-medium text-slate-900">{t("healthChecks.permissions.loading")}</p>
          {checksLoadingPhase === "listing" ? (
            <p className="mt-1 text-xs text-slate-600">
              {t("healthChecks.permissions.progress.listing")}
            </p>
          ) : null}
          {checksLoadingPhase === "analyzing" ? (
            <p className="mt-1 text-xs text-slate-600">
              {t("healthChecks.permissions.progress.analyzing")}
            </p>
          ) : null}
        </div>
      ) : null}
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.permissions.drift.title")}</CardTitle>
          <CardDescription>{t("healthChecks.permissions.drift.description")}</CardDescription>
        </CardHeader>
        <CardContent>
          {permissionsStatus === "error" ? (
            <ApiError message={permissionsError ?? t("healthChecks.errors.permissions")} />
          ) : null}
          {permissionsStatus === "success" && permissionsStats ? (
            <div className="mb-4 rounded-md border border-slate-200 bg-slate-50 p-3 text-sm text-slate-800">
              <div className="font-medium text-slate-900">
                {t("healthChecks.permissions.stats.title")}
              </div>
              <dl className="mt-2 grid gap-x-4 gap-y-1 text-xs sm:grid-cols-2">
                <div className="flex justify-between gap-2 border-b border-slate-200/80 py-1 sm:border-0">
                  <dt className="text-slate-600">{t("healthChecks.permissions.stats.groups")}</dt>
                  <dd className="tabular-nums font-medium text-slate-900">
                    {permissionsStats.groupCount.toLocaleString()}
                  </dd>
                </div>
                <div className="flex justify-between gap-2 border-b border-slate-200/80 py-1 sm:border-0">
                  <dt className="text-slate-600">
                    {t("healthChecks.permissions.stats.uniqueScopeLists")}
                  </dt>
                  <dd className="tabular-nums font-medium text-slate-900">
                    {permissionsStats.uniqueScopeLists.toLocaleString()}
                  </dd>
                </div>
                <div className="flex justify-between gap-2 border-b border-slate-200/80 py-1 sm:border-0">
                  <dt className="text-slate-600">
                    {t("healthChecks.permissions.stats.totalCapabilities")}
                  </dt>
                  <dd className="tabular-nums font-medium text-slate-900">
                    {permissionsStats.totalCapabilityRows.toLocaleString()}
                  </dd>
                </div>
                <div className="flex justify-between gap-2 border-b border-slate-200/80 py-1 sm:border-0">
                  <dt className="text-slate-600">
                    {t("healthChecks.permissions.stats.distinctTypes")}
                  </dt>
                  <dd className="tabular-nums font-medium text-slate-900">
                    {permissionsStats.distinctCapabilityTypes.toLocaleString()}
                  </dd>
                </div>
                <div className="flex justify-between gap-2 border-b border-slate-200/80 py-1 sm:border-0">
                  <dt className="text-slate-600">
                    {t("healthChecks.permissions.stats.groupsNoCapabilities")}
                  </dt>
                  <dd className="tabular-nums font-medium text-slate-900">
                    {permissionsStats.groupsWithNoCapabilities.toLocaleString()}
                  </dd>
                </div>
                <div className="flex justify-between gap-2 border-b border-slate-200/80 py-1 sm:border-0">
                  <dt className="text-slate-600">
                    {t("healthChecks.permissions.stats.allScopeRows")}
                  </dt>
                  <dd className="tabular-nums font-medium text-slate-900">
                    {permissionsStats.capabilityRowsAllScope.toLocaleString()}
                  </dd>
                </div>
                <div className="flex justify-between gap-2 border-b border-slate-200/80 py-1 sm:border-0">
                  <dt className="text-slate-600">
                    {t("healthChecks.permissions.stats.explicitScopeRows")}
                  </dt>
                  <dd className="tabular-nums font-medium text-slate-900">
                    {permissionsStats.capabilityRowsWithExplicitScopeLists.toLocaleString()}
                  </dd>
                </div>
                <div className="flex justify-between gap-2 py-1 sm:col-span-2">
                  <dt className="text-slate-600">
                    {t("healthChecks.permissions.stats.driftFindings")}
                  </dt>
                  <dd className="tabular-nums font-medium text-slate-900">
                    {permissionsStats.driftPairCount.toLocaleString()}
                  </dd>
                </div>
              </dl>
            </div>
          ) : null}
          {permissionsStatus === "success" ? (
            permissionScopeDrift.length > 0 ? (
              <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                <div className="font-medium">
                  {t("healthChecks.permissions.drift.count", {
                    count: permissionScopeDrift.length,
                  })}
                </div>
                <div className="mt-2 space-y-2">
                  {permissionScopeDrift.map((finding) => {
                    const isOpen = openId === finding.id;
                    return (
                      <div
                        key={finding.id}
                        className="rounded-md border border-amber-200 bg-white p-2 text-amber-900"
                      >
                        <div className="flex flex-wrap items-center justify-between gap-2 text-sm">
                          <span className={pc.trim()}>{finding.summary}</span>
                          <button
                            type="button"
                            className="rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-xs font-medium text-amber-900 hover:bg-amber-100"
                            onClick={() => setOpenId(isOpen ? null : finding.id)}
                          >
                            {isOpen
                              ? t("healthChecks.permissions.drift.explain.hide")
                              : t("healthChecks.permissions.drift.explain.show")}
                          </button>
                        </div>
                        {isOpen ? (
                          <div className="mt-2 rounded-md border border-amber-100 bg-amber-50 p-2 text-xs text-amber-900">
                            <div className="font-semibold">
                              {t("healthChecks.permissions.drift.common")}
                            </div>
                            {finding.common.length > 0 ? (
                              <ul className={`mt-1 list-disc space-y-1 pl-5${pc}`}>
                                {finding.common.map((value) => (
                                  <li key={`common-${value}`}>{value}</li>
                                ))}
                              </ul>
                            ) : (
                              <div className="mt-1 text-amber-700">{t("healthChecks.permissions.drift.emptyList")}</div>
                            )}
                            <div className="mt-2 font-semibold">
                              {t("healthChecks.permissions.drift.uniqueToPrefix")}<span className={pc.trim()}>{finding.leftGroup}</span>{t("healthChecks.permissions.drift.uniqueToSuffix")}
                            </div>
                            {finding.leftOnly.length > 0 ? (
                              <ul className={`mt-1 list-disc space-y-1 pl-5${pc}`}>
                                {finding.leftOnly.map((value) => (
                                  <li key={`left-${value}`}>{value}</li>
                                ))}
                              </ul>
                            ) : (
                              <div className="mt-1 text-amber-700">{t("healthChecks.permissions.drift.emptyList")}</div>
                            )}
                            <div className="mt-2 font-semibold">
                              {t("healthChecks.permissions.drift.uniqueToPrefix")}<span className={pc.trim()}>{finding.rightGroup}</span>{t("healthChecks.permissions.drift.uniqueToSuffix")}
                            </div>
                            {finding.rightOnly.length > 0 ? (
                              <ul className={`mt-1 list-disc space-y-1 pl-5${pc}`}>
                                {finding.rightOnly.map((value) => (
                                  <li key={`right-${value}`}>{value}</li>
                                ))}
                              </ul>
                            ) : (
                              <div className="mt-1 text-amber-700">{t("healthChecks.permissions.drift.emptyList")}</div>
                            )}
                          </div>
                        ) : null}
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : (
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.permissions.drift.none")}
              </div>
            )
          ) : null}
          {permissionsStatus === "success" && compliantGroups && compliantGroups.length > 0 ? (
            <div className="mt-4">
              <button
                type="button"
                className="cursor-pointer mb-2 text-sm font-medium text-slate-700 hover:text-slate-900"
                onClick={() => setShowCompliant((v) => !v)}
              >
                {showCompliant ? "▾" : "▸"} Compliant groups ({compliantGroups.length})
              </button>
              {showCompliant ? (
                <div className="overflow-x-auto rounded-md border border-slate-200">
                  <table className="w-full text-xs">
                    <thead className="bg-slate-50">
                      <tr className="text-left text-slate-500">
                        <th className="px-3 py-2">Group</th>
                        <th className="px-3 py-2 text-right">Capabilities</th>
                        <th className="px-3 py-2">Why compliant</th>
                        <th className="px-3 py-2">Details</th>
                      </tr>
                    </thead>
                    <tbody className={`divide-y divide-slate-100${pc}`}>
                      {compliantGroups.map((g) => (
                        <tr key={g.groupName} className="text-slate-600">
                          <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-800">
                            {g.groupName}
                          </td>
                          <td className="px-3 py-2 text-right tabular-nums">
                            {g.capabilityCount}
                          </td>
                          <td className="whitespace-nowrap px-3 py-2">
                            <span className="mr-1.5">{REASON_ICONS[g.reason] ?? "✓"}</span>
                            {g.label}
                          </td>
                          <td className="px-3 py-2 text-slate-500">
                            {g.details}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : null}
            </div>
          ) : null}
        </CardContent>
      </Card>
    </>
  );
}
