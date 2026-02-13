import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { useState } from "react";
import { useI18n } from "@/shared/i18n";
import type { LoadState, PermissionScopeDriftEntry } from "./types";

type PermissionsHealthPanelProps = {
  permissionsStatus: LoadState;
  permissionsError: string | null;
  permissionScopeDrift: PermissionScopeDriftEntry[];
};

export function PermissionsHealthPanel({
  permissionsStatus,
  permissionsError,
  permissionScopeDrift,
}: PermissionsHealthPanelProps) {
  const { t } = useI18n();
  const [openId, setOpenId] = useState<string | null>(null);

  return (
    <Card>
      <CardHeader>
        <CardTitle>{t("healthChecks.permissions.drift.title")}</CardTitle>
        <CardDescription>{t("healthChecks.permissions.drift.description")}</CardDescription>
      </CardHeader>
      <CardContent>
        {permissionsStatus === "loading" ? (
          <div className="text-sm text-slate-600">{t("healthChecks.permissions.loading")}</div>
        ) : null}
        {permissionsStatus === "error" ? (
          <ApiError message={permissionsError ?? t("healthChecks.errors.permissions")} />
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
                        <span>{finding.summary}</span>
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
                            <ul className="mt-1 list-disc space-y-1 pl-5">
                              {finding.common.map((value) => (
                                <li key={`common-${value}`}>{value}</li>
                              ))}
                            </ul>
                          ) : (
                            <div className="mt-1">{t("healthChecks.permissions.drift.none")}</div>
                          )}
                          <div className="mt-2 font-semibold">
                            {t("healthChecks.permissions.drift.uniqueLeft", {
                              group: finding.leftGroup,
                            })}
                          </div>
                          {finding.leftOnly.length > 0 ? (
                            <ul className="mt-1 list-disc space-y-1 pl-5">
                              {finding.leftOnly.map((value) => (
                                <li key={`left-${value}`}>{value}</li>
                              ))}
                            </ul>
                          ) : (
                            <div className="mt-1">{t("healthChecks.permissions.drift.none")}</div>
                          )}
                          <div className="mt-2 font-semibold">
                            {t("healthChecks.permissions.drift.uniqueRight", {
                              group: finding.rightGroup,
                            })}
                          </div>
                          {finding.rightOnly.length > 0 ? (
                            <ul className="mt-1 list-disc space-y-1 pl-5">
                              {finding.rightOnly.map((value) => (
                                <li key={`right-${value}`}>{value}</li>
                              ))}
                            </ul>
                          ) : (
                            <div className="mt-1">{t("healthChecks.permissions.drift.none")}</div>
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
      </CardContent>
    </Card>
  );
}
