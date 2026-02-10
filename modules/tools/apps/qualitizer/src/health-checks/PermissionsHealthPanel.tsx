import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { useI18n } from "@/shared/i18n";
import type { LoadState } from "./types";

type PermissionsHealthPanelProps = {
  permissionsStatus: LoadState;
  permissionsError: string | null;
  permissionScopeDrift: string[];
};

export function PermissionsHealthPanel({
  permissionsStatus,
  permissionsError,
  permissionScopeDrift,
}: PermissionsHealthPanelProps) {
  const { t } = useI18n();

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
              <ul className="mt-2 list-disc space-y-1 pl-5 text-amber-900">
                {permissionScopeDrift.map((finding) => (
                  <li key={finding}>{finding}</li>
                ))}
              </ul>
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
