import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { useI18n } from "@/shared/i18n";
import { usePrivateMode } from "@/shared/PrivateModeContext";
import type { LoadState } from "./types";

export type NoopTransformation = {
  id: string;
  name: string;
  writes: number;
  noops: number;
};

export type DmvInconsistency = {
  modelKey: string;
  space: string;
  externalId: string;
  usages: Array<{
    transformationId: string;
    transformationName: string;
    version: string | undefined;
  }>;
};

type TransformationsHealthPanelProps = {
  noopStatus: LoadState;
  noopError: string | null;
  noopTransformations: NoopTransformation[];
  noopTotal: number;
  dmvStatus: LoadState;
  dmvError: string | null;
  dmvInconsistencies: DmvInconsistency[];
};

export function TransformationsHealthPanel({
  noopStatus,
  noopError,
  noopTransformations,
  noopTotal,
  dmvStatus,
  dmvError,
  dmvInconsistencies,
}: TransformationsHealthPanelProps) {
  const { t } = useI18n();
  const { isPrivateMode } = usePrivateMode();
  const pc = isPrivateMode ? " private-mask" : "";

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.transformations.noops.title")}</CardTitle>
          <CardDescription>
            {t("healthChecks.transformations.noops.description")}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {noopStatus === "loading" ? (
            <div className="text-sm text-slate-600">
              {t("healthChecks.transformations.loading")}
            </div>
          ) : null}
          {noopStatus === "error" ? (
            <ApiError
              message={
                noopError ?? t("healthChecks.transformations.noops.error")
              }
            />
          ) : null}
          {noopStatus === "success" ? (
            noopTransformations.length > 0 ? (
              <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-900">
                <div className="font-medium">
                  {t("healthChecks.transformations.noops.count", {
                    count: noopTransformations.length,
                    total: noopTotal,
                  })}
                </div>
                <ul className={`mt-2 list-disc space-y-1 pl-5 text-red-900${pc}`}>
                  {noopTransformations.map((tr) => (
                    <li key={tr.id}>
                      <span className="font-medium">{tr.name}</span>
                      <span className="text-red-700">
                        {" "}
                        · {t("healthChecks.transformations.noops.detail", {
                          writes: tr.writes.toLocaleString(),
                        })}
                      </span>
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.transformations.noops.allGood", {
                  total: noopTotal,
                })}
              </div>
            )
          ) : null}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>
            {t("healthChecks.dataModelVersioning.title")}
          </CardTitle>
          <CardDescription>
            {t("healthChecks.dataModelVersioning.description")}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {dmvStatus === "loading" ? (
            <div className="text-sm text-slate-600">
              {t("healthChecks.loading")}
            </div>
          ) : null}
          {dmvStatus === "error" ? (
            <ApiError
              message={
                dmvError ?? t("healthChecks.dataModelVersioning.error")
              }
            />
          ) : null}
          {dmvStatus === "success" ? (
            dmvInconsistencies.length > 0 ? (
              <div className="space-y-3">
                <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                  <div className="font-medium">
                    {t(
                      "healthChecks.dataModelVersioning.inconsistenciesCount",
                      { count: dmvInconsistencies.length }
                    )}
                  </div>
                  <ul className={`mt-2 list-disc space-y-2 pl-5 text-amber-900${pc}`}>
                    {dmvInconsistencies.map((group) => {
                      const byVersion = new Map<
                        string,
                        typeof group.usages
                      >();
                      for (const u of group.usages) {
                        const v = u.version ?? "(unspecified)";
                        const list = byVersion.get(v) ?? [];
                        list.push(u);
                        byVersion.set(v, list);
                      }
                      const modelLabel =
                        group.space && group.externalId
                          ? `${group.space} · ${group.externalId}`
                          : group.modelKey || "(unknown)";
                      return (
                        <li key={group.modelKey}>
                          <span className="font-medium">{modelLabel}</span>
                          <ul className="mt-1 list-[circle] pl-5 text-amber-800">
                            {Array.from(byVersion.entries()).map(
                              ([version, usages]) => (
                                <li key={version}>
                                  {version}:{" "}
                                  {usages
                                    .map((u) => u.transformationName)
                                    .join(", ")}
                                </li>
                              )
                            )}
                          </ul>
                        </li>
                      );
                    })}
                  </ul>
                </div>
              </div>
            ) : (
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.dataModelVersioning.allConsistent")}
              </div>
            )
          ) : null}
        </CardContent>
      </Card>
    </>
  );
}
