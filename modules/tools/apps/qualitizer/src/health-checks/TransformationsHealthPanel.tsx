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
import {
  TRANSFORMATIONS_HEALTH_TX_PAGE_SIZE,
  type DmvInconsistency,
  type NoopTransformation,
} from "./transformations-health-types";
import type { LoadState } from "./types";

export type { DmvInconsistency, NoopTransformation };

type ChecksLoadingPhase = "listing" | "remaining" | "queries" | "dmv" | "noop" | null;

type TransformationsHealthPanelProps = {
  noopStatus: LoadState;
  noopError: string | null;
  noopTransformations: NoopTransformation[];
  noopTotal: number;
  dmvStatus: LoadState;
  dmvError: string | null;
  dmvInconsistencies: DmvInconsistency[];
  checksLoadingPhase?: ChecksLoadingPhase;
  noopCheckProgress?: { current: number; total: number } | null;
  transformationsSampleMode?: boolean;
  onLoadAllTransformations?: () => void;
};

export function TransformationsHealthPanel({
  noopStatus,
  noopError,
  noopTransformations,
  noopTotal,
  dmvStatus,
  dmvError,
  dmvInconsistencies,
  checksLoadingPhase = null,
  noopCheckProgress = null,
  transformationsSampleMode = false,
  onLoadAllTransformations,
}: TransformationsHealthPanelProps) {
  const { t } = useI18n();
  const { isPrivateMode } = usePrivateMode();
  const pc = isPrivateMode ? " private-mask" : "";

  const checksLoading =
    dmvStatus === "loading" || noopStatus === "loading";

  const phaseLine =
    checksLoading && checksLoadingPhase === "listing"
      ? t("healthChecks.transformations.progress.listing", {
          limit: TRANSFORMATIONS_HEALTH_TX_PAGE_SIZE,
        })
      : checksLoading && checksLoadingPhase === "remaining"
        ? t("healthChecks.transformations.progress.remaining")
        : checksLoading && checksLoadingPhase === "queries"
          ? t("healthChecks.transformations.progress.queries")
          : checksLoading && checksLoadingPhase === "dmv"
            ? t("healthChecks.transformations.progress.dmv")
            : checksLoading && checksLoadingPhase === "noop"
              ? t("healthChecks.transformations.progress.noop")
              : null;

  return (
    <>
      {checksLoading ? (
        <div className="rounded-md border border-slate-200 bg-sky-50 px-3 py-3 text-sm text-slate-700">
          <p className="font-medium text-slate-900">
            {t("healthChecks.transformations.loading")}
          </p>
          {phaseLine ? <p className="mt-1 text-xs text-slate-600">{phaseLine}</p> : null}
          {checksLoading && noopCheckProgress ? (
            <p className="mt-1 text-xs text-slate-600">
              {t("healthChecks.transformations.progress.noopCount", {
                current: noopCheckProgress.current,
                total: noopCheckProgress.total,
              })}
            </p>
          ) : null}
        </div>
      ) : null}
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.transformations.noops.title")}</CardTitle>
          <CardDescription>
            {t("healthChecks.transformations.noops.description")}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {noopStatus === "error" ? (
            <ApiError
              message={
                noopError ?? t("healthChecks.transformations.noops.error")
              }
            />
          ) : null}
          {noopStatus === "success" ? (
            noopTransformations.length > 0 ? (
              <div className="space-y-3">
                {transformationsSampleMode ? (
                  <p className="text-xs text-amber-800">
                    {t("healthChecks.transformations.partialDisclaimer")}
                  </p>
                ) : null}
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
              </div>
            ) : (
              <div className="space-y-3">
                {transformationsSampleMode ? (
                  <p className="text-xs text-amber-800">
                    {t("healthChecks.transformations.partialDisclaimer")}
                  </p>
                ) : null}
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.transformations.noops.allGood", {
                  total: noopTotal,
                })}
              </div>
              </div>
            )
          ) : null}
          {noopStatus === "success" && transformationsSampleMode && onLoadAllTransformations ? (
            <div className="mt-3 flex flex-wrap items-center gap-2">
              <span className="text-xs text-slate-600">
                {t("healthChecks.transformations.sampleLimit", {
                  count: noopTotal,
                })}
              </span>
              <button
                type="button"
                className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50"
                onClick={onLoadAllTransformations}
              >
                {t("healthChecks.transformations.loadAll")}
              </button>
            </div>
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
                {transformationsSampleMode ? (
                  <p className="text-xs text-amber-800">
                    {t("healthChecks.transformations.partialDisclaimer")}
                  </p>
                ) : null}
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
              <div className="space-y-3">
                {transformationsSampleMode ? (
                  <p className="text-xs text-amber-800">
                    {t("healthChecks.transformations.partialDisclaimer")}
                  </p>
                ) : null}
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.dataModelVersioning.allConsistent")}
              </div>
              </div>
            )
          ) : null}
        </CardContent>
      </Card>
    </>
  );
}
