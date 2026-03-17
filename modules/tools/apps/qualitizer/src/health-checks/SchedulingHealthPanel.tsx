import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { useI18n } from "@/shared/i18n";
import type { LoadState, ScheduleEntry } from "./types";

type ScheduleOverlap = {
  key: string;
  schedules: ScheduleEntry[];
  exampleOffsets: string[];
};

type SchedulingHealthPanelProps = {
  status: LoadState;
  error: string | null;
  schedules: ScheduleEntry[];
  overlaps: ScheduleOverlap[];
};

export function SchedulingHealthPanel({
  status,
  error,
  schedules,
  overlaps,
}: SchedulingHealthPanelProps) {
  const { t } = useI18n();

  return (
    <Card>
      <CardHeader>
        <CardTitle>{t("healthChecks.scheduling.title")}</CardTitle>
        <CardDescription>{t("healthChecks.scheduling.description")}</CardDescription>
      </CardHeader>
      <CardContent>
        {status === "loading" ? (
          <div className="text-sm text-slate-600">{t("healthChecks.scheduling.loading")}</div>
        ) : null}
        {status === "error" ? (
          <ApiError message={error ?? t("healthChecks.scheduling.error")} />
        ) : null}
        {status === "success" ? (
          <>
            {(() => {
              const counts = {
                function: schedules.filter((schedule) => schedule.type === "function").length,
                transformation: schedules.filter(
                  (schedule) => schedule.type === "transformation"
                ).length,
                workflow: schedules.filter((schedule) => schedule.type === "workflow").length,
              };
              return (
                <div className="rounded-md border border-slate-200 bg-white p-3 text-sm text-slate-700">
                  <div>{t("healthChecks.scheduling.counts.function", { count: counts.function })}</div>
                  <div>
                    {t("healthChecks.scheduling.counts.transformation", {
                      count: counts.transformation,
                    })}
                  </div>
                  <div>{t("healthChecks.scheduling.counts.workflow", { count: counts.workflow })}</div>
                </div>
              );
            })()}
            <div className="mt-3 rounded-md border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
              <div className="font-medium">{t("healthChecks.scheduling.offsetExample.title")}</div>
              <div className="mt-1 text-xs">
                {t("healthChecks.scheduling.offsetExample.body")}
              </div>
            </div>
            {overlaps.length > 0 ? (
              <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                <div className="font-medium">
                  {t("healthChecks.scheduling.overlaps.count", { count: overlaps.length })}
                </div>
                <p className="mt-2 text-xs text-amber-900">
                  {t("healthChecks.scheduling.overlaps.note")}
                </p>
                <div className="mt-3 space-y-3 text-xs text-amber-900">
                  {overlaps.map((overlap) => (
                    <div key={overlap.key} className="rounded-md border border-amber-200 bg-white p-3">
                      <div className="text-xs font-semibold text-amber-900">
                        {t("healthChecks.scheduling.overlaps.cron", { cron: overlap.key })}
                      </div>
                      <ul className="mt-2 list-disc space-y-1 pl-4">
                        {overlap.schedules.map((schedule) => (
                          <li key={`${schedule.type}-${schedule.id}`}>
                            {t(`healthChecks.scheduling.types.${schedule.type}`)} · {schedule.name}{" "}
                            <span className="text-amber-900/80">({schedule.cron})</span>
                          </li>
                        ))}
                      </ul>
                      {overlap.exampleOffsets.length > 0 ? (
                        <div className="mt-2 text-[11px] text-amber-900">
                          <div className="font-semibold">
                            {t("healthChecks.scheduling.overlaps.offsetTitle")}
                          </div>
                          <div className="mt-1">
                            {overlap.exampleOffsets.join(", ")}
                          </div>
                        </div>
                      ) : null}
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <div className="mt-3 rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.scheduling.overlaps.none")}
              </div>
            )}
          </>
        ) : null}
      </CardContent>
    </Card>
  );
}
