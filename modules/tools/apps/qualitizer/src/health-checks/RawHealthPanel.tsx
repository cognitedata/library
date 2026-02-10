import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { useI18n } from "@/shared/i18n";
import type { LoadState, RawDatabaseSummary, RawTableSummary } from "./types";

type RawHealthPanelProps = {
  rawStatus: LoadState;
  rawError: string | null;
  rawAvailabilityMessage: string | null;
  rawDatabases: RawDatabaseSummary[];
  rawTables: RawTableSummary[];
  rawDbProcessed: number;
  rawDbTotal: number;
  rawTableScanned: number;
  rawSampleTotal: number;
  rawSampleProcessed: number;
  emptyRawTables: RawTableSummary[];
  formatIsoDate: (value?: number) => string;
  isOlderThanSixMonths: (value?: number) => boolean;
  renderProgressBar: (value: number, total: number) => React.ReactNode;
};

export function RawHealthPanel({
  rawStatus,
  rawError,
  rawAvailabilityMessage,
  rawDatabases,
  rawTables,
  rawDbProcessed,
  rawDbTotal,
  rawTableScanned,
  rawSampleTotal,
  rawSampleProcessed,
  emptyRawTables,
  formatIsoDate,
  isOlderThanSixMonths,
  renderProgressBar,
}: RawHealthPanelProps) {
  const { t } = useI18n();

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.raw.overview.title")}</CardTitle>
          <CardDescription>{t("healthChecks.raw.overview.description")}</CardDescription>
        </CardHeader>
        <CardContent>
          {rawStatus === "loading" ? (
            <div className="text-sm text-slate-600">{t("healthChecks.loading")}</div>
          ) : null}
          {rawStatus === "error" ? (
            <ApiError message={rawError ?? t("healthChecks.errors.rawMetadata")} />
          ) : null}
          {rawStatus !== "loading" && rawStatus !== "error" && rawAvailabilityMessage ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
              {rawAvailabilityMessage}
            </div>
          ) : null}
          {rawStatus !== "error" && !rawAvailabilityMessage ? (
            <div className="rounded-md border border-slate-200 bg-white p-3 text-sm text-slate-700">
              <div className="font-medium text-slate-900">
                {t("healthChecks.raw.overview.counts", {
                  databases: rawDatabases.length,
                  tables: rawTables.length,
                })}
              </div>
              {rawStatus === "loading" ? (
                <div className="mt-2 text-xs text-slate-500">
                  {t("healthChecks.raw.overview.databasesProcessed", {
                    processed: rawDbProcessed,
                    total: rawDbTotal || "?",
                  })}
                  {renderProgressBar(rawDbProcessed, rawDbTotal || 1)}
                  <div className="mt-2">
                    {t("healthChecks.raw.overview.tablesScanned", {
                      count: rawTableScanned,
                    })}
                  </div>
                </div>
              ) : null}
              <div className="mt-1 text-xs text-slate-500">
                {t("healthChecks.raw.overview.sampleNote")}
              </div>
            </div>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.raw.emptyTables.title")}</CardTitle>
          <CardDescription>{t("healthChecks.raw.emptyTables.description")}</CardDescription>
        </CardHeader>
        <CardContent>
          {rawStatus === "loading" ? (
            <div className="text-sm text-slate-600">{t("healthChecks.loading")}</div>
          ) : null}
          {rawStatus === "error" ? (
            <ApiError message={rawError ?? t("healthChecks.errors.rawMetadata")} />
          ) : null}
          {rawStatus === "success" && !rawAvailabilityMessage ? (
            emptyRawTables.length > 0 ? (
              <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                <div className="font-medium">
                  {t("healthChecks.raw.emptyTables.count", { count: emptyRawTables.length })}
                </div>
                <ul className="mt-2 list-disc space-y-1 pl-5 text-amber-900">
                  {emptyRawTables.map((table) => (
                    <li key={`${table.dbName}:${table.name}`}>
                      <span>{table.name}</span> · <span>{table.dbName}</span> ·{" "}
                      <span>
                        {t("healthChecks.raw.emptyTables.created", {
                          date: formatIsoDate(table.createdTime),
                        })}
                      </span>
                      {isOlderThanSixMonths(table.createdTime) ? (
                        <span className="ml-2 rounded bg-red-100 px-2 py-0.5 text-xs font-semibold text-red-700">
                          {t("healthChecks.raw.emptyTables.alert")}
                        </span>
                      ) : null}
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.raw.emptyTables.none")}
              </div>
            )
          ) : null}
          {rawStatus === "loading" && rawSampleTotal > 0 ? (
            <div className="mt-2 text-xs text-slate-500">
              {t("healthChecks.raw.emptyTables.sampling", {
                processed: rawSampleProcessed,
                total: rawSampleTotal,
              })}
              {renderProgressBar(rawSampleProcessed, rawSampleTotal)}
            </div>
          ) : null}
        </CardContent>
      </Card>
    </>
  );
}
