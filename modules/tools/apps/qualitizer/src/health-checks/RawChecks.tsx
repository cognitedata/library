import { useEffect, useMemo, useState } from "react";
import { Loader } from "@/shared/Loader";
import { useAppSdk } from "@/shared/auth";
import { useI18n } from "@/shared/i18n";
import { RawHealthPanel } from "./RawHealthPanel";
import { toTimestamp, formatIsoDate, isOlderThanSixMonths } from "./health-checks-utils";
import type { LoadState, RawDatabaseSummary, RawTableSummary } from "./types";

const RAW_SAMPLE_DB_LIMIT = 10;
const RAW_SAMPLE_TABLES_PER_DB = 100;

type Props = { onBack: () => void };

export function RawChecks({ onBack }: Props) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { t } = useI18n();

  const [rawStatus, setRawStatus] = useState<LoadState>("idle");
  const [rawError, setRawError] = useState<string | null>(null);
  const [rawDatabases, setRawDatabases] = useState<RawDatabaseSummary[]>([]);
  const [rawTables, setRawTables] = useState<RawTableSummary[]>([]);
  const [rawAvailabilityMessage, setRawAvailabilityMessage] = useState<string | null>(null);
  const [rawDbTotal, setRawDbTotal] = useState(0);
  const [rawDbProcessed, setRawDbProcessed] = useState(0);
  const [rawTableScanned, setRawTableScanned] = useState(0);
  const [rawSampleTotal, setRawSampleTotal] = useState(0);
  const [rawSampleProcessed, setRawSampleProcessed] = useState(0);
  const [rawLoadAll, setRawLoadAll] = useState(false);
  const [showLoader, setShowLoader] = useState(false);

  useEffect(() => {
    setShowLoader(rawStatus === "loading");
  }, [rawStatus]);

  useEffect(() => {
    if (isSdkLoading) return;
    if (!sdk.raw?.listDatabases || !sdk.raw?.listTables || !sdk.raw?.listRows) {
      setRawAvailabilityMessage(t("healthChecks.raw.unavailable"));
      setRawStatus("success");
      return;
    }
    let cancelled = false;
    const loadAll = rawLoadAll;

    const loadRaw = async () => {
      setRawStatus("loading");
      setRawError(null);
      setRawAvailabilityMessage(null);
      setRawDbTotal(0);
      setRawDbProcessed(0);
      setRawTableScanned(0);
      setRawSampleTotal(0);
      setRawSampleProcessed(0);
      try {
        const databases: RawDatabaseSummary[] = [];
        let dbCursor: string | undefined;
        do {
          const response = await sdk.raw.listDatabases({
            limit: loadAll ? 100 : Math.min(100, RAW_SAMPLE_DB_LIMIT - databases.length),
            cursor: dbCursor,
          });
          const batch = (response.items ?? []) as RawDatabaseSummary[];
          databases.push(...batch);
          if (!loadAll && databases.length >= RAW_SAMPLE_DB_LIMIT) {
            databases.length = RAW_SAMPLE_DB_LIMIT;
            break;
          }
          dbCursor = response.nextCursor ?? undefined;
        } while (dbCursor);
        if (!cancelled) {
          setRawDatabases(databases);
          setRawDbTotal(databases.length);
        }

        const tables: RawTableSummary[] = [];
        let processedDatabases = 0;
        for (const database of databases) {
          let tableCursor: string | undefined;
          let tablesInDb = 0;
          do {
            const response = await sdk.raw.listTables(database.name, {
              limit: loadAll ? 100 : Math.min(100, RAW_SAMPLE_TABLES_PER_DB - tablesInDb),
              cursor: tableCursor,
            });
            const items = (response.items ?? []) as unknown as Array<Record<string, unknown>>;
            const mapped = items.map((item) => ({
              dbName: database.name,
              name: String(item.name),
              rowCount: typeof item.rowCount === "number" ? item.rowCount : undefined,
              lastUpdatedTime: toTimestamp(item.lastUpdatedTime),
              createdTime: toTimestamp(item.createdTime),
            }));
            tables.push(...mapped);
            tablesInDb += mapped.length;
            if (!cancelled) setRawTableScanned(tables.length);
            tableCursor = response.nextCursor ?? undefined;
            if (!loadAll && tablesInDb >= RAW_SAMPLE_TABLES_PER_DB) break;
          } while (tableCursor);
          processedDatabases += 1;
          if (!cancelled) setRawDbProcessed(processedDatabases);
        }

        const sampledTables: RawTableSummary[] = [];
        setRawSampleTotal(tables.length);
        for (const table of tables) {
          if (table.rowCount != null && table.lastUpdatedTime != null) {
            sampledTables.push(table);
            if (!cancelled) setRawSampleProcessed((prev) => prev + 1);
            continue;
          }
          let cursor: string | undefined;
          let sampleCount = 0;
          do {
            const response = await sdk.raw.listRows(table.dbName, table.name, {
              limit: 10,
              cursor,
            });
            const items = (response.items ?? []) as unknown as Array<Record<string, unknown>>;
            sampleCount += items.length;
            cursor = response.nextCursor ?? undefined;
            if (sampleCount >= 10) break;
          } while (cursor);
          sampledTables.push({ ...table, sampleRowCount: sampleCount });
          if (!cancelled) setRawSampleProcessed((prev) => prev + 1);
        }

        if (!cancelled) {
          setRawDatabases(databases);
          setRawTables(sampledTables);
          setRawStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setRawError(error instanceof Error ? error.message : "Failed to load Raw metadata");
          setRawStatus("error");
        }
      }
    };

    loadRaw();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, t, rawLoadAll]);

  const emptyRawTables = useMemo(
    () =>
      rawTables
        .filter((table) => {
          if (table.rowCount === 0) return true;
          if (table.rowCount == null) return table.sampleRowCount === 0;
          return false;
        })
        .sort((a, b) => a.dbName.localeCompare(b.dbName) || a.name.localeCompare(b.name)),
    [rawTables]
  );

  const renderProgressBar = (value: number, total: number) => {
    const safeTotal = total > 0 ? total : 0;
    const percent = safeTotal > 0 ? Math.min(100, (value / safeTotal) * 100) : 0;
    return (
      <div className="mt-2 h-2 w-full rounded-full bg-slate-100">
        <div className="h-2 rounded-full bg-slate-900" style={{ width: `${percent}%` }} />
      </div>
    );
  };

  return (
    <section className="flex flex-col gap-4">
      <header className="flex items-start justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h2 className="text-2xl font-semibold text-slate-900">
            Raw Table Checks
          </h2>
          <p className="text-sm text-slate-500">
            Empty tables, stale data, and Raw API availability
          </p>
        </div>
        <button
          type="button"
          className="cursor-pointer shrink-0 rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
          onClick={onBack}
        >
          Back to checks
        </button>
      </header>
      <RawHealthPanel
        rawStatus={rawStatus}
        rawError={rawError}
        rawAvailabilityMessage={rawAvailabilityMessage}
        rawDatabases={rawDatabases}
        rawTables={rawTables}
        rawDbProcessed={rawDbProcessed}
        rawDbTotal={rawDbTotal}
        rawTableScanned={rawTableScanned}
        rawSampleTotal={rawSampleTotal}
        rawSampleProcessed={rawSampleProcessed}
        emptyRawTables={emptyRawTables}
        formatIsoDate={formatIsoDate}
        isOlderThanSixMonths={isOlderThanSixMonths}
        renderProgressBar={renderProgressBar}
        rawIsSample={!rawLoadAll}
        onLoadAll={() => setRawLoadAll(true)}
      />
      <Loader
        open={showLoader}
        onClose={() => setShowLoader(false)}
        title="Running Raw table checks…"
      />
    </section>
  );
}
