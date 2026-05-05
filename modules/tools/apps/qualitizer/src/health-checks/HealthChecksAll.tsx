import { useEffect, useMemo, useRef, useState } from "react";
import { Loader } from "@/shared/Loader";
import { useAppSdk } from "@/shared/auth";
import { useAppData } from "@/shared/data-cache";
import { useI18n } from "@/shared/i18n";
import { ModelingHealthPanel } from "./ModelingHealthPanel";
import { RawHealthPanel } from "./RawHealthPanel";
import { FunctionsHealthPanel } from "./FunctionsHealthPanel";
import { PermissionsHealthPanel } from "./PermissionsHealthPanel";
import { SchedulingHealthPanel } from "./SchedulingHealthPanel";
import { TransformationsHealthPanel } from "./TransformationsHealthPanel";
import { usePermissionsHealthChecks } from "./usePermissionsHealthChecks";
import { useTransformationsHealthChecks } from "./useTransformationsHealthChecks";
import {
  toTimestamp,
  formatIsoDate,
  isOlderThanSixMonths,
  parsePythonVersion,
} from "./health-checks-utils";
import type {
  ContainerSummary,
  FunctionSummary,
  LoadState,
  RawDatabaseSummary,
  RawTableSummary,
  ScheduleEntry,
  SpaceSummary,
  ViewDetail,
} from "./types";

type Props = { onBack: () => void };

export function HealthChecksAll({ onBack }: Props) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { t } = useI18n();
  const {
    dataModels,
    dataModelsStatus,
    dataModelsError,
    views,
    viewsStatus,
    viewsError,
    loadDataModels,
    loadViews,
    retrieveViews,
  } = useAppData();
  const [viewDetailsStatus, setViewDetailsStatus] = useState<LoadState>("idle");
  const [viewDetailsError, setViewDetailsError] = useState<string | null>(null);
  const [viewDetails, setViewDetails] = useState<ViewDetail[]>([]);
  const [containersStatus, setContainersStatus] = useState<LoadState>("idle");
  const [containersError, setContainersError] = useState<string | null>(null);
  const [containers, setContainers] = useState<ContainerSummary[]>([]);
  const [spacesStatus, setSpacesStatus] = useState<LoadState>("idle");
  const [spacesError, setSpacesError] = useState<string | null>(null);
  const [spaces, setSpaces] = useState<SpaceSummary[]>([]);
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
  const [viewDetailsTotal, setViewDetailsTotal] = useState(0);
  const [viewDetailsProcessed, setViewDetailsProcessed] = useState(0);
  const [functionsStatus, setFunctionsStatus] = useState<LoadState>("idle");
  const [functionsError, setFunctionsError] = useState<string | null>(null);
  const [functions, setFunctions] = useState<FunctionSummary[]>([]);
  const [schedulesStatus, setSchedulesStatus] = useState<LoadState>("idle");
  const [schedulesError, setSchedulesError] = useState<string | null>(null);
  const [schedules, setSchedules] = useState<ScheduleEntry[]>([]);
  const [showLoader, setShowLoader] = useState(false);

  const [rawLoadAll, setRawLoadAll] = useState(false);
  const consecutiveErrorsRef = useRef(0);
  const [circuitBreakerTripped, setCircuitBreakerTripped] = useState(false);
  const prevStatusesRef = useRef<Record<string, LoadState>>({});

  const {
    dmvStatus: dataModelVersioningStatus,
    dmvError: dataModelVersioningError,
    dmvInconsistencies: dataModelVersioningInconsistencies,
    noopStatus,
    noopError,
    noopTransformations,
    noopTotal,
    transformationsSampleMode: transformationsHealthSampleMode,
    onLoadAllTransformations,
    checksLoadingPhase,
    noopCheckProgress,
  } = useTransformationsHealthChecks({
    sdk,
    isSdkLoading,
    enabled: !circuitBreakerTripped,
  });

  const {
    permissionsStatus,
    permissionsError,
    permissionScopeDrift,
    compliantGroups,
    permissionsStats,
    checksLoadingPhase: permissionsChecksLoadingPhase,
  } = usePermissionsHealthChecks({
    sdk,
    isSdkLoading,
    enabled: !circuitBreakerTripped,
  });

  const RAW_SAMPLE_DB_LIMIT = 10;
  const RAW_SAMPLE_TABLES_PER_DB = 100;

  const isDashboardLoading =
    dataModelsStatus === "loading" ||
    viewsStatus === "loading" ||
    viewDetailsStatus === "loading" ||
    containersStatus === "loading" ||
    spacesStatus === "loading" ||
    rawStatus === "loading" ||
    functionsStatus === "loading" ||
    permissionsStatus === "loading" ||
    schedulesStatus === "loading" ||
    dataModelVersioningStatus === "loading" ||
    noopStatus === "loading";

  useEffect(() => {
    setShowLoader(isDashboardLoading);
  }, [isDashboardLoading]);

  useEffect(() => {
    const statuses: Record<string, LoadState> = {
      dataModels: dataModelsStatus,
      views: viewsStatus,
      viewDetails: viewDetailsStatus,
      spaces: spacesStatus,
      containers: containersStatus,
      functions: functionsStatus,
      permissions: permissionsStatus,
      schedules: schedulesStatus,
      raw: rawStatus,
      dataModelVersioning: dataModelVersioningStatus,
      noops: noopStatus,
    };
    let hadNewError = false;
    let hadSuccess = false;
    for (const [key, status] of Object.entries(statuses)) {
      const prev = prevStatusesRef.current[key];
      if (status === "error" && prev !== "error") hadNewError = true;
      if (status === "success") hadSuccess = true;
      prevStatusesRef.current[key] = status;
    }
    if (hadSuccess) consecutiveErrorsRef.current = 0;
    if (hadNewError) {
      consecutiveErrorsRef.current += 1;
      if (consecutiveErrorsRef.current >= 3) setCircuitBreakerTripped(true);
    }
  }, [
    dataModelsStatus, viewsStatus, viewDetailsStatus, spacesStatus,
    containersStatus, functionsStatus, permissionsStatus, schedulesStatus,
    rawStatus, dataModelVersioningStatus, noopStatus,
  ]);

  useEffect(() => {
    if (circuitBreakerTripped) return;
    loadDataModels();
    loadViews();
  }, [loadDataModels, loadViews, circuitBreakerTripped]);

  useEffect(() => {
    if (circuitBreakerTripped || isSdkLoading) return;
    if (viewsStatus !== "success") return;
    let cancelled = false;

    const loadViewDetails = async () => {
      setViewDetailsStatus("loading");
      setViewDetailsError(null);
      setViewDetailsTotal(views.length);
      setViewDetailsProcessed(0);
      try {
        const viewRefs = views.map((v) => ({
          space: v.space, externalId: v.externalId, version: v.version,
        }));
        const result: ViewDetail[] = [];
        for (let i = 0; i < viewRefs.length; i += 50) {
          const batch = viewRefs.slice(i, i + 50);
          const response = (await retrieveViews(batch, {
            includeInheritedProperties: true,
          })) as { items?: ViewDetail[] };
          result.push(...(response.items ?? []));
          if (!cancelled) {
            setViewDetails([...result]);
            setViewDetailsProcessed(result.length);
          }
        }
        if (!cancelled) setViewDetailsStatus("success");
      } catch (error) {
        if (!cancelled) {
          setViewDetailsError(error instanceof Error ? error.message : t("healthChecks.errors.viewDetails"));
          setViewDetailsStatus("error");
        }
      }
    };

    const loadSpaces = async () => {
      setSpacesStatus("loading");
      setSpacesError(null);
      try {
        const items: SpaceSummary[] = [];
        let cursor: string | undefined;
        do {
          const response = await sdk.spaces.list({
            includeGlobal: true, limit: 100, cursor,
          }) as { items?: SpaceSummary[]; nextCursor?: string | null };
          items.push(...(response.items ?? []));
          cursor = response.nextCursor ?? undefined;
        } while (cursor);
        if (!cancelled) { setSpaces(items); setSpacesStatus("success"); }
      } catch (error) {
        if (!cancelled) {
          setSpacesError(error instanceof Error ? error.message : t("healthChecks.errors.spaces"));
          setSpacesStatus("error");
        }
      }
    };

    const loadContainers = async () => {
      setContainersStatus("loading");
      setContainersError(null);
      try {
        const items = await sdk.containers
          .list({ includeGlobal: true, limit: 1000 })
          .autoPagingToArray();
        if (!cancelled) { setContainers(items as ContainerSummary[]); setContainersStatus("success"); }
      } catch (error) {
        if (!cancelled) {
          setContainersError(error instanceof Error ? error.message : t("healthChecks.errors.containers"));
          setContainersStatus("error");
        }
      }
    };

    loadViewDetails();
    loadSpaces();
    loadContainers();
    return () => { cancelled = true; };
  }, [circuitBreakerTripped, isSdkLoading, views, viewsStatus, sdk, t, retrieveViews]);

  useEffect(() => {
    if (circuitBreakerTripped || isSdkLoading) return;
    let cancelled = false;
    const loadFunctions = async () => {
      setFunctionsStatus("loading");
      setFunctionsError(null);
      try {
        const items: FunctionSummary[] = [];
        let cursor: string | undefined;
        do {
          const response = await sdk.post<{ items?: FunctionSummary[]; nextCursor?: string | null }>(
            `/api/v1/projects/${sdk.project}/functions/list`,
            { data: JSON.stringify({ limit: 100, cursor }) }
          );
          items.push(...(response.data?.items ?? []));
          cursor = response.data?.nextCursor ?? undefined;
        } while (cursor);
        if (!cancelled) { setFunctions(items); setFunctionsStatus("success"); }
      } catch (error) {
        if (!cancelled) {
          setFunctionsError(error instanceof Error ? error.message : t("healthChecks.errors.functions"));
          setFunctionsStatus("error");
        }
      }
    };
    loadFunctions();
    return () => { cancelled = true; };
  }, [circuitBreakerTripped, isSdkLoading, sdk, t]);

  useEffect(() => {
    if (circuitBreakerTripped || isSdkLoading) return;
    let cancelled = false;
    const readCron = (item: Record<string, unknown>) => {
      const direct = item.cron ?? item.cronExpression;
      if (typeof direct === "string" && direct.trim()) return direct.trim();
      const triggerRule = item.triggerRule as Record<string, unknown> | undefined;
      const triggerDirect = triggerRule?.cron ?? triggerRule?.cronExpression;
      if (typeof triggerDirect === "string" && triggerDirect.trim()) return triggerDirect.trim();
      const schedule = item.schedule as Record<string, unknown> | undefined;
      const nested = schedule?.cron ?? schedule?.cronExpression;
      if (typeof nested === "string" && nested.trim()) return nested.trim();
      return "";
    };
    const loadSchedules = async () => {
      setSchedulesStatus("loading");
      setSchedulesError(null);
      setSchedules([]);
      try {
        const all: ScheduleEntry[] = [];
        const functionSchedules = (await sdk.post(`/api/v1/projects/${sdk.project}/functions/schedules/list`, { data: { limit: 1000 } })) as { data?: { items?: Array<Record<string, unknown>> } };
        for (const item of functionSchedules.data?.items ?? []) {
          const cron = readCron(item);
          if (!cron) continue;
          const name = (item.name as string | undefined) ?? (item.functionExternalId as string | undefined) ?? (item.functionId as string | undefined) ?? "Function schedule";
          all.push({ type: "function", id: String(item.id ?? item.functionId ?? name), name, cron });
        }
        const transformationSchedules = (await sdk.get(`/api/v1/projects/${sdk.project}/transformations/schedules`, { params: { limit: "1000" } })) as { data?: { items?: Array<Record<string, unknown>> } };
        for (const item of transformationSchedules.data?.items ?? []) {
          const cron = readCron(item);
          if (!cron) continue;
          const name = (item.name as string | undefined) ?? (item.transformationExternalId as string | undefined) ?? (item.transformationId as string | undefined) ?? "Transformation schedule";
          all.push({ type: "transformation", id: String(item.id ?? item.transformationId ?? name), name, cron });
        }
        let triggerCursor: string | undefined;
        do {
          const workflowTriggers = (await sdk.get(`/api/v1/projects/${sdk.project}/workflows/triggers`, { params: { limit: "1000", cursor: triggerCursor } })) as { data?: { items?: Array<Record<string, unknown>>; nextCursor?: string } };
          for (const item of workflowTriggers.data?.items ?? []) {
            const cron = readCron(item);
            if (!cron) continue;
            const workflowId = item.workflowExternalId as string | undefined;
            const triggerId = (item.externalId as string | undefined) ?? (item.id as string | undefined) ?? "trigger";
            const name = workflowId ? `${workflowId} · ${triggerId}` : triggerId;
            all.push({ type: "workflow", id: String(item.externalId ?? item.id ?? name), name, cron });
          }
          triggerCursor = workflowTriggers.data?.nextCursor;
        } while (triggerCursor);
        if (!cancelled) { setSchedules(all); setSchedulesStatus("success"); }
      } catch (error) {
        if (!cancelled) {
          setSchedulesError(error instanceof Error ? error.message : t("healthChecks.scheduling.error"));
          setSchedulesStatus("error");
        }
      }
    };
    loadSchedules();
    return () => { cancelled = true; };
  }, [circuitBreakerTripped, isSdkLoading, sdk, t]);

  useEffect(() => {
    if (circuitBreakerTripped || isSdkLoading) return;
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
      setRawDbTotal(0); setRawDbProcessed(0); setRawTableScanned(0); setRawSampleTotal(0); setRawSampleProcessed(0);
      try {
        const databases: RawDatabaseSummary[] = [];
        let dbCursor: string | undefined;
        do {
          const response = await sdk.raw.listDatabases({ limit: loadAll ? 100 : Math.min(100, RAW_SAMPLE_DB_LIMIT - databases.length), cursor: dbCursor });
          databases.push(...((response.items ?? []) as RawDatabaseSummary[]));
          if (!loadAll && databases.length >= RAW_SAMPLE_DB_LIMIT) { databases.length = RAW_SAMPLE_DB_LIMIT; break; }
          dbCursor = response.nextCursor ?? undefined;
        } while (dbCursor);
        if (!cancelled) { setRawDatabases(databases); setRawDbTotal(databases.length); }
        const tables: RawTableSummary[] = [];
        let processedDatabases = 0;
        for (const database of databases) {
          let tableCursor: string | undefined;
          let tablesInDb = 0;
          do {
            const response = await sdk.raw.listTables(database.name, { limit: loadAll ? 100 : Math.min(100, RAW_SAMPLE_TABLES_PER_DB - tablesInDb), cursor: tableCursor });
            const items = (response.items ?? []) as unknown as Array<Record<string, unknown>>;
            tables.push(...items.map((item) => ({ dbName: database.name, name: String(item.name), rowCount: typeof item.rowCount === "number" ? item.rowCount : undefined, lastUpdatedTime: toTimestamp(item.lastUpdatedTime), createdTime: toTimestamp(item.createdTime) })));
            tablesInDb += items.length;
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
          if (table.rowCount != null && table.lastUpdatedTime != null) { sampledTables.push(table); if (!cancelled) setRawSampleProcessed((p) => p + 1); continue; }
          let cursor: string | undefined;
          let sampleCount = 0;
          do {
            const response = await sdk.raw.listRows(table.dbName, table.name, { limit: 10, cursor });
            sampleCount += ((response.items ?? []) as unknown as unknown[]).length;
            cursor = response.nextCursor ?? undefined;
            if (sampleCount >= 10) break;
          } while (cursor);
          sampledTables.push({ ...table, sampleRowCount: sampleCount });
          if (!cancelled) setRawSampleProcessed((p) => p + 1);
        }
        if (!cancelled) { setRawDatabases(databases); setRawTables(sampledTables); setRawStatus("success"); }
      } catch (error) {
        if (!cancelled) { setRawError(error instanceof Error ? error.message : t("healthChecks.errors.rawMetadata")); setRawStatus("error"); }
      }
    };
    loadRaw();
    return () => { cancelled = true; };
  }, [circuitBreakerTripped, isSdkLoading, sdk, t, rawLoadAll]);

  const usedViewKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const m of dataModels) for (const v of m.views ?? []) keys.add(`${v.space}:${v.externalId}:${v.version ?? "latest"}`);
    return keys;
  }, [dataModels]);

  const unusedViews = useMemo(() => views.filter((v) => !usedViewKeys.has(`${v.space}:${v.externalId}:${v.version ?? "latest"}`)).sort((a, b) => (a.name ?? a.externalId).localeCompare(b.name ?? b.externalId)), [views, usedViewKeys]);
  const viewsWithoutContainers = useMemo(() => viewDetails.filter((v) => { const p = Object.values(v.properties ?? {}); return p.length === 0 || p.every((pp) => !pp.container); }).sort((a, b) => (a.name ?? a.externalId).localeCompare(b.name ?? b.externalId)), [viewDetails]);
  const usedContainerKeys = useMemo(() => { const keys = new Set<string>(); for (const v of viewDetails) for (const p of Object.values(v.properties ?? {})) if (p.container?.space && p.container?.externalId) keys.add(`${p.container.space}:${p.container.externalId}`); return keys; }, [viewDetails]);
  const unusedContainers = useMemo(() => containers.filter((c) => !usedContainerKeys.has(`${c.space}:${c.externalId}`)).sort((a, b) => (a.name ?? a.externalId).localeCompare(b.name ?? b.externalId)), [containers, usedContainerKeys]);
  const unusedSpaces = useMemo(() => { const used = new Set<string>(); for (const m of dataModels) used.add(m.space); for (const v of views) used.add(v.space); for (const c of containers) used.add(c.space); return spaces.filter((s) => !used.has(s.space)).sort((a, b) => (a.name ?? a.space).localeCompare(b.name ?? b.space)); }, [containers, dataModels, spaces, views]);
  const emptyRawTables = useMemo(() => rawTables.filter((t) => { if (t.rowCount === 0) return true; if (t.rowCount == null) return t.sampleRowCount === 0; return false; }).sort((a, b) => a.dbName.localeCompare(b.dbName) || a.name.localeCompare(b.name)), [rawTables]);

  const functionsByRuntime = useMemo(() => { const map = new Map<string, FunctionSummary[]>(); for (const fn of functions) { const rt = fn.runtime ?? t("healthChecks.functions.runtime.unknown"); map.set(rt, [...(map.get(rt) ?? []), fn]); } return map; }, [functions, t]);
  const runtimeList = useMemo(() => Array.from(functionsByRuntime.keys()).sort(), [functionsByRuntime]);
  const lowPythonFunctions = useMemo(() => functions.filter((fn) => { const v = parsePythonVersion(fn.runtime); if (!v) return false; if (v.major !== 3) return true; return v.minor < 12; }).sort((a, b) => (a.name ?? a.id).localeCompare(b.name ?? b.id)), [functions]);
  const scheduleOverlaps = useMemo(() => {
    const byKey = new Map<string, ScheduleEntry[]>();
    const parseKey = (cron: string) => { const p = cron.trim().split(/\s+/); return p.length < 2 ? cron : `${p[0]} ${p[1]}`; };
    for (const s of schedules) { const key = parseKey(s.cron); byKey.set(key, [...(byKey.get(key) ?? []), s]); }
    const buildOffset = (cron: string, offset: number) => { const p = cron.trim().split(/\s+/); if (p.length < 2) return ""; if (!/^\d+$/.test(p[0])) return ""; return [String((Number(p[0]) + offset) % 60), ...p.slice(1)].join(" "); };
    const overlaps: Array<{ key: string; schedules: ScheduleEntry[]; exampleOffsets: string[] }> = [];
    for (const [key, items] of byKey.entries()) {
      if (items.length < 2) continue;
      const examples: string[] = [];
      const first = items[0]?.cron ?? "";
      for (const o of [5, 10, 15]) { const ex = buildOffset(first, o); if (ex) examples.push(ex); }
      overlaps.push({ key, schedules: items, exampleOffsets: examples });
    }
    return overlaps.sort((a, b) => b.schedules.length - a.schedules.length);
  }, [schedules]);

  const renderProgressBar = (value: number, total: number) => {
    const safeTotal = total > 0 ? total : 0;
    const percent = safeTotal > 0 ? Math.min(100, (value / safeTotal) * 100) : 0;
    return (<div className="mt-2 h-2 w-full rounded-full bg-slate-100"><div className="h-2 rounded-full bg-slate-900" style={{ width: `${percent}%` }} /></div>);
  };

  return (
    <section className="flex flex-col gap-4">
      <header className="flex items-start justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h2 className="text-2xl font-semibold text-slate-900">{t("healthChecks.title")}</h2>
          <p className="text-sm text-slate-500">{t("healthChecks.subtitle")}</p>
        </div>
        <button type="button" className="cursor-pointer shrink-0 rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50" onClick={onBack}>Back to checks</button>
      </header>
      {circuitBreakerTripped ? (
        <div className="rounded-md border border-amber-300 bg-amber-50 p-4 text-sm text-amber-900">
          <div className="font-medium">{t("healthChecks.circuitBreaker.title")}</div>
          <p className="mt-1 text-amber-800">{t("healthChecks.circuitBreaker.description")}</p>
        </div>
      ) : null}
      <FunctionsHealthPanel functionsStatus={functionsStatus} functionsError={functionsError} runtimeList={runtimeList} functionsByRuntime={functionsByRuntime} lowPythonFunctions={lowPythonFunctions} />
      <SchedulingHealthPanel status={schedulesStatus} error={schedulesError} schedules={schedules} overlaps={scheduleOverlaps} />
      <TransformationsHealthPanel
        noopStatus={noopStatus}
        noopError={noopError}
        noopTransformations={noopTransformations}
        noopTotal={noopTotal}
        dmvStatus={dataModelVersioningStatus}
        dmvError={dataModelVersioningError}
        dmvInconsistencies={dataModelVersioningInconsistencies}
        checksLoadingPhase={checksLoadingPhase}
        noopCheckProgress={noopCheckProgress}
        transformationsSampleMode={transformationsHealthSampleMode}
        onLoadAllTransformations={onLoadAllTransformations}
      />
      <ModelingHealthPanel dataModelsStatus={dataModelsStatus} viewsStatus={viewsStatus} viewDetailsStatus={viewDetailsStatus} containersStatus={containersStatus} spacesStatus={spacesStatus} dataModelsError={dataModelsError} viewsError={viewsError} viewDetailsError={viewDetailsError} containersError={containersError} spacesError={spacesError} unusedViews={unusedViews} viewsWithoutContainers={viewsWithoutContainers} unusedContainers={unusedContainers} unusedSpaces={unusedSpaces} viewDetailsProcessed={viewDetailsProcessed} viewDetailsTotal={viewDetailsTotal} renderProgressBar={renderProgressBar} />
      <RawHealthPanel rawStatus={rawStatus} rawError={rawError} rawAvailabilityMessage={rawAvailabilityMessage} rawDatabases={rawDatabases} rawTables={rawTables} rawDbProcessed={rawDbProcessed} rawDbTotal={rawDbTotal} rawTableScanned={rawTableScanned} rawSampleTotal={rawSampleTotal} rawSampleProcessed={rawSampleProcessed} emptyRawTables={emptyRawTables} formatIsoDate={formatIsoDate} isOlderThanSixMonths={isOlderThanSixMonths} renderProgressBar={renderProgressBar} rawIsSample={!rawLoadAll} onLoadAll={() => setRawLoadAll(true)} />
      <PermissionsHealthPanel
        permissionsStatus={permissionsStatus}
        permissionsError={permissionsError}
        permissionScopeDrift={permissionScopeDrift}
        compliantGroups={compliantGroups}
        permissionsStats={permissionsStats}
        checksLoadingPhase={permissionsChecksLoadingPhase}
      />
      <Loader open={showLoader} onClose={() => setShowLoader(false)} title={t("healthChecks.loader.title")} />
    </section>
  );
}
