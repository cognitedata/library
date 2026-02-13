import { useEffect, useMemo, useState } from "react";
import { Loader } from "@/shared/Loader";
import { useAppSdk } from "@/shared/auth";
import { useAppData } from "@/shared/data-cache";
import { useI18n } from "@/shared/i18n";
import { ModelingHealthPanel } from "./ModelingHealthPanel";
import { RawHealthPanel } from "./RawHealthPanel";
import { FunctionsHealthPanel } from "./FunctionsHealthPanel";
import { PermissionsHealthPanel } from "./PermissionsHealthPanel";
import { SchedulingHealthPanel } from "./SchedulingHealthPanel";
import type {
  ContainerSummary,
  FunctionSummary,
  GroupSummary,
  LoadState,
  NormalizedCapability,
  PermissionScopeDriftEntry,
  RawDatabaseSummary,
  RawTableSummary,
  ScheduleEntry,
  SpaceSummary,
  ViewDetail,
} from "./types";

export function HealthChecks() {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { t } = useI18n();
  const showInternal =
    import.meta.env.VITE_SHOW_INTERNAL === "true" || import.meta.env.VITE_STANDALONE !== "true";
  const {
    dataModels,
    dataModelsStatus,
    dataModelsError,
    views,
    viewsStatus,
    viewsError,
    loadDataModels,
    loadViews,
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
  const [permissionsStatus, setPermissionsStatus] = useState<LoadState>("idle");
  const [permissionsError, setPermissionsError] = useState<string | null>(null);
  const [groups, setGroups] = useState<GroupSummary[]>([]);
  const [showLoader, setShowLoader] = useState(false);

  const toTimestamp = (value: unknown) => {
    if (value instanceof Date) return value.getTime();
    if (typeof value === "number") return value;
    return undefined;
  };

  const normalizeCapability = (capability: Record<string, unknown>): NormalizedCapability => {
    const entries = Object.entries(capability).filter(([key]) => key !== "projectUrlNames");
    if (entries.length === 0) {
      return { name: t("healthChecks.permissions.drift.unknownCapability") };
    }
    const [name, value] = entries[0];
    const normalized = value as NormalizedCapability;
    return { ...normalized, name: name.replace("Acl", "") };
  };

  const extractScopeEntries = (scope?: Record<string, unknown>) => {
    const entries: Array<{ type: string; items: string[] }> = [];
    if (!scope) return entries;
    const datasetScope = scope["datasetScope"] as { ids?: number[] } | undefined;
    if (datasetScope?.ids?.length) {
      entries.push({
        type: "datasetScope.ids",
        items: datasetScope.ids.map((value) => String(value)).sort(),
      });
    }
    const idScope = scope["idScope"] as { ids?: number[] } | undefined;
    if (idScope?.ids?.length) {
      entries.push({
        type: "idScope.ids",
        items: idScope.ids.map((value) => String(value)).sort(),
      });
    }
    const spaceIdScope = scope["spaceIdScope"] as { spaceIds?: string[] } | undefined;
    if (spaceIdScope?.spaceIds?.length) {
      entries.push({
        type: "spaceIdScope.spaceIds",
        items: [...spaceIdScope.spaceIds].sort(),
      });
    }
    const tableScope = scope["tableScope"] as
      | { dbsToTables?: Record<string, string[]> }
      | undefined;
    if (tableScope?.dbsToTables) {
      const tableKeys = Object.entries(tableScope.dbsToTables).flatMap(([db, tables]) => {
        if (Array.isArray(tables)) {
          return tables.map((table) => `${db}.${table}`);
        }
        if (typeof tables === "string") {
          return [`${db}.${tables}`];
        }
        return [];
      });
      if (tableKeys.length > 0) {
        entries.push({
          type: "tableScope.dbsToTables",
          items: tableKeys.sort(),
        });
      }
    }
    return entries;
  };

  const formatIsoDate = (value?: number) => {
    if (!value) return t("healthChecks.raw.unknownDate");
    return new Date(value).toISOString().slice(0, 10);
  };

  const isOlderThanSixMonths = (value?: number) => {
    if (!value) return false;
    const sixMonthsMs = 1000 * 60 * 60 * 24 * 30 * 6;
    return value < Date.now() - sixMonthsMs;
  };

  const renderProgressBar = (value: number, total: number) => {
    const safeTotal = total > 0 ? total : 0;
    const percent = safeTotal > 0 ? Math.min(100, (value / safeTotal) * 100) : 0;
    return (
      <div className="mt-2 h-2 w-full rounded-full bg-slate-100">
        <div className="h-2 rounded-full bg-slate-900" style={{ width: `${percent}%` }} />
      </div>
    );
  };

  const isDashboardLoading =
    dataModelsStatus === "loading" ||
    viewsStatus === "loading" ||
    viewDetailsStatus === "loading" ||
    containersStatus === "loading" ||
    spacesStatus === "loading" ||
    rawStatus === "loading" ||
    functionsStatus === "loading" ||
    permissionsStatus === "loading" ||
    schedulesStatus === "loading";

  useEffect(() => {
    setShowLoader(isDashboardLoading);
  }, [isDashboardLoading]);

  useEffect(() => {
    loadDataModels();
    loadViews();
  }, [loadDataModels, loadViews]);

  useEffect(() => {
    if (isSdkLoading) return;
    if (viewsStatus !== "success") return;
    let cancelled = false;

    const loadViewDetails = async () => {
      setViewDetailsStatus("loading");
      setViewDetailsError(null);
      setViewDetailsTotal(views.length);
      setViewDetailsProcessed(0);
      try {
        const viewRefs = views.map((view) => ({
          space: view.space,
          externalId: view.externalId,
          version: view.version,
        }));

        const result: ViewDetail[] = [];
        for (let i = 0; i < viewRefs.length; i += 50) {
          const batch = viewRefs.slice(i, i + 50);
          const response = (await sdk.views.retrieve(batch as never, {
            includeInheritedProperties: true,
          })) as { items?: ViewDetail[] };
          result.push(...(response.items ?? []));
          if (!cancelled) {
            setViewDetails([...result]);
            setViewDetailsProcessed(result.length);
          }
        }
        if (!cancelled) {
          setViewDetailsStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setViewDetailsError(
            error instanceof Error ? error.message : t("healthChecks.errors.viewDetails")
          );
          setViewDetailsStatus("error");
        }
      }
    };

    const loadSpaces = async () => {
      setSpacesStatus("loading");
      setSpacesError(null);
      try {
        const items = await sdk.spaces
          .list({ includeGlobal: true, limit: 1000 })
          .autoPagingToArray();
        if (!cancelled) {
          setSpaces(items as SpaceSummary[]);
          setSpacesStatus("success");
        }
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
        if (!cancelled) {
          setContainers(items as ContainerSummary[]);
          setContainersStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setContainersError(
            error instanceof Error ? error.message : t("healthChecks.errors.containers")
          );
          setContainersStatus("error");
        }
      }
    };

    loadViewDetails();
    loadSpaces();
    loadContainers();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, views, viewsStatus, sdk, t]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;

    const loadFunctions = async () => {
      setFunctionsStatus("loading");
      setFunctionsError(null);
      try {
        const items: FunctionSummary[] = [];
        let cursor: string | undefined;
        do {
          const response = await sdk.post<{
            items?: FunctionSummary[];
            nextCursor?: string | null;
          }>(`/api/v1/projects/${sdk.project}/functions/list`, {
            data: JSON.stringify({ limit: 100, cursor }),
          });
          items.push(...(response.data?.items ?? []));
          cursor = response.data?.nextCursor ?? undefined;
        } while (cursor);

        if (!cancelled) {
          setFunctions(items);
          setFunctionsStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setFunctionsError(
            error instanceof Error ? error.message : t("healthChecks.errors.functions")
          );
          setFunctionsStatus("error");
        }
      }
    };

    loadFunctions();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, t]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const loadPermissions = async () => {
      setPermissionsStatus("loading");
      setPermissionsError(null);
      try {
        const groupResponse = (await sdk.groups.list({ all: true })) as GroupSummary[];
        if (!cancelled) {
          setGroups(groupResponse);
          setPermissionsStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setPermissionsError(
            error instanceof Error ? error.message : t("healthChecks.errors.permissions")
          );
          setPermissionsStatus("error");
        }
      }
    };

    loadPermissions();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, t]);

  useEffect(() => {
    if (isSdkLoading) return;
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

        const functionSchedules = (await sdk.post(
          `/api/v1/projects/${sdk.project}/functions/schedules/list`,
          {
            data: { limit: 1000 },
          }
        )) as { data?: { items?: Array<Record<string, unknown>> } };
        for (const item of functionSchedules.data?.items ?? []) {
          const cron = readCron(item);
          if (!cron) continue;
          const name =
            (item.name as string | undefined) ??
            (item.functionExternalId as string | undefined) ??
            (item.functionId as string | undefined) ??
            "Function schedule";
          const id = String(item.id ?? item.functionId ?? name);
          all.push({ type: "function", id, name, cron });
        }

        const transformationSchedules = (await sdk.get(
          `/api/v1/projects/${sdk.project}/transformations/schedules`,
          {
            params: { limit: "1000" },
          }
        )) as { data?: { items?: Array<Record<string, unknown>> } };
        for (const item of transformationSchedules.data?.items ?? []) {
          const cron = readCron(item);
          if (!cron) continue;
          const name =
            (item.name as string | undefined) ??
            (item.transformationExternalId as string | undefined) ??
            (item.transformationId as string | undefined) ??
            "Transformation schedule";
          const id = String(item.id ?? item.transformationId ?? name);
          all.push({ type: "transformation", id, name, cron });
        }

        let triggerCursor: string | undefined;
        do {
          const workflowTriggers = (await sdk.get(
            `/api/v1/projects/${sdk.project}/workflows/triggers`,
            {
              params: {
                limit: "1000",
                cursor: triggerCursor,
              },
            }
          )) as { data?: { items?: Array<Record<string, unknown>>; nextCursor?: string } };
          for (const item of workflowTriggers.data?.items ?? []) {
            const cron = readCron(item);
            if (!cron) continue;
            const workflowId = item.workflowExternalId as string | undefined;
            const triggerId =
              (item.externalId as string | undefined) ??
              (item.id as string | undefined) ??
              "trigger";
            const name = workflowId ? `${workflowId} · ${triggerId}` : triggerId;
            const id = String(item.externalId ?? item.id ?? name);
            all.push({ type: "workflow", id, name, cron });
          }
          triggerCursor = workflowTriggers.data?.nextCursor;
        } while (triggerCursor);

        if (!cancelled) {
          setSchedules(all);
          setSchedulesStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setSchedulesError(
            error instanceof Error ? error.message : t("healthChecks.scheduling.error")
          );
          setSchedulesStatus("error");
        }
      }
    };

    loadSchedules();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, t]);

  useEffect(() => {
    if (isSdkLoading) return;
    if (!sdk.raw?.listDatabases || !sdk.raw?.listTables || !sdk.raw?.listRows) {
      setRawAvailabilityMessage(t("healthChecks.raw.unavailable"));
      setRawStatus("success");
      return;
    }
    let cancelled = false;

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
            limit: 100,
            cursor: dbCursor,
          });
          databases.push(...((response.items ?? []) as RawDatabaseSummary[]));
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
          do {
            const response = await sdk.raw.listTables(database.name, {
              limit: 100,
              cursor: tableCursor,
            });
            const items = (response.items ?? []) as unknown as Array<Record<string, unknown>>;
            tables.push(
              ...items.map((item) => ({
                dbName: database.name,
                name: String(item.name),
                rowCount: typeof item.rowCount === "number" ? item.rowCount : undefined,
                lastUpdatedTime: toTimestamp(item.lastUpdatedTime),
                createdTime: toTimestamp(item.createdTime),
              }))
            );
            if (!cancelled) {
              setRawTableScanned(tables.length);
            }
            tableCursor = response.nextCursor ?? undefined;
          } while (tableCursor);
          processedDatabases += 1;
          if (!cancelled) {
            setRawDbProcessed(processedDatabases);
          }
        }

        const sampledTables: RawTableSummary[] = [];
        setRawSampleTotal(tables.length);
        for (const table of tables) {
          if (table.rowCount != null && table.lastUpdatedTime != null) {
            sampledTables.push(table);
            if (!cancelled) {
              setRawSampleProcessed((prev) => prev + 1);
            }
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
            if (sampleCount >= 10) {
              break;
            }
          } while (cursor);

          sampledTables.push({
            ...table,
            sampleRowCount: sampleCount,
          });
          if (!cancelled) {
            setRawSampleProcessed((prev) => prev + 1);
          }
        }

        if (!cancelled) {
          setRawDatabases(databases);
          setRawTables(sampledTables);
          setRawStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setRawError(
            error instanceof Error ? error.message : t("healthChecks.errors.rawMetadata")
          );
          setRawStatus("error");
        }
      }
    };

    loadRaw();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, t]);

  const usedViewKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const model of dataModels) {
      for (const view of model.views ?? []) {
        const key = `${view.space}:${view.externalId}:${view.version ?? "latest"}`;
        keys.add(key);
      }
    }
    return keys;
  }, [dataModels]);

  const unusedViews = useMemo(() => {
    return views
      .filter((view) => {
        const key = `${view.space}:${view.externalId}:${view.version ?? "latest"}`;
        return !usedViewKeys.has(key);
      })
      .sort((a, b) => (a.name ?? a.externalId).localeCompare(b.name ?? b.externalId));
  }, [views, usedViewKeys]);

  const viewsWithoutContainers = useMemo(() => {
    return viewDetails
      .filter((view) => {
        const properties = Object.values(view.properties ?? {});
        return properties.length === 0 || properties.every((property) => !property.container);
      })
      .sort((a, b) => (a.name ?? a.externalId).localeCompare(b.name ?? b.externalId));
  }, [viewDetails]);

  const usedContainerKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const view of viewDetails) {
      for (const property of Object.values(view.properties ?? {})) {
        if (property.container?.space && property.container?.externalId) {
          keys.add(`${property.container.space}:${property.container.externalId}`);
        }
      }
    }
    return keys;
  }, [viewDetails]);

  const unusedContainers = useMemo(() => {
    return containers
      .filter((container) => !usedContainerKeys.has(`${container.space}:${container.externalId}`))
      .sort((a, b) => (a.name ?? a.externalId).localeCompare(b.name ?? b.externalId));
  }, [containers, usedContainerKeys]);

  const unusedSpaces = useMemo(() => {
    const usedSpaces = new Set<string>();
    for (const model of dataModels) {
      usedSpaces.add(model.space);
    }
    for (const view of views) {
      usedSpaces.add(view.space);
    }
    for (const container of containers) {
      usedSpaces.add(container.space);
    }
    return spaces
      .filter((space) => !usedSpaces.has(space.space))
      .sort((a, b) => (a.name ?? a.space).localeCompare(b.name ?? b.space));
  }, [containers, dataModels, spaces, views]);

  const emptyRawTables = useMemo(() => {
    return rawTables
      .filter((table) => {
        if (table.rowCount === 0) return true;
        if (table.rowCount == null) {
          return table.sampleRowCount === 0;
        }
        return false;
      })
      .sort((a, b) => a.dbName.localeCompare(b.dbName) || a.name.localeCompare(b.name));
  }, [rawTables]);

  const functionsByRuntime = useMemo(() => {
    const map = new Map<string, FunctionSummary[]>();
    for (const fn of functions) {
      const runtime = fn.runtime ?? t("healthChecks.functions.runtime.unknown");
      const list = map.get(runtime) ?? [];
      list.push(fn);
      map.set(runtime, list);
    }
    return map;
  }, [functions, t]);

  const runtimeList = useMemo(() => {
    return Array.from(functionsByRuntime.keys()).sort((a, b) => a.localeCompare(b));
  }, [functionsByRuntime]);

  const parsePythonVersion = (runtime?: string) => {
    if (!runtime) return null;
    const match = runtime.match(/py(\d{2,3})/i);
    if (!match) return null;
    const raw = match[1];
    if (raw.length === 2) {
      return { major: Number(raw[0]), minor: Number(raw[1]) };
    }
    if (raw.length === 3) {
      return { major: Number(raw[0]), minor: Number(raw.slice(1)) };
    }
    return null;
  };

  const lowPythonFunctions = useMemo(() => {
    return functions
      .filter((fn) => {
        const version = parsePythonVersion(fn.runtime);
        if (!version) return false;
        if (version.major !== 3) return true;
        return version.minor < 12;
      })
      .sort((a, b) => (a.name ?? a.id).localeCompare(b.name ?? b.id));
  }, [functions]);

  const permissionScopeDrift = useMemo(() => {
    const findings: PermissionScopeDriftEntry[] = [];
    const seen = new Set<string>();
    const byCapability = new Map<
      string,
      Array<{ groupName: string; scopeType: string; items: string[] }>
    >();

    for (const group of groups) {
      const groupName = group.name ?? t("permissions.group.fallback", { id: group.id });
      for (const cap of group.capabilities ?? []) {
        const normalized = normalizeCapability(cap);
        const actions = (normalized.actions ?? []).slice().sort().join("|");
        const key = `${normalized.name}::${actions}`;
        for (const entry of extractScopeEntries(normalized.scope)) {
          const list = byCapability.get(key) ?? [];
          list.push({ groupName, scopeType: entry.type, items: entry.items });
          byCapability.set(key, list);
        }
      }
    }

    const meetsOverlapThreshold = (a: string[], b: string[]) => {
      const aSet = new Set(a);
      const bSet = new Set(b);
      let common = 0;
      for (const value of aSet) if (bSet.has(value)) common += 1;
      if (common < 3) return false;
      const overlap = common / Math.max(aSet.size, bSet.size);
      return overlap >= 0.8;
    };

    for (const [capabilityKey, entries] of byCapability.entries()) {
      const groupedByType = entries.reduce<Record<string, typeof entries>>((acc, entry) => {
        acc[entry.scopeType] = acc[entry.scopeType] ?? [];
        acc[entry.scopeType].push(entry);
        return acc;
      }, {});

      for (const [scopeType, scopeEntries] of Object.entries(groupedByType)) {
        for (let i = 0; i < scopeEntries.length; i += 1) {
          for (let j = i + 1; j < scopeEntries.length; j += 1) {
            const left = scopeEntries[i];
            const right = scopeEntries[j];
            if (left.groupName === right.groupName) continue;
            if (left.items.join("|") === right.items.join("|")) continue;
            if (!meetsOverlapThreshold(left.items, right.items)) continue;
            const readableName = capabilityKey.split("::")[0];
            const actionSuffix = capabilityKey.split("::")[1];
            const leftSet = new Set(left.items);
            const rightSet = new Set(right.items);
            const common = left.items.filter((value) => rightSet.has(value));
            const leftOnly = left.items.filter((value) => !rightSet.has(value));
            const rightOnly = right.items.filter((value) => !leftSet.has(value));
            const summary = t("healthChecks.permissions.drift.itemDiff", {
              capability: readableName,
              actions: actionSuffix || t("healthChecks.permissions.drift.noActions"),
              scopeType,
              left: left.groupName,
              right: right.groupName,
            });
            const id = [
              capabilityKey,
              scopeType,
              left.groupName,
              right.groupName,
              left.items.join("|"),
              right.items.join("|"),
            ].join("::");
            if (seen.has(id)) continue;
            seen.add(id);
            findings.push({
              id,
              summary,
              scopeType,
              leftGroup: left.groupName,
              rightGroup: right.groupName,
              common,
              leftOnly,
              rightOnly,
            });
          }
        }
      }
    }

    return findings.slice(0, 25);
  }, [groups, t]);

  const scheduleOverlaps = useMemo(() => {
    const byKey = new Map<string, ScheduleEntry[]>();

    const parseKey = (cron: string) => {
      const parts = cron.trim().split(/\s+/);
      if (parts.length < 2) return cron;
      const [minute, hour] = parts;
      return `${minute} ${hour}`;
    };

    for (const schedule of schedules) {
      const key = parseKey(schedule.cron);
      byKey.set(key, [...(byKey.get(key) ?? []), schedule]);
    }

    const buildOffset = (cron: string, offset: number) => {
      const parts = cron.trim().split(/\s+/);
      if (parts.length < 2) return "";
      const minute = parts[0];
      if (!/^\d+$/.test(minute)) return "";
      const nextMinute = (Number(minute) + offset) % 60;
      return [String(nextMinute), ...parts.slice(1)].join(" ");
    };

    const overlaps: Array<{ key: string; schedules: ScheduleEntry[]; exampleOffsets: string[] }> = [];
    for (const [key, items] of byKey.entries()) {
      if (items.length < 2) continue;
      const examples: string[] = [];
      const firstCron = items[0]?.cron ?? "";
      const offsets = [5, 10, 15];
      for (const offset of offsets) {
        const example = buildOffset(firstCron, offset);
        if (example) examples.push(example);
      }
      overlaps.push({ key, schedules: items, exampleOffsets: examples });
    }
    return overlaps.sort((a, b) => b.schedules.length - a.schedules.length);
  }, [schedules]);

  return (
    <section className="flex flex-col gap-4">
      <header className="flex flex-col gap-1">
        <h2 className="text-2xl font-semibold text-slate-900">
          {t("healthChecks.title")}
        </h2>
        <p className="text-sm text-slate-500">{t("healthChecks.subtitle")}</p>
      </header>
      <FunctionsHealthPanel
        functionsStatus={functionsStatus}
        functionsError={functionsError}
        runtimeList={runtimeList}
        functionsByRuntime={functionsByRuntime}
        lowPythonFunctions={lowPythonFunctions}
      />
      <SchedulingHealthPanel
        status={schedulesStatus}
        error={schedulesError}
        schedules={schedules}
        overlaps={scheduleOverlaps}
      />
      {showInternal ? (
        <ModelingHealthPanel
          dataModelsStatus={dataModelsStatus}
          viewsStatus={viewsStatus}
          viewDetailsStatus={viewDetailsStatus}
          containersStatus={containersStatus}
          spacesStatus={spacesStatus}
          dataModelsError={dataModelsError}
          viewsError={viewsError}
          viewDetailsError={viewDetailsError}
          containersError={containersError}
          spacesError={spacesError}
          unusedViews={unusedViews}
          viewsWithoutContainers={viewsWithoutContainers}
          unusedContainers={unusedContainers}
          unusedSpaces={unusedSpaces}
          viewDetailsProcessed={viewDetailsProcessed}
          viewDetailsTotal={viewDetailsTotal}
          renderProgressBar={renderProgressBar}
        />
      ) : null}
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
      />
      <PermissionsHealthPanel
        permissionsStatus={permissionsStatus}
        permissionsError={permissionsError}
        permissionScopeDrift={permissionScopeDrift}
      />
      <Loader
        open={showLoader}
        onClose={() => setShowLoader(false)}
        title={t("healthChecks.loader.title")}
      />
    </section>
  );
}
