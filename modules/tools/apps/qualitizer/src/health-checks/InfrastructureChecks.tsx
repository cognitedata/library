import { useEffect, useMemo, useRef, useState } from "react";
import { Loader } from "@/shared/Loader";
import { useAppSdk } from "@/shared/auth";
import { FunctionsHealthPanel } from "./FunctionsHealthPanel";
import { SchedulingHealthPanel } from "./SchedulingHealthPanel";
import { parsePythonVersion } from "./health-checks-utils";
import type { FunctionSummary, LoadState, ScheduleEntry } from "./types";

type Props = { onBack: () => void };

export function InfrastructureChecks({ onBack }: Props) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();

  const [functionsStatus, setFunctionsStatus] = useState<LoadState>("idle");
  const [functionsError, setFunctionsError] = useState<string | null>(null);
  const [functions, setFunctions] = useState<FunctionSummary[]>([]);

  const [schedulesStatus, setSchedulesStatus] = useState<LoadState>("idle");
  const [schedulesError, setSchedulesError] = useState<string | null>(null);
  const [schedules, setSchedules] = useState<ScheduleEntry[]>([]);

  const [showLoader, setShowLoader] = useState(false);
  const cancelRef = useRef(false);

  const isDashboardLoading =
    functionsStatus === "loading" ||
    schedulesStatus === "loading";

  useEffect(() => {
    setShowLoader(isDashboardLoading);
  }, [isDashboardLoading]);

  useEffect(() => {
    if (isSdkLoading) return;
    cancelRef.current = false;

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
        if (!cancelRef.current) {
          setFunctions(items);
          setFunctionsStatus("success");
        }
      } catch (error) {
        if (!cancelRef.current) {
          setFunctionsError(
            error instanceof Error ? error.message : "Failed to load functions"
          );
          setFunctionsStatus("error");
        }
      }
    };

    loadFunctions();
    return () => {
      cancelRef.current = true;
    };
  }, [isSdkLoading, sdk]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;

    const readCron = (item: Record<string, unknown>) => {
      const direct = item.cron ?? item.cronExpression;
      if (typeof direct === "string" && direct.trim()) return direct.trim();
      const triggerRule = item.triggerRule as
        | Record<string, unknown>
        | undefined;
      const triggerDirect = triggerRule?.cron ?? triggerRule?.cronExpression;
      if (typeof triggerDirect === "string" && triggerDirect.trim())
        return triggerDirect.trim();
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
          { data: { limit: 1000 } }
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
          { params: { limit: "1000" } }
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
            { params: { limit: "1000", cursor: triggerCursor } }
          )) as {
            data?: {
              items?: Array<Record<string, unknown>>;
              nextCursor?: string;
            };
          };
          for (const item of workflowTriggers.data?.items ?? []) {
            const cron = readCron(item);
            if (!cron) continue;
            const workflowId = item.workflowExternalId as string | undefined;
            const triggerId =
              (item.externalId as string | undefined) ??
              (item.id as string | undefined) ??
              "trigger";
            const name = workflowId
              ? `${workflowId} · ${triggerId}`
              : triggerId;
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
            error instanceof Error ? error.message : "Failed to load schedules"
          );
          setSchedulesStatus("error");
        }
      }
    };

    loadSchedules();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk]);

  const functionsByRuntime = useMemo(() => {
    const map = new Map<string, FunctionSummary[]>();
    for (const fn of functions) {
      const runtime = fn.runtime ?? "Unknown";
      const list = map.get(runtime) ?? [];
      list.push(fn);
      map.set(runtime, list);
    }
    return map;
  }, [functions]);

  const runtimeList = useMemo(
    () => Array.from(functionsByRuntime.keys()).sort((a, b) => a.localeCompare(b)),
    [functionsByRuntime]
  );

  const lowPythonFunctions = useMemo(
    () =>
      functions
        .filter((fn) => {
          const version = parsePythonVersion(fn.runtime);
          if (!version) return false;
          if (version.major !== 3) return true;
          return version.minor < 12;
        })
        .sort((a, b) => (a.name ?? a.id).localeCompare(b.name ?? b.id)),
    [functions]
  );

  const scheduleOverlaps = useMemo(() => {
    const byKey = new Map<string, ScheduleEntry[]>();
    const parseKey = (cron: string) => {
      const parts = cron.trim().split(/\s+/);
      if (parts.length < 2) return cron;
      return `${parts[0]} ${parts[1]}`;
    };
    for (const s of schedules) {
      const key = parseKey(s.cron);
      byKey.set(key, [...(byKey.get(key) ?? []), s]);
    }
    const buildOffset = (cron: string, offset: number) => {
      const parts = cron.trim().split(/\s+/);
      if (parts.length < 2) return "";
      const minute = parts[0];
      if (!/^\d+$/.test(minute)) return "";
      return [String((Number(minute) + offset) % 60), ...parts.slice(1)].join(" ");
    };
    const overlaps: Array<{
      key: string;
      schedules: ScheduleEntry[];
      exampleOffsets: string[];
    }> = [];
    for (const [key, items] of byKey.entries()) {
      if (items.length < 2) continue;
      const examples: string[] = [];
      const firstCron = items[0]?.cron ?? "";
      for (const o of [5, 10, 15]) {
        const ex = buildOffset(firstCron, o);
        if (ex) examples.push(ex);
      }
      overlaps.push({ key, schedules: items, exampleOffsets: examples });
    }
    return overlaps.sort((a, b) => b.schedules.length - a.schedules.length);
  }, [schedules]);

  return (
    <section className="flex flex-col gap-4">
      <header className="flex items-start justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h2 className="text-2xl font-semibold text-slate-900">
            Infrastructure Checks
          </h2>
          <p className="text-sm text-slate-500">
            Functions and scheduling overlaps
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
      <Loader
        open={showLoader}
        onClose={() => setShowLoader(false)}
        title="Running infrastructure checks…"
      />
    </section>
  );
}
