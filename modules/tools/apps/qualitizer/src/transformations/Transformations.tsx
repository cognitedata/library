import { startTransition, useEffect, useMemo, useRef, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { useI18n } from "@/shared/i18n";
import { usePrivateMode } from "@/shared/PrivateModeContext";
import { ApiError } from "@/shared/ApiError";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { LoadState } from "@/processing/types";
import { formatDuration, formatIso, toTimestamp } from "@/shared/time-utils";
import {
  isFailed as isFailedStatus,
  isSuccess as isSuccessStatus,
  uptimePercentage,
} from "@/health-checks/run-health/uptime";

function formatDurationShort(ms: number | undefined): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms}ms`;
  return formatDuration(ms);
}

/** Format large numbers as human-readable K/M (e.g. 1.2K, 3.5M). */
function formatPrettyNumber(n: number): string {
  if (n < 1000) return String(n);
  if (n < 1_000_000) {
    const k = n / 1000;
    if (k >= 1000) return `${(k / 1000).toFixed(1)}M`;
    return k % 1 === 0 ? `${k}K` : `${k.toFixed(1)}K`;
  }
  const m = n / 1_000_000;
  return m % 1 === 0 ? `${m}M` : `${m.toFixed(1)}M`;
}
import {
  parseTransformationQuery,
  getParsedInsightCounts,
  type ParsedInsightCounts,
  type ParsedInsight,
} from "./transformationChecks";
import { TransformationsHelpModal } from "./TransformationsHelpModal";

function CellSpinner() {
  return (
    <span
      className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-slate-600"
      aria-hidden
    />
  );
}

import { getTransformationPreviewUrl } from "@/shared/cdf-browser-url";
import {
  cachedTransformationJobMetrics,
  cachedTransformationsList,
} from "./transformations-cache";
import { loadTransformationJobsForWindow } from "./transformation-jobs-service";

function ExternalLinkIcon({ className }: { className?: string }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden
    >
      <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
      <polyline points="15 3 21 3 21 9" />
      <line x1="10" y1="14" x2="21" y2="3" />
    </svg>
  );
}

/** Remove lines that are comments (start with -- after leading whitespace). */
function stripSqlCommentLines(sql: string): string {
  return sql
    .split(/\r?\n/)
    .filter((line) => !line.trimStart().startsWith("--"))
    .join("\n");
}

/** Remove leading block comments and whitespace so WITH is visible. */
function stripLeadingBlockComments(sql: string): string {
  let i = 0;
  while (i < sql.length) {
    while (i < sql.length && /\s/.test(sql[i])) i += 1;
    if (i >= sql.length) break;
    if (sql[i] === "/" && sql[i + 1] === "*") {
      i += 2;
      while (i < sql.length && !(sql[i] === "*" && sql[i + 1] === "/")) i += 1;
      if (sql[i] === "*" && sql[i + 1] === "/") i += 2;
      continue;
    }
    break;
  }
  return sql.slice(i).trimStart();
}

/** True if body references any of the given names as a word (whitespace-delimited). */
function bodyReferencesPreceding(body: string, precedingNames: string[]): boolean {
  const normalized = body.replace(/\s+/g, " ");
  for (const n of precedingNames) {
    if (!n) continue;
    const escaped = n.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const re = new RegExp(`(?:^|\\s)${escaped}(?:\\s|$)`, "i");
    if (re.test(normalized)) return true;
  }
  return false;
}

/**
 * Build preview query: if the CTE does not reference preceding CTEs, run just its body;
 * otherwise inject all preceding CTEs in a WITH clause.
 */
function buildCtePreviewQuery(
  names: string[],
  bodies: Record<string, string>,
  targetName: string
): { query: string; displayQuery: string; isIndependent: boolean } {
  const body = (bodies[targetName] ?? "").trim() || "SELECT 1";
  const index = names.indexOf(targetName);
  if (index <= 0) {
    return { query: body, displayQuery: body, isIndependent: true };
  }
  const preceding = names.slice(0, index);
  const independent = !bodyReferencesPreceding(body, preceding);
  if (independent) {
    return { query: body, displayQuery: body, isIndependent: true };
  }
  const through = names.slice(0, index + 1);
  const withClauses = through
    .map((n) => `${n} AS (${bodies[n]?.trim() || "SELECT 1"})`)
    .join(", ");
  const fullQuery = `WITH ${withClauses} SELECT * FROM ${targetName} LIMIT 10`;
  return {
    query: fullQuery,
    displayQuery: body,
    isIndependent: false,
  };
}

type TransformationSummary = {
  id: number | string;
  name?: string;
  query?: string;
};

type TransformationJobSummary = {
  id?: number | string;
  startedTime?: number;
  finishedTime?: number;
  status?: string;
};

type JobMetricItem = {
  name: string;
  timestamp: number;
  count: number;
  effective?: boolean;
};

function computeTransformationJobStats(
  jobs: TransformationJobSummary[],
  windowStart: number,
  windowEnd: number
): {
  count: number;
  lastRun?: number;
  totalMs: number;
  latestJobId: string | null;
  success: number;
  failed: number;
  uptime: number;
} {
  const recent = jobs.filter((job) => {
    const start = toTimestamp(job.startedTime);
    if (!start) return false;
    return start >= windowStart && start <= windowEnd;
  });
  const count = recent.length;
  const lastRun = recent.reduce<number | undefined>((acc, job) => {
    const start = toTimestamp(job.startedTime);
    if (!start) return acc;
    return acc == null || start > acc ? start : acc;
  }, undefined);
  const totalMs = recent.reduce((acc, job) => {
    const start = toTimestamp(job.startedTime);
    const end = toTimestamp(job.finishedTime);
    if (!start || !end || end < start) return acc;
    return acc + (end - start);
  }, 0);
  const success = recent.filter((job) => isSuccessStatus(job.status)).length;
  const failed = recent.filter((job) => isFailedStatus(job.status)).length;
  const uptime = uptimePercentage(success, failed);
  const sorted = [...jobs].sort(
    (a, b) => (toTimestamp(b.startedTime) ?? 0) - (toTimestamp(a.startedTime) ?? 0)
  );
  const latest = sorted[0];
  const latestJobId = latest?.id != null ? String(latest.id) : null;
  return { count, lastRun, totalMs, latestJobId, success, failed, uptime };
}

async function runWithConcurrencyLimit<T>(
  items: T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<void>
): Promise<void> {
  if (items.length === 0) return;
  const maxConcurrent = Math.max(1, Math.floor(concurrency));
  let nextIndex = 0;
  const runWorker = async () => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= items.length) return;
      await worker(items[index], index);
    }
  };
  const workers = Array.from({ length: Math.min(maxConcurrent, items.length) }, () => runWorker());
  await Promise.all(workers);
}

function aggregateJobMetrics(items: JobMetricItem[]): {
  reads: number;
  writes: number;
  noops: number;
  rateLimit429: number;
} {
  const byName = new Map<string, { timestamp: number; count: number }>();
  for (const item of items) {
    const prev = byName.get(item.name);
    if (!prev || item.timestamp > prev.timestamp) {
      byName.set(item.name, { timestamp: item.timestamp, count: item.count });
    }
  }
  let reads = 0;
  let writes = 0;
  let noops = 0;
  let rateLimit429 = 0;
  for (const [name, { count }] of byName) {
    if (
      name.includes(".rows.read") ||
      name === "instances.read" ||
      name.startsWith("instances.read.")
    ) {
      reads += count;
    }
    if (name === "instances.upserted") writes = count;
    if (name === "instances.upsertedNoop") noops = count;
    if (name.includes(".429.")) rateLimit429 += count;
  }
  return { reads, writes, noops, rateLimit429 };
}

/** Smaller API pages load faster; cursor fetches the rest without huge single responses. */
const TRANSFORMATIONS_LIST_PAGE_LIMIT = 200;
const TRANSFORMATIONS_JOBS_CONCURRENCY = 3;
const TRANSFORMATIONS_FILTER_MIN_CHARS = 3;
const TRANSFORMATIONS_FILTER_DEBOUNCE_MS = 350;

type TransformationsListProps = {
  transformationToSelect?: string | null;
  onTransformationSelected?: () => void;
  /** Ref to call when parent wants to clear selection (e.g. list sub-nav clicked). */
  clearSelectionRef?: React.MutableRefObject<(() => void) | null>;
};

export function TransformationsList({
  transformationToSelect,
  onTransformationSelected,
  clearSelectionRef,
}: TransformationsListProps = {}) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { t } = useI18n();
  const { isPrivateMode } = usePrivateMode();
  const pc = isPrivateMode ? " private-mask" : "";
  const [status, setStatus] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [loadingMessage, setLoadingMessage] = useState<string | null>(null);
  const [transformations, setTransformations] = useState<TransformationSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [statsById, setStatsById] = useState<
    Record<string, { count: number; lastRun?: number; totalMs: number }>
  >({});
  const [uptimeById, setUptimeById] = useState<
    Record<string, { success: number; failed: number; uptime: number }>
  >({});
  const [countsById, setCountsById] = useState<Record<string, ParsedInsightCounts>>({});
  const [metricsById, setMetricsById] = useState<
    Record<string, { reads: number; writes: number; noops: number; rateLimit429: number }>
  >({});
  /** Latest job id per transformation (from jobs list). Key present after jobs fetch; null = no job. */
  const [latestJobById, setLatestJobById] = useState<Record<string, string | null>>({});
  const metricsJobFetchedRef = useRef<Record<string, string>>({});
  const [ctePreviews, setCtePreviews] = useState<
    Record<
      string,
      {
        status: LoadState;
        error?: string;
        rows: Array<Record<string, unknown>>;
        query?: string;
        fullQuery?: string;
        durationMs?: number;
      }
    >
  >({});
  const [cteQueryExpanded, setCteQueryExpanded] = useState<Set<string>>(new Set());
  const [showHelp, setShowHelp] = useState(false);
  type TableSortKey = "name" | "count" | "lastRun" | "totalMs" | "success" | "failed";
  const [sortKey, setSortKey] = useState<TableSortKey>("totalMs");
  const [sortDesc, setSortDesc] = useState(true);
  const PAGE_SIZE = 20;
  const [page, setPage] = useState(0);
  const [searchQuery, setSearchQuery] = useState("");
  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState("");
  const toSelectRef = useRef(transformationToSelect);
  toSelectRef.current = transformationToSelect;

  useEffect(() => {
    if (!searchQuery.trim()) {
      setDebouncedSearchQuery("");
      return;
    }
    const id = window.setTimeout(() => {
      setDebouncedSearchQuery(searchQuery);
    }, TRANSFORMATIONS_FILTER_DEBOUNCE_MS);
    return () => window.clearTimeout(id);
  }, [searchQuery]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const loadList = async () => {
      setStatus("loading");
      setErrorMessage(null);
      setLoadingMessage("Loading transformations list...");
      try {
        const items: TransformationSummary[] = [];
        let cursor: string | undefined;
        let listPageCount = 0;
        do {
          const response = (await cachedTransformationsList(sdk, {
            includePublic: "true",
            limit: String(TRANSFORMATIONS_LIST_PAGE_LIMIT),
            ...(cursor ? { cursor } : {}),
          })) as {
            data?: { items?: TransformationSummary[]; nextCursor?: string | null };
          };
          items.push(...(response.data?.items ?? []));
          listPageCount += 1;
          if (!cancelled) {
            setLoadingMessage(
              `Loading transformations list... fetched ${items.length} rows across ${listPageCount} page${listPageCount === 1 ? "" : "s"}`
            );
          }
          const next = response.data?.nextCursor;
          cursor = next && String(next).trim() !== "" ? String(next) : undefined;
        } while (cursor && !cancelled);

        const windowEnd = Date.now();
        const windowStart = windowEnd - 24 * 60 * 60 * 1000;
        const nextStats: Record<string, { count: number; lastRun?: number; totalMs: number }> = {};
        const nextUptime: Record<string, { success: number; failed: number; uptime: number }> = {};
        const nextLatest: Record<string, string | null> = {};

        let jobsProcessed = 0;
        await runWithConcurrencyLimit(
          items,
          TRANSFORMATIONS_JOBS_CONCURRENCY,
          async (item) => {
            if (cancelled) return;
            const id = String(item.id);
            try {
              const jobs = (await loadTransformationJobsForWindow({
                sdk,
                transformationId: id,
                windowStart,
                windowEnd,
              })) as TransformationJobSummary[];
              const { count, lastRun, totalMs, latestJobId, success, failed, uptime } =
                computeTransformationJobStats(jobs, windowStart, windowEnd);
              nextStats[id] = { count, lastRun, totalMs };
              nextUptime[id] = { success, failed, uptime };
              nextLatest[id] = latestJobId;
            } catch {
              nextStats[id] = { count: 0, totalMs: 0 };
              nextUptime[id] = { success: 0, failed: 0, uptime: 100 };
              nextLatest[id] = null;
            } finally {
              jobsProcessed += 1;
              if (!cancelled) {
                setLoadingMessage(
                  `Computing total time for sorting... ${jobsProcessed}/${items.length} (${Math.max(0, items.length - jobsProcessed)} remaining)`
                );
              }
            }
          }
        );

        const firstPage = [...items]
          .sort((a, b) => {
            const aId = String(a.id);
            const bId = String(b.id);
            const diff = (nextStats[bId]?.totalMs ?? 0) - (nextStats[aId]?.totalMs ?? 0);
            if (diff !== 0) return diff;
            const aLabel = a.name ?? aId;
            const bLabel = b.name ?? bId;
            return aLabel.localeCompare(bLabel);
          })
          .slice(0, PAGE_SIZE);
        const nextMetrics: Record<
          string,
          { reads: number; writes: number; noops: number; rateLimit429: number }
        > = {};
        for (let i = 0; i < firstPage.length; i += 1) {
          if (cancelled) return;
          const id = String(firstPage[i].id);
          const jobId = nextLatest[id];
          setLoadingMessage(
            `Loading first-page metric columns... ${i + 1}/${firstPage.length} (${Math.max(0, firstPage.length - (i + 1))} remaining)`
          );
          if (jobId == null) {
            nextMetrics[id] = { reads: 0, writes: 0, noops: 0, rateLimit429: 0 };
            continue;
          }
          try {
            const metricsRes = (await cachedTransformationJobMetrics(
              sdk,
              jobId
            )) as { data?: { items?: JobMetricItem[] } };
            nextMetrics[id] = aggregateJobMetrics(metricsRes.data?.items ?? []);
            metricsJobFetchedRef.current[id] = jobId;
          } catch {
            nextMetrics[id] = { reads: 0, writes: 0, noops: 0, rateLimit429: 0 };
            metricsJobFetchedRef.current[id] = jobId;
          }
        }

        if (!cancelled) {
          setTransformations(items);
          const toSelect =
            toSelectRef.current &&
            items.some((t) => String(t.id) === toSelectRef.current)
              ? toSelectRef.current
              : null;
          setSelectedId(toSelect);
          if (toSelect && onTransformationSelected) {
            onTransformationSelected();
          }
          setStatsById(nextStats);
          setUptimeById(nextUptime);
          setCountsById({});
          setLatestJobById(nextLatest);
          setMetricsById(nextMetrics);
          setStatus("success");
          setLoadingMessage(null);
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(
            error instanceof Error ? error.message : t("transformations.list.error")
          );
          setStatus("error");
          setLoadingMessage(null);
        }
      }
    };
    loadList();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, onTransformationSelected, t]);

  const selectedTransformation = useMemo(() => {
    if (!selectedId) return null;
    return transformations.find((item) => String(item.id) === selectedId) ?? null;
  }, [transformations, selectedId]);

  const filteredTransformations = useMemo(() => {
    const qRaw = debouncedSearchQuery.trim();
    if (qRaw.length > 0 && qRaw.length < TRANSFORMATIONS_FILTER_MIN_CHARS) {
      return transformations;
    }
    const q = qRaw.toLowerCase();
    if (!q) return transformations;
    return transformations.filter((t) => {
      const id = String(t.id).toLowerCase();
      const name = (t.name ?? "").toLowerCase();
      const query = (t.query ?? "").toLowerCase();
      return id.includes(q) || name.includes(q) || query.includes(q);
    });
  }, [transformations, debouncedSearchQuery]);

  const filterPendingDebounce =
    searchQuery.trim() !== "" && searchQuery !== debouncedSearchQuery;
  const filterTooShort =
    searchQuery.trim().length > 0 &&
    searchQuery.trim().length < TRANSFORMATIONS_FILTER_MIN_CHARS;

  const sortedTransformations = useMemo(() => {
    const items = [...filteredTransformations];
    const getStats = (id: string) => statsById[id] ?? { count: 0, totalMs: 0 };
    const getUptime = (id: string) => uptimeById[id] ?? { success: 0, failed: 0, uptime: 100 };
    return items.sort((a, b) => {
      const aId = String(a.id);
      const bId = String(b.id);
      if (sortKey === "name") {
        const aLabel = a.name ?? aId;
        const bLabel = b.name ?? bId;
        return sortDesc ? bLabel.localeCompare(aLabel) : aLabel.localeCompare(bLabel);
      }
      if (sortKey === "count") {
        const diff = getStats(bId).count - getStats(aId).count;
        return sortDesc ? diff : -diff;
      }
      if (sortKey === "totalMs") {
        const diff = getStats(bId).totalMs - getStats(aId).totalMs;
        return sortDesc ? diff : -diff;
      }
      if (sortKey === "success") {
        const diff = getUptime(bId).success - getUptime(aId).success;
        return sortDesc ? diff : -diff;
      }
      if (sortKey === "failed") {
        const diff = getUptime(bId).failed - getUptime(aId).failed;
        return sortDesc ? diff : -diff;
      }
      const aLast = getStats(aId).lastRun ?? 0;
      const bLast = getStats(bId).lastRun ?? 0;
      const diff = bLast - aLast;
      return sortDesc ? diff : -diff;
    });
  }, [filteredTransformations, sortDesc, sortKey, statsById, uptimeById]);

  const totalPages = Math.max(1, Math.ceil(sortedTransformations.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages - 1);
  const currentPageItems = useMemo(
    () =>
      sortedTransformations.slice(
        safePage * PAGE_SIZE,
        safePage * PAGE_SIZE + PAGE_SIZE
      ),
    [sortedTransformations, safePage]
  );

  useEffect(() => {
    if (status !== "success" || isSdkLoading || !sdk || currentPageItems.length === 0) return;
    const windowEnd = Date.now();
    const windowStart = windowEnd - 24 * 60 * 60 * 1000;
    let cancelled = false;

    const run = async () => {
      await runWithConcurrencyLimit(
        currentPageItems,
        TRANSFORMATIONS_JOBS_CONCURRENCY,
        async (transformation) => {
          if (cancelled) return;
          const id = String(transformation.id);
          if (statsById[id] && id in latestJobById) return;
          try {
            const jobs = (await loadTransformationJobsForWindow({
              sdk,
              transformationId: id,
              windowStart,
              windowEnd,
            })) as TransformationJobSummary[];
            const { count, lastRun, totalMs, latestJobId, success, failed, uptime } =
              computeTransformationJobStats(jobs, windowStart, windowEnd);
            if (!cancelled) {
              startTransition(() => {
                setStatsById((prev) => ({ ...prev, [id]: { count, lastRun, totalMs } }));
                setUptimeById((prev) => ({ ...prev, [id]: { success, failed, uptime } }));
                setLatestJobById((prev) => ({ ...prev, [id]: latestJobId }));
              });
            }
          } catch {
            if (!cancelled) {
              startTransition(() => {
                setStatsById((prev) => ({ ...prev, [id]: { count: 0, totalMs: 0 } }));
                setUptimeById((prev) => ({ ...prev, [id]: { success: 0, failed: 0, uptime: 100 } }));
                setLatestJobById((prev) => ({ ...prev, [id]: null }));
              });
            }
          }
        }
      );
    };

    void run();
    return () => {
      cancelled = true;
    };
  }, [currentPageItems, isSdkLoading, latestJobById, sdk, statsById, status]);

  useEffect(() => {
    if (page >= totalPages && totalPages > 0) {
      setPage(totalPages - 1);
    }
  }, [page, totalPages]);

  useEffect(() => {
    setPage(0);
  }, [searchQuery]);

  useEffect(() => {
    const items = sortedTransformations.slice(
      safePage * PAGE_SIZE,
      safePage * PAGE_SIZE + PAGE_SIZE
    );
    if (items.length === 0) return;
    let cancelled = false;
    const nextCounts: Record<string, ParsedInsightCounts> = {};
    for (const t of items) {
      const q = t.query?.trim();
      if (!q) continue;
      const id = String(t.id);
      nextCounts[id] = getParsedInsightCounts(parseTransformationQuery(q), q);
    }
    if (!cancelled) {
      setCountsById(nextCounts);
    }
    return () => {
      cancelled = true;
    };
  }, [safePage, sortedTransformations]);

  useEffect(() => {
    if (status !== "success" || isSdkLoading || !sdk) return;
    const items = sortedTransformations.slice(
      safePage * PAGE_SIZE,
      safePage * PAGE_SIZE + PAGE_SIZE
    );
    if (items.length === 0) return;
    if (items.some((t) => !(String(t.id) in latestJobById))) return;

    let cancelled = false;

    const run = async () => {
      type MetricRow = { reads: number; writes: number; noops: number; rateLimit429: number };
      const pageMetrics: Record<string, MetricRow> = {};

      for (const t of items) {
        if (cancelled) return;
        const id = String(t.id);
        const jobId = latestJobById[id];
        if (jobId == null) {
          pageMetrics[id] = { reads: 0, writes: 0, noops: 0, rateLimit429: 0 };
          continue;
        }
        if (metricsJobFetchedRef.current[id] === jobId) {
          continue;
        }
        try {
          const metricsRes = (await cachedTransformationJobMetrics(
            sdk,
            jobId
          )) as { data?: { items?: JobMetricItem[] } };
          const metricItems = metricsRes.data?.items ?? [];
          const agg = aggregateJobMetrics(metricItems);
          if (!cancelled) {
            metricsJobFetchedRef.current[id] = jobId;
            pageMetrics[id] = agg;
          }
        } catch {
          if (!cancelled) {
            metricsJobFetchedRef.current[id] = jobId;
            pageMetrics[id] = { reads: 0, writes: 0, noops: 0, rateLimit429: 0 };
          }
        }
      }

      if (!cancelled && Object.keys(pageMetrics).length > 0) {
        startTransition(() => {
          setMetricsById((prev) => ({ ...prev, ...pageMetrics }));
        });
      }
    };

    void run();
    return () => {
      cancelled = true;
    };
  }, [safePage, sortedTransformations, isSdkLoading, sdk, latestJobById, status]);

  const toggleSort = (nextKey: TableSortKey) => {
    if (sortKey === nextKey) {
      setSortDesc((prev) => !prev);
    } else {
      setSortKey(nextKey);
      setSortDesc(nextKey !== "name");
    }
    setPage(0);
  };

  useEffect(() => {
    if (clearSelectionRef) {
      clearSelectionRef.current = () => setSelectedId(null);
      return () => {
        clearSelectionRef.current = null;
      };
    }
  }, [clearSelectionRef]);

  const parsedInsight = useMemo<ParsedInsight>(() => {
    const query = selectedTransformation?.query?.trim();
    return parseTransformationQuery(query ?? "");
  }, [selectedTransformation]);

  const cteInfo = useMemo(() => {
    const raw = selectedTransformation?.query ?? "";
    const query = stripSqlCommentLines(raw);
    const afterBlock = stripLeadingBlockComments(query.trimStart());
    const trimmed = afterBlock;
    const lower = trimmed.toLowerCase();
    if (!lower.startsWith("with")) {
      return { names: [] as string[], cteSection: "", bodies: {} as Record<string, string> };
    }
    let index = lower.startsWith("with") ? 4 : 0;
    const names: string[] = [];
    const bodies: Record<string, string> = {};
    const skipWhitespaceAndComments = () => {
      while (index < trimmed.length) {
        if (/\s/.test(trimmed[index])) {
          index += 1;
          continue;
        }
        if (trimmed[index] === "-" && trimmed[index + 1] === "-") {
          index += 2;
          while (index < trimmed.length && trimmed[index] !== "\n") index += 1;
          continue;
        }
        if (trimmed[index] === "/" && trimmed[index + 1] === "*") {
          index += 2;
          while (index < trimmed.length && !(trimmed[index] === "*" && trimmed[index + 1] === "/")) {
            index += 1;
          }
          if (trimmed[index] === "*" && trimmed[index + 1] === "/") {
            index += 2;
          }
          continue;
        }
        break;
      }
    };
    const readName = () => {
      skipWhitespaceAndComments();
      if (index >= trimmed.length) return "";
      const quote = trimmed[index];
      if (quote === '"' || quote === "`") {
        index += 1;
        const start = index;
        while (index < trimmed.length && trimmed[index] !== quote) index += 1;
        const value = trimmed.slice(start, index);
        index += 1;
        return value;
      }
      const start = index;
      while (index < trimmed.length && /[a-zA-Z0-9_.]/.test(trimmed[index])) index += 1;
      return trimmed.slice(start, index);
    };
    const expectAs = () => {
      skipWhitespaceAndComments();
      if (trimmed.slice(index, index + 2).toLowerCase() === "as") {
        index += 2;
      }
    };
    const skipToOpenParen = () => {
      while (index < trimmed.length && trimmed[index] !== "(") {
        if (trimmed[index] === "-" && trimmed[index + 1] === "-") {
          index += 2;
          while (index < trimmed.length && trimmed[index] !== "\n") index += 1;
          continue;
        }
        if (trimmed[index] === "/" && trimmed[index + 1] === "*") {
          index += 2;
          while (index < trimmed.length && !(trimmed[index] === "*" && trimmed[index + 1] === "/")) {
            index += 1;
          }
          if (trimmed[index] === "*" && trimmed[index + 1] === "/") {
            index += 2;
          }
          continue;
        }
        index += 1;
      }
      if (trimmed[index] === "(") index += 1;
    };
    const skipBalanced = () => {
      let depth = 1;
      let inSingle = false;
      let inDouble = false;
      let inBacktick = false;
      let escapeNext = false;
      const bodyStart = index;
      while (index < trimmed.length && depth > 0) {
        const ch = trimmed[index];
        if (escapeNext) {
          escapeNext = false;
          index += 1;
          continue;
        }
        if (!inSingle && !inDouble && !inBacktick) {
          if (ch === "-" && trimmed[index + 1] === "-") {
            index += 2;
            while (index < trimmed.length && trimmed[index] !== "\n") index += 1;
            continue;
          }
          if (ch === "/" && trimmed[index + 1] === "*") {
            index += 2;
            while (
              index < trimmed.length &&
              !(trimmed[index] === "*" && trimmed[index + 1] === "/")
            ) {
              index += 1;
            }
            if (trimmed[index] === "*" && trimmed[index + 1] === "/") {
              index += 2;
            }
            continue;
          }
        }
        if (ch === "\\") {
          escapeNext = true;
          index += 1;
          continue;
        }
        if (!inDouble && !inBacktick && ch === "'" && !escapeNext) {
          inSingle = !inSingle;
          index += 1;
          continue;
        }
        if (!inSingle && !inBacktick && ch === '"' && !escapeNext) {
          inDouble = !inDouble;
          index += 1;
          continue;
        }
        if (!inSingle && !inDouble && ch === "`" && !escapeNext) {
          inBacktick = !inBacktick;
          index += 1;
          continue;
        }
        if (!inSingle && !inDouble && !inBacktick) {
          if (ch === "(") depth += 1;
          if (ch === ")") depth -= 1;
        }
        index += 1;
      }
      const bodyEnd = Math.max(bodyStart, index - 1);
      return trimmed.slice(bodyStart, bodyEnd).trim();
    };
    while (index < trimmed.length) {
      const name = readName();
      if (!name) break;
      names.push(name);
      expectAs();
      skipToOpenParen();
      const body = skipBalanced();
      if (body) {
        bodies[name] = body;
      }
      while (index < trimmed.length && /\s/.test(trimmed[index])) index += 1;
      if (trimmed[index] === ",") {
        index += 1;
        continue;
      }
      break;
    }
    const cteSection = trimmed.slice(0, index).trim();
    return { names: Array.from(new Set(names)), cteSection, bodies };
  }, [selectedTransformation]);

  useEffect(() => {
    if (isSdkLoading) return;
    if (!selectedTransformation) return;
    if (cteInfo.names.length === 0 || !cteInfo.cteSection) {
      setCtePreviews({});
      return;
    }
    let cancelled = false;
    setCteQueryExpanded(new Set());
    // Pre-populate all CTEs as "idle" (waiting in queue) with their queries so we can show them
    const initial: Record<string, { status: LoadState; rows: Array<Record<string, unknown>>; query?: string; fullQuery?: string }> = {};
    for (const name of cteInfo.names) {
      const { query: previewQuery, displayQuery, isIndependent } = buildCtePreviewQuery(
        cteInfo.names,
        cteInfo.bodies,
        name
      );
      initial[name] = {
        status: "idle",
        rows: [],
        query: displayQuery,
        ...(isIndependent ? {} : { fullQuery: previewQuery }),
      };
    }
    setCtePreviews(initial);
    const loadPreviews = async () => {
      for (const name of cteInfo.names) {
        // Run each CTE preview serially, starting with the first
        if (cancelled) return;
        const { query: previewQuery, displayQuery, isIndependent } = buildCtePreviewQuery(
          cteInfo.names,
          cteInfo.bodies,
          name
        );
        const fullQuery = isIndependent ? undefined : previewQuery;
        setCtePreviews((prev) => ({
          ...prev,
          [name]: { status: "loading", rows: [], query: displayQuery, ...(fullQuery != null && { fullQuery }) },
        }));
        const startMs = performance.now();
        try {
          const response = (await sdk.post(
            `/api/v1/projects/${sdk.project}/transformations/query/run`,
            {
              data: {
                limit: 10,
                sourceLimit: 10,
                query: previewQuery,
                convertToString: true,
              },
            }
          )) as {
            data?: {
              results?: { items?: Array<Record<string, unknown>> };
              items?: Array<Record<string, unknown>>;
              rows?: Array<Record<string, unknown>>;
            };
          };
          const rows =
            response.data?.results?.items ??
            response.data?.items ??
            response.data?.rows ??
            [];
          const durationMs = Math.round(performance.now() - startMs);
          if (!cancelled) {
            setCtePreviews((prev) => ({
              ...prev,
              [name]: {
                status: "success",
                rows,
                query: displayQuery,
                durationMs,
                ...(fullQuery != null && { fullQuery }),
              },
            }));
          }
        } catch (error) {
          const durationMs = Math.round(performance.now() - startMs);
          if (!cancelled) {
            setCtePreviews((prev) => ({
              ...prev,
              [name]: {
                status: "error",
                rows: [],
                error: error instanceof Error ? error.message : "Failed to preview CTE.",
                query: displayQuery,
                durationMs,
                ...(fullQuery != null && { fullQuery }),
              },
            }));
          }
        }
      }
    };
    loadPreviews();
    return () => {
      cancelled = true;
    };
  }, [cteInfo, isSdkLoading, sdk, selectedTransformation]);

  return (
    <section className="flex flex-col gap-4">
      <Card>
        <CardHeader>
          <CardTitle>{t("transformations.title")}</CardTitle>
          <CardDescription>{t("transformations.help.subtitle")}</CardDescription>
        </CardHeader>
        <CardContent>
          {status === "loading" ? (
            <div className="min-h-[240px] space-y-2 text-sm text-slate-600">
              <div>{t("transformations.list.loading")}</div>
              {loadingMessage ? (
                <div className="rounded-md border border-sky-200 bg-sky-50 px-3 py-2 text-xs text-sky-900">
                  {loadingMessage}
                </div>
              ) : null}
            </div>
          ) : null}
          {status === "error" ? (
            <ApiError message={errorMessage ?? t("transformations.list.error")} />
          ) : null}
          {status === "success" ? (
            transformations.length === 0 ? (
              <div className="text-sm text-slate-600">{t("transformations.list.empty")}</div>
            ) : (
              <div className="space-y-4">
                <div className="flex flex-wrap items-end gap-3">
                  {!selectedId ? (
                    <label
                      htmlFor="transformations-list-search"
                      className="flex min-w-[12rem] flex-1 flex-col gap-1.5 text-sm text-slate-700"
                    >
                      {t("transformations.list.filterLabel")}
                      <input
                        id="transformations-list-search"
                        type="search"
                        placeholder={t("transformations.list.searchPlaceholder")}
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        autoComplete="off"
                        className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-800 placeholder:text-slate-400 focus:border-slate-400 focus:outline-none focus:ring-2 focus:ring-slate-400"
                      />
                      {filterTooShort ? (
                        <span className="text-xs text-slate-500">
                          Type at least {TRANSFORMATIONS_FILTER_MIN_CHARS} characters to filter.
                        </span>
                      ) : null}
                      {filterPendingDebounce ? (
                        <span className="text-xs text-slate-500">Applying filter...</span>
                      ) : null}
                    </label>
                  ) : (
                    <div className="min-w-0 flex-1" />
                  )}
                  <div className="flex shrink-0 items-end gap-2">
                    {selectedId ? (
                      <button
                        type="button"
                        className="h-9 shrink-0 rounded-md border border-slate-200 bg-white px-3 text-sm font-medium text-slate-700 hover:bg-slate-50"
                        onClick={() => setSelectedId(null)}
                      >
                        {t("transformations.list.backToList")}
                      </button>
                    ) : null}
                    <button
                      type="button"
                      onClick={() => setShowHelp(true)}
                      className="h-9 shrink-0 rounded-md bg-blue-600 px-3 text-sm font-medium text-white hover:bg-blue-700"
                    >
                      {t("shared.help.button")}
                    </button>
                  </div>
                </div>
                <div className="rounded-md border border-slate-200 bg-white p-2">
                  {!selectedId ? (
                    <div className="max-h-[620px] overflow-auto">
                      <table className="w-full border-collapse text-left text-xs">
                        <thead className="sticky top-0 bg-slate-50 text-slate-600">
                          <tr>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.name")}>
                              <button
                                type="button"
                                className="flex items-center gap-1 text-left hover:text-slate-900"
                                onClick={() => toggleSort("name")}
                              >
                                {t("transformations.list.name")}
                                {sortKey === "name" ? (sortDesc ? " ↓" : " ↑") : ""}
                              </button>
                            </th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.runs24h")}>
                              <button
                                type="button"
                                className="flex items-center gap-1 text-left hover:text-slate-900"
                                onClick={() => toggleSort("count")}
                              >
                                {t("transformations.list.runs24h")}
                                {sortKey === "count" ? (sortDesc ? " ↓" : " ↑") : ""}
                              </button>
                            </th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.lastRun")}>
                              <button
                                type="button"
                                className="flex items-center gap-1 text-left hover:text-slate-900"
                                onClick={() => toggleSort("lastRun")}
                              >
                                {t("transformations.list.lastRun")}
                                {sortKey === "lastRun" ? (sortDesc ? " ↓" : " ↑") : ""}
                              </button>
                            </th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.totalTime")}>
                              <button
                                type="button"
                                className="flex items-center gap-1 text-left hover:text-slate-900"
                                onClick={() => toggleSort("totalMs")}
                              >
                                {t("transformations.list.totalTime")}
                                {sortKey === "totalMs" ? (sortDesc ? " ↓" : " ↑") : ""}
                              </button>
                            </th>
                            <th
                              className="px-2 py-2 font-medium"
                              title="Uptime % = successful / (successful + failed) over the last 24h."
                            >
                              Uptime 24h
                            </th>
                            <th className="whitespace-nowrap px-2 py-2 font-medium" title="Successful jobs over the last 24h.">
                              <button
                                type="button"
                                className="flex items-center gap-1 text-left hover:text-slate-900"
                                onClick={() => toggleSort("success")}
                              >
                                S
                                {sortKey === "success" ? (sortDesc ? " ↓" : " ↑") : ""}
                              </button>
                            </th>
                            <th className="whitespace-nowrap px-2 py-2 font-medium" title="Failed jobs over the last 24h.">
                              <button
                                type="button"
                                className="flex items-center gap-1 text-left hover:text-slate-900"
                                onClick={() => toggleSort("failed")}
                              >
                                F
                                {sortKey === "failed" ? (sortDesc ? " ↓" : " ↑") : ""}
                              </button>
                            </th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.reads")}>
                              {t("transformations.list.reads")}
                            </th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.writes")}>
                              {t("transformations.list.writes")}
                            </th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.noops")}>
                              {t("transformations.list.noops")}
                            </th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.rateLimit429")}>
                              {t("transformations.list.rateLimit429")}
                            </th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.err")}>Err</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.stmt")}>Stmt</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.tok")}>Tok</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.tbl")}>Tbl</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.cte")}>CTE</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.dm")}>DM</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.node")}>Node</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.unit")}>Unit</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.like")}>Like</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.rlike")}>Rlike</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.reg")}>Reg</th>
                            <th className="px-2 py-2 font-medium" title={t("transformations.list.columnHelp.nest")}>Nest</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100">
                          {currentPageItems.map((item) => {
                            const id = String(item.id);
                            const label = item.name ?? id;
                            const stats = statsById[id];
                            const statsReady = stats !== undefined;
                            const counts = countsById[id];
                            const hasQuery = Boolean(item.query?.trim());
                            const countsReady = !hasQuery || counts !== undefined;
                            return (
                              <tr
                                key={id}
                                className="cursor-pointer hover:bg-slate-50"
                                onClick={() => setSelectedId(id)}
                              >
                                <td className={`px-2 py-2 text-sm font-medium${pc}`}>{label}</td>
                                <td className="px-2 py-2">
                                  {!statsReady ? (
                                    <CellSpinner />
                                  ) : (
                                    stats.count
                                  )}
                                </td>
                                <td className="px-2 py-2">
                                  {!statsReady ? (
                                    <CellSpinner />
                                  ) : stats.lastRun ? (
                                    formatIso(stats.lastRun)
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2">
                                  {!statsReady ? (
                                    <CellSpinner />
                                  ) : stats.totalMs > 0 ? (
                                    formatDuration(stats.totalMs)
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!statsReady ? (
                                    <CellSpinner />
                                  ) : (uptimeById[id]?.success ?? 0) + (uptimeById[id]?.failed ?? 0) > 0 ? (
                                    <span
                                      className={`font-semibold ${(uptimeById[id]?.failed ?? 0) > 0 ? "text-red-700" : "text-slate-900"}`}
                                    >
                                      {(uptimeById[id]?.uptime ?? 0).toFixed(1)}%
                                    </span>
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="whitespace-nowrap px-2 py-2 tabular-nums">
                                  {!statsReady ? (
                                    <CellSpinner />
                                  ) : (
                                    <span className="text-xs text-slate-900">{uptimeById[id]?.success ?? 0}</span>
                                  )}
                                </td>
                                <td className="whitespace-nowrap px-2 py-2 tabular-nums">
                                  {!statsReady ? (
                                    <CellSpinner />
                                  ) : (
                                    <span
                                      className={`text-xs ${(uptimeById[id]?.failed ?? 0) > 0 ? "text-red-700" : "text-slate-900"}`}
                                    >
                                      {uptimeById[id]?.failed ?? 0}
                                    </span>
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {metricsById[id] === undefined ? (
                                    <CellSpinner />
                                  ) : (
                                    formatPrettyNumber(metricsById[id].reads)
                                  )}
                                </td>
                                {(() => {
                                  const m = metricsById[id];
                                  const noop_eq_writes = m != null && m.writes > 0 && m.noops === m.writes;
                                  const redCls = noop_eq_writes ? " bg-red-100 text-red-800" : "";
                                  return (
                                    <>
                                      <td className={`px-2 py-2 tabular-nums${redCls}`}>
                                        {m === undefined ? <CellSpinner /> : formatPrettyNumber(m.writes)}
                                      </td>
                                      <td className={`px-2 py-2 tabular-nums${redCls}`}>
                                        {m === undefined ? <CellSpinner /> : formatPrettyNumber(m.noops)}
                                      </td>
                                    </>
                                  );
                                })()}
                                <td className="px-2 py-2 tabular-nums">
                                  {metricsById[id] === undefined ? (
                                    <CellSpinner />
                                  ) : (
                                    formatPrettyNumber(metricsById[id].rateLimit429)
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.errors
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.statements
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.tokens
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.tables
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.cteCount
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.dataModelRefs
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.nodeReferences
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.unitLookups
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.like
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.rlike
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.regexp
                                  ) : (
                                    "—"
                                  )}
                                </td>
                                <td className="px-2 py-2 tabular-nums">
                                  {!countsReady ? (
                                    <CellSpinner />
                                  ) : counts ? (
                                    counts.nestedCalls
                                  ) : (
                                    "—"
                                  )}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                      <div className="flex items-center justify-between gap-2 border-t border-slate-200 bg-slate-50 px-2 py-2 text-xs text-slate-600">
                        <span>
                          Page {safePage + 1} of {totalPages}
                          {sortedTransformations.length > 0 ? (
                            <> ({sortedTransformations.length} total)</>
                          ) : null}
                        </span>
                        <div className="flex gap-1">
                          <button
                            type="button"
                            className="rounded border border-slate-200 px-2 py-1 hover:bg-slate-100 disabled:opacity-50"
                            disabled={safePage <= 0}
                            onClick={() => setPage((p) => Math.max(0, p - 1))}
                          >
                            Previous
                          </button>
                          <button
                            type="button"
                            className="rounded border border-slate-200 px-2 py-1 hover:bg-slate-100 disabled:opacity-50"
                            disabled={safePage >= totalPages - 1}
                            onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                          >
                            Next
                          </button>
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="flex items-center gap-2 text-sm text-slate-600">
                      <span>
                        {t("transformations.list.selected")}{" "}
                        <span className={`font-semibold text-slate-900${pc}`}>
                          {selectedTransformation?.name ?? selectedTransformation?.id ?? "—"}
                        </span>
                      </span>
                      {selectedId ? (
                        <a
                          href={getTransformationPreviewUrl(sdk.project, selectedId)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-slate-400 hover:text-slate-600"
                          title={t("transformations.list.openInFusion")}
                        >
                          <ExternalLinkIcon />
                        </a>
                      ) : null}
                    </div>
                  )}
                </div>
                {selectedId ? (
                  <>
                  <div className="rounded-md border border-slate-200 bg-white p-3">
                    <div className="text-xs font-semibold uppercase tracking-wide text-slate-400">
                      {t("transformations.list.query")}
                    </div>
                    <textarea
                      className="mt-2 h-48 w-full rounded-md border border-slate-200 bg-slate-50 p-2 text-xs text-slate-800"
                      value={selectedTransformation?.query ?? ""}
                      readOnly
                    />
                  </div>
                  <div className="rounded-md border border-slate-200 bg-white p-3">
                    <div className="text-xs font-semibold uppercase tracking-wide text-slate-400">
                      Parser insights
                    </div>
                    {parsedInsight.error ? (
                      <div className="mt-2 text-xs text-red-600">{parsedInsight.error}</div>
                    ) : null}
                    {parsedInsight.errors.length > 0 ? (
                      <div className="mt-2 space-y-2 text-xs text-red-700">
                        <div className="font-semibold">Validation errors</div>
                        <ul className="list-disc space-y-1 pl-4">
                          {parsedInsight.errors.map((entry, index) => (
                            <li key={`${entry.message ?? "error"}-${index}`}>
                              {entry.message ?? "Invalid SQL"}
                              {entry.startLine != null && entry.startCol != null
                                ? ` (L${entry.startLine}, C${entry.startCol})`
                                : ""}
                            </li>
                          ))}
                        </ul>
                      </div>
                    ) : (
                      <div className="mt-2 space-y-2 text-xs text-slate-700">
                        <div>
                          Statements: <span className="font-semibold">{parsedInsight.statementCount}</span>
                        </div>
                        <div>
                          Tokens: <span className="font-semibold">{parsedInsight.tokenCount}</span>
                        </div>
                        <div>
                          Tables:{" "}
                          <span className="font-semibold">{parsedInsight.tables.length}</span>
                        </div>
                        <div>
                          LIKE / RLIKE / REGEXP:{" "}
                          <span className="font-semibold">
                            {parsedInsight.operatorUsage.like} / {parsedInsight.operatorUsage.rlike} /{" "}
                            {parsedInsight.operatorUsage.regexp}
                          </span>
                        </div>
                      <div className="mt-2 text-xs font-semibold uppercase tracking-wide text-slate-400">
                        Data model layer ({parsedInsight.dataModelRefs.length})
                      </div>
                      {parsedInsight.dataModelRefs.length > 0 ? (
                        <div className="mt-1 space-y-2">
                          {parsedInsight.dataModelRefs.map((entry, index) => (
                            <div key={`dm-interaction-${index}`} className="rounded-md bg-slate-50 p-2">
                              <div>
                                <span className="font-semibold">Source:</span>{" "}
                                {entry.source}
                                {entry.unscoped ? " (unscoped)" : ""}
                              </div>
                              <div>
                                <span className="font-semibold">Space:</span>{" "}
                                {entry.space ?? "—"}
                              </div>
                              <div>
                                <span className="font-semibold">External ID:</span>{" "}
                                {entry.externalId ?? "—"}
                              </div>
                              <div>
                                <span className="font-semibold">Version:</span>{" "}
                                {entry.version ?? "—"}
                              </div>
                              <div>
                                <span className="font-semibold">Type external ID:</span>{" "}
                                {entry.typeExternalId ?? "—"}
                              </div>
                              {entry.relationshipProperty ? (
                                <div>
                                  <span className="font-semibold">Relationship property:</span>{" "}
                                  {entry.relationshipProperty}
                                </div>
                              ) : null}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-slate-500">
                          No data model layer usage (cdf_data_models, cdf_nodes, cdf_edges,
                          _cdf_datamodels, is_new).
                        </div>
                      )}
                      <div className="mt-3 text-xs font-semibold uppercase tracking-wide text-slate-400">
                        node_reference(...) ({parsedInsight.nodeReferences.length})
                      </div>
                      {parsedInsight.nodeReferences.length > 0 ? (
                        <div className="mt-1 space-y-2">
                          {parsedInsight.nodeReferences.map((entry, index) => (
                            <div key={`node-ref-${index}`} className="rounded-md bg-slate-50 p-2">
                              <div>
                                <span className="font-semibold">Space:</span>{" "}
                                {entry.space ?? "—"}
                              </div>
                              <div>
                                <span className="font-semibold">External ID:</span>{" "}
                                {entry.externalId ?? "—"}
                              </div>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-slate-500">No node_reference entries.</div>
                      )}
                      <div className="mt-3 text-xs font-semibold uppercase tracking-wide text-slate-400">
                        try_get_unit(...) ({parsedInsight.unitLookups.length})
                      </div>
                      {parsedInsight.unitLookups.length > 0 ? (
                        <div className="mt-1 space-y-2">
                          {parsedInsight.unitLookups.map((entry, index) => (
                            <div key={`unit-lookup-${index}`} className="rounded-md bg-slate-50 p-2">
                              <div>
                                <span className="font-semibold">Alias:</span>{" "}
                                {entry.alias ?? "—"}
                              </div>
                              <div>
                                <span className="font-semibold">Quantity:</span>{" "}
                                {entry.quantity ?? "—"}
                              </div>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-slate-500">No try_get_unit entries.</div>
                      )}
                      <div className="mt-3 text-xs font-semibold uppercase tracking-wide text-slate-400">
                        Nested calls ({parsedInsight.nestedCalls.length})
                      </div>
                      {parsedInsight.nestedCalls.length > 0 ? (
                        <ul className="mt-1 list-disc space-y-1 pl-4">
                          {parsedInsight.nestedCalls.map((entry, index) => (
                            <li key={`${entry.outer}-${entry.inner}-${index}`}>
                              {entry.outer} → {entry.inner}
                            </li>
                          ))}
                        </ul>
                      ) : (
                        <div className="text-slate-500">No nested calls detected.</div>
                      )}
                        <div className="mt-3 text-xs font-semibold uppercase tracking-wide text-slate-400">
                          Tables
                        </div>
                        {parsedInsight.tables.length > 0 ? (
                          <ul className="mt-1 list-disc space-y-1 pl-4">
                            {parsedInsight.tables.map((table) => (
                              <li key={table}>{table}</li>
                            ))}
                          </ul>
                        ) : (
                          <div className="text-slate-500">No tables detected.</div>
                        )}
                      </div>
                    )}
                  </div>
                    <div className="rounded-md border border-slate-200 bg-white p-3">
                      <div className="flex items-center justify-between gap-2">
                        <div className="text-xs font-semibold uppercase tracking-wide text-slate-400">
                          CTE previews
                        </div>
                        <div className="text-xs text-slate-500">
                          {cteInfo.names.length} CTEs
                        </div>
                      </div>
                      {cteInfo.names.length === 0 ? (
                        <div className="mt-2 text-sm text-slate-600">No CTEs detected.</div>
                      ) : (
                        <div className="mt-2 space-y-3">
                          {(() => {
                            const durationByCte = cteInfo.names.map(
                              (n) => ctePreviews[n]?.durationMs ?? 0
                            );
                            const totalMs = durationByCte.reduce((a, b) => a + b, 0);
                            const anyLoading = cteInfo.names.some(
                              (n) =>
                                ctePreviews[n]?.status === "loading" ||
                                ctePreviews[n]?.status === "idle"
                            );
                            return cteInfo.names.length > 0 ? (
                              <div className="mb-3 rounded-md border border-slate-200 bg-slate-50 p-2">
                                <div className="mb-1 flex items-center gap-2 text-[11px] font-medium text-slate-500">
                                  CTE execution time
                                  {anyLoading ? (
                                    <span className="flex items-center gap-1.5 text-amber-600">
                                      <span
                                        className="inline-block h-3 w-3 animate-spin rounded-full border-2 border-amber-400 border-t-amber-600"
                                        aria-hidden
                                      />
                                      {t("transformations.cte.timelineLoading")}
                                    </span>
                                  ) : null}
                                </div>
                                <div className="flex h-6 w-full min-w-0 gap-px rounded">
                                  {cteInfo.names.map((name, idx) => {
                                    const preview = ctePreviews[name];
                                    const ms = preview?.durationMs ?? 0;
                                    const flexBasis = totalMs > 0 ? ms : 1;
                                    const colors = [
                                      "bg-blue-400",
                                      "bg-cyan-400",
                                      "bg-teal-400",
                                      "bg-emerald-400",
                                      "bg-amber-400",
                                      "bg-orange-400",
                                    ];
                                    const color = colors[idx % colors.length];
                                    return (
                                      <div
                                        key={name}
                                        className={`flex min-w-0 shrink-0 items-center justify-center overflow-hidden rounded-sm ${color} text-[10px] font-medium text-slate-800`}
                                        style={{ flex: `${flexBasis} 1 0` }}
                                        title={`${name}: ${formatDurationShort(ms)}`}
                                      >
                                        {ms > 0 ? (
                                          <span className="truncate px-0.5">{name}</span>
                                        ) : null}
                                      </div>
                                    );
                                  })}
                                </div>
                                <div className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-[10px] text-slate-600">
                                  {cteInfo.names.map((name) => {
                                    const preview = ctePreviews[name];
                                    const ms = preview?.durationMs;
                                    return (
                                      <span key={name}>
                                        {name}: {formatDurationShort(ms)}
                                      </span>
                                    );
                                  })}
                                </div>
                              </div>
                            ) : null;
                          })()}
                          {cteInfo.names.map((name) => {
                            const preview = ctePreviews[name];
                            const rows = preview?.rows ?? [];
                            const columns = rows.length > 0 ? Object.keys(rows[0] ?? {}) : [];
                            const showFull = cteQueryExpanded.has(name);
                            const queryToShow =
                              preview && showFull && preview.fullQuery
                                ? preview.fullQuery
                                : preview?.query;
                            const canExpand = Boolean(preview?.fullQuery);
                            const toggleExpand = () => {
                              setCteQueryExpanded((prev) => {
                                const next = new Set(prev);
                                if (next.has(name)) next.delete(name);
                                else next.add(name);
                                return next;
                              });
                            };
                            return (
                              <div key={name} className="rounded-md border border-slate-200 bg-slate-50 p-3">
                                <div className="flex items-center justify-between gap-2">
                                  <div className="text-sm font-semibold text-slate-800">{name}</div>
                                  {preview?.durationMs != null ? (
                                    <span className="text-xs text-slate-500 tabular-nums">
                                      {formatDurationShort(preview.durationMs)}
                                    </span>
                                  ) : null}
                                </div>
                                {preview?.status === "idle" ? (
                                  <div className="mt-2 space-y-2">
                                    {queryToShow ? (
                                      <pre className="whitespace-pre-wrap rounded-md border border-slate-200 bg-white p-2 text-[11px] text-slate-700">
                                        {queryToShow}
                                      </pre>
                                    ) : null}
                                    {canExpand ? (
                                      <button
                                        type="button"
                                        className="text-xs text-slate-600 underline hover:text-slate-800"
                                        onClick={toggleExpand}
                                      >
                                        {showFull ? "Show short query" : "Show full query"}
                                      </button>
                                    ) : null}
                                    <div className="text-xs text-slate-600">
                                      {t("transformations.cte.awaitingPreviews")}
                                    </div>
                                  </div>
                                ) : preview?.status === "loading" ? (
                                  <div className="mt-2 space-y-2">
                                    {queryToShow ? (
                                      <pre className="whitespace-pre-wrap rounded-md border border-slate-200 bg-white p-2 text-[11px] text-slate-700">
                                        {queryToShow}
                                      </pre>
                                    ) : null}
                                    {canExpand ? (
                                      <button
                                        type="button"
                                        className="text-xs text-slate-600 underline hover:text-slate-800"
                                        onClick={toggleExpand}
                                      >
                                        {showFull ? "Show short query" : "Show full query"}
                                      </button>
                                    ) : null}
                                    <div className="text-xs text-slate-600">
                                      {t("transformations.cte.loadingPreview")}
                                    </div>
                                  </div>
                                ) : preview?.status === "error" ? (
                                  <div className="mt-2 space-y-2">
                                    {queryToShow ? (
                                      <pre className="whitespace-pre-wrap rounded-md border border-slate-200 bg-white p-2 text-[11px] text-slate-700">
                                        {queryToShow}
                                      </pre>
                                    ) : null}
                                    {canExpand ? (
                                      <button
                                        type="button"
                                        className="text-xs text-slate-600 underline hover:text-slate-800"
                                        onClick={toggleExpand}
                                      >
                                        {showFull ? "Show short query" : "Show full query"}
                                      </button>
                                    ) : null}
                                    <ApiError message={preview.error ?? "Failed to preview CTE."} />
                                  </div>
                                ) : preview?.status === "success" && rows.length === 0 ? (
                                  <div className="mt-2 space-y-2">
                                    {queryToShow ? (
                                      <pre className="whitespace-pre-wrap rounded-md border border-slate-200 bg-white p-2 text-[11px] text-slate-700">
                                        {queryToShow}
                                      </pre>
                                    ) : null}
                                    {canExpand ? (
                                      <button
                                        type="button"
                                        className="text-xs text-slate-600 underline hover:text-slate-800"
                                        onClick={toggleExpand}
                                      >
                                        {showFull ? "Show short query" : "Show full query"}
                                      </button>
                                    ) : null}
                                    <div className="text-xs text-slate-600">
                                      {t("transformations.cte.noRowsReturned")}
                                    </div>
                                  </div>
                                ) : (
                                  <div className="mt-2 space-y-2">
                                    {queryToShow ? (
                                      <pre className="whitespace-pre-wrap rounded-md border border-slate-200 bg-white p-2 text-[11px] text-slate-700">
                                        {queryToShow}
                                      </pre>
                                    ) : null}
                                    {canExpand ? (
                                      <button
                                        type="button"
                                        className="text-xs text-slate-600 underline hover:text-slate-800"
                                        onClick={toggleExpand}
                                      >
                                        {showFull ? "Show short query" : "Show full query"}
                                      </button>
                                    ) : null}
                                  <div className="mt-2 overflow-auto rounded-md border border-slate-200 bg-white">
                                    <table className="min-w-full border-collapse text-left text-xs">
                                      <thead className="bg-slate-50 text-slate-600">
                                        <tr>
                                          {columns.map((col) => (
                                            <th key={col} className="px-2 py-1 font-medium">
                                              {col}
                                            </th>
                                          ))}
                                        </tr>
                                      </thead>
                                      <tbody className="divide-y divide-slate-200">
                                        {rows.map((row, index) => (
                                          <tr key={`${name}-row-${index}`} className="text-slate-700">
                                            {columns.map((col) => (
                                              <td key={`${name}-${index}-${col}`} className="px-2 py-1">
                                                {String(row[col] ?? "")}
                                              </td>
                                            ))}
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>
                                  </div>
                                )}
                              </div>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  </>
                ) : null}
              </div>
            )
          ) : null}
        </CardContent>
      </Card>
      <TransformationsHelpModal
        open={showHelp}
        onClose={() => setShowHelp(false)}
        subView="list"
      />
    </section>
  );
}
