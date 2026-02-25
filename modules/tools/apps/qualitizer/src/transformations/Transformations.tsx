import { useEffect, useMemo, useRef, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { useI18n } from "@/shared/i18n";
import { ApiError } from "@/shared/ApiError";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { LoadState } from "@/processing/types";
import { formatDuration, formatIso, toTimestamp } from "@/shared/time-utils";

function formatDurationShort(ms: number | undefined): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms}ms`;
  return formatDuration(ms);
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
  startedTime?: number;
  finishedTime?: number;
  status?: string;
};

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
  const [status, setStatus] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [transformations, setTransformations] = useState<TransformationSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [statsById, setStatsById] = useState<
    Record<string, { count: number; lastRun?: number; totalMs: number }>
  >({});
  const [countsById, setCountsById] = useState<Record<string, ParsedInsightCounts>>({});
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
  type TableSortKey = "name" | "count" | "lastRun" | "totalMs";
  const [sortKey, setSortKey] = useState<TableSortKey>("totalMs");
  const [sortDesc, setSortDesc] = useState(true);
  const PAGE_SIZE = 20;
  const [page, setPage] = useState(0);
  const [searchQuery, setSearchQuery] = useState("");
  const toSelectRef = useRef(transformationToSelect);
  toSelectRef.current = transformationToSelect;

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const loadList = async () => {
      setStatus("loading");
      setErrorMessage(null);
      try {
        const response = (await sdk.get(`/api/v1/projects/${sdk.project}/transformations`, {
          params: { includePublic: "true", limit: "1000" },
        })) as { data?: { items?: TransformationSummary[] } };
        const items = response.data?.items ?? [];
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
          setStatsById({});
          setCountsById({});
          setStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(
            error instanceof Error ? error.message : t("transformations.list.error")
          );
          setStatus("error");
        }
      }
    };
    loadList();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, onTransformationSelected, t]);

  useEffect(() => {
    if (isSdkLoading || transformations.length === 0) return;
    const windowEnd = Date.now();
    const windowStart = windowEnd - 24 * 60 * 60 * 1000;
    let cancelled = false;
    transformations.forEach((transformation) => {
      const id = String(transformation.id);
      sdk
        .get(`/api/v1/projects/${sdk.project}/transformations/jobs`, {
          params: { limit: "1000", transformationId: id },
        })
        .then((jobResponse) => {
          if (cancelled) return;
          const data = (jobResponse as { data?: { items?: TransformationJobSummary[] } }).data;
          const jobs = data?.items ?? [];
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
          setStatsById((prev) => ({ ...prev, [id]: { count, lastRun, totalMs } }));
        })
        .catch(() => {
          if (!cancelled) {
            setStatsById((prev) => ({ ...prev, [id]: { count: 0, totalMs: 0 } }));
          }
        });
    });
    return () => {
      cancelled = true;
    };
  }, [transformations, sdk, isSdkLoading]);

  const selectedTransformation = useMemo(() => {
    if (!selectedId) return null;
    return transformations.find((item) => String(item.id) === selectedId) ?? null;
  }, [transformations, selectedId]);

  const filteredTransformations = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    if (!q) return transformations;
    return transformations.filter((t) => {
      const id = String(t.id).toLowerCase();
      const name = (t.name ?? "").toLowerCase();
      const query = (t.query ?? "").toLowerCase();
      return id.includes(q) || name.includes(q) || query.includes(q);
    });
  }, [transformations, searchQuery]);

  const sortedTransformations = useMemo(() => {
    const items = [...filteredTransformations];
    const getStats = (id: string) => statsById[id] ?? { count: 0, totalMs: 0 };
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
      const aLast = getStats(aId).lastRun ?? 0;
      const bLast = getStats(bId).lastRun ?? 0;
      const diff = bLast - aLast;
      return sortDesc ? diff : -diff;
    });
  }, [filteredTransformations, statsById, sortKey, sortDesc]);

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
    setCountsById({});
    const withQuery = items.filter((t) => t.query?.trim());
    let index = 0;
    const run = () => {
      if (cancelled || index >= withQuery.length) return;
      const t = withQuery[index];
      const id = String(t.id);
      const query = t.query!.trim();
      const counts = getParsedInsightCounts(parseTransformationQuery(query), query);
      setCountsById((prev) => ({ ...prev, [id]: counts }));
      index += 1;
      if (index < withQuery.length) {
        setTimeout(run, 0);
      }
    };
    setTimeout(run, 0);
    return () => {
      cancelled = true;
    };
  }, [safePage, sortedTransformations]);

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
            <div className="text-sm text-slate-600">{t("transformations.list.loading")}</div>
          ) : null}
          {status === "error" ? (
            <ApiError message={errorMessage ?? t("transformations.list.error")} />
          ) : null}
          {status === "success" ? (
            transformations.length === 0 ? (
              <div className="text-sm text-slate-600">{t("transformations.list.empty")}</div>
            ) : (
              <div className="space-y-4">
                <div className="rounded-md border border-slate-200 bg-white p-2">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="text-xs font-semibold uppercase tracking-wide text-slate-400">
                      {t("transformations.title")}
                    </div>
                    <div className="flex items-center gap-2">
                      {!selectedId ? (
                        <input
                          type="search"
                          placeholder={t("transformations.list.searchPlaceholder")}
                          value={searchQuery}
                          onChange={(e) => setSearchQuery(e.target.value)}
                          className="rounded-md border border-slate-200 px-2 py-1.5 text-xs text-slate-800 placeholder:text-slate-400 focus:border-slate-400 focus:outline-none"
                        />
                      ) : null}
                      {selectedId ? (
                        <button
                          type="button"
                          className="rounded-md border border-slate-200 px-2 py-1 text-xs text-slate-700 hover:bg-slate-50"
                          onClick={() => setSelectedId(null)}
                        >
                          {t("transformations.list.backToList")}
                        </button>
                      ) : null}
                      <button
                        type="button"
                        onClick={() => setShowHelp(true)}
                        className="rounded-md bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
                      >
                        {t("shared.help.button")}
                      </button>
                    </div>
                  </div>
                  {!selectedId ? (
                    <div className="mt-2 max-h-[620px] overflow-auto">
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
                                <td className="px-2 py-2 text-sm font-medium">{label}</td>
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
                    <div className="mt-2 flex items-center gap-2 text-sm text-slate-600">
                      <span>
                        {t("transformations.list.selected")}{" "}
                        <span className="font-semibold text-slate-900">
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
                        cdf_data_models(...) ({parsedInsight.dataModelRefs.length})
                      </div>
                      {parsedInsight.dataModelRefs.length > 0 ? (
                        <div className="mt-1 space-y-2">
                          {parsedInsight.dataModelRefs.map((entry, index) => (
                            <div key={`cdf-data-model-${index}`} className="rounded-md bg-slate-50 p-2">
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
                        <div className="text-slate-500">No cdf_data_models references.</div>
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
