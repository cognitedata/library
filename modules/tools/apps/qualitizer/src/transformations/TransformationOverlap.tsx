import { useEffect, useRef, useState } from "react";
import { useI18n } from "@/shared/i18n";
import { useAppSdk } from "@/shared/auth";
import { ApiError } from "@/shared/ApiError";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { LoadState } from "@/processing/types";
import { toTimestamp } from "@/shared/time-utils";
import type { TransformationWithQuery } from "./overlapAnalysis";
import {
  findIdenticalFragments,
  findNearDuplicateFragments,
  type IdenticalFragment,
  type NearDuplicateGroup,
} from "./overlapAnalysis";
import { fetchTransformationsByIds } from "./fetchTransformationsByIds";
import { TransformationsHelpModal } from "./TransformationsHelpModal";

type TransformationSummary = {
  id: number | string;
  name?: string;
  query?: string;
};

type JobSummary = { startedTime?: number; finishedTime?: number; status?: string };

type ProgressState = {
  phase: string;
  current: number;
  total: number;
} | null;

const OVERLAP_SAMPLE_LIMIT = 20;

export function TransformationOverlap() {
  const { t } = useI18n();
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const [status, setStatus] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [itemsWithQuery, setItemsWithQuery] = useState<TransformationWithQuery[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [analyzeAll, setAnalyzeAll] = useState(false);
  const [progress, setProgress] = useState<ProgressState>(null);
  const [identical, setIdentical] = useState<IdenticalFragment[] | null>(null);
  const [nearDuplicates, setNearDuplicates] = useState<NearDuplicateGroup[] | null>(null);
  const [showHelp, setShowHelp] = useState(false);
  const cancelledRef = useRef(false);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const load = async () => {
      setStatus("loading");
      setErrorMessage(null);
      setProgress({ phase: "Loading transformations…", current: 0, total: 0 });
      try {
        const response = (await sdk.get(
          `/api/v1/projects/${sdk.project}/transformations`,
          { params: { includePublic: "true", limit: "1000" } }
        )) as { data?: { items?: TransformationSummary[] } };
        const items = response.data?.items ?? [];
        if (!cancelled) setTotalCount(items.length);
        if (items.length === 0) {
          if (!cancelled) {
            setItemsWithQuery([]);
            setStatus("success");
            setProgress(null);
          }
          return;
        }

        const windowEnd = Date.now();
        const windowStart = windowEnd - 24 * 60 * 60 * 1000;
        const totalMsById: Record<string, number> = {};
        const jobPromises = items.map(async (t) => {
          const id = String(t.id);
          try {
            const jobResponse = await sdk.get(
              `/api/v1/projects/${sdk.project}/transformations/jobs`,
              { params: { limit: "1000", transformationId: id } }
            );
            const data = (jobResponse as { data?: { items?: JobSummary[] } }).data;
            const jobs = data?.items ?? [];
            const recent = jobs.filter((job) => {
              const start = toTimestamp(job.startedTime);
              if (!start) return false;
              return start >= windowStart && start <= windowEnd;
            });
            const totalMs = recent.reduce((acc, job) => {
              const start = toTimestamp(job.startedTime);
              const end = toTimestamp(job.finishedTime);
              if (!start || !end || end < start) return acc;
              return acc + (end - start);
            }, 0);
            return { id, totalMs };
          } catch {
            return { id, totalMs: 0 };
          }
        });

        const jobResults = await Promise.all(jobPromises);
        if (cancelled) return;
        jobResults.forEach((r) => {
          totalMsById[r.id] = r.totalMs;
        });

        let toAnalyze = items;
        if (!analyzeAll && items.length > OVERLAP_SAMPLE_LIMIT) {
          const withMs = items
            .map((t) => ({ t, totalMs: totalMsById[String(t.id)] ?? 0 }))
            .sort((a, b) => b.totalMs - a.totalMs);
          toAnalyze = withMs.slice(0, OVERLAP_SAMPLE_LIMIT).map((x) => x.t);
        }

        setProgress({
          phase: "Fetching transformation queries…",
          current: 0,
          total: toAnalyze.length,
        });
        const project = sdk.project;
        const idsNeedingQuery = [
          ...new Set(
            toAnalyze
              .filter((t) => t.query == null || t.query === "")
              .map((t) => String(t.id))
          ),
        ];
        const queryById =
          idsNeedingQuery.length > 0
            ? await fetchTransformationsByIds(sdk, project, idsNeedingQuery)
            : new Map();
        const withQuery: TransformationWithQuery[] = [];
        for (let i = 0; i < toAnalyze.length; i++) {
          if (cancelled) return;
          if (i > 0 && i % 25 === 0) {
            setProgress({
              phase: "Fetching transformation queries…",
              current: i,
              total: toAnalyze.length,
            });
          }
          const t = toAnalyze[i];
          let query = t.query;
          if (query == null || query === "") {
            query = queryById.get(String(t.id))?.query ?? "";
          }
          if (!query?.trim()) continue;
          withQuery.push({
            id: String(t.id),
            name: t.name,
            query,
          });
        }
        if (!cancelled) {
          setItemsWithQuery(withQuery);
          setStatus("success");
          setProgress({
            phase: "Starting overlap analysis…",
            current: 0,
            total: withQuery.length,
          });
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(
            error instanceof Error ? error.message : "Failed to load transformations."
          );
          setStatus("error");
          setProgress(null);
        }
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, analyzeAll]);

  useEffect(() => {
    if (status !== "success" || itemsWithQuery.length === 0) return;
    cancelledRef.current = false;
    setIdentical(null);
    setNearDuplicates(null);
    const total = itemsWithQuery.length;
    const run = () => {
      setProgress({
        phase: "Finding identical fragments…",
        current: 0,
        total,
      });
      const identicalResult = findIdenticalFragments(
        itemsWithQuery,
        undefined,
        undefined,
        undefined,
        (phase, current, t) => {
          if (cancelledRef.current) return;
          setProgress((prev) => (prev ? { ...prev, phase, current, total: t } : null));
        }
      );
      if (cancelledRef.current) return;
      setIdentical(identicalResult);
      setProgress({
        phase: "Finding near-duplicate fragments…",
        current: 0,
        total,
      });
      const nearResult = findNearDuplicateFragments(
        itemsWithQuery,
        undefined,
        undefined,
        undefined,
        (phase, current, t) => {
          if (cancelledRef.current) return;
          setProgress((prev) => (prev ? { ...prev, phase, current, total: t } : null));
        }
      );
      if (cancelledRef.current) return;
      setNearDuplicates(nearResult);
      setProgress(null);
    };
    const t = setTimeout(run, 50);
    return () => {
      cancelledRef.current = true;
      clearTimeout(t);
    };
  }, [status, itemsWithQuery]);

  return (
    <section className="flex flex-col gap-4">
      <Card>
        <CardHeader>
          <div className="flex items-start justify-between gap-2">
            <div>
              <CardTitle>Transformation overlap</CardTitle>
              <CardDescription>
                Find identical and near-duplicate SQL fragments across transformations to suggest
                reusable functions. Whitespace differences are ignored.
              </CardDescription>
            </div>
            <button
              type="button"
              onClick={() => setShowHelp(true)}
              className="shrink-0 rounded-md bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
            >
              {t("shared.help.button")}
            </button>
          </div>
        </CardHeader>
        <CardContent>
          {status === "loading" ? (
            <div className="text-sm text-slate-600">Loading transformations…</div>
          ) : null}
          {status === "error" ? (
            <ApiError message={errorMessage ?? "Failed to load transformations."} />
          ) : null}
          {progress ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 p-4">
              <div className="text-sm font-medium text-slate-800">{progress.phase}</div>
              {progress.total > 0 ? (
                <div className="mt-1 text-xs text-slate-600">
                  {progress.current} / {progress.total} transformations
                  {progress.total > OVERLAP_SAMPLE_LIMIT
                    ? " — may take several minutes for many transformations."
                    : ""}
                </div>
              ) : null}
              <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-slate-200">
                <div
                  className="h-full bg-slate-500 transition-all duration-300"
                  style={{
                    width:
                      progress.total > 0
                        ? `${Math.round((100 * progress.current) / progress.total)}%`
                        : "30%",
                  }}
                />
              </div>
            </div>
          ) : null}
          {status === "success" && !progress ? (
            itemsWithQuery.length === 0 ? (
              <div className="text-sm text-slate-600">
                No transformations with query text found.
              </div>
            ) : identical === null ? (
              <div className="text-sm text-slate-600">Starting overlap analysis…</div>
            ) : (
              <div className="space-y-6">
                {!analyzeAll && totalCount > OVERLAP_SAMPLE_LIMIT ? (
                  <div
                    className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800"
                    role="alert"
                  >
                    Only {OVERLAP_SAMPLE_LIMIT} out of {totalCount} transformations are analyzed
                    (those with the most run time in the last 24 hours).{" "}
                    <button
                      type="button"
                      className="ml-2 rounded-md border border-amber-300 bg-white px-2 py-1 text-xs font-medium text-amber-800 hover:bg-amber-50"
                      onClick={() => setAnalyzeAll(true)}
                    >
                      Analyze All
                    </button>
                  </div>
                ) : null}
                <div className="text-xs text-slate-500">
                  {itemsWithQuery.length} transformation(s) with query loaded.
                </div>

                <div className="rounded-md border border-slate-200 bg-slate-50/50 p-3">
                  <h3 className="text-sm font-semibold text-slate-800">
                    Identical fragments (same SQL, ignoring whitespace)
                  </h3>
                  <p className="mt-1 text-xs text-slate-600">
                    Substrings that appear in at least two different transformations.
                    Use these to extract shared logic into reusable functions.
                  </p>
                  {(identical ?? []).length === 0 ? (
                    <div className="mt-3 text-xs text-slate-500">
                      No identical fragments found.
                    </div>
                  ) : (
                    <ul className="mt-3 space-y-4">
                      {(identical ?? []).map((frag, idx) => (
                        <IdenticalFragmentCard key={idx} fragment={frag} />
                      ))}
                    </ul>
                  )}
                </div>

                <div className="rounded-md border border-slate-200 bg-slate-50/50 p-3">
                  <h3 className="text-sm font-semibold text-slate-800">
                    Near-duplicate fragments (copy-paste with small edits)
                  </h3>
                  <p className="mt-1 text-xs text-slate-600">
                    Fragments that match after replacing string/numeric literals (e.g. different
                    IDs in WHERE). Good candidates for parameterized functions.
                  </p>
                  {(nearDuplicates ?? []).length === 0 ? (
                    <div className="mt-3 text-xs text-slate-500">
                      No near-duplicate groups found.
                    </div>
                  ) : (
                    <ul className="mt-3 space-y-4">
                      {(nearDuplicates ?? []).map((group, idx) => (
                        <NearDuplicateCard key={idx} group={group} />
                      ))}
                    </ul>
                  )}
                </div>
              </div>
            )
          ) : null}
        </CardContent>
      </Card>
      <TransformationsHelpModal
        open={showHelp}
        onClose={() => setShowHelp(false)}
        subView="overlap"
      />
    </section>
  );
}

function IdenticalFragmentCard({ fragment }: { fragment: IdenticalFragment }) {
  const [expanded, setExpanded] = useState(false);
  const label = fragment.occurrences.length + " transformation(s)";
  const showText = expanded ? fragment.normalized : fragment.snippet;
  return (
    <li className="rounded-md border border-slate-200 bg-white p-3">
      <button
        type="button"
        className="flex w-full items-start justify-between gap-2 text-left"
        onClick={() => setExpanded((e) => !e)}
      >
        <span className="text-xs font-medium text-slate-700">{label}</span>
        <span className="text-slate-400">{expanded ? "▼" : "▶"}</span>
      </button>
      <pre className="mt-2 max-h-[50vh] overflow-auto rounded border border-slate-100 bg-slate-50 p-2 text-[11px] text-slate-800 whitespace-pre-wrap break-all">
        {showText}
      </pre>
      {expanded ? (
        <div className="mt-2 text-xs text-slate-600">
          <span className="font-medium">In: </span>
          {fragment.occurrences
            .map((o) => (o.name ? `${o.name} (${o.id})` : o.id))
            .join(", ")}
        </div>
      ) : null}
    </li>
  );
}

function NearDuplicateCard({ group }: { group: NearDuplicateGroup }) {
  const [expanded, setExpanded] = useState(false);
  const showTemplate = expanded ? group.template : group.snippet;
  return (
    <li className="rounded-md border border-slate-200 bg-white p-3">
      <button
        type="button"
        className="flex w-full items-start justify-between gap-2 text-left"
        onClick={() => setExpanded((e) => !e)}
      >
        <span className="text-xs font-medium text-slate-700">
          {group.count} transformation(s) — template
        </span>
        <span className="text-slate-400">{expanded ? "▼" : "▶"}</span>
      </button>
      <pre className="mt-2 max-h-[50vh] overflow-auto rounded border border-slate-100 bg-slate-50 p-2 text-[11px] text-slate-800 whitespace-pre-wrap break-all">
        {showTemplate}
      </pre>
      {expanded ? (
        <div className="mt-3 space-y-2">
          <div className="text-xs font-medium text-slate-600">Examples (normalized):</div>
          {group.examples.slice(0, 5).map((ex, i) => (
            <div key={i} className="rounded border border-slate-100 bg-white p-2">
              <div className="text-[11px] text-slate-500">
                {ex.name ?? ex.id}
              </div>
              <pre className="mt-1 max-h-[40vh] overflow-auto text-[11px] text-slate-700 whitespace-pre-wrap break-all">
                {ex.normalized}
              </pre>
            </div>
          ))}
          {group.examples.length > 5 ? (
            <div className="text-xs text-slate-500">
              +{group.examples.length - 5} more
            </div>
          ) : null}
        </div>
      ) : null}
    </li>
  );
}
