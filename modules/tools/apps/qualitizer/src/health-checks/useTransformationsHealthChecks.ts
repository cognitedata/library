import type { CogniteClient } from "@cognite/sdk";
import { useCallback, useEffect, useRef, useState } from "react";
import { extractDataModelRefs } from "@/transformations/transformationChecks";
import { fetchTransformationsByIds } from "@/transformations/fetchTransformationsByIds";
import type { NoopTransformation, DmvInconsistency } from "./transformations-health-types";
import { TRANSFORMATIONS_HEALTH_TX_PAGE_SIZE } from "./transformations-health-types";
import type { LoadState } from "./types";

const TX_LIST_PAGE_SIZE = TRANSFORMATIONS_HEALTH_TX_PAGE_SIZE;

type TxListItem = { id: number | string; name?: string; query?: string };

type UseTransformationsHealthChecksArgs = {
  sdk: CogniteClient;
  isSdkLoading: boolean;
  enabled?: boolean;
};

export function useTransformationsHealthChecks({
  sdk,
  isSdkLoading,
  enabled = true,
}: UseTransformationsHealthChecksArgs) {
  const [dmvStatus, setDmvStatus] = useState<LoadState>("idle");
  const [dmvError, setDmvError] = useState<string | null>(null);
  const [dmvInconsistencies, setDmvInconsistencies] = useState<DmvInconsistency[]>([]);

  const [noopStatus, setNoopStatus] = useState<LoadState>("idle");
  const [noopError, setNoopError] = useState<string | null>(null);
  const [noopTransformations, setNoopTransformations] = useState<NoopTransformation[]>([]);
  const [noopTotal, setNoopTotal] = useState(0);

  const [txLoadAll, setTxLoadAll] = useState(false);
  const [transformationsHasMore, setTransformationsHasMore] = useState(false);
  const [checksLoadingPhase, setChecksLoadingPhase] = useState<
    "listing" | "remaining" | "queries" | "dmv" | "noop" | null
  >(null);
  const [noopCheckProgress, setNoopCheckProgress] = useState<{
    current: number;
    total: number;
  } | null>(null);

  const itemsAccRef = useRef<TxListItem[]>([]);
  const cursorResumeRef = useRef<string | undefined>(undefined);

  const fetchTxPage = useCallback(
    async (cursor?: string) => {
      const params: Record<string, string> = {
        includePublic: "true",
        limit: String(TX_LIST_PAGE_SIZE),
      };
      if (cursor) params.cursor = cursor;
      const response = (await sdk.get(`/api/v1/projects/${sdk.project}/transformations`, {
        params,
      })) as {
        data?: { items?: TxListItem[]; nextCursor?: string };
      };
      return {
        items: response.data?.items ?? [],
        nextCursor: response.data?.nextCursor ?? undefined,
      };
    },
    [sdk]
  );

  useEffect(() => {
    setTxLoadAll(false);
    itemsAccRef.current = [];
    cursorResumeRef.current = undefined;
  }, [sdk]);

  useEffect(() => {
    if (!enabled || isSdkLoading) return;
    let cancelled = false;

    type ModelUsage = {
      transformationId: string;
      transformationName: string;
      version: string | undefined;
    };

    const buildModelKey = (space: string | undefined, externalId: string | undefined) =>
      `${space ?? ""}:${externalId ?? ""}`;

    type JobMetricItem = { name: string; timestamp: number; count: number };
    type JobSummary = { id?: number | string; startedTime?: number };

    const aggregateJobMetrics = (items: JobMetricItem[]) => {
      const byName = new Map<string, { timestamp: number; count: number }>();
      for (const item of items) {
        const prev = byName.get(item.name);
        if (!prev || item.timestamp > prev.timestamp)
          byName.set(item.name, { timestamp: item.timestamp, count: item.count });
      }
      let writes = 0;
      let noops = 0;
      for (const [name, { count }] of byName) {
        if (name === "instances.upserted") writes = count;
        if (name === "instances.upsertedNoop") noops = count;
      }
      return { writes, noops };
    };

    const run = async () => {
      setDmvStatus("loading");
      setNoopStatus("loading");
      setDmvError(null);
      setNoopError(null);
      setChecksLoadingPhase(null);
      setNoopCheckProgress(null);

      try {
        let items: TxListItem[];

        if (!txLoadAll) {
          setChecksLoadingPhase("listing");
          const { items: firstBatch, nextCursor } = await fetchTxPage(undefined);
          if (cancelled) return;
          items = firstBatch;
          itemsAccRef.current = items;
          cursorResumeRef.current = nextCursor;
          setTransformationsHasMore(Boolean(nextCursor));
        } else {
          setChecksLoadingPhase("remaining");
          items = [...itemsAccRef.current];
          let c = cursorResumeRef.current;
          while (c) {
            const { items: batch, nextCursor } = await fetchTxPage(c);
            if (cancelled) return;
            items.push(...batch);
            c = nextCursor ?? undefined;
          }
          itemsAccRef.current = items;
          cursorResumeRef.current = undefined;
          setTransformationsHasMore(false);
        }

        setChecksLoadingPhase("queries");
        const idsMissingQuery = items
          .filter((tr) => !(tr.query ?? "").trim())
          .map((tr) => String(tr.id));
        const queryById = await fetchTransformationsByIds(sdk, sdk.project, idsMissingQuery);
        if (cancelled) return;

        setChecksLoadingPhase("dmv");
        const byModel = new Map<string, { space: string; externalId: string; usages: ModelUsage[] }>();
        for (const tr of items) {
          if (cancelled) return;
          let query = tr.query ?? "";
          if (!String(query).trim()) query = queryById.get(String(tr.id))?.query ?? "";
          if (!query?.trim()) continue;

          const refs = extractDataModelRefs(query);
          const id = String(tr.id);
          const name = tr.name ?? id;
          const seenVersions = new Set<string>();
          for (const ref of refs) {
            const space = ref.space ?? "";
            const externalId = ref.externalId ?? "";
            const key = buildModelKey(space, externalId);
            if (!key || key === ":") continue;

            const version = ref.version?.trim() || undefined;
            const usageKey = `${id}::${version ?? ""}`;
            if (seenVersions.has(usageKey)) continue;
            seenVersions.add(usageKey);

            const existing = byModel.get(key);
            const usage: ModelUsage = {
              transformationId: id,
              transformationName: name,
              version,
            };

            if (existing) {
              const alreadyHas = existing.usages.some(
                (u) => u.transformationId === id && u.version === version
              );
              if (!alreadyHas) existing.usages.push(usage);
            } else {
              byModel.set(key, { space, externalId, usages: [usage] });
            }
          }
        }

        const inconsistencies: DmvInconsistency[] = [];
        for (const [key, { space, externalId, usages }] of byModel.entries()) {
          const versions = [...new Set(usages.map((u) => u.version ?? "(unspecified)"))];
          if (versions.length > 1) {
            inconsistencies.push({ modelKey: key, space, externalId, usages });
          }
        }
        inconsistencies.sort((a, b) => a.modelKey.localeCompare(b.modelKey));

        if (!cancelled) {
          setDmvInconsistencies(inconsistencies);
          setDmvStatus("success");
        }

        setChecksLoadingPhase("noop");
        setNoopTotal(items.length);
        const flagged: NoopTransformation[] = [];
        const n = items.length;
        for (let i = 0; i < items.length; i++) {
          if (cancelled) return;
          const tr = items[i]!;
          if (i === 0 || (i + 1) % 5 === 0 || i === items.length - 1) {
            setNoopCheckProgress({ current: i + 1, total: n });
          }
          const id = String(tr.id);
          try {
            const jobRes = (await sdk.get(
              `/api/v1/projects/${sdk.project}/transformations/jobs`,
              { params: { limit: "1", transformationId: id } }
            )) as { data?: { items?: JobSummary[] } };
            const latestJob = jobRes.data?.items?.[0];
            if (!latestJob?.id) continue;

            const metricsRes = (await sdk.get(
              `/api/v1/projects/${sdk.project}/transformations/jobs/${latestJob.id}/metrics`
            )) as { data?: { items?: JobMetricItem[] } };
            const { writes, noops } = aggregateJobMetrics(metricsRes.data?.items ?? []);
            if (writes > 0 && noops === writes) {
              flagged.push({ id, name: tr.name ?? id, writes, noops });
            }
          } catch {
            /* skip individual failures */
          }
        }

        if (!cancelled) {
          setNoopTransformations(flagged);
          setNoopStatus("success");
          setChecksLoadingPhase(null);
          setNoopCheckProgress(null);
        }
      } catch (error) {
        if (!cancelled) {
          const msg = error instanceof Error ? error.message : "Failed to run transformation checks";
          setDmvError(msg);
          setNoopError(msg);
          setDmvStatus("error");
          setNoopStatus("error");
          setChecksLoadingPhase(null);
          setNoopCheckProgress(null);
        }
      }
    };

    run();
    return () => {
      cancelled = true;
    };
  }, [enabled, isSdkLoading, sdk, txLoadAll, fetchTxPage]);

  const onLoadAllTransformations = useCallback(() => setTxLoadAll(true), []);

  return {
    dmvStatus,
    dmvError,
    dmvInconsistencies,
    noopStatus,
    noopError,
    noopTransformations,
    noopTotal,
    transformationsHasMore,
    transformationsSampleMode: !txLoadAll && transformationsHasMore,
    onLoadAllTransformations,
    checksLoadingPhase,
    noopCheckProgress,
  };
}
