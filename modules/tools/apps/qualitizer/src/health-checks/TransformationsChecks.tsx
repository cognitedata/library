import { useEffect, useState } from "react";
import { Loader } from "@/shared/Loader";
import { useAppSdk } from "@/shared/auth";
import { extractDataModelRefs } from "@/transformations/transformationChecks";
import { fetchTransformationsByIds } from "@/transformations/fetchTransformationsByIds";
import {
  TransformationsHealthPanel,
  type NoopTransformation,
  type DmvInconsistency,
} from "./TransformationsHealthPanel";
import type { LoadState } from "./types";

type Props = { onBack: () => void };

export function TransformationsChecks({ onBack }: Props) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();

  const [dmvStatus, setDmvStatus] = useState<LoadState>("idle");
  const [dmvError, setDmvError] = useState<string | null>(null);
  const [dmvInconsistencies, setDmvInconsistencies] = useState<DmvInconsistency[]>([]);

  const [noopStatus, setNoopStatus] = useState<LoadState>("idle");
  const [noopError, setNoopError] = useState<string | null>(null);
  const [noopTransformations, setNoopTransformations] = useState<NoopTransformation[]>([]);
  const [noopTotal, setNoopTotal] = useState(0);

  const [showLoader, setShowLoader] = useState(false);

  const isDashboardLoading =
    dmvStatus === "loading" || noopStatus === "loading";

  useEffect(() => {
    setShowLoader(isDashboardLoading);
  }, [isDashboardLoading]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;

    type ModelUsage = {
      transformationId: string;
      transformationName: string;
      version: string | undefined;
    };

    const buildModelKey = (
      space: string | undefined,
      externalId: string | undefined
    ) => `${space ?? ""}:${externalId ?? ""}`;

    const loadDataModelVersioning = async () => {
      setDmvStatus("loading");
      setDmvError(null);
      try {
        const response = (await sdk.get(
          `/api/v1/projects/${sdk.project}/transformations`,
          { params: { includePublic: "true", limit: "1000" } }
        )) as {
          data?: {
            items?: Array<{
              id: number | string;
              name?: string;
              query?: string;
            }>;
          };
        };
        const items = response.data?.items ?? [];
        const idsMissingQuery = items
          .filter((tr) => !(tr.query ?? "").trim())
          .map((tr) => String(tr.id));
        const queryById = await fetchTransformationsByIds(sdk, sdk.project, idsMissingQuery);

        const byModel = new Map<
          string,
          { space: string; externalId: string; usages: ModelUsage[] }
        >();

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
          const versions = [
            ...new Set(usages.map((u) => u.version ?? "(unspecified)")),
          ];
          if (versions.length > 1) {
            inconsistencies.push({ modelKey: key, space, externalId, usages });
          }
        }
        inconsistencies.sort((a, b) => a.modelKey.localeCompare(b.modelKey));

        if (!cancelled) {
          setDmvInconsistencies(inconsistencies);
          setDmvStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setDmvError(
            error instanceof Error
              ? error.message
              : "Failed to check data model versioning"
          );
          setDmvStatus("error");
        }
      }
    };

    loadDataModelVersioning();
    return () => { cancelled = true; };
  }, [isSdkLoading, sdk]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;

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

    const loadNoops = async () => {
      setNoopStatus("loading");
      setNoopError(null);
      try {
        const response = (await sdk.get(
          `/api/v1/projects/${sdk.project}/transformations`,
          { params: { includePublic: "true", limit: "1000" } }
        )) as {
          data?: { items?: Array<{ id: number | string; name?: string }> };
        };
        const items = response.data?.items ?? [];
        setNoopTotal(items.length);
        const flagged: NoopTransformation[] = [];

        for (const tr of items) {
          if (cancelled) return;
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
            const { writes, noops } = aggregateJobMetrics(
              metricsRes.data?.items ?? []
            );
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
        }
      } catch (error) {
        if (!cancelled) {
          setNoopError(
            error instanceof Error
              ? error.message
              : "Failed to load transformation metrics"
          );
          setNoopStatus("error");
        }
      }
    };

    loadNoops();
    return () => { cancelled = true; };
  }, [isSdkLoading, sdk]);

  return (
    <section className="flex flex-col gap-4">
      <header className="flex items-start justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h2 className="text-2xl font-semibold text-slate-900">
            Transformations Checks
          </h2>
          <p className="text-sm text-slate-500">
            Write efficiency and data model version consistency
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

      <TransformationsHealthPanel
        noopStatus={noopStatus}
        noopError={noopError}
        noopTransformations={noopTransformations}
        noopTotal={noopTotal}
        dmvStatus={dmvStatus}
        dmvError={dmvError}
        dmvInconsistencies={dmvInconsistencies}
      />
      <Loader
        open={showLoader}
        onClose={() => setShowLoader(false)}
        title="Running transformations checks…"
      />
    </section>
  );
}
