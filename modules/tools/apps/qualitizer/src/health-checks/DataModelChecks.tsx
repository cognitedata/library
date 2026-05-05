import { useEffect, useMemo, useState } from "react";
import { Loader } from "@/shared/Loader";
import { useAppSdk } from "@/shared/auth";
import { useAppData } from "@/shared/data-cache";
import { ModelingHealthPanel } from "./ModelingHealthPanel";
import type {
  ContainerSummary,
  LoadState,
  SpaceSummary,
  ViewDetail,
} from "./types";

type Props = { onBack: () => void };

export function DataModelChecks({ onBack }: Props) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
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
  const [viewDetailsTotal, setViewDetailsTotal] = useState(0);
  const [viewDetailsProcessed, setViewDetailsProcessed] = useState(0);
  const [containersStatus, setContainersStatus] = useState<LoadState>("idle");
  const [containersError, setContainersError] = useState<string | null>(null);
  const [containers, setContainers] = useState<ContainerSummary[]>([]);
  const [spacesStatus, setSpacesStatus] = useState<LoadState>("idle");
  const [spacesError, setSpacesError] = useState<string | null>(null);
  const [spaces, setSpaces] = useState<SpaceSummary[]>([]);
  const [showLoader, setShowLoader] = useState(false);

  const isDashboardLoading =
    dataModelsStatus === "loading" ||
    viewsStatus === "loading" ||
    viewDetailsStatus === "loading" ||
    containersStatus === "loading" ||
    spacesStatus === "loading";

  useEffect(() => {
    setShowLoader(isDashboardLoading);
  }, [isDashboardLoading]);

  useEffect(() => {
    loadDataModels();
    loadViews();
  }, [loadDataModels, loadViews]);

  useEffect(() => {
    if (isSdkLoading || viewsStatus !== "success") return;
    let cancelled = false;

    const loadViewDetails = async () => {
      setViewDetailsStatus("loading");
      setViewDetailsError(null);
      setViewDetailsTotal(views.length);
      setViewDetailsProcessed(0);
      try {
        const viewRefs = views.map((v) => ({
          space: v.space,
          externalId: v.externalId,
          version: v.version,
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
          setViewDetailsError(
            error instanceof Error ? error.message : "Failed to load view details"
          );
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
          const response = (await sdk.spaces.list({
            includeGlobal: true,
            limit: 100,
            cursor,
          })) as { items?: SpaceSummary[]; nextCursor?: string | null };
          items.push(...(response.items ?? []));
          cursor = response.nextCursor ?? undefined;
        } while (cursor);
        if (!cancelled) {
          setSpaces(items);
          setSpacesStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setSpacesError(
            error instanceof Error ? error.message : "Failed to load spaces"
          );
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
            error instanceof Error ? error.message : "Failed to load containers"
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
  }, [isSdkLoading, views, viewsStatus, sdk, retrieveViews]);

  const usedViewKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const model of dataModels) {
      for (const view of model.views ?? []) {
        keys.add(`${view.space}:${view.externalId}:${view.version ?? "latest"}`);
      }
    }
    return keys;
  }, [dataModels]);

  const unusedViews = useMemo(
    () =>
      views
        .filter(
          (v) =>
            !usedViewKeys.has(
              `${v.space}:${v.externalId}:${v.version ?? "latest"}`
            )
        )
        .sort((a, b) =>
          (a.name ?? a.externalId).localeCompare(b.name ?? b.externalId)
        ),
    [views, usedViewKeys]
  );

  const viewsWithoutContainers = useMemo(
    () =>
      viewDetails
        .filter((v) => {
          const props = Object.values(v.properties ?? {});
          return props.length === 0 || props.every((p) => !p.container);
        })
        .sort((a, b) =>
          (a.name ?? a.externalId).localeCompare(b.name ?? b.externalId)
        ),
    [viewDetails]
  );

  const usedContainerKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const v of viewDetails) {
      for (const p of Object.values(v.properties ?? {})) {
        if (p.container?.space && p.container?.externalId) {
          keys.add(`${p.container.space}:${p.container.externalId}`);
        }
      }
    }
    return keys;
  }, [viewDetails]);

  const unusedContainers = useMemo(
    () =>
      containers
        .filter(
          (c) => !usedContainerKeys.has(`${c.space}:${c.externalId}`)
        )
        .sort((a, b) =>
          (a.name ?? a.externalId).localeCompare(b.name ?? b.externalId)
        ),
    [containers, usedContainerKeys]
  );

  const unusedSpaces = useMemo(() => {
    const used = new Set<string>();
    for (const m of dataModels) used.add(m.space);
    for (const v of views) used.add(v.space);
    for (const c of containers) used.add(c.space);
    return spaces
      .filter((s) => !used.has(s.space))
      .sort((a, b) => (a.name ?? a.space).localeCompare(b.name ?? b.space));
  }, [containers, dataModels, spaces, views]);

  const renderProgressBarJsx = (value: number, total: number) => {
    const safeTotal = total > 0 ? total : 0;
    const percent = safeTotal > 0 ? Math.min(100, (value / safeTotal) * 100) : 0;
    return (
      <div className="mt-2 h-2 w-full rounded-full bg-slate-100">
        <div
          className="h-2 rounded-full bg-slate-900"
          style={{ width: `${percent}%` }}
        />
      </div>
    );
  };

  return (
    <section className="flex flex-col gap-4">
      <header className="flex items-start justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h2 className="text-2xl font-semibold text-slate-900">
            Data Model Checks
          </h2>
          <p className="text-sm text-slate-500">
            Unused views, containers, spaces, and views without backing containers
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
        renderProgressBar={renderProgressBarJsx}
      />
      <Loader
        open={showLoader}
        onClose={() => setShowLoader(false)}
        title="Running data model checks…"
      />
    </section>
  );
}
