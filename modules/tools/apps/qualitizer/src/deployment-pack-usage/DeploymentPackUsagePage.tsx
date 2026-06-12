import { useEffect, useState } from "react";
import { useSdkManager } from "@/shared/SdkManager";
import { trackDeploymentPackUsageMixpanel } from "@/shared/deploymentPackUsageMixpanel";
import { DEPLOYMENT_PACKS } from "./deployment-packs";
import { detectDeploymentPackUsage } from "./detect";
import { fetchLiveDeploymentPackProbeContext } from "./live-probe-context";
import type { DeploymentPackUsageResult } from "./types";

type LoadState = "idle" | "loading" | "done" | "error";

const KIND_LABEL: Record<string, string> = {
  function: "Function",
  dataModel: "Data Model",
  transformation: "Transformation",
  locationFilter: "Location Filter",
};

function StatusBadge({ inUse }: { inUse: boolean }) {
  if (inUse) {
    return (
      <span className="inline-flex items-center gap-1 rounded-full bg-emerald-50 px-2 py-0.5 text-xs font-semibold text-emerald-700 ring-1 ring-emerald-200">
        <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
        Migrated
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 rounded-full bg-amber-50 px-2 py-0.5 text-xs font-semibold text-amber-700 ring-1 ring-amber-200">
      <span className="h-1.5 w-1.5 rounded-full bg-amber-500" />
      Not migrated
    </span>
  );
}

function SignalList({
  items,
  variant,
}: {
  items: DeploymentPackUsageResult["matched"];
  variant: "matched" | "missing";
}) {
  if (items.length === 0) return null;
  const colorClass =
    variant === "matched" ? "text-emerald-700 bg-emerald-50" : "text-amber-700 bg-amber-50";
  return (
    <ul className="mt-1 space-y-0.5">
      {items.map((item) => (
        <li
          key={`${item.kind}:${item.detail}`}
          className={`rounded px-1.5 py-0.5 font-mono text-[10px] ${colorClass}`}
        >
          <span className="mr-1 font-sans font-medium opacity-60">
            {KIND_LABEL[item.kind] ?? item.kind}
          </span>
          {item.detail}
        </li>
      ))}
    </ul>
  );
}

function PackRow({ result }: { result: DeploymentPackUsageResult }) {
  const [expanded, setExpanded] = useState(false);
  const hasDetail = result.matched.length > 0 || result.missing.length > 0;

  return (
    <div
      className={`rounded-lg border px-4 py-3 transition ${
        result.inUse ? "border-slate-100 bg-white" : "border-amber-100 bg-amber-50/40"
      }`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-sm font-semibold text-slate-800">{result.packName}</span>
            <StatusBadge inUse={result.inUse} />
          </div>
          <p className="mt-0.5 text-xs text-slate-400 line-clamp-2">{result.description}</p>
        </div>
        {hasDetail && (
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            className="shrink-0 rounded p-1 text-xs text-slate-400 transition hover:bg-slate-100 hover:text-slate-600"
            aria-label={expanded ? "Collapse signals" : "Expand signals"}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth={1.5}
              stroke="currentColor"
              className={`h-4 w-4 transition-transform ${expanded ? "rotate-180" : ""}`}
              aria-hidden
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="m19.5 8.25-7.5 7.5-7.5-7.5" />
            </svg>
          </button>
        )}
      </div>
      {expanded && hasDetail && (
        <div className="mt-3 grid gap-2 border-t border-slate-100 pt-3 sm:grid-cols-2">
          {result.matched.length > 0 && (
            <div>
              <p className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-emerald-600">
                ✓ Detected signals
              </p>
              <SignalList items={result.matched} variant="matched" />
            </div>
          )}
          {result.missing.length > 0 && (
            <div>
              <p className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-amber-600">
                ✗ Missing signals
              </p>
              <SignalList items={result.missing} variant="missing" />
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export function DeploymentPackUsagePage() {
  const { sdk, project } = useSdkManager();
  const [loadState, setLoadState] = useState<LoadState>("idle");
  const [results, setResults] = useState<DeploymentPackUsageResult[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!project?.trim()) return;
    const p = project.trim();
    let cancelled = false;
    setLoadState("loading");
    setError(null);

    void (async () => {
      try {
        const ctx = await fetchLiveDeploymentPackProbeContext(sdk, p);
        if (cancelled) return;
        const detected = await detectDeploymentPackUsage(DEPLOYMENT_PACKS, ctx);
        if (cancelled) return;
        setResults(detected);
        setLoadState("done");
        trackDeploymentPackUsageMixpanel(p, detected, { force: true });
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
        setLoadState("error");
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [sdk, project]);

  const notMigrated = results.filter((r) => !r.inUse);
  const migrated = results.filter((r) => r.inUse);

  return (
    <div className="flex flex-col gap-6">
      <div>
        <h2 className="text-lg font-semibold text-slate-900">Deployment Pack Usage</h2>
        <p className="mt-1 text-sm text-slate-500">
          Shows which library deployment packs are active in{" "}
          <span className="font-medium text-slate-700">{project ?? "—"}</span>. Packs marked{" "}
          <span className="font-semibold text-amber-700">Not migrated</span> have not been deployed
          to this project yet.
        </p>
      </div>

      {loadState === "idle" || loadState === "loading" ? (
        <div className="flex items-center gap-3 rounded-lg border border-slate-100 bg-white px-4 py-6 text-sm text-slate-400">
          <svg
            className="h-4 w-4 animate-spin text-slate-300"
            xmlns="http://www.w3.org/2000/svg"
            fill="none"
            viewBox="0 0 24 24"
            aria-hidden
          >
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
            />
          </svg>
          Probing CDF for deployment pack signals…
        </div>
      ) : loadState === "error" ? (
        <div className="rounded-lg border border-red-100 bg-red-50 px-4 py-4 text-sm text-red-700">
          <p className="font-semibold">Detection failed</p>
          <p className="mt-1 font-mono text-xs">{error}</p>
        </div>
      ) : (
        <>
          {/* Summary row */}
          <div className="flex flex-wrap gap-3">
            <div className="flex items-center gap-2 rounded-lg border border-slate-100 bg-white px-4 py-3">
              <span className="text-2xl font-bold text-slate-900">{results.length}</span>
              <span className="text-xs text-slate-400">total packs</span>
            </div>
            <div className="flex items-center gap-2 rounded-lg border border-emerald-100 bg-emerald-50 px-4 py-3">
              <span className="text-2xl font-bold text-emerald-700">{migrated.length}</span>
              <span className="text-xs text-emerald-600">migrated</span>
            </div>
            <div className="flex items-center gap-2 rounded-lg border border-amber-100 bg-amber-50 px-4 py-3">
              <span className="text-2xl font-bold text-amber-700">{notMigrated.length}</span>
              <span className="text-xs text-amber-600">not migrated</span>
            </div>
          </div>

          {/* Not migrated section */}
          {notMigrated.length > 0 && (
            <section>
              <h3 className="mb-3 text-xs font-semibold uppercase tracking-wide text-amber-600">
                Not migrated ({notMigrated.length})
              </h3>
              <div className="flex flex-col gap-2">
                {notMigrated.map((r) => (
                  <PackRow key={r.packId} result={r} />
                ))}
              </div>
            </section>
          )}

          {/* Migrated section */}
          {migrated.length > 0 && (
            <section>
              <h3 className="mb-3 text-xs font-semibold uppercase tracking-wide text-emerald-600">
                Migrated ({migrated.length})
              </h3>
              <div className="flex flex-col gap-2">
                {migrated.map((r) => (
                  <PackRow key={r.packId} result={r} />
                ))}
              </div>
            </section>
          )}
        </>
      )}
    </div>
  );
}
