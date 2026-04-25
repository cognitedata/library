import { useCallback, useEffect, useState } from "react";
import {
  getInstancesByIdsCacheStats,
  getInstancesListCacheStats,
} from "@/shared/instances-cache";
import { getAssetNodeCacheStats } from "@/shared/asset-node-cache";
import {
  getTransformationByIdRowCacheStats,
  getTransformationJobMetricsCacheStats,
  getTransformationJobsCacheStats,
  getTransformationsListCacheStats,
} from "@/transformations/transformations-cache";
import {
  getDmsDataModelsListCacheStats,
  getDmsDataModelsRetrieveCacheStats,
  getDmsViewsListCacheStats,
  getDmsViewsRetrieveCacheStats,
} from "@/shared/dms-catalog-cache";
import { getAssetsDiscoveryCacheStats } from "@/shared/assets-discovery-cache";
import { getSecurityGroupsListCacheStats } from "@/shared/security-groups-cache";

function pct(n: number) {
  return `${(n * 100).toFixed(1)}%`;
}

function barColor(fillRate: number) {
  if (fillRate >= 0.95) return "bg-red-500";
  if (fillRate >= 0.8) return "bg-amber-500";
  return "bg-emerald-500";
}

export function LruCacheStatsPanel({ onClose }: { onClose: () => void }) {
  const [tick, setTick] = useState(0);
  const refresh = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    const id = window.setInterval(refresh, 1500);
    return () => window.clearInterval(id);
  }, [refresh]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const rows = [
    getInstancesListCacheStats(),
    getInstancesByIdsCacheStats(),
    getAssetNodeCacheStats(),
    getTransformationsListCacheStats(),
    getTransformationJobsCacheStats(),
    getTransformationJobMetricsCacheStats(),
    getTransformationByIdRowCacheStats(),
    getDmsDataModelsListCacheStats(),
    getDmsViewsListCacheStats(),
    getDmsDataModelsRetrieveCacheStats(),
    getDmsViewsRetrieveCacheStats(),
    getAssetsDiscoveryCacheStats(),
    getSecurityGroupsListCacheStats(),
  ];
  void tick;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
      role="presentation"
    >
      <div
        className="w-full max-w-2xl rounded-lg bg-white shadow-lg"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-labelledby="lru-cache-stats-title"
      >
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <h2 id="lru-cache-stats-title" className="text-sm font-semibold text-slate-900">
            LRU cache diagnostics
          </h2>
          <button
            type="button"
            className="cursor-pointer rounded-md px-2 py-1 text-sm text-slate-600 hover:bg-slate-100"
            onClick={onClose}
          >
            Close
          </button>
        </div>
        <div className="max-h-[75vh] space-y-4 overflow-auto p-4">
          <p className="text-xs text-slate-500">
            Entry counts refresh every 1.5s. Fill rate is entries relative to{" "}
            <code className="rounded bg-slate-100 px-1">max</code>. When fill approaches 100%,
            older entries are evicted and API traffic may increase. Rows include instance, asset,
            transformation, data-model catalog, assets discovery, and security group list caches.
          </p>
          <table className="w-full border-collapse text-xs">
            <thead>
              <tr className="border-b border-slate-200 text-left text-slate-500">
                <th className="py-2 pr-2 font-medium">Cache</th>
                <th className="py-2 pr-2 font-medium">Entries</th>
                <th className="py-2 pr-2 font-medium">Fill</th>
                <th className="py-2 pr-2 font-medium">TTL</th>
                <th className="py-2 font-medium">Size / maxSize</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.id} className="border-b border-slate-100 align-top">
                  <td className="py-2 pr-2">
                    <div className="font-medium text-slate-800">{row.label}</div>
                    <div className="mt-0.5 font-mono text-[10px] text-slate-400">{row.id}</div>
                  </td>
                  <td className="py-2 pr-2 tabular-nums text-slate-700">
                    {row.size} / {row.max}
                  </td>
                  <td className="py-2 pr-2">
                    <div className="flex items-center gap-2">
                      <div className="h-2 w-24 overflow-hidden rounded-full bg-slate-100">
                        <div
                          className={`h-2 rounded-full transition-all ${barColor(row.fillRate)}`}
                          style={{ width: `${Math.min(100, row.fillRate * 100)}%` }}
                        />
                      </div>
                      <span className="tabular-nums text-slate-600">{pct(row.fillRate)}</span>
                    </div>
                  </td>
                  <td className="py-2 pr-2 tabular-nums text-slate-600">
                    {row.ttlMs >= 60_000
                      ? `${Math.round(row.ttlMs / 60_000)} min`
                      : `${Math.round(row.ttlMs / 1000)} s`}
                  </td>
                  <td className="py-2 tabular-nums text-slate-600">
                    {row.calculatedSize} / {row.maxSize}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
