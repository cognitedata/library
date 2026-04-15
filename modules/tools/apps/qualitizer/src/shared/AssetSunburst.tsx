import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { useAppData } from "@/shared/data-cache";
import { useLimits } from "@/shared/LimitsContext";
import { Sunburst } from "@/shared/Sunburst";
import { SunburstData } from "@/shared/quality-types";
import { ApiError } from "@/shared/ApiError";
import { cachedInstancesList } from "@/shared/instances-cache";
import { setAssetNodes, resizeAssetNodeCache } from "@/shared/asset-node-cache";
import { getAssetUrl, CDF_CLUSTER } from "@/shared/cdf-browser-url";

type DataModelReference = {
  space: string;
  externalId: string;
  version?: string;
  name?: string;
};

type ViewReference = {
  space: string;
  externalId: string;
  version?: string;
  name?: string;
};

type ViewDefinition = {
  space: string;
  externalId: string;
  version: string;
};

type DataModelDetails = {
  views?: ViewDefinition[];
};

type NodeSummary = {
  externalId: string;
  space: string;
  properties?: Record<string, Record<string, unknown>>;
};

type EdgeSummary = {
  startNode?: { space: string; externalId: string };
  endNode?: { space: string; externalId: string };
};

type AssetSunburstProps = {
  model?: DataModelReference | null;
  view?: ViewReference | null;
  assetViews?: ViewReference[];
  maxDepth?: number;
  spaceFilter?: string | null;
};

type LoadState = "idle" | "loading" | "success" | "error";

export function AssetSunburst({ model, view, assetViews, maxDepth, spaceFilter }: AssetSunburstProps) {
  const { sdk } = useAppSdk();
  const { retrieveDataModels, retrieveViews } = useAppData();
  const [status, setStatus] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [data, setData] = useState<SunburstData | null>(null);
  const [childlessRootExamples, setChildlessRootExamples] = useState<ChildlessRootSample[]>([]);
  const [trueChildlessRootCount, setTrueChildlessRootCount] = useState(0);
  const [orphanedNodeCount, setOrphanedNodeCount] = useState(0);
  const [totalAssetCount, setTotalAssetCount] = useState<number | null>(null);
  const { assetLimit, sunburstMaxDepth } = useLimits();
  resizeAssetNodeCache(assetLimit);
  const effectiveMaxDepth = maxDepth ?? sunburstMaxDepth;
  const [loadedAssets, setLoadedAssets] = useState(0);
  const [loadingPhase, setLoadingPhase] = useState<string>("Loading assets…");
  const requestIdRef = useRef(0);

  const assetViewsKey = useMemo(
    () => (assetViews ?? []).map((v) => `${v.space}:${v.externalId}:${v.version ?? ""}`).join("|"),
    [assetViews]
  );

  useEffect(() => {
    if (!model && !view && (!assetViews || assetViews.length === 0)) {
      setStatus("idle");
      setErrorMessage(null);
      setData(null);
      return;
    }

    let cancelled = false;
    const loadAssets = async () => {
      const requestId = requestIdRef.current + 1;
      requestIdRef.current = requestId;
      setStatus("loading");
      setErrorMessage(null);
      setData(null);
      setLoadedAssets(0);
      setTotalAssetCount(null);
      setOrphanedNodeCount(0);
      setLoadingPhase("Loading assets…");
      try {
        let viewSources: ViewDefinition[] = [];

        if (assetViews && assetViews.length > 0) {
          const refs = assetViews.map((v) => ({
            space: v.space,
            externalId: v.externalId,
            ...(v.version ? { version: v.version } : {}),
          }));
          const response = (await retrieveViews(refs, {
            includeInheritedProperties: false,
          })) as {
            items?: Array<{ space: string; externalId: string; version?: string }>;
          };
          for (const item of response.items ?? []) {
            if (item.version) {
              viewSources.push({
                space: item.space,
                externalId: item.externalId,
                version: item.version,
              });
            }
          }
        } else if (view) {
          if (view.space !== "cdf_cdm" || view.externalId !== "CogniteAsset") {
            if (!cancelled) {
              setStatus("success");
              setData(null);
            }
            return;
          }

          if (!view.version) {
            throw new Error("Selected view is missing a version.");
          }

          viewSources = [{
            space: view.space,
            externalId: view.externalId,
            version: view.version,
          }];
        } else if (model) {
          const reference = model.version
            ? {
                space: model.space,
                externalId: model.externalId,
                version: model.version,
              }
            : {
                space: model.space,
                externalId: model.externalId,
              };

          const response = (await retrieveDataModels([reference], { inlineViews: true })) as {
            items?: Array<{ views?: ViewDefinition[] }>;
          };
          const details = response.items?.[0] as DataModelDetails | undefined;
          const views = details?.views ?? [];
          const candidate = views.find(
            (viewItem) => viewItem.space === "cdf_cdm" && viewItem.externalId === "CogniteAsset"
          );

          if (!candidate) {
            if (!cancelled) {
              setStatus("success");
              setData(null);
            }
            return;
          }

          viewSources = [candidate];
        }

        if (viewSources.length === 0) {
          if (!cancelled) {
            setStatus("success");
            setData(null);
          }
          return;
        }

        const sources = viewSources.map((vs) => ({
          source: {
            type: "view" as const,
            space: vs.space,
            externalId: vs.externalId,
            version: vs.version,
          },
        }));

        let aggregateTotal: number | null = null;
        try {
          let total = 0;
          for (const vs of viewSources) {
            const aggParams: Record<string, unknown> = {
              instanceType: "node",
              view: { type: "view", space: vs.space, externalId: vs.externalId, version: vs.version },
              aggregates: [{ count: { property: "externalId" } }],
            };
            if (spaceFilter) {
              aggParams.filter = { equals: { property: ["node", "space"], value: spaceFilter } };
            }
            const r = await (sdk.instances.aggregate as (p: unknown) => Promise<unknown>)(aggParams) as {
              items?: Array<{ aggregates?: Array<{ value?: number }> }>;
            };
            total += r.items?.[0]?.aggregates?.[0]?.value ?? 0;
          }
          aggregateTotal = total;
        } catch {
          // non-critical — proceed without total
        }
        if (!cancelled && requestIdRef.current === requestId) {
          setTotalAssetCount(aggregateTotal);
        }

        const assets: NodeSummary[] = [];
        let cursor: string | undefined;
        const maxQueries = 100;

        let loadedCount = 0;
        for (let i = 0; i < maxQueries; i += 1) {
          const remaining = assetLimit - assets.length;
          if (remaining <= 0) break;
          const listParams: Record<string, unknown> = {
            instanceType: "node",
            sources,
            limit: Math.min(1000, remaining),
            cursor,
          };
          if (spaceFilter) {
            listParams.filter = {
              equals: { property: ["node", "space"], value: spaceFilter },
            };
          }
          const listResponse = await cachedInstancesList(sdk, listParams);

          const batchItems = listResponse.items as NodeSummary[];
          assets.push(...batchItems);
          setAssetNodes(sdk.project, batchItems);
          loadedCount += batchItems.length;
          if (!cancelled && requestIdRef.current === requestId) {
            setLoadedAssets(loadedCount);
          }
          cursor = listResponse.nextCursor ?? undefined;
          if (!cursor) break;
        }
        if (!cancelled && requestIdRef.current === requestId) {
          setLoadingPhase("Loading contextualization links…");
        }
        const assetLinks = await loadAssetLinks(sdk);
        const linkedNodes = new Set<string>();

        for (const edge of assetLinks) {
          if (edge.startNode) {
            linkedNodes.add(`${edge.startNode.space}:${edge.startNode.externalId}`);
          }
          if (edge.endNode) {
            linkedNodes.add(`${edge.endNode.space}:${edge.endNode.externalId}`);
          }
        }

        if (!cancelled && requestIdRef.current === requestId) {
          setLoadingPhase("Building hierarchy tree…");
        }
        const result = buildAssetSunburst(assets, effectiveMaxDepth, linkedNodes);

        if (!cancelled && requestIdRef.current === requestId) {
          setData(result.tree);
          setChildlessRootExamples(result.childlessRoots);
          setTrueChildlessRootCount(result.trueChildlessRootCount);
          setOrphanedNodeCount(result.orphanedNodeCount);
          setStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(error instanceof Error ? error.message : "Failed to load asset hierarchy.");
          setStatus("error");
        }
      }
    };

    loadAssets();

    return () => {
      cancelled = true;
    };
  }, [sdk, model, view, assetViewsKey, effectiveMaxDepth, assetLimit, spaceFilter]);

  const title = useMemo(() => {
    if (view) {
      return `${view.name ?? view.externalId} · CogniteAsset hierarchy`;
    }
    if (!model) return "Asset hierarchy";
    return `${model.name ?? model.externalId} · CogniteAsset hierarchy`;
  }, [model, view]);

  const [showInfo, setShowInfo] = useState(false);
  const toggleInfo = useCallback(() => setShowInfo((prev) => !prev), []);
  const [showChildlessExamples, setShowChildlessExamples] = useState(false);
  const [selectedChildlessRoot, setSelectedChildlessRoot] = useState<ChildlessRootSample | null>(null);

  const projectName = (sdk as { project?: string }).project ?? "";

  const stats = useMemo(() => {
    if (!data) return null;
    return computeTreeStats(data);
  }, [data]);

  const isFlatWarning =
    stats != null &&
    trueChildlessRootCount > 1000;

  const filteredData = useMemo(() => {
    if (!isFlatWarning || !data) return data ?? null;
    const kept = (data.children ?? []).filter(
      (c) => (c.children?.length ?? 0) > 0
    );
    if (kept.length === 0) return null;
    const count = (arr: SunburstData[]): number =>
      arr.reduce((a, n) => a + 1 + count(n.children ?? []), 0);
    const total = kept.reduce((s, c) => s + 1 + count(c.children ?? []), 0);
    if (total < 100) return null;
    return { ...data, children: kept };
  }, [isFlatWarning, data]);

  const filteredStats = useMemo(() => {
    if (!filteredData || filteredData === data) return null;
    return computeTreeStats(filteredData);
  }, [filteredData, data]);

  if (!model && !view && (!assetViews || assetViews.length === 0)) {
    return null;
  }

  if (status === "error") {
    return (
      <ApiError message={errorMessage ?? "Failed to load asset hierarchy."} />
    );
  }

  if (status === "loading") {
    const pct = Math.min((loadedAssets / assetLimit) * 100, 100);
    const pastAssetPhase = loadingPhase !== "Loading assets…";
    const totalLabel = totalAssetCount != null
      ? `${totalAssetCount.toLocaleString()} total`
      : null;
    return (
      <div className="rounded-md border border-slate-200 bg-white p-4 text-sm text-slate-600">
        <div className="flex items-center justify-between">
          <span>{loadingPhase}</span>
          {!pastAssetPhase ? (
            <span>
              {loadedAssets.toLocaleString()} / {assetLimit.toLocaleString()}
              {totalLabel ? <span className="ml-1 text-slate-400">({totalLabel})</span> : null}
            </span>
          ) : (
            <span className="text-slate-400">
              {loadedAssets.toLocaleString()} assets loaded
              {totalLabel ? ` of ${totalLabel}` : ""}
            </span>
          )}
        </div>
        <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-slate-100">
          <div
            className={`h-full transition-all ${pastAssetPhase ? "bg-emerald-600 animate-pulse" : "bg-slate-900"}`}
            style={{ width: `${pastAssetPhase ? 100 : pct}%` }}
          />
        </div>
      </div>
    );
  }

  if (!data) {
    const viewNames = (assetViews ?? []).map((v) => v.name ?? v.externalId).join(", ");
    const context = assetViews && assetViews.length > 0
      ? `No asset instances were found for ${assetViews.length === 1 ? "view" : "views"}: ${viewNames}. The sunburst requires nodes with a CogniteAsset-compatible view to build the hierarchy.`
      : model
        ? "This data model does not include a CogniteAsset view, or there are no asset instances populated for it."
        : "No asset instances were found for this view.";
    return (
      <div className="rounded-md border border-slate-200 bg-white p-4 text-sm text-slate-500">
        {context}
      </div>
    );
  }

  const flatWarningBanner = isFlatWarning && stats ? (
    <div className="rounded-md border border-amber-200 bg-amber-50/50 p-4">
      <div className="flex items-center gap-2 text-sm font-medium text-amber-800">
        <span className="text-amber-500">⚠</span>
        Partially flat hierarchy — {trueChildlessRootCount.toLocaleString()} childless root assets excluded
      </div>
      <p className="mt-2 text-sm leading-relaxed text-amber-700">
        Out of <strong>{stats.totalNodes.toLocaleString()}</strong> total assets,{" "}
        <strong>{trueChildlessRootCount.toLocaleString()}</strong> are root nodes
        without any children. These appear to have no{" "}
        <code className="rounded bg-amber-100 px-1 py-0.5 text-xs font-mono">parent</code>{" "}
        direct relation set and no descendants, so they add no hierarchical information.
      </p>
      {filteredData ? (
        <p className="mt-2 text-sm leading-relaxed text-amber-700">
          The sunburst below shows only the{" "}
          <strong>{(stats.totalNodes - trueChildlessRootCount).toLocaleString()}</strong>{" "}
          remaining assets that form an actual hierarchy. All childless root assets are
          excluded from the visualization.
        </p>
      ) : (
        <p className="mt-2 text-sm leading-relaxed text-amber-700">
          After removing these childless roots, fewer than 100 assets remain with hierarchy,
          which is too few to display a meaningful sunburst.
        </p>
      )}
      <div className="mt-3 rounded border border-amber-200 bg-white p-3">
        <div className="text-xs font-medium text-slate-600">Common causes</div>
        <ul className="mt-1.5 list-disc space-y-1 pl-4 text-xs text-slate-600">
          <li>The data model does not define a <code className="rounded bg-slate-100 px-1 py-0.5 font-mono">parent</code> property on the asset view.</li>
          <li>The <code className="rounded bg-slate-100 px-1 py-0.5 font-mono">parent</code> property exists but has not been populated with data (all values are null).</li>
          <li>The parent references point to assets in a different space or view that was not loaded.</li>
          <li>The hierarchy is modeled using edges instead of a direct relation property — this sunburst only reads direct relation properties, not edges.</li>
        </ul>
      </div>
      {childlessRootExamples.length > 0 ? (
        <div className="mt-3">
          <button
            type="button"
            className="cursor-pointer text-xs font-medium text-amber-700 hover:text-amber-900"
            onClick={() => setShowChildlessExamples((p) => !p)}
          >
            {showChildlessExamples ? "▾" : "▸"} Show sample childless root assets ({childlessRootExamples.length})
          </button>
          {showChildlessExamples ? (
            <>
              <div className="mt-2 max-h-64 overflow-auto rounded border border-amber-200 bg-white">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-slate-50">
                    <tr className="text-left text-slate-500">
                      <th className="px-2 py-1.5">Name / External ID</th>
                      <th className="px-2 py-1.5">Space</th>
                      <th className="px-2 py-1.5 text-right">CDF</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {childlessRootExamples.map((cr) => (
                      <tr
                        key={`${cr.space}:${cr.externalId}`}
                        className="cursor-pointer hover:bg-slate-50"
                        onClick={() => setSelectedChildlessRoot(cr)}
                      >
                        <td className="px-2 py-1.5 text-slate-700" title={cr.externalId}>
                          {cr.name ?? cr.externalId}
                          {cr.name ? (
                            <span className="ml-1 text-slate-400">{cr.externalId}</span>
                          ) : null}
                        </td>
                        <td className="px-2 py-1.5 text-slate-500">{cr.space}</td>
                        <td className="px-2 py-1.5 text-right">
                          <a
                            href={getAssetUrl(projectName, cr.space, cr.externalId)}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-blue-600 hover:underline"
                            title={`Open in CDF (${CDF_CLUSTER})`}
                            onClick={(e) => e.stopPropagation()}
                          >
                            Open ↗
                          </a>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="mt-1 text-[10px] text-slate-400">Click a row to inspect properties</div>
            </>
          ) : null}
          {selectedChildlessRoot ? (
            <ChildlessRootDetailPopup
              node={selectedChildlessRoot}
              project={projectName}
              onClose={() => setSelectedChildlessRoot(null)}
            />
          ) : null}
        </div>
      ) : null}
    </div>
  ) : null;

  const displayData = isFlatWarning ? filteredData : data;
  const displayStats = isFlatWarning ? filteredStats ?? stats : stats;

  const isTruncated = totalAssetCount != null && loadedAssets < totalAssetCount;
  const samplePct = totalAssetCount != null && totalAssetCount > 0
    ? Math.min((loadedAssets / totalAssetCount) * 100, 100)
    : null;
  const hierarchyNodes = stats ? stats.totalNodes : 0;
  const disconnected = orphanedNodeCount;

  const sampleBanner = isTruncated ? (
    <div className="rounded-md border border-blue-200 bg-blue-50/50 p-4">
      <div className="flex items-center gap-2 text-sm font-medium text-blue-800">
        <span className="text-blue-500">ℹ</span>
        Sample coverage: {samplePct != null ? `${samplePct.toFixed(1)}%` : "—"} ({loadedAssets.toLocaleString()} of {totalAssetCount!.toLocaleString()} assets)
      </div>
      <p className="mt-2 text-sm leading-relaxed text-blue-700">
        The asset limit is set to <strong>{assetLimit.toLocaleString()}</strong>, so only{" "}
        <strong>{loadedAssets.toLocaleString()}</strong> of the{" "}
        <strong>{totalAssetCount!.toLocaleString()}</strong> total assets were loaded.
        The visualization below is based on this sample.
      </p>
      {disconnected > 0 ? (
        <p className="mt-2 text-sm leading-relaxed text-blue-700">
          Of the loaded assets, <strong>{disconnected.toLocaleString()}</strong>{" "}
          ({hierarchyNodes > 0 ? ((disconnected / loadedAssets) * 100).toFixed(1) : "0"}%) have
          a <code className="rounded bg-blue-100 px-1 py-0.5 text-xs font-mono">parent</code> reference
          to an asset that was not included in the sample. These orphaned nodes appear
          as disconnected roots in the sunburst — their true position in the hierarchy
          is unknown because their ancestors were not loaded.
        </p>
      ) : null}
      <p className="mt-2 text-xs text-blue-500">
        Increase the asset limit in Settings to improve coverage.
      </p>
    </div>
  ) : null;

  if (isFlatWarning && !filteredData) {
    return (
      <div className="flex flex-col gap-4">
        {sampleBanner}
        {flatWarningBanner}
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      {sampleBanner}
      {flatWarningBanner}
      <div className="relative">
        <button
          type="button"
          className="absolute right-2 top-2 z-10 flex h-7 w-7 cursor-pointer items-center justify-center rounded-full border border-slate-200 bg-white text-xs font-semibold text-slate-500 shadow-sm hover:bg-slate-50 hover:text-slate-700"
          onClick={toggleInfo}
          title="Hierarchy statistics"
        >
          i
        </button>
        {displayData ? <Sunburst title={title} data={displayData} /> : null}
        {showInfo && displayStats ? (
          <div className="absolute right-2 top-11 z-20 w-72 rounded-lg border border-slate-200 bg-white p-4 shadow-lg">
            <div className="mb-3 flex items-center justify-between">
              <span className="text-sm font-semibold text-slate-800">Hierarchy statistics</span>
              <button
                type="button"
                className="cursor-pointer text-xs text-slate-400 hover:text-slate-600"
                onClick={toggleInfo}
              >
                Close
              </button>
            </div>
            <dl className="space-y-1.5 text-xs">
              <div className="flex justify-between">
                <dt className="text-slate-500">Total assets</dt>
                <dd className="font-medium text-slate-800">{displayStats.totalNodes.toLocaleString()}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">Root assets (depth 1)</dt>
                <dd className="font-medium text-slate-800">{displayStats.rootCount.toLocaleString()}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">Deepest level</dt>
                <dd className="font-medium text-slate-800">{displayStats.maxDepth}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">Leaf assets (no children)</dt>
                <dd className="font-medium text-slate-800">{displayStats.leafCount.toLocaleString()}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">Avg. children per branch</dt>
                <dd className="font-medium text-slate-800">{displayStats.avgChildrenPerBranch}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-slate-500">Overall coverage</dt>
                <dd className="font-medium text-slate-800">{displayStats.coverage}%</dd>
              </div>
            </dl>
            {displayStats.depthBreakdown.length > 0 ? (
              <div className="mt-3 border-t border-slate-100 pt-2">
                <div className="mb-1 text-xs font-semibold text-slate-600">Assets by depth</div>
                <div className="space-y-0.5">
                  {displayStats.depthBreakdown.map(({ depth, count, pct }) => (
                    <div key={depth} className="flex items-center gap-2 text-xs">
                      <span className="w-16 text-slate-500">Depth {depth}</span>
                      <div className="flex-1">
                        <div className="h-2 w-full overflow-hidden rounded-full bg-slate-100">
                          <div
                            className="h-full rounded-full bg-slate-400"
                            style={{ width: `${pct}%` }}
                          />
                        </div>
                      </div>
                      <span className="w-12 text-right font-medium text-slate-700">
                        {count.toLocaleString()}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}

type TreeStats = {
  totalNodes: number;
  rootCount: number;
  childlessRootCount: number;
  maxDepth: number;
  leafCount: number;
  avgChildrenPerBranch: string;
  coverage: string;
  depthBreakdown: Array<{ depth: number; count: number; pct: number }>;
};

function computeTreeStats(root: SunburstData): TreeStats {
  const depthCounts = new Map<number, number>();
  let totalNodes = 0;
  let leafCount = 0;
  let branchCount = 0;
  let totalChildren = 0;
  let maxDepth = 0;
  let childlessRootCount = 0;

  const walk = (node: SunburstData) => {
    if (node.level === 0) {
      for (const child of node.children ?? []) {
        walk(child);
      }
      return;
    }

    totalNodes++;
    depthCounts.set(node.level, (depthCounts.get(node.level) ?? 0) + 1);
    if (node.level > maxDepth) maxDepth = node.level;

    const children = node.children ?? [];
    if (children.length === 0) {
      leafCount++;
      if (node.level === 1) childlessRootCount++;
    } else {
      branchCount++;
      totalChildren += children.length;
      for (const child of children) {
        walk(child);
      }
    }
  };

  walk(root);

  const rootCount = depthCounts.get(1) ?? 0;
  const avgChildrenPerBranch = branchCount > 0
    ? (totalChildren / branchCount).toFixed(1)
    : "—";
  const coverage = root.coverage != null ? root.coverage.toFixed(1) : "—";

  const depthBreakdown: Array<{ depth: number; count: number; pct: number }> = [];
  const sortedDepths = Array.from(depthCounts.keys()).sort((a, b) => a - b);
  for (const depth of sortedDepths) {
    const count = depthCounts.get(depth) ?? 0;
    depthBreakdown.push({
      depth,
      count,
      pct: totalNodes > 0 ? (count / totalNodes) * 100 : 0,
    });
  }

  return {
    totalNodes,
    rootCount,
    childlessRootCount,
    maxDepth,
    leafCount,
    avgChildrenPerBranch,
    coverage,
    depthBreakdown,
  };
}

type ChildlessRootSample = {
  space: string;
  externalId: string;
  name: string | null;
  properties?: Record<string, Record<string, unknown>>;
};

type BuildResult = {
  tree: SunburstData | null;
  childlessRoots: ChildlessRootSample[];
  trueChildlessRootCount: number;
  orphanedNodeCount: number;
};

function buildAssetSunburst(
  nodes: NodeSummary[],
  maxDepth: number,
  linkedNodes: Set<string>
): BuildResult {
  if (nodes.length === 0) return { tree: null, childlessRoots: [], trueChildlessRootCount: 0, orphanedNodeCount: 0 };

  const nodeMap = new Map<string, NodeSummary>();
  for (const node of nodes) {
    nodeMap.set(`${node.space}:${node.externalId}`, node);
  }

  const childrenByKey = new Map<string, string[]>();
  const rootKeys = new Set<string>(nodeMap.keys());
  const trueRootKeys = new Set<string>();

  for (const node of nodes) {
    const nodeKey = `${node.space}:${node.externalId}`;
    const parentRef = findParentRef(node);
    if (!parentRef) {
      trueRootKeys.add(nodeKey);
      continue;
    }
    const parentKey = `${parentRef.space}:${parentRef.externalId}`;
    if (!nodeMap.has(parentKey)) continue;

    rootKeys.delete(nodeKey);
    const children = childrenByKey.get(parentKey) ?? [];
    children.push(nodeKey);
    childrenByKey.set(parentKey, children);
  }

  const roots = Array.from(rootKeys);

  const MAX_CHILDLESS_SAMPLES = 20;
  const childlessRoots: ChildlessRootSample[] = [];
  for (const key of roots) {
    if (childlessRoots.length >= MAX_CHILDLESS_SAMPLES) break;
    if (!trueRootKeys.has(key)) continue;
    const hasChildren = (childrenByKey.get(key) ?? []).length > 0;
    if (hasChildren) continue;
    const node = nodeMap.get(key);
    if (!node) continue;
    childlessRoots.push({
      space: node.space,
      externalId: node.externalId,
      name: extractNodeName(node),
      properties: node.properties,
    });
  }

  let trueChildlessRootCount = 0;
  let orphanedNodeCount = 0;
  for (const key of roots) {
    if (!trueRootKeys.has(key)) {
      orphanedNodeCount++;
      continue;
    }
    if ((childrenByKey.get(key) ?? []).length === 0) trueChildlessRootCount++;
  }

  let nextId = 1;

  const buildNode = (
    key: string,
    depth: number
  ): { data: SunburstData; total: number; linked: number } => {
    const node = nodeMap.get(key);
    const name = extractNodeName(node) ?? node?.externalId ?? key;
    const level = depth;
    const childrenKeys = childrenByKey.get(key) ?? [];
    let total = 1;
    let linked = linkedNodes.has(key) ? 1 : 0;

    if (depth < maxDepth && childrenKeys.length > 0) {
      const leafKeys: string[] = [];
      const branchKeys: string[] = [];
      for (const ck of childrenKeys) {
        const grandchildren = childrenByKey.get(ck) ?? [];
        if (grandchildren.length > 0) {
          branchKeys.push(ck);
        } else {
          leafKeys.push(ck);
        }
      }

      const builtChildren: Array<{ data: SunburstData; total: number; linked: number }> = [];

      for (const bk of branchKeys) {
        builtChildren.push(buildNode(bk, depth + 1));
      }

      if (leafKeys.length > 0) {
        let leafTotal = leafKeys.length;
        let leafLinked = 0;
        for (const lk of leafKeys) {
          if (linkedNodes.has(lk)) leafLinked++;
        }
        const leafCoverage = leafTotal > 0 ? (leafLinked / leafTotal) * 100 : 0;
        builtChildren.push({
          data: {
            id: nextId++,
            name: `All ${name}'s ${leafKeys.length} direct leaf children`,
            value: leafKeys.length,
            coverage: leafCoverage,
            level: depth + 1,
          },
          total: leafTotal,
          linked: leafLinked,
        });
      }

      for (const child of builtChildren) {
        total += child.total;
        linked += child.linked;
      }
      const coverage = total > 0 ? (linked / total) * 100 : 0;
      return {
        data: {
          id: nextId++,
          name,
          children: builtChildren.map((child) => child.data),
          coverage,
          level,
        },
        total,
        linked,
      };
    }

    const coverage = total > 0 ? (linked / total) * 100 : 0;
    return {
      data: {
        id: nextId++,
        name,
        value: 1,
        coverage,
        level,
      },
      total,
      linked,
    };
  };

  const rootChildren = roots.map((key) => buildNode(key, 1));
  if (rootChildren.length === 0) return { tree: null, childlessRoots, trueChildlessRootCount, orphanedNodeCount };

  const total = rootChildren.reduce((sum, child) => sum + child.total, 0);
  const linked = rootChildren.reduce((sum, child) => sum + child.linked, 0);
  const coverage = total > 0 ? (linked / total) * 100 : 0;

  return {
    tree: {
      id: nextId++,
      name: "CogniteAsset",
      children: rootChildren.map((child) => child.data),
      coverage,
      level: 0,
    },
    childlessRoots,
    trueChildlessRootCount,
    orphanedNodeCount,
  };
}

function extractNodeName(node: NodeSummary | undefined): string | null {
  if (!node?.properties) return null;
  for (const spaceGroup of Object.values(node.properties)) {
    if (!spaceGroup || typeof spaceGroup !== "object") continue;
    for (const viewGroup of Object.values(spaceGroup as Record<string, unknown>)) {
      if (!viewGroup || typeof viewGroup !== "object" || Array.isArray(viewGroup)) continue;
      const name = (viewGroup as Record<string, unknown>).name;
      if (typeof name === "string" && name.length > 0) return name;
    }
  }
  return null;
}

function findParentRef(node: NodeSummary): { space: string; externalId: string } | null {
  if (!node.properties) return null;

  for (const props of Object.values(node.properties)) {
    const direct = findParentInObject(props, 2);
    if (direct) return direct;
  }

  return null;
}

function findParentInObject(
  value: unknown,
  remainingDepth: number
): { space: string; externalId: string } | null {
  if (remainingDepth < 0) return null;

  if (Array.isArray(value)) {
    for (const item of value) {
      const direct = findParentInObject(item, remainingDepth);
      if (direct) return direct;
    }
    return null;
  }

  if (!value || typeof value !== "object") return null;

  for (const [key, entry] of Object.entries(value)) {
    if (key.toLowerCase().includes("parent")) {
      const direct = extractDirectRelation(entry);
      if (direct) return direct;
    }

    if (remainingDepth > 0) {
      const nested = findParentInObject(entry, remainingDepth - 1);
      if (nested) return nested;
    }
  }

  return null;
}

function extractDirectRelation(value: unknown): { space: string; externalId: string } | null {
  if (Array.isArray(value)) {
    for (const item of value) {
      const direct = extractDirectRelation(item);
      if (direct) return direct;
    }
    return null;
  }

  if (!value || typeof value !== "object") return null;

  const candidate = value as { space?: unknown; externalId?: unknown };
  if (typeof candidate.space === "string" && typeof candidate.externalId === "string") {
    return { space: candidate.space, externalId: candidate.externalId };
  }

  return null;
}

async function loadAssetLinks(sdk: ReturnType<typeof useAppSdk>["sdk"]) {
  const edges: EdgeSummary[] = [];
  let cursor: string | undefined;
  const maxQueries = 100;
  const typeFilter = {
    or: [
      { equals: { property: ["edge", "type"], value: { space: "cdf_cdm", externalId: "diagrams.AssetLink" } } },
      { equals: { property: ["edge", "type"], value: { space: "cdf_cdm", externalId: "diagrams.FileLink" } } },
    ],
  };

  for (let i = 0; i < maxQueries; i += 1) {
    const response = await cachedInstancesList(sdk, {
      instanceType: "edge",
      limit: 1000,
      cursor,
      filter: typeFilter as never,
    });
    edges.push(...(response.items as EdgeSummary[]));
    cursor = response.nextCursor ?? undefined;
    if (!cursor) break;
  }

  return edges;
}

function ChildlessRootDetailPopup({
  node,
  project,
  onClose,
}: {
  node: ChildlessRootSample;
  project: string;
  onClose: () => void;
}) {
  const viewGroups: Array<{ viewKey: string; entries: Array<[string, unknown]> }> = [];

  if (node.properties) {
    for (const [spaceKey, spaceGroup] of Object.entries(node.properties)) {
      if (!spaceGroup || typeof spaceGroup !== "object") continue;
      for (const [viewKey, viewGroup] of Object.entries(
        spaceGroup as Record<string, unknown>
      )) {
        if (!viewGroup || typeof viewGroup !== "object" || Array.isArray(viewGroup))
          continue;
        const entries = Object.entries(viewGroup as Record<string, unknown>).filter(
          ([, v]) => v !== null && v !== undefined
        );
        if (entries.length > 0) {
          viewGroups.push({
            viewKey: `${spaceKey}/${viewKey}`,
            entries: entries.sort(([a], [b]) => a.localeCompare(b)),
          });
        }
      }
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4">
      <div className="w-full max-w-3xl rounded-lg bg-white shadow-lg">
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
            <span>
              {node.externalId}{" "}
              <span className="font-normal text-slate-500">({node.space})</span>
            </span>
            <a
              href={getAssetUrl(project, node.space, node.externalId)}
              target="_blank"
              rel="noopener noreferrer"
              className="shrink-0 rounded border border-blue-200 bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-600 hover:bg-blue-100"
              title={`Open in CDF (${CDF_CLUSTER})`}
            >
              Open in CDF ↗
            </a>
          </div>
          <button
            type="button"
            className="cursor-pointer rounded-md px-2 py-1 text-sm text-slate-600 hover:bg-slate-100"
            onClick={onClose}
          >
            Close
          </button>
        </div>
        <div className="max-h-[70vh] overflow-auto p-4">
          {viewGroups.length === 0 ? (
            <p className="text-sm text-slate-400">No properties available.</p>
          ) : (
            viewGroups.map((vg) => (
              <div key={vg.viewKey} className="mb-4">
                <div className="mb-1 inline-block rounded-md bg-slate-100 px-2 py-0.5 text-xs font-semibold text-slate-600">
                  {vg.viewKey}
                </div>
                <table className="w-full text-xs">
                  <tbody>
                    {vg.entries.map(([key, val]) => (
                      <tr key={key} className="border-b border-slate-50">
                        <td className="w-48 px-2 py-1 font-medium text-slate-600">
                          {key}
                        </td>
                        <td className="px-2 py-1 text-slate-800">
                          {typeof val === "object"
                            ? JSON.stringify(val, null, 2)
                            : String(val ?? "")}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
