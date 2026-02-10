import { useEffect, useMemo, useRef, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { useAppData } from "@/shared/data-cache";
import { Sunburst } from "@/shared/Sunburst";
import { SunburstData } from "@/shared/quality-types";
import { ApiError } from "@/shared/ApiError";

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
  maxDepth?: number;
};

type LoadState = "idle" | "loading" | "success" | "error";

export function AssetSunburst({ model, view, maxDepth = 5 }: AssetSunburstProps) {
  const { sdk } = useAppSdk();
  const { retrieveDataModels } = useAppData();
  const [status, setStatus] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [data, setData] = useState<SunburstData | null>(null);
  const [assetLimit, setAssetLimit] = useState(() => {
    const stored = window.localStorage.getItem("assetLimit");
    const parsed = stored ? Number(stored) : 10000;
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 10000;
  });
  const [loadedAssets, setLoadedAssets] = useState(0);
  const requestIdRef = useRef(0);

  useEffect(() => {
    if (!model && !view) {
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
      try {
        let assetView: ViewDefinition | null = null;

        if (view) {
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

          assetView = {
            space: view.space,
            externalId: view.externalId,
            version: view.version,
          };
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

          assetView = candidate;
        }

        if (!assetView) {
          if (!cancelled) {
            setStatus("success");
            setData(null);
          }
          return;
        }

        const assets: NodeSummary[] = [];
        let cursor: string | undefined;
        const maxQueries = 100;

        let loadedCount = 0;
        for (let i = 0; i < maxQueries; i += 1) {
          const remaining = assetLimit - assets.length;
          if (remaining <= 0) break;
          const listResponse = await sdk.instances.list({
            instanceType: "node",
            sources: [
              {
                source: {
                  type: "view" as const,
                  space: assetView.space,
                  externalId: assetView.externalId,
                  version: assetView.version,
                },
              },
            ],
            limit: Math.min(1000, remaining),
            cursor,
          });

          const batchItems = listResponse.items as NodeSummary[];
          assets.push(...batchItems);
          loadedCount += batchItems.length;
          if (!cancelled && requestIdRef.current === requestId) {
            setLoadedAssets(loadedCount);
          }
          cursor = listResponse.nextCursor ?? undefined;
          if (!cursor) break;
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

        const tree = buildAssetSunburst(assets, maxDepth, linkedNodes);

        if (!cancelled && requestIdRef.current === requestId) {
          setData(tree);
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
  }, [sdk, model, view, maxDepth, assetLimit]);

  useEffect(() => {
    const handleUpdate = () => {
      const stored = window.localStorage.getItem("assetLimit");
      const parsed = stored ? Number(stored) : 10000;
      const next = Number.isFinite(parsed) && parsed > 0 ? parsed : 10000;
      setAssetLimit(next);
    };
    window.addEventListener("asset-limit-update", handleUpdate);
    window.addEventListener("storage", handleUpdate);
    return () => {
      window.removeEventListener("asset-limit-update", handleUpdate);
      window.removeEventListener("storage", handleUpdate);
    };
  }, []);

  const title = useMemo(() => {
    if (view) {
      return `${view.name ?? view.externalId} · CogniteAsset hierarchy`;
    }
    if (!model) return "Asset hierarchy";
    return `${model.name ?? model.externalId} · CogniteAsset hierarchy`;
  }, [model, view]);

  if (!model && !view) {
    return null;
  }

  if (status === "error") {
    return (
      <ApiError message={errorMessage ?? "Failed to load asset hierarchy."} />
    );
  }

  if (status === "loading") {
    return (
      <div className="rounded-md border border-slate-200 bg-white p-4 text-sm text-slate-600">
        <div className="flex items-center justify-between">
          <span>Loading asset hierarchy...</span>
          <span>
            {loadedAssets.toLocaleString()} / {assetLimit.toLocaleString()}
          </span>
        </div>
        <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-slate-100">
          <div
            className="h-full bg-slate-900"
            style={{
              width: `${Math.min((loadedAssets / assetLimit) * 100, 100)}%`,
            }}
          />
        </div>
      </div>
    );
  }

  if (!data) {
    return null;
  }

  return <Sunburst title={title} data={data} />;
}

function buildAssetSunburst(
  nodes: NodeSummary[],
  maxDepth: number,
  linkedNodes: Set<string>
): SunburstData | null {
  if (nodes.length === 0) return null;

  const nodeMap = new Map<string, NodeSummary>();
  for (const node of nodes) {
    nodeMap.set(`${node.space}:${node.externalId}`, node);
  }

  const childrenByKey = new Map<string, string[]>();
  const rootKeys = new Set<string>(nodeMap.keys());

  for (const node of nodes) {
    const nodeKey = `${node.space}:${node.externalId}`;
    const parentRef = findParentRef(node);
    if (!parentRef) continue;
    const parentKey = `${parentRef.space}:${parentRef.externalId}`;
    if (!nodeMap.has(parentKey)) continue;

    rootKeys.delete(nodeKey);
    const children = childrenByKey.get(parentKey) ?? [];
    children.push(nodeKey);
    childrenByKey.set(parentKey, children);
  }

  const roots = Array.from(rootKeys);
  let nextId = 1;

  const buildNode = (
    key: string,
    depth: number
  ): { data: SunburstData; total: number; linked: number } => {
    const node = nodeMap.get(key);
    const name = node?.externalId ?? key;
    const level = depth;
    const childrenKeys = childrenByKey.get(key) ?? [];
    let total = 1;
    let linked = linkedNodes.has(key) ? 1 : 0;

    if (depth < maxDepth && childrenKeys.length > 0) {
      const children = childrenKeys.map((childKey) => buildNode(childKey, depth + 1));
      for (const child of children) {
        total += child.total;
        linked += child.linked;
      }
      const coverage = total > 0 ? (linked / total) * 100 : 0;
      return {
        data: {
          id: nextId++,
          name,
          children: children.map((child) => child.data),
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
  const total = rootChildren.reduce((sum, child) => sum + child.total, 0);
  const linked = rootChildren.reduce((sum, child) => sum + child.linked, 0);
  const coverage = total > 0 ? (linked / total) * 100 : 0;

  return {
    id: nextId++,
    name: "CogniteAsset",
    children: rootChildren.map((child) => child.data),
    coverage,
    level: 0,
  };
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
    equals: {
      property: ["edge", "type"],
      value: {
        space: "cdf_cdm",
        externalId: "diagrams.AssetLink",
      },
    },
  };

  for (let i = 0; i < maxQueries; i += 1) {
    const response = await sdk.instances.list({
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
