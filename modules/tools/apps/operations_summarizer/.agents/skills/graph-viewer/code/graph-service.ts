import type { CogniteClient } from "@cognite/sdk";
import type {
  CDFEdge,
  CDFNode,
  DataModelInfo,
  GraphData,
  ReverseRelationQuery,
  ViewPriorityConfig,
} from "./types";

// =============================================================================
// Node Type Detection
// =============================================================================

const DEFAULT_VIEW_TYPE_PRIORITY = [
  "ISA95Asset",
  "Equipment",
  "Area",
  "Site",
  "Enterprise",
  "WorkCell",
  "WorkCenter",
  "ProcessCell",
  "ProcessArea",
  "ProductionLine",
  "ProductionRun",
  "Product",
  "WorkOrder",
  "WorkUnit",
  "QualityAlert",
  "FaultCode",
  "CogniteAsset",
  "CogniteEquipment",
  "CogniteFile",
  "CogniteTimeSeries",
  "CogniteActivity",
  "Cognite3DModel",
  "CogniteDescribable",
  "CogniteSourceable",
  "CogniteVisualizable",
  "CogniteSchedulable",
];

const DEFAULT_SKIP_VIEWS_FOR_TYPE = new Set([
  "CogniteDescribable",
  "CogniteSourceable",
  "CogniteVisualizable",
  "CogniteSchedulable",
  "CogniteSourceSystem",
]);

const DEFAULT_SKIP_VIEWS_FOR_PROPERTIES = new Set([
  "CogniteSourceable",
  "CogniteVisualizable",
  "CogniteSchedulable",
]);

function deriveNodeTypeFromProperties(
  properties: Record<string, Record<string, unknown>> | undefined,
  viewPriorityConfig?: ViewPriorityConfig
): { space: string; externalId: string } | undefined {
  if (!properties) return undefined;

  const viewTypePriority = viewPriorityConfig?.viewTypePriority ?? DEFAULT_VIEW_TYPE_PRIORITY;
  const skipViewsForType = viewPriorityConfig?.skipViewsForType
    ? new Set(viewPriorityConfig.skipViewsForType)
    : DEFAULT_SKIP_VIEWS_FOR_TYPE;

  const viewKeys: Array<{
    space: string;
    externalId: string;
    priority: number;
  }> = [];

  for (const [spaceKey, viewsObj] of Object.entries(properties)) {
    if (typeof viewsObj !== "object" || viewsObj === null) continue;

    for (const viewKey of Object.keys(viewsObj)) {
      const viewExternalId = viewKey.split("/")[0];
      if (skipViewsForType.has(viewExternalId)) continue;

      const priorityIndex = viewTypePriority.indexOf(viewExternalId);
      const priority = priorityIndex >= 0 ? priorityIndex : 100;

      viewKeys.push({
        space: spaceKey,
        externalId: viewExternalId,
        priority,
      });
    }
  }

  if (viewKeys.length === 0) return undefined;

  viewKeys.sort((a, b) => a.priority - b.priority);

  return {
    space: viewKeys[0].space,
    externalId: viewKeys[0].externalId,
  };
}

// =============================================================================
// Query with cursor-based pagination
// =============================================================================

const QUERY_PAGE_LIMIT = 10_000;

/**
 * Paginate a CDF instances.query selection until either:
 *   1. the API has no more cursors,
 *   2. the cumulative number of items reaches `maxTotal` (hard cap), or
 *   3. an empty page is returned.
 *
 * `maxTotal` is a HARD MAXIMUM. The function never returns more than `maxTotal`
 * items, and it stops fetching additional pages as soon as the cap is reached.
 * Pass `Infinity` to disable the cap (legacy "fetch everything" behaviour).
 */
async function queryWithCursorPagination<T>(
  client: CogniteClient,
  selectionName: string,
  query: {
    with: Record<
      string,
      {
        nodes?: { filter?: unknown };
        edges?: { filter?: unknown };
        limit?: number;
      }
    >;
    select: Record<string, { sources?: unknown[]; sort?: unknown[] }>;
    includeTyping?: boolean;
  },
  limitPerPage: number = QUERY_PAGE_LIMIT,
  maxTotal: number = Infinity
): Promise<T[]> {
  const results: T[] = [];
  let cursors: Record<string, string> | undefined;

  while (results.length < maxTotal) {
    const remaining = maxTotal - results.length;
    const pageLimit = Math.max(1, Math.min(limitPerPage, remaining));

    const withWithLimit = { ...query.with };
    const firstKey = Object.keys(withWithLimit)[0];
    if (firstKey && withWithLimit[firstKey]) {
      withWithLimit[firstKey] = {
        ...withWithLimit[firstKey],
        limit: pageLimit,
      };
    }
    const request = {
      ...query,
      with: withWithLimit,
      cursors,
    };
    const res = await client.instances.query(
      request as Parameters<CogniteClient["instances"]["query"]>[0]
    );
    const chunk = (res.items[selectionName] ?? []) as T[];
    results.push(...chunk);
    cursors =
      res.nextCursor && Object.keys(res.nextCursor).length > 0
        ? (res.nextCursor as Record<string, string>)
        : undefined;
    if (chunk.length === 0 || !cursors?.[selectionName]) break;
  }

  return results.length > maxTotal ? results.slice(0, maxTotal) : results;
}

// =============================================================================
// fetchNodeDetails
// =============================================================================

export async function fetchNodeDetails(
  client: CogniteClient | null,
  space: string,
  externalId: string
): Promise<CDFNode | null> {
  if (!client) {
    throw new Error("CDF client is not available");
  }

  try {
    const inspectResult = await client.instances.inspect({
      items: [
        {
          instanceType: "node" as const,
          space,
          externalId,
        },
      ],
      inspectionOperations: {
        involvedViews: {
          allVersions: false,
        },
      },
    });

    const involvedViews = inspectResult.items?.[0]?.inspectionResults?.involvedViews || [];

    const sources = involvedViews.slice(0, 10).map((view) => ({
      source: {
        type: "view" as const,
        space: view.space,
        externalId: view.externalId,
        version: view.version ?? "latest",
      },
    }));

    const response = await client.instances.retrieve({
      items: [
        {
          instanceType: "node" as const,
          space,
          externalId,
        },
      ],
      includeTyping: true,
      sources: sources.length > 0 ? sources : undefined,
    });

    if (response.items.length === 0) {
      return null;
    }

    const item = response.items[0];
    const properties = item.properties as Record<string, Record<string, unknown>>;
    const derivedType = deriveNodeTypeFromProperties(properties);

    return {
      space: item.space,
      externalId: item.externalId,
      instanceType: "node" as const,
      version: item.version,
      createdTime: item.createdTime,
      lastUpdatedTime: item.lastUpdatedTime,
      type: item.type || derivedType,
      properties,
    };
  } catch (error) {
    console.error("[GraphViewer] Error fetching node details:", error);
    return null;
  }
}

// =============================================================================
// fetchConnectedNodes
// =============================================================================

export interface ExpandNodeResult {
  newNodes: CDFNode[];
  newEdges: CDFEdge[];
  connectedNodeIds: string[];
}

export async function fetchConnectedNodes(
  client: CogniteClient | null,
  nodeSpace: string,
  nodeExternalId: string,
  existingNodeIds: Set<string>,
  _dataModel?: DataModelInfo,
  limit = 100,
  whitelistedRelationProps?: Set<string>,
  coreReverseQueries?: ReverseRelationQuery[],
  viewPriorityConfig?: ViewPriorityConfig
): Promise<ExpandNodeResult> {
  if (!client) {
    throw new Error("CDF client is not available. Please ensure you are authenticated.");
  }

  const extractDirectRelations = (
    properties: Record<string, Record<string, unknown>> | undefined
  ): Array<{ space: string; externalId: string }> => {
    if (!properties) return [];

    const refs: Array<{ space: string; externalId: string }> = [];
    const seen = new Set<string>();

    const processValue = (val: unknown) => {
      if (val && typeof val === "object" && "space" in val && "externalId" in val) {
        const ref = val as { space: string; externalId: string };
        if (typeof ref.space === "string" && typeof ref.externalId === "string") {
          const key = `${ref.space}:${ref.externalId}`;
          if (!seen.has(key)) {
            seen.add(key);
            refs.push({ space: ref.space, externalId: ref.externalId });
          }
        }
      } else if (Array.isArray(val)) {
        val.forEach(processValue);
      }
    };

    for (const spaceObj of Object.values(properties)) {
      if (typeof spaceObj !== "object" || spaceObj === null) continue;
      for (const viewObj of Object.values(spaceObj)) {
        if (typeof viewObj !== "object" || viewObj === null) continue;
        for (const [propName, propVal] of Object.entries(viewObj as Record<string, unknown>)) {
          if (!whitelistedRelationProps || whitelistedRelationProps.has(propName)) {
            processValue(propVal);
          }
        }
      }
    }

    return refs;
  };

  const [sourceNodeRefs, edgeResponse] = await Promise.all([
    (async () => {
      try {
        const sourceInspect = await client.instances.inspect({
          items: [
            {
              instanceType: "node" as const,
              space: nodeSpace,
              externalId: nodeExternalId,
            },
          ],
          inspectionOperations: { involvedViews: { allVersions: false } },
        });

        if (sourceInspect.items.length > 0) {
          const involvedViews =
            (
              sourceInspect.items[0] as {
                inspectionResults?: {
                  involvedViews?: Array<{
                    space: string;
                    externalId: string;
                    version: string;
                  }>;
                };
              }
            ).inspectionResults?.involvedViews || [];

          const sources = involvedViews.slice(0, 10).map((v) => ({
            source: {
              type: "view" as const,
              space: v.space,
              externalId: v.externalId,
              version: v.version,
            },
          }));

          if (sources.length > 0) {
            const sourceNodeResponse = await client.instances.retrieve({
              items: [
                {
                  instanceType: "node" as const,
                  space: nodeSpace,
                  externalId: nodeExternalId,
                },
              ],
              sources,
              includeTyping: true,
            });

            if (sourceNodeResponse.items.length > 0) {
              const props = sourceNodeResponse.items[0].properties as Record<
                string,
                Record<string, unknown>
              >;
              return extractDirectRelations(props);
            }
          }
        }
        return [];
      } catch {
        return [];
      }
    })(),

    (async () => {
      type EdgeItem = {
        space: string;
        externalId: string;
        version: number;
        createdTime: number;
        lastUpdatedTime: number;
        type: { space: string; externalId: string };
        startNode: { space: string; externalId: string };
        endNode: { space: string; externalId: string };
        properties?: Record<string, Record<string, unknown>>;
      };
      const items = await queryWithCursorPagination<EdgeItem>(
        client,
        "edges",
        {
          with: {
            edges: {
              edges: {
                filter: {
                  or: [
                    {
                      equals: {
                        property: ["edge", "startNode"],
                        value: { space: nodeSpace, externalId: nodeExternalId },
                      },
                    },
                    {
                      equals: {
                        property: ["edge", "endNode"],
                        value: { space: nodeSpace, externalId: nodeExternalId },
                      },
                    },
                  ],
                },
              },
            },
          },
          select: { edges: {} },
          includeTyping: true,
        },
        Math.min(limit * 2, QUERY_PAGE_LIMIT),
        // Hard cap: never return more than `limit` edges per expansion. This
        // upper bound protects the consumer from runaway pagination and matches
        // the documented contract of `initialConnectionLimit`.
        limit
      );
      return { items };
    })(),
  ]);

  const newEdges: CDFEdge[] = edgeResponse.items.map((edgeItem) => {
    const item = edgeItem as {
      space: string;
      externalId: string;
      version: number;
      createdTime: number;
      lastUpdatedTime: number;
      type: { space: string; externalId: string };
      startNode: { space: string; externalId: string };
      endNode: { space: string; externalId: string };
      properties?: Record<string, Record<string, unknown>>;
    };

    return {
      space: item.space,
      externalId: item.externalId,
      instanceType: "edge" as const,
      version: item.version,
      createdTime: item.createdTime,
      lastUpdatedTime: item.lastUpdatedTime,
      type: item.type,
      startNode: item.startNode,
      endNode: item.endNode,
      properties: item.properties,
    };
  });

  const connectedNodeRefs = new Map<string, { space: string; externalId: string }>();

  for (const edge of newEdges) {
    const startKey = `${edge.startNode.space}:${edge.startNode.externalId}`;
    const endKey = `${edge.endNode.space}:${edge.endNode.externalId}`;

    if (!existingNodeIds.has(startKey)) {
      connectedNodeRefs.set(startKey, edge.startNode);
    }
    if (!existingNodeIds.has(endKey)) {
      connectedNodeRefs.set(endKey, edge.endNode);
    }
  }

  const sourceNodeKey = `${nodeSpace}:${nodeExternalId}`;
  const directRelationEdges: CDFEdge[] = [];

  for (const ref of sourceNodeRefs) {
    const refKey = `${ref.space}:${ref.externalId}`;
    if (
      refKey !== sourceNodeKey &&
      !existingNodeIds.has(refKey) &&
      !connectedNodeRefs.has(refKey)
    ) {
      connectedNodeRefs.set(refKey, ref);

      directRelationEdges.push({
        space: nodeSpace,
        externalId: `synthetic:${nodeExternalId}->direct->${ref.externalId}`,
        instanceType: "edge" as const,
        version: 1,
        createdTime: Date.now(),
        lastUpdatedTime: Date.now(),
        type: { space: "synthetic", externalId: "direct-relation" },
        startNode: { space: nodeSpace, externalId: nodeExternalId },
        endNode: { space: ref.space, externalId: ref.externalId },
        properties: {},
      });
    }
  }

  newEdges.push(...directRelationEdges);

  // Reverse relation queries
  const reverseRelationRefs: Array<{ space: string; externalId: string }> = [];
  const CORE_REVERSE_QUERIES = coreReverseQueries ?? [];

  try {
    const nodeRef = { space: nodeSpace, externalId: nodeExternalId };

    // Spread the per-expansion budget across all configured reverse queries so
    // the total number of nodes contributed by reverse relations stays within
    // `limit`. Each query gets at least 1 row.
    const perQueryLimit =
      CORE_REVERSE_QUERIES.length > 0
        ? Math.max(1, Math.ceil(limit / CORE_REVERSE_QUERIES.length))
        : limit;

    const queryPromises = CORE_REVERSE_QUERIES.map(
      async ([viewSpace, viewExternalId, viewVersion, propertyName, isList]) => {
        try {
          const propertyPath = [
            viewSpace,
            `${viewExternalId}/${viewVersion}`,
            propertyName,
          ];

          const filter = isList
            ? {
                containsAny: {
                  property: propertyPath,
                  values: [nodeRef],
                },
              }
            : {
                equals: {
                  property: propertyPath,
                  value: nodeRef,
                },
              };

          const items = await queryWithCursorPagination<{
            space: string;
            externalId: string;
          }>(
            client,
            "nodes",
            {
              with: {
                nodes: {
                  nodes: { filter },
                },
              },
              select: { nodes: {} },
              includeTyping: false,
            },
            Math.min(50, perQueryLimit),
            perQueryLimit
          );
          return items.map((item) => ({
            space: item.space,
            externalId: item.externalId,
          }));
        } catch {
          return [];
        }
      }
    );

    const results = await Promise.all(queryPromises);
    for (const refs of results) {
      reverseRelationRefs.push(...refs);
    }
  } catch {
    // Silently ignore reverse relation query failures
  }

  const syntheticEdges: CDFEdge[] = [];

  for (const ref of reverseRelationRefs) {
    const refKey = `${ref.space}:${ref.externalId}`;
    if (
      refKey !== sourceNodeKey &&
      !existingNodeIds.has(refKey) &&
      !connectedNodeRefs.has(refKey)
    ) {
      connectedNodeRefs.set(refKey, ref);

      syntheticEdges.push({
        space: ref.space,
        externalId: `synthetic:${ref.externalId}->assets->${nodeExternalId}`,
        instanceType: "edge" as const,
        version: 1,
        createdTime: Date.now(),
        lastUpdatedTime: Date.now(),
        type: { space: "cdf_cdm", externalId: "references-asset" },
        startNode: { space: ref.space, externalId: ref.externalId },
        endNode: { space: nodeSpace, externalId: nodeExternalId },
        properties: {},
      });
    }
  }

  newEdges.push(...syntheticEdges);

  const connectedNodeIds = newEdges.flatMap((edge) => [
    `${edge.startNode.space}:${edge.startNode.externalId}`,
    `${edge.endNode.space}:${edge.endNode.externalId}`,
  ]);

  const nodesToFetch = Array.from(connectedNodeRefs.values()).slice(0, limit);

  let newNodes: CDFNode[] = [];

  if (nodesToFetch.length > 0) {
    const inspectResponse = await client.instances.inspect({
      items: nodesToFetch.map((ref) => ({
        instanceType: "node" as const,
        space: ref.space,
        externalId: ref.externalId,
      })),
      inspectionOperations: { involvedViews: {} },
    });

    const nodeTypeMap = new Map<string, { space: string; externalId: string }>();
    const allViews = new Map<string, { space: string; externalId: string; version: string }>();

    for (const inspectItem of inspectResponse.items) {
      const item = inspectItem as {
        space: string;
        externalId: string;
        inspectionResults?: {
          involvedViews?: Array<{
            space: string;
            externalId: string;
            version: string;
          }>;
        };
      };
      const key = `${item.space}:${item.externalId}`;
      const views = item.inspectionResults?.involvedViews || [];

      const skipViewsForProperties = viewPriorityConfig?.skipViewsForProperties
        ? new Set(viewPriorityConfig.skipViewsForProperties)
        : DEFAULT_SKIP_VIEWS_FOR_PROPERTIES;
      const skipViewsForType = viewPriorityConfig?.skipViewsForType
        ? new Set(viewPriorityConfig.skipViewsForType)
        : DEFAULT_SKIP_VIEWS_FOR_TYPE;

      for (const view of views) {
        if (!skipViewsForProperties.has(view.externalId)) {
          const viewKey = `${view.space}:${view.externalId}`;
          if (!allViews.has(viewKey)) {
            allViews.set(viewKey, view);
          }
        }
      }

      const domainView = views.find(
        (v) => !v.space.startsWith("cdf_cdm") && !skipViewsForType.has(v.externalId)
      );
      const cdmView = views.find(
        (v) => v.space.startsWith("cdf_cdm") && !skipViewsForType.has(v.externalId)
      );
      const bestView = domainView || cdmView;

      if (bestView) {
        nodeTypeMap.set(key, {
          space: bestView.space,
          externalId: bestView.externalId,
        });
      }
    }

    const sources = [
      {
        source: {
          type: "view" as const,
          space: "cdf_cdm",
          externalId: "CogniteDescribable",
          version: "v1",
        },
      },
      ...Array.from(allViews.values()).map((v) => ({
        source: {
          type: "view" as const,
          space: v.space,
          externalId: v.externalId,
          version: v.version,
        },
      })),
    ];

    const retrieveItems = nodesToFetch.map((ref) => ({
      instanceType: "node" as const,
      space: ref.space,
      externalId: ref.externalId,
    }));

    let nodeResponse: Awaited<ReturnType<CogniteClient["instances"]["retrieve"]>>;
    try {
      nodeResponse = await client.instances.retrieve({
        items: retrieveItems,
        includeTyping: true,
      });
    } catch {
      nodeResponse = { items: [] };
    }

    const propertiesMap = new Map<string, Record<string, Record<string, unknown>>>();
    if (sources.length > 0 && nodeResponse.items.length > 0) {
      try {
        const propsResponse = await client.instances.retrieve({
          items: retrieveItems,
          sources,
          includeTyping: true,
        });

        for (const item of propsResponse.items) {
          const key = `${item.space}:${item.externalId}`;
          if (item.properties) {
            propertiesMap.set(key, item.properties as Record<string, Record<string, unknown>>);
          }
        }
      } catch {
        // Continue without additional properties
      }
    }

    const nodesWithoutProps = retrieveItems.filter((item) => {
      const key = `${item.space}:${item.externalId}`;
      return !propertiesMap.has(key);
    });

    if (nodesWithoutProps.length > 0) {
      const individualFetches = nodesWithoutProps.map(async (nodeRef) => {
        const key = `${nodeRef.space}:${nodeRef.externalId}`;
        try {
          const inspectResult = await client.instances.inspect({
            items: [nodeRef],
            inspectionOperations: { involvedViews: { allVersions: false } },
          });

          const views =
            (
              inspectResult.items?.[0] as {
                inspectionResults?: {
                  involvedViews?: Array<{
                    space: string;
                    externalId: string;
                    version: string;
                  }>;
                };
              }
            )?.inspectionResults?.involvedViews || [];

          if (views.length > 0) {
            const nodeSources = views.slice(0, 10).map((v) => ({
              source: {
                type: "view" as const,
                space: v.space,
                externalId: v.externalId,
                version: v.version,
              },
            }));

            const propsResp = await client.instances.retrieve({
              items: [nodeRef],
              sources: nodeSources,
              includeTyping: true,
            });

            if (propsResp.items.length > 0 && propsResp.items[0].properties) {
              propertiesMap.set(
                key,
                propsResp.items[0].properties as Record<string, Record<string, unknown>>
              );
            }
          }
        } catch {
          // Silently ignore individual fetch failures
        }
      });

      await Promise.all(individualFetches);
    }

    newNodes = nodeResponse.items
      .filter(
        (item): item is typeof item & { instanceType: "node" } =>
          (item as { instanceType?: string }).instanceType === "node" || !("instanceType" in item)
      )
      .map((item) => {
        const key = `${item.space}:${item.externalId}`;
        const inspectType = nodeTypeMap.get(key);
        const mergedProps =
          propertiesMap.get(key) || (item.properties as Record<string, Record<string, unknown>>);
        const derivedType =
          deriveNodeTypeFromProperties(mergedProps, viewPriorityConfig) || inspectType;

        return {
          space: item.space,
          externalId: item.externalId,
          instanceType: "node" as const,
          version: item.version,
          createdTime: item.createdTime,
          lastUpdatedTime: item.lastUpdatedTime,
          type: item.type || derivedType,
          properties: mergedProps,
        };
      });
  }

  return { newNodes, newEdges, connectedNodeIds };
}

// =============================================================================
// Graph utility functions (pure, no API calls)
// =============================================================================

export function getGraphStats(graphData: GraphData) {
  const nodeTypes = new Map<string, number>();
  const connectionTypes = new Map<string, number>();

  graphData.nodes.forEach((node) => {
    const type = node.data.type?.externalId || "Unknown";
    nodeTypes.set(type, (nodeTypes.get(type) || 0) + 1);
  });

  graphData.connections.forEach((connection) => {
    const type = connection.data.type.externalId;
    connectionTypes.set(type, (connectionTypes.get(type) || 0) + 1);
  });

  return {
    totalNodes: graphData.nodes.length,
    totalConnections: graphData.connections.length,
    nodeTypes: Object.fromEntries(nodeTypes),
    connectionTypes: Object.fromEntries(connectionTypes),
  };
}

export function getConnectedNodes(graphData: GraphData, nodeId: string) {
  const connectedNodeIds = new Set<string>();

  graphData.connections.forEach((connection) => {
    if (connection.source === nodeId) {
      connectedNodeIds.add(connection.target);
    }
    if (connection.target === nodeId) {
      connectedNodeIds.add(connection.source);
    }
  });

  return graphData.nodes.filter((node) => connectedNodeIds.has(node.id));
}

export function getNodeEdges(graphData: GraphData, nodeId: string) {
  return graphData.connections.filter(
    (connection) => connection.source === nodeId || connection.target === nodeId
  );
}
