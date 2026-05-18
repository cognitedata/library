import type { WorkflowCanvasDocument, WorkflowCanvasNode } from "../types/workflowCanvas";
import { defaultFilterNodeFilters, readFilters } from "./filtersConfigModel";

export type FilterNodeRef = {
  node: WorkflowCanvasNode;
  subgraphPath: string[];
};

export function defaultFilterNodeConfig(): Record<string, unknown> {
  return {
    description: "Instance filter",
    filters: defaultFilterNodeFilters(),
  };
}

function walkFilterNodes(
  doc: WorkflowCanvasDocument,
  subgraphPath: string[],
  out: FilterNodeRef[]
): void {
  for (const n of doc.nodes) {
    if (n.kind === "instance_filter") {
      out.push({ node: n, subgraphPath: [...subgraphPath] });
    }
    if (n.kind === "subgraph") {
      const inner = n.data?.inner_canvas;
      if (inner && typeof inner === "object" && Array.isArray(inner.nodes)) {
        walkFilterNodes(inner, [...subgraphPath, n.id], out);
      }
    }
  }
}

export function listFilterNodeRefs(canvas: WorkflowCanvasDocument): FilterNodeRef[] {
  const out: FilterNodeRef[] = [];
  walkFilterNodes(canvas, [], out);
  return out;
}

/** Locate a filter node by canvas id (root canvas and nested subgraphs). */
export function findFilterNodeRef(
  canvas: WorkflowCanvasDocument,
  nodeId: string
): FilterNodeRef | null {
  const id = nodeId.trim();
  if (!id) return null;
  return listFilterNodeRefs(canvas).find((r) => r.node.id === id) ?? null;
}

/** Patch filter config when only the node id is known (falls back to top-level nodes). */
export function patchFilterNodeById(
  canvas: WorkflowCanvasDocument,
  nodeId: string,
  config: Record<string, unknown>
): WorkflowCanvasDocument {
  const ref = findFilterNodeRef(canvas, nodeId);
  if (ref) return patchFilterNode(canvas, ref, config);
  return {
    ...canvas,
    nodes: canvas.nodes.map((n) =>
      n.id === nodeId && n.kind === "instance_filter"
        ? {
            ...n,
            data: {
              ...n.data,
              config,
              label: String(config.description ?? n.data?.label ?? "Instance filter").trim() || "Instance filter",
            },
          }
        : n
    ),
  };
}

export function filterNodeLocationKey(ref: FilterNodeRef): string {
  const path = ref.subgraphPath.length ? ref.subgraphPath.join("/") : "@";
  return `${path}:${ref.node.id}`;
}

export function readFilterConfigFromData(
  data: WorkflowCanvasNode["data"] | undefined
): Record<string, unknown> {
  const cfg = data?.config;
  if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) {
    return defaultFilterNodeConfig();
  }
  const out = { ...(cfg as Record<string, unknown>) };
  if (readFilters(out).length === 0) {
    out.filters = defaultFilterNodeFilters();
  }
  return out;
}

export function readFilterConfig(node: WorkflowCanvasNode): Record<string, unknown> {
  return readFilterConfigFromData(node.data);
}

function updateCanvasAtPath(
  canvas: WorkflowCanvasDocument,
  subgraphPath: string[],
  updater: (doc: WorkflowCanvasDocument) => WorkflowCanvasDocument
): WorkflowCanvasDocument {
  if (subgraphPath.length === 0) return updater(canvas);
  const [head, ...rest] = subgraphPath;
  return {
    ...canvas,
    nodes: canvas.nodes.map((n) => {
      if (n.id !== head || n.kind !== "subgraph") return n;
      const inner = n.data?.inner_canvas;
      if (!inner || typeof inner !== "object" || !Array.isArray(inner.nodes)) return n;
      return {
        ...n,
        data: {
          ...n.data,
          inner_canvas: updateCanvasAtPath(inner, rest, updater),
        },
      };
    }),
  };
}

export function patchFilterNode(
  canvas: WorkflowCanvasDocument,
  ref: FilterNodeRef,
  config: Record<string, unknown>
): WorkflowCanvasDocument {
  return updateCanvasAtPath(canvas, ref.subgraphPath, (doc) => ({
    ...doc,
    nodes: doc.nodes.map((n) =>
      n.id === ref.node.id
        ? {
            ...n,
            data: {
              ...n.data,
              config,
              label: String(config.description ?? n.data?.label ?? "Instance filter").trim() || "Instance filter",
            },
          }
        : n
    ),
  }));
}

export function filterNodeListLabel(node: WorkflowCanvasNode): string {
  const cfg = readFilterConfig(node);
  const dsc = cfg.description != null ? String(cfg.description).trim() : "";
  return dsc || node.id;
}
