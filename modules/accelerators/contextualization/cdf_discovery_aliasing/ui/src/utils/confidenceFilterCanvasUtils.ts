import type { WorkflowCanvasDocument, WorkflowCanvasNode } from "../types/workflowCanvas";

export type ConfidenceFilterNodeRef = {
  node: WorkflowCanvasNode;
  subgraphPath: string[];
};

export function defaultConfidenceFilterNodeConfig(): Record<string, unknown> {
  return {
    description: "Confidence filter",
    value_field: "aliases",
    min_confidence: 0.8,
    comparison: "gte",
    drop_row_if_empty: true,
  };
}

function walkConfidenceFilterNodes(
  doc: WorkflowCanvasDocument,
  subgraphPath: string[],
  out: ConfidenceFilterNodeRef[]
): void {
  for (const n of doc.nodes) {
    if (n.kind === "confidence_filter") {
      out.push({ node: n, subgraphPath: [...subgraphPath] });
    }
    if (n.kind === "subgraph") {
      const inner = n.data?.inner_canvas;
      if (inner && typeof inner === "object" && Array.isArray(inner.nodes)) {
        walkConfidenceFilterNodes(inner, [...subgraphPath, n.id], out);
      }
    }
  }
}

export function findConfidenceFilterNodeRef(
  canvas: WorkflowCanvasDocument,
  nodeId: string
): ConfidenceFilterNodeRef | null {
  const id = nodeId.trim();
  if (!id) return null;
  const out: ConfidenceFilterNodeRef[] = [];
  walkConfidenceFilterNodes(canvas, [], out);
  return out.find((r) => r.node.id === id) ?? null;
}

export function patchConfidenceFilterNodeById(
  canvas: WorkflowCanvasDocument,
  nodeId: string,
  config: Record<string, unknown>
): WorkflowCanvasDocument {
  const ref = findConfidenceFilterNodeRef(canvas, nodeId);
  if (!ref) {
    return {
      ...canvas,
      nodes: canvas.nodes.map((n) =>
        n.id === nodeId && n.kind === "confidence_filter"
          ? {
              ...n,
              data: {
                ...n.data,
                config,
                label:
                  String(config.description ?? n.data?.label ?? "Confidence filter").trim() ||
                  "Confidence filter",
              },
            }
          : n
      ),
    };
  }
  return patchConfidenceFilterNode(canvas, ref, config);
}

export function readConfidenceFilterConfigFromData(
  data: WorkflowCanvasNode["data"] | undefined
): Record<string, unknown> {
  const cfg = data?.config;
  if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) {
    return defaultConfidenceFilterNodeConfig();
  }
  const out = { ...defaultConfidenceFilterNodeConfig(), ...(cfg as Record<string, unknown>) };
  return out;
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

export function patchConfidenceFilterNode(
  canvas: WorkflowCanvasDocument,
  ref: ConfidenceFilterNodeRef,
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
              label:
                String(config.description ?? n.data?.label ?? "Confidence filter").trim() ||
                "Confidence filter",
            },
          }
        : n
    ),
  }));
}
