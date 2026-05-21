import type { JsonObject } from "../types/scopeConfig";
import type { WorkflowCanvasDocument, WorkflowCanvasNode, WorkflowCanvasNodeData } from "../types/workflowCanvas";
import { newNodeId } from "../components/flow/flowDocumentBridge";
import {
  defaultValidationStep,
  serializeValidationNodeConfig,
} from "./validationNodeConfigModel";

export type ValidationNodeRef = {
  node: WorkflowCanvasNode;
  /** Subgraph node ids from root to the canvas that directly contains *node*. */
  subgraphPath: string[];
};

export function defaultValidationNodeConfig(): Record<string, unknown> {
  const step = defaultValidationStep([]);
  return serializeValidationNodeConfig({
    description: "Validation",
    minConfidence: "0.5",
    expressionMatch: "",
    executionMode: "ordered",
    steps: [step],
    extras: {
      validate_fields: ["aliases"],
      initial_confidence: 1.0,
    },
  });
}

function walkValidationNodes(
  doc: WorkflowCanvasDocument,
  subgraphPath: string[],
  out: ValidationNodeRef[]
): void {
  for (const n of doc.nodes) {
    if (n.kind === "validation") {
      out.push({ node: n, subgraphPath: [...subgraphPath] });
    }
    if (n.kind === "subgraph") {
      const inner = n.data?.inner_canvas;
      if (inner && typeof inner === "object" && Array.isArray(inner.nodes)) {
        walkValidationNodes(inner, [...subgraphPath, n.id], out);
      }
    }
  }
}

/** All ``kind: validation`` nodes on the root canvas and inside nested subgraphs. */
export function listValidationNodeRefs(canvas: WorkflowCanvasDocument): ValidationNodeRef[] {
  const out: ValidationNodeRef[] = [];
  walkValidationNodes(canvas, [], out);
  return out;
}

export function listValidationNodes(canvas: WorkflowCanvasDocument): WorkflowCanvasNode[] {
  return listValidationNodeRefs(canvas).map((r) => r.node);
}

export function validationNodeLocationKey(ref: ValidationNodeRef): string {
  const path = ref.subgraphPath.length ? ref.subgraphPath.join("/") : "@";
  return `${path}:${ref.node.id}`;
}

export function findValidationNodeRef(
  canvas: WorkflowCanvasDocument,
  nodeId: string,
  subgraphPath: string[] = []
): ValidationNodeRef | null {
  return (
    listValidationNodeRefs(canvas).find(
      (r) =>
        r.node.id === nodeId &&
        r.subgraphPath.length === subgraphPath.length &&
        r.subgraphPath.every((id, i) => id === subgraphPath[i])
    ) ?? null
  );
}

export function validationNodeContainerLabel(
  canvas: WorkflowCanvasDocument,
  subgraphPath: string[]
): string {
  if (subgraphPath.length === 0) return "";
  let doc = canvas;
  const parts: string[] = [];
  for (const sgId of subgraphPath) {
    const sg = doc.nodes.find((n) => n.id === sgId && n.kind === "subgraph");
    const label = String(sg?.data?.label ?? sgId).trim() || sgId;
    parts.push(label);
    const inner = sg?.data?.inner_canvas;
    if (!inner || typeof inner !== "object" || !Array.isArray(inner.nodes)) break;
    doc = inner;
  }
  return parts.join(" › ");
}

function updateCanvasAtPath(
  canvas: WorkflowCanvasDocument,
  subgraphPath: string[],
  updater: (doc: WorkflowCanvasDocument) => WorkflowCanvasDocument
): WorkflowCanvasDocument {
  if (subgraphPath.length === 0) {
    return updater(canvas);
  }
  const [head, ...rest] = subgraphPath;
  return {
    ...canvas,
    nodes: canvas.nodes.map((n) => {
      if (n.id !== head || n.kind !== "subgraph") {
        return n;
      }
      const inner = n.data?.inner_canvas;
      if (!inner || typeof inner !== "object") {
        return n;
      }
      const nextInner = updateCanvasAtPath(inner, rest, updater);
      return {
        ...n,
        data: {
          ...(n.data ?? {}),
          inner_canvas: nextInner,
        },
      };
    }),
  };
}

export function readValidationConfig(node: WorkflowCanvasNode): JsonObject {
  const data = node.data ?? {};
  const cfg = data.config;
  if (cfg && typeof cfg === "object" && !Array.isArray(cfg)) {
    return { ...(cfg as JsonObject) };
  }
  return {};
}

export function patchValidationNode(
  canvas: WorkflowCanvasDocument,
  nodeId: string,
  nextConfig: JsonObject,
  subgraphPath: string[] = []
): WorkflowCanvasDocument {
  return updateCanvasAtPath(canvas, subgraphPath, (doc) => ({
    ...doc,
    nodes: doc.nodes.map((n) => {
      if (n.id !== nodeId) return n;
      const data: WorkflowCanvasNodeData = {
        ...(n.data ?? {}),
        config: nextConfig,
        handler_family: "discovery",
      };
      return { ...n, data };
    }),
  }));
}

export function addValidationNode(
  canvas: WorkflowCanvasDocument,
  opts?: { subgraphPath?: string[] }
): {
  canvas: WorkflowCanvasDocument;
  nodeId: string;
} {
  const subgraphPath = opts?.subgraphPath ?? [];
  const id = newNodeId();
  const config = defaultValidationNodeConfig();
  const node: WorkflowCanvasNode = {
    id,
    kind: "validation",
    position: { x: 0, y: 0 },
    data: {
      label: "Validation",
      handler_family: "discovery",
      config,
    },
  };
  const next = updateCanvasAtPath(canvas, subgraphPath, (doc) => ({
    ...doc,
    nodes: [...doc.nodes, node],
  }));
  return { canvas: next, nodeId: id };
}

export function removeValidationNode(
  canvas: WorkflowCanvasDocument,
  nodeId: string,
  subgraphPath: string[] = []
): WorkflowCanvasDocument {
  return updateCanvasAtPath(canvas, subgraphPath, (doc) => ({
    ...doc,
    nodes: doc.nodes.filter((n) => n.id !== nodeId),
    edges: doc.edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
  }));
}

export function validationNodeListLabel(node: WorkflowCanvasNode): string {
  const cfg = readValidationConfig(node);
  const desc = String(cfg.description ?? "").trim();
  if (desc) return desc;
  const label = String(node.data?.label ?? "").trim();
  if (label) return label;
  return node.id;
}
