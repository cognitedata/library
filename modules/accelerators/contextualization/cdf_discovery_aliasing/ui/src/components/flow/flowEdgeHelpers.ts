import { addEdge, type Connection, type Edge, type Node } from "@xyflow/react";
import { discoveryFlowEdgeVisualDefaults, type FlowEdgeData } from "./flowDocumentBridge";
import { discoveryValidationRuleLayoutRfTypes } from "./flowConstants";

/** First ``discoveryEnd`` node id in document order, if any. */
export function findFirstDiscoveryEndNodeId(nodes: readonly Pick<Node, "id" | "type">[]): string | null {
  for (const n of nodes) {
    if (n.type === "discoveryEnd") return n.id;
  }
  return null;
}

/** Primary data edge from a persistence node ``out`` to ``discoveryEnd`` ``in``. */
export function buildPersistenceOutboundToEndDataEdge(persistenceNodeId: string, endNodeId: string): Edge {
  return {
    ...discoveryFlowEdgeVisualDefaults,
    id: `e_${persistenceNodeId}_${endNodeId}_persistenceEnd_${Date.now()}`,
    source: persistenceNodeId,
    sourceHandle: "out",
    target: endNodeId,
    targetHandle: "in",
    data: { kind: "data" } satisfies FlowEdgeData,
  };
}

export function dedupeEdgesByHandles(edges: Edge[]): Edge[] {
  const seen = new Set<string>();
  const out: Edge[] = [];
  for (const e of edges) {
    const k = `${e.source}\0${e.sourceHandle ?? ""}\0${e.target}\0${e.targetHandle ?? ""}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(e);
  }
  return out;
}

/** Data edge from reuse-drop wiring (palette / match-validation reuse). */
export function appendReuseDataEdge(edges: Edge[], sourceId: string, targetId: string): Edge[] {
  if (edges.some((x) => x.source === sourceId && x.target === targetId)) return edges;
  return addEdge(
    {
      ...discoveryFlowEdgeVisualDefaults,
      id: `e_${sourceId}_${targetId}_${Date.now()}`,
      source: sourceId,
      target: targetId,
      data: { kind: "data" } satisfies FlowEdgeData,
    },
    edges
  );
}

type GetNode = (id: string) => Node | undefined;

/**
 * Append a connection edge with `data.kind` set for aliasing and validation-rule layout chains.
 */
export function appendDiscoveryConnectionEdge(getNode: GetNode, edges: Edge[], params: Connection): Edge[] {
  const srcType = getNode(params.source)?.type;
  const tgtType = getNode(params.target)?.type;
  let edgeKind: FlowEdgeData["kind"] = "data";
  if (srcType === "discoveryTransform" && tgtType === "discoveryTransform") {
    const existing = edges.some((e) => {
      if (e.source !== params.source) return false;
      if (e.target === params.target) return false;
      return getNode(e.target)?.type === "discoveryTransform";
    });
    edgeKind = existing ? "parallel_group" : "sequence";
  } else if (srcType === "discoveryValidate" && tgtType === "discoveryValidate") {
    const existing = edges.some((e) => {
      if (e.source !== params.source) return false;
      if (e.target === params.target) return false;
      return getNode(e.target)?.type === "discoveryValidate";
    });
    edgeKind = existing ? "parallel_group" : "sequence";
  } else if (
    srcType &&
    tgtType &&
    discoveryValidationRuleLayoutRfTypes.has(srcType) &&
    discoveryValidationRuleLayoutRfTypes.has(tgtType)
  ) {
    const existingValidationRuleChainOut = edges.some((e) => {
      if (e.source !== params.source) return false;
      if (e.target === params.target) return false;
      const t = getNode(e.target)?.type;
      return Boolean(t && discoveryValidationRuleLayoutRfTypes.has(t));
    });
    edgeKind = existingValidationRuleChainOut ? "parallel_group" : "sequence";
  }
  return addEdge(
    {
      ...params,
      ...discoveryFlowEdgeVisualDefaults,
      data: { kind: edgeKind } satisfies FlowEdgeData,
    },
    edges
  );
}
