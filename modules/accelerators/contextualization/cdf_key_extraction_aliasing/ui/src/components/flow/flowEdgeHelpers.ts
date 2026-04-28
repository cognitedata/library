import { addEdge, type Connection, type Edge, type Node } from "@xyflow/react";
import { keaFlowEdgeVisualDefaults, type FlowEdgeData } from "./flowDocumentBridge";
import { keaValidationRuleLayoutRfTypes } from "./flowConstants";

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
      ...keaFlowEdgeVisualDefaults,
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
export function appendKeaConnectionEdge(getNode: GetNode, edges: Edge[], params: Connection): Edge[] {
  const srcType = getNode(params.source)?.type;
  const tgtType = getNode(params.target)?.type;
  let edgeKind: FlowEdgeData["kind"] = "data";
  if (srcType === "keaAliasing" && tgtType === "keaAliasing") {
    const existingAliasingChainOut = edges.some((e) => {
      if (e.source !== params.source) return false;
      if (e.target === params.target) return false;
      return getNode(e.target)?.type === "keaAliasing";
    });
    edgeKind = existingAliasingChainOut ? "parallel_group" : "sequence";
  } else if (
    srcType &&
    tgtType &&
    keaValidationRuleLayoutRfTypes.has(srcType) &&
    keaValidationRuleLayoutRfTypes.has(tgtType)
  ) {
    const existingValidationRuleChainOut = edges.some((e) => {
      if (e.source !== params.source) return false;
      if (e.target === params.target) return false;
      const t = getNode(e.target)?.type;
      return Boolean(t && keaValidationRuleLayoutRfTypes.has(t));
    });
    edgeKind = existingValidationRuleChainOut ? "parallel_group" : "sequence";
  }
  return addEdge(
    {
      ...params,
      ...keaFlowEdgeVisualDefaults,
      data: { kind: edgeKind } satisfies FlowEdgeData,
    },
    edges
  );
}
