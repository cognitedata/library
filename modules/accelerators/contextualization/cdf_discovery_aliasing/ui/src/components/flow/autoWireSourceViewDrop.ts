import type { Edge, Node } from "@xyflow/react";

export type AutoWireSourceViewDropResult = {
  edges: Edge[];
  /** Discovery canvas does not merge extraction rules on source-view drop. */
  mergeExtractionRules: string[];
};

/**
 * After dropping a new source view node: discovery canvas does not auto-wire edges;
 * authors connect ``query_view`` / other stages explicitly.
 */
export function buildAutoWiredEdgesForDroppedSourceView(_opts: {
  nodes: Node[];
  edges: Edge[];
  newSvNode: Node;
  svIndex: number;
  scopeDoc: Record<string, unknown>;
}): AutoWireSourceViewDropResult {
  return { edges: [], mergeExtractionRules: [] };
}
