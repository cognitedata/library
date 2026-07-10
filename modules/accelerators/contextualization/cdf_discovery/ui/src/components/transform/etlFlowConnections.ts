import type { Connection, Edge, Node } from "@xyflow/react";
import { isCohortConsumerRfType, isCohortSourceRfType } from "./cohortSourceRfTypes";
import { etlPersistenceOutboundToEndOnlyRfTypes } from "./transformFlowConstants";

const STRUCTURAL = new Set(["etlStart", "etlEnd"]);

/** Target handles that accept at most one incoming data edge (matches canvas compile rules). */
const SINGLE_INPUT_TARGET_HANDLES: Readonly<Record<string, ReadonlySet<string>>> = {
  etlWorkflowFanoutPlan: new Set(["in__input_b"]),
  etlJoin: new Set(["in__left", "in__right"]),
  etlFileAnnotation: new Set(["in__files"]),
};

function targetHandleAlreadyWired(
  edges: readonly Edge[],
  targetId: string,
  targetHandle: string,
  excludeEdgeId?: string
): boolean {
  return edges.some(
    (e) =>
      e.id !== excludeEdgeId &&
      e.target === targetId &&
      (e.targetHandle ?? "in") === targetHandle
  );
}

/** Basic ETL canvas connection rules for React Flow. */
export function isValidEtlFlowConnection(
  connection: Connection,
  getNode: (id: string) => Node | undefined,
  edges: readonly Edge[] = [],
  options?: { excludeEdgeId?: string }
): boolean {
  const src = getNode(connection.source);
  const tgt = getNode(connection.target);
  if (!src?.type || !tgt?.type) return false;
  if (src.type === "etlEnd" || tgt.type === "etlStart") return false;
  if (connection.source === connection.target) return false;
  if (STRUCTURAL.has(src.type) && src.type === "etlEnd") return false;
  if (STRUCTURAL.has(tgt.type) && tgt.type === "etlStart") return false;
  if (etlPersistenceOutboundToEndOnlyRfTypes.has(src.type)) {
    return tgt.type === "etlEnd";
  }
  if (isCohortConsumerRfType(tgt.type) && !isCohortSourceRfType(src.type)) {
    return false;
  }
  const targetHandle = connection.targetHandle ?? "in";
  const singleHandles = SINGLE_INPUT_TARGET_HANDLES[tgt.type];
  if (
    singleHandles?.has(targetHandle) &&
    targetHandleAlreadyWired(edges, connection.target, targetHandle, options?.excludeEdgeId)
  ) {
    return false;
  }
  return true;
}

export function wouldCreateCycle(edges: Edge[], source: string, target: string): boolean {
  const adj = new Map<string, Set<string>>();
  for (const e of edges) {
    if (!adj.has(e.source)) adj.set(e.source, new Set());
    adj.get(e.source)!.add(e.target);
  }
  if (!adj.has(source)) adj.set(source, new Set());
  adj.get(source)!.add(target);
  const stack = [target];
  const seen = new Set<string>();
  while (stack.length) {
    const n = stack.pop()!;
    if (n === source) return true;
    if (seen.has(n)) continue;
    seen.add(n);
    for (const next of adj.get(n) ?? []) stack.push(next);
  }
  return false;
}
