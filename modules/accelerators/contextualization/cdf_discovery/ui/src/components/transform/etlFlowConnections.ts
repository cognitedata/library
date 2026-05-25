import type { Connection, Edge, Node } from "@xyflow/react";
import { etlPersistenceOutboundToEndOnlyRfTypes } from "./transformFlowConstants";

const STRUCTURAL = new Set(["etlStart", "etlEnd"]);

/** Basic ETL canvas connection rules for React Flow. */
export function isValidEtlFlowConnection(connection: Connection, getNode: (id: string) => Node | undefined): boolean {
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
