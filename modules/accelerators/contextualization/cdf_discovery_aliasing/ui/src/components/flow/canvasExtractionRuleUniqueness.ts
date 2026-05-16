import type { WorkflowCanvasDocument } from "../../types/workflowCanvas";
import { expandCanvasForScopeSync } from "./subgraphBoundaryVirtualization";

function isRecord(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function extractionRuleNameFromNodeData(data: unknown): string | undefined {
  if (!isRecord(data)) return undefined;
  const ref = data.ref;
  if (!isRecord(ref)) return undefined;
  const v = ref.extraction_rule_name;
  return typeof v === "string" && v.trim() ? v.trim() : undefined;
}

/**
 * Returns a human-readable error when two ``kind: extraction`` nodes share the same
 * ``ref.extraction_rule_name`` (after subgraph flattening — matches Python ``compile_canvas_dag``).
 */
export function validateUniqueExtractionRuleNamesOnCanvas(
  canvas: WorkflowCanvasDocument
): string | null {
  const expanded = expandCanvasForScopeSync(canvas);
  const seen = new Map<string, string>();
  for (const n of expanded.nodes) {
    if (n.kind !== "extraction") continue;
    const name = extractionRuleNameFromNodeData(n.data as unknown);
    if (!name) continue;
    const prev = seen.get(name);
    if (prev !== undefined) {
      return `Duplicate extraction rule name "${name}" on flow nodes ${prev} and ${n.id}. Each extraction node must reference a distinct rule.`;
    }
    seen.set(name, n.id);
  }
  return null;
}
