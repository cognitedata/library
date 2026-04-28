import { appendSourceViewToExtractionAssociation } from "./workflowScopeAssociations";

type FlowNodeLike = { type?: string | null; data?: unknown } | undefined;

/**
 * When a data edge is added from a source view node to an extraction node, align scope
 * (same behavior as ``onConnect`` in the flow panel).
 *
 * Appends ``associations`` (``source_view_to_extraction``) for this source view index and rule.
 */
export function patchScopeForSourceViewToExtractionConnection(
  patchWorkflowScope: (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => void,
  sourceNode: FlowNodeLike,
  targetNode: FlowNodeLike
): void {
  if (sourceNode?.type !== "keaSourceView" || targetNode?.type !== "keaExtraction") return;
  const sData = (sourceNode.data ?? {}) as Record<string, unknown>;
  const tData = (targetNode.data ?? {}) as Record<string, unknown>;
  const refS = sData.ref;
  const refT = tData.ref;
  const svIdx =
    refS && typeof refS === "object" && !Array.isArray(refS)
      ? (refS as Record<string, unknown>).source_view_index
      : undefined;
  const ruleName =
    refT && typeof refT === "object" && !Array.isArray(refT)
      ? (refT as Record<string, unknown>).extraction_rule_name
      : undefined;
  if (typeof svIdx === "number" && ruleName != null && String(ruleName).trim()) {
    const rn = String(ruleName).trim();
    patchWorkflowScope((doc) =>
      appendSourceViewToExtractionAssociation(doc, { source_view_index: svIdx, extraction_rule_name: rn })
    );
  }
}
