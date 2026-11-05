/**
 * Map DMS list API nodes to app SummaryInstance (typed, no casts on list items).
 */

import type { NodeDefinition } from "@cognite/sdk";
import type { SummaryInstance } from "@/types/operations";

function isNodeDefinition(item: unknown): item is NodeDefinition {
  return (
    typeof item === "object" &&
    item !== null &&
    (item as { instanceType?: string }).instanceType === "node"
  );
}

export function nodeToSummaryInstance(node: NodeDefinition): SummaryInstance {
  return {
    externalId: String(node.externalId),
    space: String(node.space),
    createdTime: node.createdTime,
    properties: (node.properties ?? {}) as Record<
      string,
      Record<string, unknown>
    >,
  };
}

/** Stable React list key: DMS node identity (space + externalId). */
export function summaryInstanceNodeKey(summary: SummaryInstance): string {
  return `${summary.space}\0${summary.externalId}`;
}

/** Unit separator — not allowed in CDF space / externalId; safe in <option value>. */
const TEST_BASELINE_KEY_SEP = "\u001f";

/** Encode space + externalId for Tests page baseline `<select>` (avoids wrong row on id-only match). */
export function encodeTestBaselineSelectionKey(
  summary: SummaryInstance,
): string {
  return `${summary.space}${TEST_BASELINE_KEY_SEP}${summary.externalId}`;
}

/** Resolve picker selection to the exact summary row. Falls back to externalId-only for legacy persisted state. */
export function resolveSummaryFromTestSelection(
  summaries: SummaryInstance[],
  selectedKey: string,
): SummaryInstance | undefined {
  const sep = TEST_BASELINE_KEY_SEP;
  const idx = selectedKey.indexOf(sep);
  if (idx > 0) {
    const space = selectedKey.slice(0, idx);
    const externalId = selectedKey.slice(idx + sep.length);
    return summaries.find(
      (s) => s.space === space && s.externalId === externalId,
    );
  }
  return summaries.find((s) => s.externalId === selectedKey);
}

/**
 * Drops duplicate rows with the same DMS node id (e.g. repeated query rows).
 * Preserves first occurrence order.
 */
export function dedupeSummaryInstancesByNodeId(
  items: SummaryInstance[],
): SummaryInstance[] {
  const seen = new Set<string>();
  const out: SummaryInstance[] = [];
  for (const s of items) {
    const k = summaryInstanceNodeKey(s);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(s);
  }
  return out;
}

export function mapNodesToSummaryInstances(
  items: unknown[] | undefined | null,
): SummaryInstance[] {
  const list = items ?? [];
  return list.filter(isNodeDefinition).map(nodeToSummaryInstance);
}

export function firstSummaryInstanceOrNull(
  items: unknown[] | undefined | null,
): SummaryInstance | null {
  const list = items ?? [];
  const first = list.find(isNodeDefinition);
  return first ? nodeToSummaryInstance(first) : null;
}
