import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import { configSummaryForKind } from "./EtlNodeConfigFields";

/** Summary text shown on the canvas node card (config summary, then notes). */
export function etlFlowNodeCanvasDescription(
  kind: TransformCanvasNodeKind,
  data: Record<string, unknown> | undefined
): string {
  const d = data ?? {};
  const config =
    d.config && typeof d.config === "object" && !Array.isArray(d.config)
      ? (d.config as Record<string, unknown>)
      : {};
  const summary = configSummaryForKind(kind, config).trim();
  if (summary) return summary;
  const desc = String(config.description ?? "").trim();
  if (desc) return desc;
  const notes = d.notes != null ? String(d.notes).trim() : "";
  return notes;
}
