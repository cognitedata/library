import type { TransformHandlerCategoryId } from "../components/transform/etlHandlerRegistry";
import { TRANSFORM_HANDLER_DEFINITIONS } from "../components/transform/etlHandlerRegistry";
import type { TransformCanvasNodeKind } from "../types/transformCanvas";
import { FUSION_NODE_KINDS, PIPELINE_ORCHESTRATION_NODE_KINDS } from "./transformNodeEditorKinds";

/** Palette / canvas accent groups (main palette sections and transform handler categories). */
export type EtlPaletteGroupId =
  | "extract"
  | "transform_string"
  | "transform_structure"
  | "transform_derive"
  | "orchestration"
  | "contextualization"
  | "load"
  | "fusion"
  | "debug";

const PALETTE_GROUP_HEX: Record<EtlPaletteGroupId, string> = {
  extract: "#2563eb",
  transform_string: "#0891b2",
  transform_structure: "#7c3aed",
  transform_derive: "#db2777",
  orchestration: "#475569",
  contextualization: "#d97706",
  load: "#16a34a",
  fusion: "#ca8a04",
  debug: "#9333ea",
};

const FUSION_KIND_SET = new Set<string>(FUSION_NODE_KINDS);
const ORCHESTRATION_KIND_SET = new Set<string>(PIPELINE_ORCHESTRATION_NODE_KINDS);

export function paletteGroupCssVarName(id: EtlPaletteGroupId): string {
  return `--etl-palette-${id}`;
}

export function paletteGroupHex(id: EtlPaletteGroupId): string {
  return PALETTE_GROUP_HEX[id];
}

export function paletteGroupCssVar(id: EtlPaletteGroupId): string {
  return `var(${paletteGroupCssVarName(id)}, ${paletteGroupHex(id)})`;
}

export function handlerCategoryPaletteGroupId(
  category: TransformHandlerCategoryId | string
): EtlPaletteGroupId {
  if (category === "structure") return "transform_structure";
  if (category === "derive") return "transform_derive";
  if (category === "handlers") return "contextualization";
  return "transform_string";
}

export function paletteGroupIdForTransformHandler(handlerId: string): EtlPaletteGroupId {
  const def = TRANSFORM_HANDLER_DEFINITIONS.find((d) => d.id === handlerId);
  if (!def) return "transform_string";
  return handlerCategoryPaletteGroupId(def.category);
}

export function paletteGroupIdForStage(
  stage: TransformCanvasNodeKind,
  handlerId?: string
): EtlPaletteGroupId {
  if (stage === "score") return "transform_derive";
  if (stage === "transform") {
    const hid = handlerId?.trim();
    return hid ? paletteGroupIdForTransformHandler(hid) : "transform_string";
  }
  if (stage === "build_index") return "contextualization";
  if (stage.startsWith("query_")) return "extract";
  if (stage.startsWith("save_")) return "load";
  if (stage === "node_preview") return "debug";
  if (stage === "file_annotation" || stage === "workflow_fanout_plan") return "contextualization";
  if (ORCHESTRATION_KIND_SET.has(stage)) return "orchestration";
  if (FUSION_KIND_SET.has(stage)) return "fusion";
  if (stage === "start" || stage === "end") return "orchestration";
  return "orchestration";
}

export function defaultNodeColorForStage(
  stage: TransformCanvasNodeKind,
  handlerId?: string
): string {
  return paletteGroupHex(paletteGroupIdForStage(stage, handlerId));
}

export function resolveEtlNodeAccentColor(
  kind: TransformCanvasNodeKind,
  data: Record<string, unknown>
): string {
  const stored = data.node_color;
  if (typeof stored === "string" && stored.trim()) return stored.trim();
  const config = (data.config ?? {}) as Record<string, unknown>;
  const handlerId = String(config.handler_id ?? config.handler ?? "").trim();
  return defaultNodeColorForStage(kind, handlerId || undefined);
}
