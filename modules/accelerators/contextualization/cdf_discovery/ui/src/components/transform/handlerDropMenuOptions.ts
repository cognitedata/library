import type { MessageKey } from "../../i18n";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import {
  BUILD_INDEX_HANDLER_DEFINITIONS,
  isBuildIndexHandlerId,
} from "./etlBuildIndexHandlerRegistry";
import {
  TRANSFORM_HANDLER_CATEGORY_DEFS,
  TRANSFORM_HANDLER_DEFINITIONS,
  type DiscoveryTransformHandlerId,
  type TransformHandlerCategoryId,
} from "./etlHandlerRegistry";
import { isEtlTransformHandlerId, transformHandlerDocKey } from "../../utils/etlTransformHandlerTemplates";
import { buildIndexHandlerDocKey } from "../../utils/buildIndexHandlerTemplates";
import { handlerPaletteTooltip } from "../../utils/transformHandlerCatalog";
import type { PaletteDragPayload } from "./transformFlowDrag";

export type HandlerDropMenuOption = {
  id: string;
  handlerId: string;
  labelKey: MessageKey;
  /** i18n key for palette tooltip and editor hint (synced from Python handler description). */
  docKey: MessageKey;
  payload: PaletteDragPayload;
};

export type HandlerDropMenuGroup = {
  id: string;
  labelKey: MessageKey;
  options: HandlerDropMenuOption[];
};

/** Tooltip text from Python handler ``description`` (generated catalog), not i18n labels. */
export function handlerDropMenuOptionTooltip(opt: HandlerDropMenuOption): string {
  const payload = opt.payload;
  if (payload.kind === "etl_stage" && payload.handlerId) {
    if (payload.stage === "transform" || payload.stage === "build_index") {
      return handlerPaletteTooltip(payload.stage, payload.handlerId);
    }
  }
  return "";
}

export function stageNeedsHandlerPick(stage: TransformCanvasNodeKind): boolean {
  return stage === "transform" || stage === "build_index";
}

export function palettePayloadNeedsHandlerPick(payload: PaletteDragPayload): boolean {
  return (
    payload.kind === "etl_stage" &&
    stageNeedsHandlerPick(payload.stage) &&
    !payload.handlerId
  );
}

function buildIndexHandlerOptions(): HandlerDropMenuOption[] {
  return BUILD_INDEX_HANDLER_DEFINITIONS.map((def) => ({
    id: `build_index-${def.id}`,
    handlerId: def.id,
    labelKey: def.nameKey,
    docKey: buildIndexHandlerDocKey(def.id),
    payload: { kind: "etl_stage", stage: "build_index", handlerId: def.id },
  }));
}

function transformHandlerOption(handlerId: DiscoveryTransformHandlerId): HandlerDropMenuOption {
  const def = TRANSFORM_HANDLER_DEFINITIONS.find((d) => d.id === handlerId);
  const labelKey = def?.nameKey ?? (`transforms.handlerName.${handlerId}` as MessageKey);
  return {
    id: `transform-${handlerId}`,
    handlerId,
    labelKey,
    docKey: transformHandlerDocKey(handlerId),
    payload: { kind: "etl_stage", stage: "transform", handlerId },
  };
}

/** Draggable transform palette / connect-menu entries (one per handler). */
export function transformHandlerPaletteItems(): HandlerDropMenuOption[] {
  return transformHandlerDropMenuGroups().flatMap((g) => g.options);
}

export function transformHandlerDropMenuGroups(): HandlerDropMenuGroup[] {
  const byCategory = new Map<TransformHandlerCategoryId, HandlerDropMenuOption[]>();
  for (const cat of TRANSFORM_HANDLER_CATEGORY_DEFS) {
    byCategory.set(cat.id, []);
  }
  for (const def of TRANSFORM_HANDLER_DEFINITIONS) {
    if (!isEtlTransformHandlerId(def.id)) continue;
    const bucket = byCategory.get(def.category);
    if (bucket) bucket.push(transformHandlerOption(def.id));
  }
  return TRANSFORM_HANDLER_CATEGORY_DEFS.map((cat) => ({
    id: cat.id,
    labelKey: cat.labelKey,
    options: byCategory.get(cat.id) ?? [],
  })).filter((g) => g.options.length > 0);
}

export function handlerDropMenuGroupedOptionsForStage(
  stage: TransformCanvasNodeKind
): HandlerDropMenuGroup[] | null {
  if (stage === "transform") {
    return transformHandlerDropMenuGroups();
  }
  if (stage === "build_index") {
    return [
      {
        id: "handlers",
        labelKey: "buildIndex.handler",
        options: buildIndexHandlerOptions(),
      },
    ];
  }
  return null;
}

export function isValidHandlerForStage(
  stage: TransformCanvasNodeKind,
  handlerId: string
): boolean {
  if (stage === "transform") return isEtlTransformHandlerId(handlerId);
  if (stage === "build_index") return isBuildIndexHandlerId(handlerId);
  return false;
}
