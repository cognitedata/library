import type { MessageKey } from "../../i18n";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import {
  BUILD_INDEX_HANDLER_DEFINITIONS,
  isBuildIndexHandlerId,
} from "./etlBuildIndexHandlerRegistry";
import {
  CORE_TRANSFORM_HANDLER_IDS,
  ELT_TRANSFORM_HANDLER_IDS,
  type DiscoveryTransformHandlerId,
} from "./etlHandlerRegistry";
import { isEtlTransformHandlerId } from "../../utils/etlTransformHandlerTemplates";
import type { PaletteDragPayload } from "./transformFlowDrag";

export type HandlerDropMenuOption = {
  id: string;
  handlerId: string;
  labelKey: MessageKey;
  payload: PaletteDragPayload;
};

export type HandlerDropMenuGroup = {
  id: string;
  labelKey: MessageKey;
  options: HandlerDropMenuOption[];
};

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
    payload: { kind: "etl_stage", stage: "build_index", handlerId: def.id },
  }));
}

export function handlerDropMenuGroupedOptionsForStage(
  stage: TransformCanvasNodeKind
): HandlerDropMenuGroup[] | null {
  if (stage === "transform") {
    const core = CORE_TRANSFORM_HANDLER_IDS.filter(isEtlTransformHandlerId).map((id) => ({
      id: `transform-${id}`,
      handlerId: id,
      labelKey: `transforms.handlerName.${id}` as MessageKey,
      payload: { kind: "etl_stage" as const, stage: "transform" as const, handlerId: id },
    }));
    const elt = ELT_TRANSFORM_HANDLER_IDS.filter(isEtlTransformHandlerId).map((id) => ({
      id: `transform-${id}`,
      handlerId: id,
      labelKey: `transforms.handlerName.${id}` as MessageKey,
      payload: { kind: "etl_stage" as const, stage: "transform" as const, handlerId: id },
    }));
    return [
      { id: "core", labelKey: "transforms.handlerGroup.core", options: core },
      { id: "elt", labelKey: "transforms.handlerGroup.elt", options: elt },
    ];
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
