import type { Node } from "@xyflow/react";
import type { PaletteDragPayload } from "./FlowPalette";
import { newNodeId } from "./flowDocumentBridge";

export function createNodeFromPalette(
  payload: PaletteDragPayload,
  position: { x: number; y: number }
): Node {
  const id = newNodeId();
  if (payload.kind === "structural") {
    switch (payload.nodeKind) {
      case "source_view":
        return {
          id,
          type: "keaSourceView",
          position,
          data: { label: "Source view" },
        };
      case "extraction":
        return {
          id,
          type: "keaExtraction",
          position,
          data: { label: "Extraction", handler_family: "extraction" },
        };
      case "aliasing":
        return {
          id,
          type: "keaAliasing",
          position,
          data: { label: "Aliasing", handler_family: "aliasing" },
        };
      case "match_validation_source_view":
        return {
          id,
          type: "keaMatchValidationRuleSourceView",
          position,
          data: {
            label: "Match validation",
            validation_rule_context: "source_view",
            confidence_match_rule_name: "",
            ref: {},
          },
        };
      case "match_validation_extraction":
        return {
          id,
          type: "keaMatchValidationRuleExtraction",
          position,
          data: {
            label: "Match validation",
            validation_rule_context: "extraction",
            confidence_match_rule_name: "",
            ref: {},
          },
        };
      case "match_validation_aliasing":
        return {
          id,
          type: "keaMatchValidationRuleAliasing",
          position,
          data: {
            label: "Match validation",
            validation_rule_context: "aliasing",
            confidence_match_rule_name: "",
            ref: {},
          },
        };
      default:
        return {
          id,
          type: "keaExtraction",
          position,
          data: { label: "Extraction" },
        };
    }
  }
  if (payload.kind === "extraction") {
    return {
      id,
      type: "keaExtraction",
      position,
      data: {
        label: payload.handlerId,
        handler_id: payload.handlerId,
        handler_family: "extraction",
        preset_from_palette: true,
      },
    };
  }
  if (payload.kind === "aliasing") {
    return {
      id,
      type: "keaAliasing",
      position,
      data: {
        label: payload.handlerId,
        handler_id: payload.handlerId,
        handler_family: "aliasing",
        preset_from_palette: true,
      },
    };
  }
  if (payload.kind === "match_definition") {
    const rid = payload.ruleId.trim() || "rule";
    return {
      id,
      type: "keaMatchValidationRuleExtraction",
      position,
      data: {
        label: rid,
        validation_rule_context: "extraction",
        confidence_match_rule_name: rid,
        ref: {},
        preset_from_palette: true,
      },
    };
  }
  const _exhaustive: never = payload;
  throw new Error(`Unhandled palette payload: ${JSON.stringify(_exhaustive)}`);
}
