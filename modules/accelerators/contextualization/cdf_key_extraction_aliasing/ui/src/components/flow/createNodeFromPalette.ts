import type { Node } from "@xyflow/react";
import { emptyWorkflowCanvasDocument } from "../../types/workflowCanvas";
import type { PaletteDragPayload } from "./FlowPalette";
import { newNodeId } from "./flowDocumentBridge";
import { DEFAULT_SUBGRAPH_FRAME_PORTS } from "./subgraphInnerBoundaryHubs";

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
      case "subflow":
        return {
          id,
          type: "keaSubflow",
          position,
          data: { label: "Subflow" },
          style: { width: 380, height: 260 },
        };
      case "subgraph":
        return {
          id,
          type: "keaSubgraph",
          position,
          data: {
            label: "Subgraph",
            subflow_ports: DEFAULT_SUBGRAPH_FRAME_PORTS,
            inner_canvas: emptyWorkflowCanvasDocument(),
          },
        };
      case "match_validation_source_view":
        return {
          id,
          type: "keaMatchValidationRuleSourceView",
          position,
          data: {
            label: "Match validation",
            validation_rule_context: "source_view",
            validation_rule_name: "",
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
            validation_rule_name: "",
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
            validation_rule_name: "",
            ref: {},
          },
        };
      case "writeback_raw":
        return {
          id,
          type: "keaWritebackRaw",
          position,
          data: {
            label: "Writeback (RAW)",
            handler_family: "persistence",
            writeback_sink: "raw",
          },
        };
      case "writeback_data_modeling":
        return {
          id,
          type: "keaWritebackDataModeling",
          position,
          data: {
            label: "Writeback (Data modeling)",
            handler_family: "persistence",
            writeback_sink: "data_modeling",
          },
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
        validation_rule_name: rid,
        ref: {},
        preset_from_palette: true,
      },
    };
  }
  const _exhaustive: never = payload;
  throw new Error(`Unhandled palette payload: ${JSON.stringify(_exhaustive)}`);
}
