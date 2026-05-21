import type { Node } from "@xyflow/react";
import { emptyWorkflowCanvasDocument } from "../../types/workflowCanvas";
import type { DiscoveryPaletteStage, PaletteDragPayload } from "./FlowPalette";
import { TRANSFORM_HANDLER_IDS } from "./handlerRegistry";
import { newNodeId } from "./flowDocumentBridge";
import { DEFAULT_SUBGRAPH_FRAME_PORTS } from "./subgraphInnerBoundaryHubs";
import { defaultFilterNodeConfig } from "../../utils/filtersCanvasUtils";
import { defaultConfidenceFilterNodeConfig } from "../../utils/confidenceFilterCanvasUtils";
import { defaultTransformNodeConfig, isDiscoveryTransformHandlerId } from "../../utils/transformHandlerTemplates";

export type CreateNodeFromPaletteContext = {
  /** Prior canvas node's transform `output_field` when chaining transforms. */
  previousTransformOutputField?: string | null;
};

export function createNodeFromPalette(
  payload: PaletteDragPayload,
  position: { x: number; y: number },
  ctx?: CreateNodeFromPaletteContext
): Node {
  const id = newNodeId();
  const defaultTransformHandler = TRANSFORM_HANDLER_IDS[0] ?? "regex_substitution";
  if (payload.kind === "structural") {
    switch (payload.nodeKind) {
      case "source_view":
        return {
          id,
          type: "keaSourceView",
          position,
          data: { label: "Source view" },
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
    }
  }
  if (payload.kind === "discovery") {
    type StageMeta = { type: string; defaultLabel: string };
    const meta: Record<DiscoveryPaletteStage, StageMeta> = {
      save_view: { type: "keaViewSave", defaultLabel: "View save" },
      save_raw: { type: "keaRawSave", defaultLabel: "RAW save" },
      save_classic: { type: "keaClassicSave", defaultLabel: "Classic save" },
      query_view: { type: "keaViewQuery", defaultLabel: "View query" },
      query_raw: { type: "keaRawQuery", defaultLabel: "RAW query" },
      query_classic: { type: "keaClassicQuery", defaultLabel: "Classic query" },
      transform: { type: "keaTransform", defaultLabel: "Transform" },
      merge: { type: "keaMerge", defaultLabel: "Merge" },
      join: { type: "keaJoin", defaultLabel: "Join" },
      validation: { type: "keaDiscoveryValidate", defaultLabel: "Validation" },
      instance_filter: { type: "keaDiscoveryInstanceFilter", defaultLabel: "Instance filter" },
      confidence_filter: {
        type: "keaDiscoveryConfidenceFilter",
        defaultLabel: "Confidence filter",
      },
      inverted_index: { type: "keaInvertedIndex", defaultLabel: "Inverted index" },
    };
    const m = meta[payload.stage];
    const data: Record<string, unknown> = {
      label: m.defaultLabel,
      handler_family: "discovery",
      preset_from_palette: true,
    };
    switch (payload.stage) {
      case "query_view":
        data.config = {
          description: m.defaultLabel,
          incremental_change_processing: true,
          view_space: "cdf_cdm",
          view_external_id: "CogniteFile",
          view_version: "v1",
        };
        break;
      case "query_raw":
        data.config = { description: m.defaultLabel };
        break;
      case "query_classic":
        data.config = { description: m.defaultLabel };
        break;
      case "transform":
        {
          const raw = payload.transformHandlerId;
          const handler = raw && isDiscoveryTransformHandlerId(raw) ? raw : defaultTransformHandler;
          data.label = `Transform · ${handler}`;
          data.handler_id = handler;
          data.config = defaultTransformNodeConfig(handler, {
            previousOutputField: ctx?.previousTransformOutputField ?? null,
          });
        }
        break;
      case "merge":
        data.config = {
          description: "Merge — fan-in properties from parallel upstream transforms",
          field_policies: [
            {
              property: "aliases",
              strategy: "merge_list",
              merge_list: { unique: true, branch_order: "by_score" },
            },
            {
              property: "indexKey",
              strategy: "merge_list",
              merge_list: { unique: true, branch_order: "by_score" },
            },
          ],
        };
        break;
      case "join":
        data.config = {
          description: "Join — view cohort ↔ RAW cohort (default: name vs raw_columns.key)",
          join_type: "left",
          right_prefix: "",
          join_on: {
            and: [
              {
                operator: "IEQUALS",
                left_property: "name",
                right_property: "raw_columns.key",
              },
            ],
          },
        };
        break;
      case "validation":
        data.config = { description: m.defaultLabel };
        break;
      case "instance_filter":
        data.config = defaultFilterNodeConfig();
        break;
      case "confidence_filter":
        data.config = defaultConfidenceFilterNodeConfig();
        break;
      case "save_view":
        data.config = {
          description: m.defaultLabel,
          view_space: "cdf_cdm",
          view_external_id: "CogniteDescribable",
          view_version: "v1",
          save_fan_in_mode: "none",
        };
        break;
      case "save_raw":
        data.config = { description: m.defaultLabel, save_fan_in_mode: "none" };
        break;
      case "save_classic":
        data.config = { description: m.defaultLabel, save_fan_in_mode: "none" };
        break;
      default:
        break;
    }
    if (payload.stage === "inverted_index") {
      data.persistence_step = "inverted_index";
      data.persistence_config = { kind: "inverted_index" as const };
    }
    return {
      id,
      type: m.type,
      position,
      data,
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
