import type { MessageKey } from "../i18n";
import type { PaletteDragPayload } from "../components/flow/FlowPalette";
import {
  TRANSFORM_HANDLER_DEFINITIONS,
  transformHandlerDisplayName,
  type DiscoveryTransformHandlerId,
} from "../components/flow/handlerRegistry";
import { confidenceMatchDefinitionIds } from "./confidenceMatchDefinitionIds";
import type { TreeNode } from "../types/dataTree";

export const PALETTE_PIPELINE_ROOT = "palette:pipeline";
export const PALETTE_DATA_ROOT = "data";
export const PALETTE_TREE_ROOT = "palette_root";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

function folder(id: string, label: string): TreeNode {
  return {
    id,
    label,
    kind: "palette_folder",
    has_children: true,
  };
}

function leaf(id: string, label: string, payload: PaletteDragPayload): TreeNode {
  return {
    id,
    label,
    kind: "palette_leaf",
    has_children: false,
    meta: { palette_payload: payload },
  };
}

function hint(id: string, label: string): TreeNode {
  return {
    id,
    label,
    kind: "palette_hint",
    has_children: false,
  };
}

/** Static palette pipeline subtree keyed by parent node id. */
export function buildPaletteTreeChildrenByParent(
  t: TFn,
  scopeDocument: Record<string, unknown>
): Map<string, TreeNode[]> {
  const map = new Map<string, TreeNode[]>();

  map.set("palette:query", [
    leaf("palette:leaf:query_view", t("flow.discoveryViewQuery"), { kind: "discovery", stage: "query_view" }),
    leaf("palette:leaf:query_raw", t("flow.discoveryRawQuery"), { kind: "discovery", stage: "query_raw" }),
    leaf("palette:leaf:query_classic", t("flow.discoveryClassicQuery"), {
      kind: "discovery",
      stage: "query_classic",
    }),
    leaf("palette:leaf:query_sql", t("flow.discoverySqlQuery"), { kind: "discovery", stage: "query_sql" }),
  ]);

  map.set("palette:transform", [
    ...TRANSFORM_HANDLER_DEFINITIONS.map((def) =>
      leaf(
        `palette:leaf:transform:${def.id}`,
        transformHandlerDisplayName(def.id, t),
        {
          kind: "discovery",
          stage: "transform",
          transformHandlerId: def.id as DiscoveryTransformHandlerId,
        }
      )
    ),
  ]);

  map.set("palette:merge_join", [
    leaf("palette:leaf:merge", t("flow.discoveryMerge"), { kind: "discovery", stage: "merge" }),
    leaf("palette:leaf:join", t("flow.discoveryJoin"), { kind: "discovery", stage: "join" }),
  ]);

  map.set("palette:validate_filter", [
    leaf("palette:leaf:validation", t("flow.discoveryValidate"), { kind: "discovery", stage: "validation" }),
    leaf("palette:leaf:confidence_filter", t("flow.discoveryConfidenceFilter"), {
      kind: "discovery",
      stage: "confidence_filter",
    }),
    leaf("palette:leaf:instance_filter", t("flow.discoveryInstanceFilter"), {
      kind: "discovery",
      stage: "instance_filter",
    }),
  ]);

  map.set("palette:save", [
    leaf("palette:leaf:save_view", t("flow.discoveryViewSave"), { kind: "discovery", stage: "save_view" }),
    leaf("palette:leaf:save_raw", t("flow.discoveryRawSave"), { kind: "discovery", stage: "save_raw" }),
    leaf("palette:leaf:save_classic", t("flow.discoveryClassicSave"), {
      kind: "discovery",
      stage: "save_classic",
    }),
    leaf("palette:leaf:inverted_index", t("flow.discoveryInvertedIndex"), {
      kind: "discovery",
      stage: "inverted_index",
    }),
  ]);

  map.set("palette:structural", [
    leaf("palette:leaf:subgraph", t("flow.structuralSubgraph"), { kind: "structural", nodeKind: "subgraph" }),
    leaf("palette:leaf:match_validation_aliasing", t("flow.validationRuleLayoutAliasing"), {
      kind: "structural",
      nodeKind: "match_validation_aliasing",
    }),
  ]);

  const ruleIds = confidenceMatchDefinitionIds(scopeDocument);
  map.set(
    "palette:validation_rules",
    ruleIds.length > 0
      ? ruleIds.map((id) =>
          leaf(`palette:leaf:match_def:${id}`, id, { kind: "match_definition", ruleId: id })
        )
      : [hint("palette:hint:validation_rules", t("flow.paletteValidationRulesEmpty"))]
  );

  map.set(PALETTE_PIPELINE_ROOT, [
    folder("palette:query", t("flow.paletteSectionQuery")),
    folder("palette:transform", t("flow.paletteSectionTransform")),
    folder("palette:merge_join", t("flow.paletteSectionMergeJoin")),
    folder("palette:validate_filter", t("flow.paletteSectionValidateFilter")),
    folder("palette:save", t("flow.paletteSectionSave")),
    folder("palette:structural", t("flow.paletteStructural")),
    folder("palette:validation_rules", t("flow.paletteValidationRuleDefinitions")),
  ]);

  map.set(PALETTE_TREE_ROOT, [
    {
      id: PALETTE_DATA_ROOT,
      label: t("flow.paletteSectionCdfData"),
      kind: "folder",
      has_children: true,
      meta: { domain: "data" },
    },
    {
      id: PALETTE_PIPELINE_ROOT,
      label: t("flow.palettePipeline"),
      kind: "palette_folder",
      has_children: true,
    },
  ]);

  return map;
}
