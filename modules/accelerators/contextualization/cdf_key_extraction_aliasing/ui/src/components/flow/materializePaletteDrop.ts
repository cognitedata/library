import { flushSync } from "react-dom";
import type { Edge, Node } from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import { createNodeFromPalette } from "./createNodeFromPalette";
import { validationRuleLayoutReuseOnDrop } from "./matchValidationReuseOnDrop";
import type { PaletteDragPayload } from "./FlowPalette";
import { keaValidationRuleLayoutRfTypes } from "./flowConstants";
import { appendAliasingRuleForHandler } from "./ensureAliasingRuleForPaletteDrop";
import { appendEmptySourceView } from "./ensureSourceViewForPaletteDrop";
import { buildAutoWiredEdgesForDroppedSourceView } from "./autoWireSourceViewDrop";
import {
  appendUniqueMatchDefinitionStub,
  ensureConfidenceMatchRuleDefinitionStub,
} from "./ensureMatchDefinitionStub";
import { appendSourceViewToExtractionAssociation } from "./workflowScopeAssociations";
import { sanitizeRuleNamePrefix } from "../../utils/ruleNaming";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

export type MaterializePaletteDropInput = {
  payload: PaletteDragPayload;
  position: { x: number; y: number };
  nodes: Node[];
  edges: Edge[];
  workflowScopeDoc: Record<string, unknown>;
  patchWorkflowScope: (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => void;
  t: TFn;
  /** When false, always materialize a new node (used by connect-end menu). Default true. */
  allowValidationRuleLayoutReuse?: boolean;
};

export type MaterializePaletteDropResult =
  | { outcome: "reuse"; headId: string; connectFromId?: string }
  | { outcome: "create"; node: Node; extraEdges: Edge[] };

/**
 * Shared palette-drop materialization: scope stubs, source-view rows, validation-rule definition ids,
 * and auto-wiring for structural source views.
 */
export function materializePaletteDrop(input: MaterializePaletteDropInput): MaterializePaletteDropResult {
  const {
    payload,
    position,
    nodes,
    edges,
    workflowScopeDoc,
    patchWorkflowScope,
    t,
    allowValidationRuleLayoutReuse = true,
  } = input;

  if (allowValidationRuleLayoutReuse) {
    const reuse = validationRuleLayoutReuseOnDrop(payload, position, nodes, edges, workflowScopeDoc);
    if (reuse.action === "reuse") {
      return { outcome: "reuse", headId: reuse.headId, connectFromId: reuse.connectFromId };
    }
  }

  let extraEdgesFromSv: Edge[] = [];
  let node = createNodeFromPalette(payload, position);
  const rfType = node.type ?? "";
  if (keaValidationRuleLayoutRfTypes.has(rfType)) {
    const data = (node.data ?? {}) as Record<string, unknown>;
    const cm =
      data.validation_rule_name != null ? String(data.validation_rule_name).trim() : "";
    if (!cm) {
      const ctxRaw =
        data.validation_rule_context != null ? String(data.validation_rule_context) : "extraction";
      const matchPrefix = sanitizeRuleNamePrefix(`match_${ctxRaw}`, "match");
      const { doc: nextDoc, newId } = appendUniqueMatchDefinitionStub(workflowScopeDoc, matchPrefix);
      patchWorkflowScope(() => nextDoc);
      node = {
        ...node,
        data: {
          ...data,
          validation_rule_name: newId,
          label: newId,
        },
      };
    } else {
      patchWorkflowScope((doc) => ensureConfidenceMatchRuleDefinitionStub(doc, cm));
    }
  }
  if (payload.kind === "aliasing") {
    const { doc: nextDoc, ruleName } = appendAliasingRuleForHandler(workflowScopeDoc, payload.handlerId);
    patchWorkflowScope(() => nextDoc);
    const data = (node.data ?? {}) as Record<string, unknown>;
    const prevRef =
      data.ref && typeof data.ref === "object" && !Array.isArray(data.ref)
        ? { ...(data.ref as Record<string, unknown>) }
        : {};
    prevRef.aliasing_rule_name = ruleName;
    node = {
      ...node,
      data: {
        ...data,
        label: ruleName,
        ref: prevRef,
      },
    };
  }
  if (payload.kind === "structural" && payload.nodeKind === "source_view") {
    const { doc: nextDoc, index } = appendEmptySourceView(workflowScopeDoc);
    flushSync(() => {
      patchWorkflowScope(() => nextDoc);
    });
    const data = (node.data ?? {}) as Record<string, unknown>;
    const label = t("sourceViews.unnamedView", { index: String(index + 1) });
    node = {
      ...node,
      data: {
        ...data,
        label,
        ref: { source_view_index: index },
      },
    };
    const auto = buildAutoWiredEdgesForDroppedSourceView({
      nodes,
      edges,
      newSvNode: node,
      svIndex: index,
      scopeDoc: nextDoc,
    });
    extraEdgesFromSv = auto.edges;
    if (auto.mergeExtractionRules.length > 0) {
      flushSync(() => {
        patchWorkflowScope((doc) => {
          let d = doc;
          for (const rn of auto.mergeExtractionRules) {
            d = appendSourceViewToExtractionAssociation(d, {
              source_view_index: index,
              extraction_rule_name: rn,
            });
          }
          return d;
        });
      });
    }
  }

  return { outcome: "create", node, extraEdges: extraEdgesFromSv };
}
