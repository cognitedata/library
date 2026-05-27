import type { MessageKey } from "../i18n";
import type { TreeNode } from "../types/discoveryNodes";
import { governanceArtifactCreateContextFromNode } from "./governanceTreeNew";
import {
  DATA_SAVED_QUERIES,
  TRANSFORM_PIPELINE_PREFIX,
  TRANSFORM_PIPELINES,
  TRANSFORM_ROOT,
  TRANSFORM_TEMPLATE_PREFIX,
} from "./treeNodeIds";
import { templateIdFromNode } from "./transformTabs";

export type TreeToolbarNewAction =
  | { kind: "transform_pipeline" }
  | { kind: "transform_pipeline_from_template"; templateId: string }
  | { kind: "saved_query" }
  | { kind: "governance_space_artifact"; parentRel: string }
  | { kind: "governance_group_artifact"; parentRel: string };

/** Whether the tree toolbar / context menu should offer a primary New action for this node. */
export function resolveTreeToolbarNewAction(
  node: Pick<TreeNode, "id" | "kind" | "meta"> | null | undefined
): TreeToolbarNewAction | null {
  if (!node?.id) return null;

  const govCtx = governanceArtifactCreateContextFromNode(node);
  if (govCtx?.kind === "spaces") {
    return { kind: "governance_space_artifact", parentRel: govCtx.parentRel };
  }
  if (govCtx?.kind === "groups") {
    return { kind: "governance_group_artifact", parentRel: govCtx.parentRel };
  }

  if (node.id === DATA_SAVED_QUERIES || node.kind === "saved_query") {
    return { kind: "saved_query" };
  }

  if (node.kind === "etl_template" || node.id.startsWith(TRANSFORM_TEMPLATE_PREFIX)) {
    const templateId = templateIdFromNode(node);
    if (templateId) {
      return { kind: "transform_pipeline_from_template", templateId };
    }
    return null;
  }

  if (
    node.id === TRANSFORM_ROOT ||
    node.id === TRANSFORM_PIPELINES ||
    node.kind === "etl_pipeline" ||
    node.id.startsWith(TRANSFORM_PIPELINE_PREFIX)
  ) {
    return { kind: "transform_pipeline" };
  }

  return null;
}

export function treeToolbarNewLabels(action: TreeToolbarNewAction): {
  labelKey: MessageKey;
  titleKey: MessageKey;
} {
  switch (action.kind) {
    case "transform_pipeline":
      return {
        labelKey: "transform.pipelines.new",
        titleKey: "transform.pipelines.newTitle",
      };
    case "transform_pipeline_from_template":
      return {
        labelKey: "transform.pipelines.newFromTemplate",
        titleKey: "transform.pipelines.newFromTemplateTitle",
      };
    case "saved_query":
      return {
        labelKey: "discovery.newSavedQuery",
        titleKey: "discovery.newSavedQueryTitle",
      };
    case "governance_space_artifact":
      return {
        labelKey: "governance.create.new",
        titleKey: "governance.create.spaceTitle",
      };
    case "governance_group_artifact":
      return {
        labelKey: "governance.create.new",
        titleKey: "governance.create.groupTitle",
      };
  }
}
