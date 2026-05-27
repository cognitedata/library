import type { MessageKey } from "../i18n";
import type { TreeNode } from "../types/discoveryNodes";
import {
  EXTRACT_ROOT,
  FUSION_ADMIN,
  FUSION_GROUPS,
  FUSION_SPACES,
  MONITOR_ROOT,
  TRANSFORM_PIPELINES,
  TRANSFORM_ROOT,
  TRANSFORM_TEMPLATES,
} from "./treeNodeIds";

const TREE_LABEL_KEYS: Record<string, MessageKey> = {
  [EXTRACT_ROOT]: "tree.extract",
  [TRANSFORM_ROOT]: "transform.tree.root",
  [TRANSFORM_PIPELINES]: "transform.tree.pipelines",
  [TRANSFORM_TEMPLATES]: "transform.tree.templates",
  [FUSION_SPACES]: "fusion.tree.spaces",
  [FUSION_ADMIN]: "fusion.tree.admin",
  [FUSION_GROUPS]: "fusion.tree.groups",
  [MONITOR_ROOT]: "tree.monitor",
};

export function treeNodeDisplayLabel(
  node: Pick<TreeNode, "id" | "label">,
  t: (key: MessageKey) => string
): string {
  const key = TREE_LABEL_KEYS[node.id];
  return key ? t(key) : node.label;
}
