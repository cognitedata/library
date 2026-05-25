import type { MessageKey } from "../i18n";
import type { TransformCanvasNodeKind } from "../types/transformCanvas";
import type { OpenTarget, TreeNode } from "../types/discoveryNodes";

const QUERYABLE_KINDS = new Set([
  "classic_resource",
  "raw_table",
  "dm_view",
  "fusion_dm_view",
]);

export type DataTreeEntityDragPayload = {
  kind: "data_tree_entity";
  node: TreeNode;
};

export type EntityDropStages = {
  query: TransformCanvasNodeKind;
  save: TransformCanvasNodeKind;
};

export type EntityDropMenuOption =
  | {
      id: string;
      kind: "stage";
      stage: TransformCanvasNodeKind;
      labelKey: MessageKey;
    }
  | {
      id: string;
      kind: "query_save_pair";
      labelKey: MessageKey;
    };

export function entityDropStages(node: TreeNode): EntityDropStages | null {
  const target = node.open_target;
  if (!target) return null;
  if (target.type === "dm_instances") {
    return { query: "query_view", save: "save_view" };
  }
  if (target.type === "raw_rows") {
    return { query: "query_raw", save: "save_raw" };
  }
  if (target.type === "classic_list") {
    return { query: "query_classic", save: "save_classic" };
  }
  return null;
}

export function entityDropMenuOptions(node: TreeNode): EntityDropMenuOption[] | null {
  const stages = entityDropStages(node);
  if (!stages) return null;
  return [
    {
      id: `entity-query-${stages.query}`,
      kind: "stage",
      stage: stages.query,
      labelKey: "transform.entityDrop.query",
    },
    {
      id: `entity-save-${stages.save}`,
      kind: "stage",
      stage: stages.save,
      labelKey: "transform.entityDrop.save",
    },
    {
      id: `entity-pair-${stages.query}-${stages.save}`,
      kind: "query_save_pair",
      labelKey: "transform.entityDrop.queryToSave",
    },
  ];
}

export function queryStageForTreeNode(node: TreeNode): TransformCanvasNodeKind | null {
  return entityDropStages(node)?.query ?? null;
}

export function canDropDataTreeEntity(node: TreeNode): boolean {
  if (!QUERYABLE_KINDS.has(node.kind) || !node.open_target) {
    return false;
  }
  return entityDropStages(node) != null;
}

export function seedConfigForEntityDrop(
  node: TreeNode,
  stage: TransformCanvasNodeKind
): Record<string, unknown> {
  const target = node.open_target;
  const label = node.label.trim() || "Entity";
  if (!target) return { description: label };

  if (target.type === "dm_instances") {
    const base = {
      description: label,
      view_space: target.view_space,
      view_external_id: target.view_external_id,
      view_version: target.view_version,
    };
    if (stage === "query_view") {
      return { ...base, batch_size: 1000 };
    }
    return base;
  }

  if (target.type === "raw_rows") {
    const db = target.database;
    const table = target.table;
    return {
      description: label,
      source_raw_db: db,
      raw_db: db,
      source_raw_table_key: table,
      raw_table_key: table,
    };
  }

  if (target.type === "classic_list") {
    return {
      description: label,
      resource_type: target.resource_type,
    };
  }

  return { description: label };
}

/** @deprecated Use seedConfigForEntityDrop */
export function seedQueryConfigForTreeNode(node: TreeNode): Record<string, unknown> {
  const stage = queryStageForTreeNode(node);
  if (!stage) return { description: node.label.trim() || "Entity" };
  return seedConfigForEntityDrop(node, stage);
}

export function openTargetKey(target: OpenTarget): string {
  if (target.type === "classic_list") return `classic:${target.resource_type}`;
  if (target.type === "dm_instances") {
    return `dm:${target.view_space}:${target.view_external_id}:${target.view_version}`;
  }
  if (target.type === "raw_rows") {
    return `raw:${target.database}:${target.table}`;
  }
  return target.type;
}
