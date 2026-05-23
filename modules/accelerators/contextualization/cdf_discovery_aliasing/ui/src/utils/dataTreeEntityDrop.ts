import type { DiscoveryPaletteStage } from "../components/flow/FlowPalette";
import type { JsonObject } from "../types/scopeConfig";
import type { OpenTarget, TreeNode } from "../types/dataTree";

const QUERYABLE_KINDS = new Set(["classic_resource", "raw_table", "dm_view"]);

export function canDropDataTreeEntity(node: TreeNode): boolean {
  if (!QUERYABLE_KINDS.has(node.kind) || !node.open_target) {
    return false;
  }
  return entityDropStages(node) != null;
}

export function entityDropStages(node: TreeNode): {
  query: DiscoveryPaletteStage;
  save: DiscoveryPaletteStage;
} | null {
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

export function seedConfigForEntityDrop(
  node: TreeNode,
  stage: DiscoveryPaletteStage,
  schema_space?: string
): JsonObject {
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
      return { ...base, incremental_change_processing: true };
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

  if (stage === "query_view" && schema_space) {
    return {
      description: label,
      view_space: schema_space,
      incremental_change_processing: true,
    };
  }

  return { description: label };
}

export function openTargetKey(target: OpenTarget): string {
  if (target.type === "classic_list") return `classic:${target.resource_type}`;
  if (target.type === "dm_instances") {
    return `dm:${target.view_space}:${target.view_external_id}:${target.view_version}`;
  }
  return `raw:${target.database}:${target.table}`;
}
