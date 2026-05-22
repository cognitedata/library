import type { DataModelRef, TreeNode } from "../types/explorerNodes";

export function dataModelRefFromNode(node: TreeNode): DataModelRef | null {
  if (node.kind !== "dm_data_model" || !node.meta) return null;
  const { space, external_id, version, name } = node.meta;
  if (typeof space !== "string" || typeof external_id !== "string" || typeof version !== "string") {
    return null;
  }
  return {
    space,
    external_id,
    version,
    name: typeof name === "string" ? name : undefined,
  };
}

export function dataModelTabKey(ref: DataModelRef): string {
  return `dm-model:${ref.space}:${ref.external_id}:${ref.version}`;
}

export function dataModelTabLabel(ref: DataModelRef): string {
  const base = `${ref.external_id} (${ref.version})`;
  return ref.name?.trim() ? `${ref.name.trim()} — ${base}` : base;
}
