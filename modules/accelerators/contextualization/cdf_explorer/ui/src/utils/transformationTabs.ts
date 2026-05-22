import type { TransformationDocumentTab, TreeNode } from "../types/explorerNodes";

export function transformationTabKey(transformationId: number): string {
  return `tx:${transformationId}`;
}

export function transformationLabelFromMeta(meta: Record<string, unknown> | undefined): string {
  const name = typeof meta?.name === "string" ? meta.name.trim() : "";
  if (name) return name;
  const ext = typeof meta?.external_id === "string" ? meta.external_id.trim() : "";
  if (ext) return ext;
  const id = meta?.id;
  return id != null ? `Transformation ${id}` : "Transformation";
}

export function transformationIdFromNode(node: TreeNode): number | null {
  if (node.kind !== "transformation" || !node.meta) return null;
  const id = node.meta.id;
  if (typeof id === "number" && Number.isFinite(id)) return id;
  if (typeof id === "string" && id.trim() !== "") {
    const n = Number(id);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

export function createTransformationTab(
  transformationId: number,
  label: string
): TransformationDocumentTab {
  return {
    kind: "transformation",
    id: transformationTabKey(transformationId),
    label,
    transformationId,
    detail: null,
    loading: true,
    error: null,
  };
}
