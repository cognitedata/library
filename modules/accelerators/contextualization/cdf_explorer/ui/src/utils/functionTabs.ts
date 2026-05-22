import type { FunctionDocumentTab, TreeNode } from "../types/explorerNodes";

export function functionTabKey(functionId: string): string {
  return `fn:${functionId}`;
}

export function functionLabelFromMeta(meta: Record<string, unknown> | undefined): string {
  const name = typeof meta?.name === "string" ? meta.name.trim() : "";
  if (name) return name;
  const ext = typeof meta?.external_id === "string" ? meta.external_id.trim() : "";
  if (ext) return ext;
  const id = meta?.id;
  return id != null ? `Function ${id}` : "Function";
}

export function functionIdFromNode(node: TreeNode): string | null {
  if (node.kind !== "function" || !node.meta) return null;
  const id = node.meta.id;
  if (id == null) return null;
  const s = String(id).trim();
  return s || null;
}

export function createFunctionTab(functionId: string, label: string): FunctionDocumentTab {
  return {
    kind: "function",
    id: functionTabKey(functionId),
    label,
    functionId,
    detail: null,
    loading: true,
    error: null,
  };
}
