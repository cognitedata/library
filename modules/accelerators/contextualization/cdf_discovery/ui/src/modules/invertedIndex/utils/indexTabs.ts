import type { TreeNode } from "../../../types/discoveryNodes";
import type { InvertedIndexDocumentTab, InvertedIndexTabKind } from "../types";

const KIND_BY_TREE_KIND: Record<string, InvertedIndexTabKind> = {
  inverted_index_dashboard: "inverted_index_dashboard",
  inverted_index_configuration: "inverted_index_configuration",
  inverted_index_build_metadata: "inverted_index_build_metadata",
  inverted_index_build_annotations: "inverted_index_build_annotations",
  inverted_index_target_driven: "inverted_index_target_driven",
  inverted_index_query: "inverted_index_query",
  inverted_index_file_context: "inverted_index_file_context",
  inverted_index_tag_reuse: "inverted_index_tag_reuse",
};

export function invertedIndexKindFromNode(node: TreeNode): InvertedIndexTabKind | null {
  const kind = node.kind ?? "";
  return KIND_BY_TREE_KIND[kind] ?? null;
}

export function tabIdForInvertedIndexKind(kind: InvertedIndexTabKind): string {
  return `index:${kind}`;
}

export function createInvertedIndexTab(
  kind: InvertedIndexTabKind,
  label: string,
  navNodeId: string
): InvertedIndexDocumentTab {
  return { id: tabIdForInvertedIndexKind(kind), kind, label, navNodeId };
}

export function isInvertedIndexDashboardTab(tab: InvertedIndexDocumentTab): boolean {
  return tab.kind === "inverted_index_dashboard";
}

export function isInvertedIndexConfigurationTab(tab: InvertedIndexDocumentTab): boolean {
  return tab.kind === "inverted_index_configuration";
}

export function isInvertedIndexBuildMetadataTab(tab: InvertedIndexDocumentTab): boolean {
  return tab.kind === "inverted_index_build_metadata";
}

export function isInvertedIndexBuildAnnotationsTab(tab: InvertedIndexDocumentTab): boolean {
  return tab.kind === "inverted_index_build_annotations";
}

export function isInvertedIndexQueryTab(tab: InvertedIndexDocumentTab): boolean {
  return tab.kind === "inverted_index_query";
}

export function isInvertedIndexFileContextTab(tab: InvertedIndexDocumentTab): boolean {
  return tab.kind === "inverted_index_file_context";
}

export function isInvertedIndexTargetDrivenTab(tab: InvertedIndexDocumentTab): boolean {
  return tab.kind === "inverted_index_target_driven";
}

export function isInvertedIndexTagReuseTab(tab: InvertedIndexDocumentTab): boolean {
  return tab.kind === "inverted_index_tag_reuse";
}
