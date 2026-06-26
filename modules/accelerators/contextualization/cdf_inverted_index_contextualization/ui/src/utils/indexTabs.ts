import type { IndexDocumentTab, IndexNavNode, IndexTabKind } from "../types/indexWorkspace";

const NAV_NODES: IndexNavNode[] = [
  {
    id: "inverted-index/indexing",
    labelKey: "nav.indexing",
    children: [
      { id: "inverted-index/overview", labelKey: "nav.overview", kind: "overview" },
      {
        id: "inverted-index/ops",
        labelKey: "nav.ops",
        children: [
          {
            id: "inverted-index/ops/build-metadata",
            labelKey: "nav.buildMetadata",
            kind: "build-metadata",
          },
          {
            id: "inverted-index/ops/build-annotations",
            labelKey: "nav.buildAnnotations",
            kind: "build-annotations",
          },
          {
            id: "inverted-index/ops/target-driven",
            labelKey: "nav.targetDriven",
            kind: "target-driven",
          },
        ],
      },
      { id: "inverted-index/query", labelKey: "nav.query", kind: "query" },
      { id: "inverted-index/file", labelKey: "nav.fileContext", kind: "file-context" },
      { id: "inverted-index/tag-reuse", labelKey: "nav.tagReuse", kind: "tag-reuse" },
    ],
  },
];

export function indexNavTree(): IndexNavNode[] {
  return NAV_NODES;
}

export function findNavNode(id: string): IndexNavNode | undefined {
  const walk = (nodes: IndexNavNode[]): IndexNavNode | undefined => {
    for (const node of nodes) {
      if (node.id === id) return node;
      if (node.children) {
        const found = walk(node.children);
        if (found) return found;
      }
    }
    return undefined;
  };
  return walk(NAV_NODES);
}

export function tabIdForKind(kind: IndexTabKind): string {
  return `tab:${kind}`;
}

export function createIndexTab(
  kind: IndexTabKind,
  label: string,
  navNodeId: string
): IndexDocumentTab {
  return { id: tabIdForKind(kind), kind, label, navNodeId };
}

export function isOverviewTab(tab: IndexDocumentTab): boolean {
  return tab.kind === "overview";
}

export function isBuildMetadataTab(tab: IndexDocumentTab): boolean {
  return tab.kind === "build-metadata";
}

export function isBuildAnnotationsTab(tab: IndexDocumentTab): boolean {
  return tab.kind === "build-annotations";
}

export function isQueryTab(tab: IndexDocumentTab): boolean {
  return tab.kind === "query";
}

export function isFileContextTab(tab: IndexDocumentTab): boolean {
  return tab.kind === "file-context";
}

export function isTargetDrivenTab(tab: IndexDocumentTab): boolean {
  return tab.kind === "target-driven";
}

export function isTagReuseTab(tab: IndexDocumentTab): boolean {
  return tab.kind === "tag-reuse";
}

export function isSettingsTab(tab: IndexDocumentTab): boolean {
  return tab.kind === "settings";
}
