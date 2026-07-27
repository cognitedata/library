import type { CogniteClient } from "@cognite/sdk";
import { cachedViewsRetrieve } from "@/shared/dms-catalog-cache";

export type ViewInheritanceRef = {
  space: string;
  externalId: string;
  version: string;
};

export type ViewInheritanceNode = {
  id: string;
  space: string;
  externalId: string;
  version: string;
  name: string | null;
  depth: number;
  children: ViewInheritanceNode[];
  cycle: boolean;
  truncated: boolean;
  missing: boolean;
};

export type ViewInheritanceTree = {
  root: ViewInheritanceNode;
  nodeCount: number;
  truncated: boolean;
};

type ViewDefinitionRow = {
  space: string;
  externalId: string;
  version: string;
  name?: string;
  implements?: Array<{
    type?: string;
    space: string;
    externalId: string;
    version: string;
  }>;
};

const DEFAULT_MAX_DEPTH = 12;
const DEFAULT_MAX_NODES = 80;

function hasRecordShape(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function viewInheritanceKey(ref: ViewInheritanceRef): string {
  return `${ref.space}\x1f${ref.externalId}\x1f${ref.version}`;
}

function parseViewDefinition(item: unknown): ViewDefinitionRow | null {
  if (!hasRecordShape(item)) return null;
  const space = item.space;
  const externalId = item.externalId;
  const version = item.version;
  if (typeof space !== "string" || typeof externalId !== "string" || typeof version !== "string") {
    return null;
  }

  const implementsRaw = item.implements;
  const implementsList = Array.isArray(implementsRaw)
    ? implementsRaw
        .map((entry) => {
          if (!hasRecordShape(entry)) return null;
          if (typeof entry.space !== "string" || typeof entry.externalId !== "string") return null;
          if (typeof entry.version !== "string") return null;
          return {
            type: typeof entry.type === "string" ? entry.type : undefined,
            space: entry.space,
            externalId: entry.externalId,
            version: entry.version,
          };
        })
        .filter((entry): entry is NonNullable<typeof entry> => entry !== null)
    : undefined;

  return {
    space,
    externalId,
    version,
    name: typeof item.name === "string" ? item.name : undefined,
    implements: implementsList,
  };
}

async function retrieveViewDefinition(
  sdk: CogniteClient,
  ref: ViewInheritanceRef
): Promise<ViewDefinitionRow | null> {
  const response = (await cachedViewsRetrieve(sdk, [
    {
      space: ref.space,
      externalId: ref.externalId,
      version: ref.version,
    },
  ])) as { items?: unknown[] };

  return parseViewDefinition(response.items?.[0]);
}

function stubNode(
  ref: ViewInheritanceRef,
  depth: number,
  flags: { cycle?: boolean; truncated?: boolean; missing?: boolean }
): ViewInheritanceNode {
  return {
    id: viewInheritanceKey(ref),
    space: ref.space,
    externalId: ref.externalId,
    version: ref.version,
    name: null,
    depth,
    children: [],
    cycle: flags.cycle === true,
    truncated: flags.truncated === true,
    missing: flags.missing === true,
  };
}

export async function buildViewInheritanceTree(
  sdk: CogniteClient,
  rootRef: ViewInheritanceRef,
  options?: { maxDepth?: number; maxNodes?: number }
): Promise<ViewInheritanceTree> {
  const maxDepth = options?.maxDepth ?? DEFAULT_MAX_DEPTH;
  const maxNodes = options?.maxNodes ?? DEFAULT_MAX_NODES;
  let nodeCount = 0;
  let truncated = false;

  async function buildNode(
    ref: ViewInheritanceRef,
    depth: number,
    visiting: Set<string>
  ): Promise<ViewInheritanceNode> {
    const id = viewInheritanceKey(ref);
    if (visiting.has(id)) {
      return { ...stubNode(ref, depth, { cycle: true }), name: ref.externalId };
    }
    if (depth > maxDepth || nodeCount >= maxNodes) {
      truncated = true;
      return stubNode(ref, depth, { truncated: true });
    }

    visiting.add(id);
    nodeCount += 1;

    const definition = await retrieveViewDefinition(sdk, ref);
    if (definition === null) {
      visiting.delete(id);
      return stubNode(ref, depth, { missing: true });
    }

    const implementsRefs =
      definition.implements?.map((entry) => ({
        space: entry.space,
        externalId: entry.externalId,
        version: entry.version,
      })) ?? [];

    const children: ViewInheritanceNode[] = [];
    for (const childRef of implementsRefs) {
      if (nodeCount >= maxNodes) {
        truncated = true;
        children.push(stubNode(childRef, depth + 1, { truncated: true }));
        continue;
      }
      children.push(await buildNode(childRef, depth + 1, new Set(visiting)));
    }

    visiting.delete(id);

    return {
      id,
      space: definition.space,
      externalId: definition.externalId,
      version: definition.version,
      name: definition.name ?? null,
      depth,
      children,
      cycle: false,
      truncated: false,
      missing: false,
    };
  }

  const root = await buildNode(rootRef, 0, new Set());
  return { root, nodeCount, truncated };
}

export function flattenViewInheritanceTree(tree: ViewInheritanceTree): ViewInheritanceRef[] {
  const refs: ViewInheritanceRef[] = [];

  const visit = (node: ViewInheritanceNode) => {
    refs.push({
      space: node.space,
      externalId: node.externalId,
      version: node.version,
    });
    for (const child of node.children) {
      visit(child);
    }
  };

  visit(tree.root);
  return refs;
}

export async function collectViewInheritanceClosure(
  sdk: CogniteClient,
  roots: ViewInheritanceRef[]
): Promise<ViewInheritanceRef[]> {
  const byKey = new Map<string, ViewInheritanceRef>();

  const add = (ref: ViewInheritanceRef) => {
    byKey.set(viewInheritanceKey(ref), ref);
  };

  await Promise.all(
    roots.map(async (root) => {
      add(root);
      const tree = await buildViewInheritanceTree(sdk, root);
      for (const ref of flattenViewInheritanceTree(tree)) {
        add(ref);
      }
    })
  );

  return [...byKey.values()].sort((a, b) =>
    `${a.space}/${a.externalId}/${a.version}`.localeCompare(`${b.space}/${b.externalId}/${b.version}`)
  );
}
