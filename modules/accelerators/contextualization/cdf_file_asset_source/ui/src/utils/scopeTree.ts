import type { ScopeHierarchyData, ScopeNode } from "../types/assetConfig";

export type ScopePath = number[];

export type DocumentRow = {
  fileId: string;
  path: ScopePath;
  pathLabel: string;
  systemName: string;
};

export function pathKey(path: ScopePath): string {
  return path.join("/");
}

export function ancestorBranchKeys(selectedPath: ScopePath): string[] {
  const out: string[] = [];
  for (let len = 1; len < selectedPath.length; len++) {
    out.push(pathKey(selectedPath.slice(0, len)));
  }
  return out;
}

export function collectBranchKeysWithChildren(nodes: ScopeNode[], base: ScopePath = []): string[] {
  const out: string[] = [];
  nodes.forEach((n, i) => {
    const p = [...base, i];
    const kids = n.locations ?? [];
    if (kids.length > 0) {
      out.push(pathKey(p));
      out.push(...collectBranchKeysWithChildren(kids, p));
    }
  });
  return out;
}

export function pathLabel(nodes: ScopeNode[], path: ScopePath): string {
  const parts: string[] = [];
  let cur: ScopeNode[] = nodes;
  for (const i of path) {
    const n = cur[i];
    if (!n) break;
    parts.push(n.name?.trim() || n.id?.trim() || `(${i})`);
    cur = n.locations ?? [];
  }
  return parts.join(" → ");
}

export function getNodeAtPath(nodes: ScopeNode[], path: ScopePath): ScopeNode | null {
  if (path.length === 0) return null;
  let cur: ScopeNode | undefined = nodes[path[0]];
  if (!cur) return null;
  for (let k = 1; k < path.length; k++) {
    const children: ScopeNode[] | undefined = cur.locations;
    if (!children) return null;
    cur = children[path[k]];
    if (!cur) return null;
  }
  return cur;
}

export function replaceAtPath(nodes: ScopeNode[], path: ScopePath, node: ScopeNode): ScopeNode[] {
  if (path.length === 0) return nodes;
  const [i, ...rest] = path;
  if (rest.length === 0) {
    return nodes.map((n, j) => (j === i ? node : n));
  }
  const row = nodes[i];
  if (!row) return nodes;
  const locs = row.locations ? [...row.locations] : [];
  return nodes.map((n, j) =>
    j === i ? { ...n, locations: replaceAtPath(locs, rest, node) } : n
  );
}

export function collectDocumentRows(nodes: ScopeNode[], base: ScopePath = []): DocumentRow[] {
  const out: DocumentRow[] = [];
  nodes.forEach((n, i) => {
    const p = [...base, i];
    const kids = n.locations ?? [];
    if (kids.length === 0) {
      const systemName = n.name?.trim() || pathLabel(nodes, p);
      for (const fileId of n.files ?? []) {
        const id = fileId.trim();
        if (id) {
          out.push({
            fileId: id,
            path: p,
            pathLabel: pathLabel(nodes, p),
            systemName,
          });
        }
      }
    } else {
      out.push(...collectDocumentRows(kids, p));
    }
  });
  return out;
}

export function collectLeafPaths(nodes: ScopeNode[], base: ScopePath = []): ScopePath[] {
  const out: ScopePath[] = [];
  nodes.forEach((n, i) => {
    const p = [...base, i];
    const kids = n.locations ?? [];
    if (kids.length === 0) out.push(p);
    else out.push(...collectLeafPaths(kids, p));
  });
  return out;
}

export function setFilesAtPath(
  data: ScopeHierarchyData,
  path: ScopePath,
  files: string[]
): ScopeHierarchyData {
  const node = getNodeAtPath(data.scope, path);
  if (!node) return data;
  return {
    ...data,
    scope: replaceAtPath(data.scope, path, { ...node, files, locations: [] }),
  };
}

export function addFileAtPath(
  data: ScopeHierarchyData,
  path: ScopePath,
  fileId: string
): ScopeHierarchyData {
  const node = getNodeAtPath(data.scope, path);
  if (!node) return data;
  const id = fileId.trim();
  if (!id) return data;
  const existing = new Set((node.files ?? []).map((f) => f.trim()).filter(Boolean));
  if (existing.has(id)) return data;
  return setFilesAtPath(data, path, [...existing, id]);
}

export function removeFileFromTree(
  data: ScopeHierarchyData,
  path: ScopePath,
  fileId: string
): ScopeHierarchyData {
  const node = getNodeAtPath(data.scope, path);
  if (!node) return data;
  const next = (node.files ?? []).filter((f) => f.trim() !== fileId.trim());
  return setFilesAtPath(data, path, next);
}

export function moveFileInTree(
  data: ScopeHierarchyData,
  fromPath: ScopePath,
  fileId: string,
  toPath: ScopePath
): ScopeHierarchyData {
  let next = removeFileFromTree(data, fromPath, fileId);
  next = addFileAtPath(next, toPath, fileId);
  return next;
}

export function dedupeFilesInTree(data: ScopeHierarchyData): ScopeHierarchyData {
  const walk = (nodes: ScopeNode[]): ScopeNode[] =>
    nodes.map((n) => {
      const kids = n.locations ?? [];
      if (kids.length === 0) {
        const seen = new Set<string>();
        const files: string[] = [];
        for (const f of n.files ?? []) {
          const id = f.trim();
          if (!id || seen.has(id)) continue;
          seen.add(id);
          files.push(id);
        }
        return { ...n, files, locations: [] };
      }
      return { ...n, locations: walk(kids) };
    });
  return { ...data, scope: walk(data.scope) };
}
