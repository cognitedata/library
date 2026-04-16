/** Build a sorted directory tree from repo-relative paths (forward slashes). */

export type ArtifactTreeNode =
  | { kind: "dir"; name: string; relPath: string; children: ArtifactTreeNode[] }
  | { kind: "file"; name: string; rel: string };

interface MutableNode {
  subdirs: Map<string, MutableNode>;
  files: { name: string; rel: string }[];
}

function emptyNode(): MutableNode {
  return { subdirs: new Map(), files: [] };
}

export function buildArtifactTree(paths: string[]): ArtifactTreeNode[] {
  const root = emptyNode();
  for (const rel of paths) {
    const parts = rel.split("/").filter(Boolean);
    if (parts.length === 0) continue;
    let cur = root;
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      const isLast = i === parts.length - 1;
      if (isLast) {
        cur.files.push({ name: part, rel });
      } else {
        if (!cur.subdirs.has(part)) cur.subdirs.set(part, emptyNode());
        cur = cur.subdirs.get(part)!;
      }
    }
  }

  function toChildren(n: MutableNode, prefix: string): ArtifactTreeNode[] {
    const dirs = [...n.subdirs.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([name, sub]) => {
        const relPath = prefix ? `${prefix}/${name}` : name;
        return {
          kind: "dir" as const,
          name,
          relPath,
          children: toChildren(sub, relPath),
        };
      });
    const files = [...n.files]
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((f) => ({ kind: "file" as const, name: f.name, rel: f.rel }));
    return [...dirs, ...files];
  }

  return toChildren(root, "");
}

/** Directory prefixes for `rel` (not including the file name), for expand state. */
export function ancestorDirPrefixes(rel: string): string[] {
  const parts = rel.split("/").filter(Boolean);
  if (parts.length <= 1) return [];
  const out: string[] = [];
  for (let i = 0; i < parts.length - 1; i++) {
    out.push(parts.slice(0, i + 1).join("/"));
  }
  return out;
}

/** All directory `relPath` values in a built tree (depth-first). */
export function collectDirRelPathsFromTree(nodes: ArtifactTreeNode[]): string[] {
  const out: string[] = [];
  function walk(ns: ArtifactTreeNode[]) {
    for (const n of ns) {
      if (n.kind === "dir") {
        out.push(n.relPath);
        walk(n.children);
      }
    }
  }
  walk(nodes);
  return out;
}
