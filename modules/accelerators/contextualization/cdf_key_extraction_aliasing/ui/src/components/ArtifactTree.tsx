import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import {
  ancestorDirPrefixes,
  buildArtifactTree,
  collectDirRelPathsFromTree,
  type ArtifactTreeNode,
} from "../utils/artifactTree";

type Props = {
  paths: string[];
  selectedPath: string | null;
  onSelectFile: (rel: string) => void;
};

export function ArtifactTree({ paths, selectedPath, onSelectFile }: Props) {
  const { t } = useAppSettings();
  const [query, setQuery] = useState("");

  const filteredPaths = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return paths;
    return paths.filter((p) => p.toLowerCase().includes(q));
  }, [paths, query]);

  const tree = useMemo(() => buildArtifactTree(filteredPaths), [filteredPaths]);

  const [expanded, setExpanded] = useState<Set<string>>(() => new Set());

  useEffect(() => {
    const rootDirs = tree
      .filter((n): n is Extract<ArtifactTreeNode, { kind: "dir" }> => n.kind === "dir")
      .map((d) => d.relPath);
    const allDirsUnderFilter = collectDirRelPathsFromTree(tree);
    setExpanded((prev) => {
      const next = new Set(prev);
      if (query.trim()) {
        for (const d of allDirsUnderFilter) next.add(d);
      } else {
        for (const r of rootDirs) next.add(r);
      }
      if (selectedPath) for (const a of ancestorDirPrefixes(selectedPath)) next.add(a);
      return next;
    });
  }, [tree, selectedPath, query]);

  const toggleDir = (relPath: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(relPath)) next.delete(relPath);
      else next.add(relPath);
      return next;
    });
  };

  const searching = query.trim().length > 0;
  const noMatches = paths.length > 0 && searching && filteredPaths.length === 0;

  let treeBody: ReactNode;
  if (paths.length === 0) {
    treeBody = <p className="kea-hint kea-art-tree-empty">—</p>;
  } else if (noMatches) {
    treeBody = <p className="kea-hint kea-art-tree-empty">{t("artifacts.noSearchResults")}</p>;
  } else {
    treeBody = (
      <ul className="kea-art-tree" role="tree">
        {tree.map((node, i) => (
          <ArtifactTreeRows
            key={node.kind === "dir" ? node.relPath : `file-${node.rel}-${i}`}
            node={node}
            depth={0}
            expanded={expanded}
            onToggleDir={toggleDir}
            onSelectFile={onSelectFile}
            selectedPath={selectedPath}
          />
        ))}
      </ul>
    );
  }

  return (
    <div className="kea-art-tree-panel">
      <label className="kea-label kea-art-tree-search-label" title={t("artifacts.searchLabel.tooltip")}>
        {t("artifacts.searchLabel")}
        <input
          type="search"
          className="kea-input kea-art-tree-search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={t("artifacts.searchPlaceholder")}
          autoComplete="off"
          spellCheck={false}
          disabled={paths.length === 0}
        />
      </label>
      <div className="kea-artifact-tree-wrap">{treeBody}</div>
    </div>
  );
}

function ArtifactTreeRows({
  node,
  depth,
  expanded,
  onToggleDir,
  onSelectFile,
  selectedPath,
}: {
  node: ArtifactTreeNode;
  depth: number;
  expanded: Set<string>;
  onToggleDir: (relPath: string) => void;
  onSelectFile: (rel: string) => void;
  selectedPath: string | null;
}) {
  const { t } = useAppSettings();
  if (node.kind === "file") {
    const active = selectedPath === node.rel;
    return (
      <li className="kea-art-tree-li" role="none">
        <button
          type="button"
          role="treeitem"
          className={active ? "kea-art-tree-file kea-art-tree-file--active" : "kea-art-tree-file"}
          style={{ paddingLeft: 8 + depth * 14 }}
          onClick={() => onSelectFile(node.rel)}
        >
          {node.name}
        </button>
      </li>
    );
  }

  const isOpen = expanded.has(node.relPath);
  return (
    <li className="kea-art-tree-li" role="none">
      <div
        className="kea-art-tree-dir-row"
        style={{ paddingLeft: depth * 14 }}
        role="presentation"
      >
        <button
          type="button"
          className="kea-art-tree-chevron"
          aria-expanded={isOpen}
          aria-label={isOpen ? t("artifacts.treeCollapse") : t("artifacts.treeExpand")}
          onClick={() => onToggleDir(node.relPath)}
        >
          <span aria-hidden>{isOpen ? "▼" : "▶"}</span>
        </button>
        <button
          type="button"
          role="treeitem"
          className="kea-art-tree-dir-label"
          onClick={() => onToggleDir(node.relPath)}
        >
          {node.name}
        </button>
      </div>
      {isOpen && (
        <ul className="kea-art-tree-nested" role="group">
          {node.children.map((child, i) => (
            <ArtifactTreeRows
              key={child.kind === "dir" ? child.relPath : `file-${child.rel}-${i}`}
              node={child}
              depth={depth + 1}
              expanded={expanded}
              onToggleDir={onToggleDir}
              onSelectFile={onSelectFile}
              selectedPath={selectedPath}
            />
          ))}
        </ul>
      )}
    </li>
  );
}
