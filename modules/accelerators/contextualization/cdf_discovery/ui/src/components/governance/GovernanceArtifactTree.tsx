import { useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import {
  ancestorDirPrefixes,
  buildArtifactTree,
  collectDirRelPathsFromTree,
  type ArtifactTreeNode,
} from "../../utils/artifactTree";

type Props = {
  paths: string[];
  selectedPath: string | null;
  onSelectFile: (rel: string) => void;
};

export function GovernanceArtifactTree({ paths, selectedPath, onSelectFile }: Props) {
  const { t } = useAppSettings();
  const [query, setQuery] = useState("");
  const tree = useMemo(() => {
    const q = query.trim().toLowerCase();
    const filtered = q ? paths.filter((p) => p.toLowerCase().includes(q)) : paths;
    return buildArtifactTree(filtered);
  }, [paths, query]);

  const [expanded, setExpanded] = useState<Set<string>>(() => new Set());

  useEffect(() => {
    const rootDirs = tree
      .filter((n): n is Extract<ArtifactTreeNode, { kind: "dir" }> => n.kind === "dir")
      .map((d) => d.relPath);
    setExpanded((prev) => {
      const next = new Set(prev);
      for (const r of rootDirs) next.add(r);
      if (selectedPath) {
        for (const a of ancestorDirPrefixes(selectedPath)) next.add(a);
      }
      if (query.trim()) {
        for (const d of collectDirRelPathsFromTree(tree)) next.add(d);
      }
      return next;
    });
  }, [tree, selectedPath, query]);

  if (paths.length === 0) {
    return <p className="disc-empty-hint">{t("governance.artifacts.empty")}</p>;
  }

  const filteredCount = query.trim()
    ? paths.filter((p) => p.toLowerCase().includes(query.trim().toLowerCase())).length
    : paths.length;
  if (filteredCount === 0) {
    return <p className="disc-empty-hint">{t("governance.artifacts.noMatch")}</p>;
  }

  return (
    <div className="disc-gov-art-tree-wrap">
      <label className="disc-gov-label disc-gov-art-tree-search">
        {t("governance.artifacts.filter")}
        <input
          className="disc-input"
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={t("governance.artifacts.filterPlaceholder")}
        />
      </label>
      <ul className="disc-gov-art-tree" role="tree">
        {tree.map((node, i) => (
          <ArtifactTreeRows
            key={node.kind === "dir" ? node.relPath : `file-${node.rel}-${i}`}
            node={node}
            depth={0}
            expanded={expanded}
            onToggleDir={(relPath) =>
              setExpanded((prev) => {
                const next = new Set(prev);
                if (next.has(relPath)) next.delete(relPath);
                else next.add(relPath);
                return next;
              })
            }
            onSelectFile={onSelectFile}
            selectedPath={selectedPath}
          />
        ))}
      </ul>
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
      <li className="disc-gov-art-tree-li" role="none">
        <button
          type="button"
          role="treeitem"
          className={`disc-gov-artifact-item${active ? " disc-gov-artifact-item--active" : ""}`}
          style={{ paddingLeft: `${8 + depth * 14}px` }}
          onClick={() => onSelectFile(node.rel)}
        >
          {node.name}
        </button>
      </li>
    );
  }

  const isOpen = expanded.has(node.relPath);
  return (
    <li className="disc-gov-art-tree-li" role="none">
      <div className="disc-gov-art-tree-dir-row" style={{ paddingLeft: `${depth * 14}px` }}>
        <button
          type="button"
          className="disc-gov-art-tree-chevron"
          aria-expanded={isOpen}
          aria-label={isOpen ? t("artifacts.treeCollapse") : t("artifacts.treeExpand")}
          onClick={() => onToggleDir(node.relPath)}
        >
          {isOpen ? "▼" : "▶"}
        </button>
        <button
          type="button"
          role="treeitem"
          className="disc-gov-art-tree-dir-label"
          onClick={() => onToggleDir(node.relPath)}
        >
          {node.name}
        </button>
      </div>
      {isOpen && (
        <ul className="disc-gov-art-tree-nested" role="group">
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
