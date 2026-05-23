import { useCallback, useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { DocumentCatalogEditor } from "./DocumentCatalogEditor";
import { DeferredCommitInput, DeferredCommitTextarea } from "./DeferredCommitTextField";
import type { ScopeHierarchyData, ScopeNode } from "../types/assetConfig";
import { emptyScopeNode } from "../types/assetConfig";
import {
  ancestorBranchKeys,
  collectBranchKeysWithChildren,
  getNodeAtPath,
  pathKey,
  pathLabel,
  replaceAtPath,
  type ScopePath,
} from "../utils/scopeTree";

type Props = {
  value: ScopeHierarchyData;
  onChange: (next: ScopeHierarchyData) => void;
};

type EditorView = "hierarchy" | "documents";

function addChildAtPath(nodes: ScopeNode[], path: ScopePath): ScopeNode[] {
  if (path.length === 0) return [...nodes, emptyScopeNode()];
  const [i, ...rest] = path;
  const row = nodes[i];
  if (!row) return nodes;
  const locs = row.locations ? [...row.locations] : [];
  if (rest.length === 0) {
    return nodes.map((n, j) =>
      j === i ? { ...n, locations: [...locs, emptyScopeNode()] } : n
    );
  }
  return nodes.map((n, j) =>
    j === i ? { ...n, locations: addChildAtPath(locs, rest) } : n
  );
}

function removeAtPath(nodes: ScopeNode[], path: ScopePath): ScopeNode[] {
  const [i, ...rest] = path;
  if (rest.length === 0) return nodes.filter((_, j) => j !== i);
  const row = nodes[i];
  if (!row?.locations) return nodes;
  return nodes.map((n, j) =>
    j === i ? { ...n, locations: removeAtPath(row.locations!, rest) } : n
  );
}

function ScopeTree({
  nodes,
  path,
  selectedPath,
  onSelect,
  expandedBranches,
  onToggleBranch,
}: {
  nodes: ScopeNode[];
  path: ScopePath;
  selectedPath: ScopePath;
  onSelect: (p: ScopePath) => void;
  expandedBranches: Set<string>;
  onToggleBranch: (key: string) => void;
}) {
  const { t } = useAppSettings();
  const nested = path.length > 0;
  return (
    <ul className={nested ? "fas-dim-tree-nested" : "fas-dim-tree-root"} role="group">
      {nodes.map((n, i) => {
        const p = [...path, i];
        const key = pathKey(p);
        const sel =
          p.length === selectedPath.length && p.every((v, j) => v === selectedPath[j]);
        const kids = n.locations ?? [];
        const hasKids = kids.length > 0;
        const open = hasKids && expandedBranches.has(key);
        const label = n.name?.trim() || t("scope.unnamed");
        const fileCount = (n.files ?? []).length;
        return (
          <li key={key} className="fas-dim-tree-li" role="none">
            <div className="fas-dim-tree-row">
              {hasKids ? (
                <button
                  type="button"
                  className={
                    open
                      ? "fas-dim-tree-chevron fas-dim-tree-chevron--open"
                      : "fas-dim-tree-chevron"
                  }
                  aria-expanded={open}
                  aria-label={t("scope.toggleBranch")}
                  onClick={(e) => {
                    e.stopPropagation();
                    onToggleBranch(key);
                  }}
                >
                  <span aria-hidden>▾</span>
                </button>
              ) : (
                <span className="fas-dim-tree-chevron-spacer" aria-hidden />
              )}
              <button
                type="button"
                role="treeitem"
                aria-selected={sel}
                className={sel ? "fas-tree-node fas-tree-node--selected" : "fas-tree-node"}
                onClick={() => onSelect(p)}
              >
                <span className="fas-dim-tree-node-label">{label}</span>
                {fileCount > 0 ? (
                  <span className="fas-dim-tree-node-meta">
                    {t("scope.fileCount", { count: String(fileCount) })}
                  </span>
                ) : null}
              </button>
            </div>
            {hasKids && open ? (
              <ScopeTree
                nodes={kids}
                path={p}
                selectedPath={selectedPath}
                onSelect={onSelect}
                expandedBranches={expandedBranches}
                onToggleBranch={onToggleBranch}
              />
            ) : null}
          </li>
        );
      })}
    </ul>
  );
}

export function ScopeHierarchyEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const tree = value.scope ?? [];
  const levelsKey = JSON.stringify(value.hierarchy_levels ?? []);
  const [levelsDraft, setLevelsDraft] = useState(() => value.hierarchy_levels.join(", "));
  const [selectedPath, setSelectedPath] = useState<ScopePath>(() => (tree.length ? [0] : []));
  const [expandedBranches, setExpandedBranches] = useState<Set<string>>(() => new Set());
  const [editorView, setEditorView] = useState<EditorView>("hierarchy");

  useEffect(() => {
    setLevelsDraft(value.hierarchy_levels.join(", "));
  }, [levelsKey]);

  useEffect(() => {
    setExpandedBranches((prev) => {
      const next = new Set(prev);
      for (const k of ancestorBranchKeys(selectedPath)) {
        next.add(k);
      }
      return next;
    });
  }, [selectedPath]);

  const selected = selectedPath.length > 0 ? getNodeAtPath(tree, selectedPath) : null;
  const isLeaf = selected != null && (selected.locations?.length ?? 0) === 0;

  const commit = useCallback(
    (next: ScopeHierarchyData) => onChange(next),
    [onChange]
  );

  const updateNode = (patch: Partial<ScopeNode>) => {
    if (!selected) return;
    commit({
      ...value,
      scope: replaceAtPath(tree, selectedPath, { ...selected, ...patch }),
    });
  };

  const updateLevels = (text: string) => {
    const hierarchy_levels = text
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    commit({ ...value, hierarchy_levels });
  };

  const addRoot = () => {
    commit({ ...value, scope: [...tree, emptyScopeNode()] });
    setSelectedPath([tree.length]);
  };

  const addChild = () => {
    if (selectedPath.length === 0) return;
    const next = addChildAtPath(tree, selectedPath);
    commit({ ...value, scope: next });
    const parent = getNodeAtPath(next, selectedPath);
    const idx = (parent?.locations?.length ?? 1) - 1;
    setSelectedPath([...selectedPath, idx]);
    setExpandedBranches((e) => new Set(e).add(pathKey(selectedPath)));
  };

  const removeSel = () => {
    if (selectedPath.length === 0) return;
    const next = removeAtPath(tree, selectedPath);
    commit({ ...value, scope: next });
    setSelectedPath((p) => {
      if (p.length === 1) {
        const idx = Math.max(0, p[0] - 1);
        return next.length ? [Math.min(idx, next.length - 1)] : [];
      }
      return p.slice(0, -1);
    });
  };

  const expandAll = () => {
    setExpandedBranches(new Set(collectBranchKeysWithChildren(tree)));
  };

  const collapseToSelection = () => {
    setExpandedBranches(new Set(ancestorBranchKeys(selectedPath)));
  };

  return (
    <div className="fas-scope-hierarchy">
      <nav className="fas-tabs fas-tabs--sub fas-scope-hierarchy__tabs" aria-label={t("scope.editorNav")}>
        <button
          type="button"
          className={`fas-tab${editorView === "hierarchy" ? " fas-tab--active" : ""}`}
          onClick={() => setEditorView("hierarchy")}
        >
          {t("scope.view.hierarchy")}
        </button>
        <button
          type="button"
          className={`fas-tab${editorView === "documents" ? " fas-tab--active" : ""}`}
          onClick={() => setEditorView("documents")}
        >
          {t("scope.view.documents")}
        </button>
      </nav>

      {editorView === "documents" ? (
        <DocumentCatalogEditor
          value={value}
          onChange={commit}
          selectedLeafPath={isLeaf ? selectedPath : selectedPath}
          onSelectLeafPath={(p) => {
            setSelectedPath(p);
            setEditorView("hierarchy");
          }}
        />
      ) : (
        <div className="fas-grid-2">
          <div className="fas-stack">
            <label className="fas-label" title={t("scope.levelsLabel.tooltip")}>
              {t("scope.levelsLabel")}
              <input
                className="fas-input"
                value={levelsDraft}
                onChange={(e) => setLevelsDraft(e.target.value)}
                onBlur={() => updateLevels(levelsDraft)}
              />
            </label>
            <div className="fas-toolbar" style={{ marginBottom: 0 }}>
              <button type="button" className="fas-btn fas-btn--sm" onClick={addRoot}>
                {t("scope.addRoot")}
              </button>
              <button type="button" className="fas-btn fas-btn--sm" onClick={addChild} disabled={!selected}>
                {t("scope.addChild")}
              </button>
              <button
                type="button"
                className="fas-btn fas-btn--sm fas-btn--danger"
                onClick={removeSel}
                disabled={!selected}
              >
                {t("scope.remove")}
              </button>
            </div>
            <p className="fas-dim-tree-legend" title={t("scope.hierarchyTree.tooltip")}>
              {t("scope.hierarchyTree")}
            </p>
            <div className="fas-toolbar fas-dim-tree-toolbar">
              <button
                type="button"
                className="fas-btn fas-btn--sm fas-btn--ghost"
                onClick={expandAll}
                disabled={tree.length === 0}
              >
                {t("scope.expandAll")}
              </button>
              <button
                type="button"
                className="fas-btn fas-btn--sm fas-btn--ghost"
                onClick={collapseToSelection}
                disabled={tree.length === 0}
              >
                {t("scope.collapseAll")}
              </button>
            </div>
            <div className="fas-tree fas-dim-tree" role="tree" aria-label={t("scope.hierarchyTree")}>
              {tree.length === 0 ? (
                <span className="fas-tree-empty">{t("scope.noLocations")}</span>
              ) : (
                <ScopeTree
                  nodes={tree}
                  path={[]}
                  selectedPath={selectedPath}
                  onSelect={setSelectedPath}
                  expandedBranches={expandedBranches}
                  onToggleBranch={(key) => {
                    setExpandedBranches((prev) => {
                      const next = new Set(prev);
                      if (next.has(key)) next.delete(key);
                      else next.add(key);
                      return next;
                    });
                  }}
                />
              )}
            </div>
          </div>
          <div className="fas-properties-panel">
            <h3 title={t("scope.properties.tooltip")}>{t("scope.properties")}</h3>
            {selected ? (
              <div className="fas-stack">
                <p className="fas-hint fas-scope-path-line">
                  {t("scope.path")}: {pathLabel(tree, selectedPath)}
                </p>
                <label className="fas-label" title={t("scope.field.name.tooltip")}>
                  {t("scope.field.name")}
                  <DeferredCommitInput
                    className="fas-input"
                    committedValue={selected.name ?? ""}
                    syncKey={selectedPath.join("/")}
                    onCommit={(v) => updateNode({ name: v })}
                  />
                </label>
                <label className="fas-label" title={t("scope.field.description.tooltip")}>
                  {t("scope.field.description")}
                  <DeferredCommitTextarea
                    className="fas-textarea"
                    rows={3}
                    committedValue={selected.description ?? ""}
                    syncKey={selectedPath.join("/")}
                    onCommit={(v) => updateNode({ description: v })}
                    spellCheck
                    style={{ minHeight: "4.5rem", fontFamily: "inherit" }}
                  />
                </label>
                {isLeaf ? (
                  <>
                    <label className="fas-label" title={t("scope.field.files.tooltip")}>
                      {t("scope.field.files")}
                      <DeferredCommitTextarea
                        className="fas-textarea fas-textarea--mono"
                        rows={10}
                        committedValue={(selected.files ?? []).join("\n")}
                        syncKey={`${selectedPath.join("/")}-files`}
                        onCommit={(v) => {
                          const files = v
                            .split(/\r?\n/)
                            .map((s) => s.trim())
                            .filter(Boolean);
                          updateNode({ files, locations: [] });
                        }}
                        spellCheck={false}
                      />
                    </label>
                    <button
                      type="button"
                      className="fas-btn fas-btn--sm fas-btn--ghost"
                      onClick={() => setEditorView("documents")}
                    >
                      {t("scope.openDocumentCatalog")}
                    </button>
                  </>
                ) : (
                  <p className="fas-hint" style={{ marginBottom: 0 }}>
                    {t("scope.leafHint")}
                  </p>
                )}
              </div>
            ) : (
              <span className="fas-tree-empty">{t("scope.selectNode")}</span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
