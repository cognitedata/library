import { useCallback, useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { AliasingScopeHierarchy, LocationNode } from "../types/scopeConfig";
import { emptyLocationNode } from "../types/scopeConfig";

type Props = {
  value: AliasingScopeHierarchy;
  onChange: (next: AliasingScopeHierarchy) => void;
};

type NormalizedHierarchy = { levels: string[]; locations: LocationNode[] };

function normalizeHierarchy(v: AliasingScopeHierarchy): NormalizedHierarchy {
  return {
    levels: Array.isArray(v.levels) ? [...v.levels] : [],
    locations: Array.isArray(v.locations) ? v.locations : [],
  };
}

function replaceAtPath(
  nodes: LocationNode[],
  path: number[],
  node: LocationNode
): LocationNode[] {
  if (path.length === 0) return nodes;
  const [i, ...rest] = path;
  if (rest.length === 0) {
    return nodes.map((n, j) => (j === i ? node : n));
  }
  const row = nodes[i];
  if (!row) return nodes;
  const locs: LocationNode[] = row.locations ? [...row.locations] : [];
  return nodes.map((n, j) =>
    j === i ? { ...n, locations: replaceAtPath(locs, rest, node) } : n
  );
}

function removeAtPath(nodes: LocationNode[], path: number[]): LocationNode[] {
  if (path.length === 0) return nodes;
  const [i, ...rest] = path;
  if (rest.length === 0) {
    return nodes.filter((_, j) => j !== i);
  }
  const row = nodes[i];
  if (!row?.locations) return nodes;
  const nextChild = removeAtPath(row.locations, rest);
  return nodes.map((n, j) => (j === i ? { ...n, locations: nextChild } : n));
}

function pathKey(path: number[]): string {
  return path.join("/");
}

function collectBranchKeysWithChildren(
  nodes: LocationNode[],
  base: number[] = []
): string[] {
  const out: string[] = [];
  nodes.forEach((n, i) => {
    const p = [...base, i];
    const kids = n.locations;
    if (kids && kids.length > 0) {
      out.push(pathKey(p));
      out.push(...collectBranchKeysWithChildren(kids, p));
    }
  });
  return out;
}

function ancestorBranchKeys(selectedPath: number[]): string[] {
  const out: string[] = [];
  for (let len = 1; len < selectedPath.length; len++) {
    out.push(pathKey(selectedPath.slice(0, len)));
  }
  return out;
}

function addChildAtPath(nodes: LocationNode[], path: number[]): LocationNode[] {
  if (path.length === 0) {
    return [...nodes, emptyLocationNode()];
  }
  const [i, ...rest] = path;
  const row = nodes[i];
  if (!row) return nodes;
  const locs: LocationNode[] = row.locations ? [...row.locations] : [];
  if (rest.length === 0) {
    return nodes.map((n, j) =>
      j === i
        ? { ...n, locations: [...locs, emptyLocationNode()] }
        : n
    );
  }
  return nodes.map((n, j) =>
    j === i ? { ...n, locations: addChildAtPath(locs, rest) } : n
  );
}

function getNodeAtPath(nodes: LocationNode[], path: number[]): LocationNode | null {
  let cur: LocationNode | undefined = nodes[path[0]];
  if (!cur) return null;
  for (let k = 1; k < path.length; k++) {
    const nextLevel: LocationNode[] | undefined = cur.locations;
    if (!nextLevel) return null;
    const nextNode: LocationNode | undefined = nextLevel[path[k]];
    if (!nextNode) return null;
    cur = nextNode;
  }
  return cur;
}

function LocationTreeList({
  nodes,
  path,
  selectedPath,
  onSelect,
  expandedBranches,
  onToggleBranch,
}: {
  nodes: LocationNode[];
  path: number[];
  selectedPath: number[];
  onSelect: (p: number[]) => void;
  expandedBranches: Set<string>;
  onToggleBranch: (key: string) => void;
}) {
  const { t } = useAppSettings();
  const nested = path.length > 0;
  return (
    <ul
      className={nested ? "kea-dim-tree-nested" : "kea-dim-tree-root"}
      role="group"
    >
      {nodes.map((n, i) => {
        const p = [...path, i];
        const key = pathKey(p);
        const sel =
          p.length === selectedPath.length && p.every((v, j) => v === selectedPath[j]);
        const label = n.name || n.id || t("scope.unnamed");
        const childList = n.locations;
        const hasChildren = Boolean(childList && childList.length > 0);
        const open = hasChildren && expandedBranches.has(key);
        return (
          <li key={key} className="kea-dim-tree-li" role="none">
            <div className="kea-dim-tree-row">
              {hasChildren ? (
                <button
                  type="button"
                  className={
                    open
                      ? "kea-dim-tree-chevron kea-dim-tree-chevron--open"
                      : "kea-dim-tree-chevron"
                  }
                  aria-expanded={open}
                  aria-label={t("scope.toggleBranch")}
                  title={t("scope.toggleBranch")}
                  onClick={(e) => {
                    e.stopPropagation();
                    onToggleBranch(key);
                  }}
                >
                  <span aria-hidden>▾</span>
                </button>
              ) : (
                <span className="kea-dim-tree-chevron-spacer" aria-hidden />
              )}
              <button
                type="button"
                role="treeitem"
                aria-selected={sel}
                className={sel ? "kea-tree-node kea-tree-node--selected" : "kea-tree-node"}
                onClick={() => onSelect(p)}
              >
                <span className="kea-dim-tree-node-id">{n.id ? String(n.id) : "—"}</span>
                <span className="kea-dim-tree-node-label">{label}</span>
              </button>
            </div>
            {hasChildren && open && (
              <LocationTreeList
                nodes={childList!}
                path={p}
                selectedPath={selectedPath}
                onSelect={onSelect}
                expandedBranches={expandedBranches}
                onToggleBranch={onToggleBranch}
              />
            )}
          </li>
        );
      })}
    </ul>
  );
}

export function ScopeHierarchyEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const h = normalizeHierarchy(value);
  const tree = h.locations || [];

  const [selectedPath, setSelectedPath] = useState<number[]>(() =>
    tree.length ? [0] : []
  );
  const [expandedBranches, setExpandedBranches] = useState<Set<string>>(
    () => new Set()
  );

  const selected =
    selectedPath.length > 0 ? getNodeAtPath(tree, selectedPath) : null;

  useEffect(() => {
    setExpandedBranches((prev) => {
      const next = new Set(prev);
      for (const k of ancestorBranchKeys(selectedPath)) {
        next.add(k);
      }
      return next;
    });
  }, [selectedPath]);

  const commit = useCallback(
    (next: NormalizedHierarchy) => {
      onChange({ ...value, levels: next.levels, locations: next.locations });
    },
    [value, onChange]
  );

  const expandAll = useCallback(() => {
    setExpandedBranches(new Set(collectBranchKeysWithChildren(tree)));
  }, [tree]);

  const collapseToSelection = useCallback(() => {
    setExpandedBranches(new Set(ancestorBranchKeys(selectedPath)));
  }, [tree, selectedPath]);

  const updateNode = (patch: Partial<LocationNode>) => {
    if (!selected) return;
    const next = { ...selected, ...patch };
    commit({
      ...h,
      locations: replaceAtPath(tree, selectedPath, next),
    });
  };

  const updateLevels = (text: string) => {
    const levels = text
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    commit({ ...h, levels });
  };

  const addRoot = () => {
    commit({
      ...h,
      locations: [...tree, emptyLocationNode()],
    });
    setSelectedPath([tree.length]);
  };

  const addChild = () => {
    if (selectedPath.length === 0) return;
    const newTree = addChildAtPath(tree, selectedPath);
    commit({ ...h, locations: newTree });
    const parent = getNodeAtPath(newTree, selectedPath);
    const idx = (parent?.locations?.length || 1) - 1;
    setSelectedPath([...selectedPath, idx]);
  };

  const removeSel = () => {
    if (selectedPath.length === 0) return;
    const nextTree = removeAtPath(tree, selectedPath);
    commit({ ...h, locations: nextTree });
    setSelectedPath((p) => {
      if (p.length === 1) {
        const idx = Math.max(0, p[0] - 1);
        return nextTree.length ? [Math.min(idx, nextTree.length - 1)] : [];
      }
      return p.slice(0, -1);
    });
  };

  return (
    <div className="kea-scope-hierarchy">
      <div className="kea-grid-2">
        <div className="kea-stack">
          <label className="kea-label" title={t("scope.levelsLabel.tooltip")}>
            {t("scope.levelsLabel")}
            <input
              className="kea-input"
              value={(h.levels || []).join(", ")}
              onChange={(e) => updateLevels(e.target.value)}
            />
          </label>
          <div className="kea-toolbar" style={{ marginBottom: 0 }}>
            <button type="button" className="kea-btn kea-btn--sm" onClick={addRoot}>
              {t("scope.addRoot")}
            </button>
            <button type="button" className="kea-btn kea-btn--sm" onClick={addChild} disabled={!selected}>
              {t("scope.addChild")}
            </button>
            <button
              type="button"
              className="kea-btn kea-btn--sm kea-btn--danger"
              onClick={removeSel}
              disabled={!selected}
            >
              {t("scope.remove")}
            </button>
          </div>
          <p className="kea-dim-tree-legend" title={t("scope.hierarchyTree.tooltip")}>
            {t("scope.hierarchyTree")}
          </p>
          <div className="kea-toolbar kea-dim-tree-toolbar">
            <button
              type="button"
              className="kea-btn kea-btn--sm kea-btn--ghost"
              onClick={expandAll}
              disabled={tree.length === 0}
            >
              {t("scope.expandAll")}
            </button>
            <button
              type="button"
              className="kea-btn kea-btn--sm kea-btn--ghost"
              onClick={collapseToSelection}
              disabled={tree.length === 0}
            >
              {t("scope.collapseAll")}
            </button>
          </div>
          <div
            className="kea-tree kea-dim-tree"
            role="tree"
            aria-label={t("scope.hierarchyTree")}
          >
            {tree.length === 0 ? (
              <span className="kea-tree-empty">{t("scope.noLocations")}</span>
            ) : (
              <LocationTreeList
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
        <div className="kea-properties-panel">
          <h3 title={t("scope.properties.tooltip")}>{t("scope.properties")}</h3>
          {selected ? (
            <div className="kea-stack">
              <label className="kea-label" title={t("scope.field.id.tooltip")}>
                {t("scope.field.id")}
                <input
                  className="kea-input"
                  value={selected.id ?? ""}
                  onChange={(e) => updateNode({ id: e.target.value })}
                />
              </label>
              <label className="kea-label" title={t("scope.field.name.tooltip")}>
                {t("scope.field.name")}
                <input
                  className="kea-input"
                  value={selected.name ?? ""}
                  onChange={(e) => updateNode({ name: e.target.value })}
                />
              </label>
              <label className="kea-label" title={t("scope.field.description.tooltip")}>
                {t("scope.field.description")}
                <input
                  className="kea-input"
                  value={selected.description ?? ""}
                  onChange={(e) => updateNode({ description: e.target.value })}
                />
              </label>
              <p className="kea-hint" style={{ marginBottom: 0 }}>
                {t("scope.path")}: {selectedPath.join(" → ")}
              </p>
            </div>
          ) : (
            <span className="kea-tree-empty">{t("scope.selectNode")}</span>
          )}
        </div>
      </div>
    </div>
  );
}
