import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";
import type { IndexNavNode } from "../../types/indexWorkspace";
import { indexNavTree } from "../../utils/indexTabs";
import { navNodeDescription } from "../../utils/navNodeDescriptions";

type Props = {
  selectedNodeId: string | null;
  onSelectNode: (id: string) => void;
  onOpenNode: (node: IndexNavNode) => void;
};

type SearchHit = {
  node: IndexNavNode;
  path: IndexNavNode[];
};

function findPathToNode(
  nodes: IndexNavNode[],
  targetId: string,
  ancestors: IndexNavNode[] = []
): IndexNavNode[] | null {
  for (const node of nodes) {
    if (node.id === targetId) return ancestors;
    if (node.children?.length) {
      const found = findPathToNode(node.children, targetId, [...ancestors, node]);
      if (found) return found;
    }
  }
  return null;
}

function collectSearchHits(
  nodes: IndexNavNode[],
  query: string,
  ancestors: IndexNavNode[],
  t: (key: MessageKey) => string
): SearchHit[] {
  const hits: SearchHit[] = [];
  for (const node of nodes) {
    const label = t(node.labelKey as MessageKey).toLowerCase();
    const path = [...ancestors];
    if (label.includes(query)) {
      hits.push({ node, path });
    }
    if (node.children?.length) {
      hits.push(...collectSearchHits(node.children, query, [...ancestors, node], t));
    }
  }
  return hits;
}

function NavIcon({ children }: { children: ReactNode }) {
  return (
    <span className="idx-nav-row__icon-box">
      <span className="idx-nav-row__icon">{children}</span>
    </span>
  );
}

function NavItemIcon({ node }: { node: IndexNavNode }) {
  const hasChildren = Boolean(node.children?.length);
  if (hasChildren) {
    return (
      <NavIcon>
        <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.75">
          <path d="M4 7h16" />
          <path d="M4 7v11h16V7" />
          <path d="M9 7V5h6v2" />
        </svg>
      </NavIcon>
    );
  }
  return (
    <NavIcon>
      <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.75">
        <path d="M8 4h8l2 2v14H6V4z" />
        <path d="M8 4v4h8" />
      </svg>
    </NavIcon>
  );
}

export function IndexNavTree({ selectedNodeId, onSelectNode, onOpenNode }: Props) {
  const { t } = useAppSettings();
  const [filter, setFilter] = useState("");
  const [stack, setStack] = useState<IndexNavNode[]>([]);
  const [slideDir, setSlideDir] = useState<"forward" | "back" | null>(null);
  const tree = useMemo(() => indexNavTree(), []);

  const currentFolder = stack[stack.length - 1] ?? null;
  const currentNodes = currentFolder?.children ?? tree;
  const isSearching = filter.trim().length > 0;

  const searchHits = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return [];
    return collectSearchHits(tree, q, [], t);
  }, [filter, t, tree]);

  useEffect(() => {
    if (!selectedNodeId) return;
    const path = findPathToNode(tree, selectedNodeId);
    if (path) setStack(path);
  }, [selectedNodeId, tree]);

  useEffect(() => {
    if (!slideDir) return;
    const timer = window.setTimeout(() => setSlideDir(null), 220);
    return () => window.clearTimeout(timer);
  }, [slideDir, stack]);

  const drillInto = useCallback(
    (node: IndexNavNode) => {
      if (!node.children?.length) return;
      setSlideDir("forward");
      setStack((prev) => [...prev, node]);
      onSelectNode(node.id);
    },
    [onSelectNode]
  );

  const drillBack = useCallback(() => {
    setSlideDir("back");
    setStack((prev) => prev.slice(0, -1));
  }, []);

  const handleItemActivate = useCallback(
    (node: IndexNavNode) => {
      onSelectNode(node.id);
      if (node.children?.length) {
        drillInto(node);
        return;
      }
      if (node.kind) onOpenNode(node);
    },
    [drillInto, onOpenNode, onSelectNode]
  );

  const handleSearchHit = useCallback(
    (hit: SearchHit) => {
      setStack(hit.path);
      onSelectNode(hit.node.id);
      if (hit.node.kind) onOpenNode(hit.node);
      else if (hit.node.children?.length) setStack([...hit.path, hit.node]);
    },
    [onOpenNode, onSelectNode]
  );

  const headerTitle = currentFolder
    ? t(currentFolder.labelKey as MessageKey)
    : t("nav.title");

  const visibleNodes = isSearching ? [] : currentNodes;

  const renderRow = (
    node: IndexNavNode,
    options: { secondaryText?: string; onActivate: () => void }
  ) => {
    const labelKey = node.labelKey as MessageKey;
    const label = t(labelKey);
    const hasChildren = Boolean(node.children?.length);
    const isSelected = selectedNodeId === node.id;
    const secondaryText =
      options.secondaryText ?? navNodeDescription(labelKey, hasChildren, t);

    return (
      <li key={node.id} className="idx-nav-item">
        <button
          type="button"
          className={`idx-nav-row${isSelected ? " idx-nav-row--selected" : ""}${
            hasChildren ? " idx-nav-row--folder" : ""
          }`}
          data-selected={isSelected}
          aria-label={
            hasChildren ? t("nav.drillInto", { name: label }) : t("nav.openNamed", { name: label })
          }
          onClick={options.onActivate}
        >
          <NavItemIcon node={node} />
          <span className="idx-nav-row__text">
            <span className="idx-nav-row__label">{label}</span>
            <span className="idx-nav-row__desc">{secondaryText}</span>
          </span>
          {hasChildren ? (
            <span className="idx-nav-row__chevron" aria-hidden>
              ›
            </span>
          ) : null}
        </button>
      </li>
    );
  };

  return (
    <nav className="idx-nav-pane idx-drill-nav" aria-label={t("a11y.navTreeLabel")}>
      <div className="idx-nav-pane__header">
        {stack.length > 0 ? (
          <button
            type="button"
            className="idx-nav-pane__back"
            onClick={drillBack}
            aria-label={t("nav.back")}
          >
            <span className="idx-nav-pane__back-icon" aria-hidden>
              ←
            </span>
            <span>{t("nav.back")}</span>
          </button>
        ) : null}
      </div>

      {!isSearching && stack.length > 0 ? (
        <div className="idx-nav-folder-title">{headerTitle}</div>
      ) : isSearching ? (
        <div className="idx-nav-folder-title idx-nav-folder-title--search">{t("nav.searchResults")}</div>
      ) : (
        <div className="idx-nav-folder-title">{t("nav.title")}</div>
      )}

      <div className="idx-nav-pane__search">
        <label className="idx-nav-pane__search-label">
          <span className="idx-visually-hidden">{t("nav.filter")}</span>
          <input
            className="idx-input idx-nav-pane__search-input"
            type="search"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder={t("nav.filterPlaceholder")}
          />
        </label>
      </div>

      <div
        className={`idx-nav-pane__body idx-nav-pane__body--${slideDir ?? "idle"}`}
        key={stack.map((n) => n.id).join("/") || "root"}
      >
        {isSearching ? (
          <ul className="idx-nav-list">
            {searchHits.length === 0 ? (
              <li className="idx-nav-empty">{t("nav.noResults")}</li>
            ) : (
              searchHits.map(({ node, path }) => {
                const breadcrumb = path.map((p) => t(p.labelKey as MessageKey)).join(" › ");
                return renderRow(node, {
                  secondaryText: breadcrumb || undefined,
                  onActivate: () => handleSearchHit({ node, path }),
                });
              })
            )}
          </ul>
        ) : (
          <ul className="idx-nav-list">
            {visibleNodes.map((node) =>
              renderRow(node, { onActivate: () => handleItemActivate(node) })
            )}
          </ul>
        )}
      </div>
    </nav>
  );
}
