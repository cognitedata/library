import type { MessageKey } from "../../i18n";
import type { TransformCanvasNode } from "../../types/transformCanvas";
import {
  transformCanvasNodeDisplayLabel,
  transformCanvasNodeKindLabel,
} from "../../utils/transformCanvasFlowSearch";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type SearchFieldProps = {
  t: TFn;
  searchQuery: string;
  onSearchQueryChange: (value: string) => void;
  disabled?: boolean;
};

export function TransformCanvasSearchField({
  t,
  searchQuery,
  onSearchQueryChange,
  disabled = false,
}: SearchFieldProps) {
  return (
    <label className="disc-dm-flow-search transform-flow-search">
      <span className="disc-dm-flow-search__label">{t("transform.search.label")}</span>
      <input
        className="disc-input disc-dm-flow-search__input"
        type="search"
        value={searchQuery}
        placeholder={t("transform.search.placeholder")}
        disabled={disabled}
        onChange={(e) => onSearchQueryChange(e.target.value)}
      />
    </label>
  );
};

type SearchResultsProps = {
  t: TFn;
  searchQuery: string;
  searchMatches: TransformCanvasNode[];
  selectedNodeId: string | null;
  onSelectNode: (nodeId: string) => void;
};

export function TransformCanvasSearchResults({
  t,
  searchQuery,
  searchMatches,
  selectedNodeId,
  onSelectNode,
}: SearchResultsProps) {
  if (searchQuery.trim().length === 0) return null;
  return (
    <div className="disc-dm-flow-search-results transform-flow-search-results">
      {searchMatches.length === 0 ? (
        <span className="disc-dm-flow-search-results__empty">{t("transform.search.noResults")}</span>
      ) : (
        <ul className="disc-dm-flow-search-results__list">
          {searchMatches.map((cn) => (
            <li key={cn.id}>
              <button
                type="button"
                className={
                  selectedNodeId === cn.id
                    ? "disc-dm-flow-search-results__item disc-dm-flow-search-results__item--active"
                    : "disc-dm-flow-search-results__item"
                }
                onClick={() => onSelectNode(cn.id)}
              >
                <span className="disc-dm-flow-search-results__name">
                  {transformCanvasNodeDisplayLabel(cn, t)}
                </span>
                <span className="disc-dm-flow-search-results__meta">
                  {transformCanvasNodeKindLabel(cn, t)}
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

type NodeListProps = {
  t: TFn;
  nodes: TransformCanvasNode[];
  selectedNodeId: string | null;
  onSelectNode: (nodeId: string) => void;
};

/** Keyboard-accessible list of all canvas nodes (complement to the graph). */
export function TransformCanvasNodeList({ t, nodes, selectedNodeId, onSelectNode }: NodeListProps) {
  if (nodes.length === 0) return null;
  return (
    <section
      className="disc-flow-node-list transform-flow-node-list"
      aria-label={t("transform.nodeList.regionLabel")}
    >
      <p className="disc-flow-node-list__hint">{t("transform.nodeList.allNodesHint")}</p>
      <ul className="disc-dm-flow-search-results__list">
        {nodes.map((cn) => (
          <li key={cn.id}>
            <button
              type="button"
              className={
                selectedNodeId === cn.id
                  ? "disc-dm-flow-search-results__item disc-dm-flow-search-results__item--active"
                  : "disc-dm-flow-search-results__item"
              }
              onClick={() => onSelectNode(cn.id)}
            >
              <span className="disc-dm-flow-search-results__name">
                {transformCanvasNodeDisplayLabel(cn, t)}
              </span>
              <span className="disc-dm-flow-search-results__meta">
                {transformCanvasNodeKindLabel(cn, t)}
              </span>
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}
