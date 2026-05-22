import type { MessageKey } from "../../i18n";
import type { WorkflowCanvasNode } from "../../types/workflowCanvas";
import { canvasNodeDisplayLabel, canvasNodeKindLabel } from "../../utils/workflowCanvasFlowSearch";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type SearchFieldProps = {
  t: TFn;
  searchQuery: string;
  onSearchQueryChange: (value: string) => void;
  disabled?: boolean;
};

export function WorkflowCanvasSearchField({
  t,
  searchQuery,
  onSearchQueryChange,
  disabled = false,
}: SearchFieldProps) {
  return (
    <label className="discovery-flow-search">
      <span className="discovery-flow-search__label">{t("flow.search")}</span>
      <input
        className="discovery-input discovery-flow-search__input"
        type="search"
        value={searchQuery}
        placeholder={t("flow.searchPlaceholder")}
        disabled={disabled}
        onChange={(e) => onSearchQueryChange(e.target.value)}
      />
    </label>
  );
}

type SearchResultsProps = {
  t: TFn;
  searchQuery: string;
  searchMatches: WorkflowCanvasNode[];
  selectedNodeId: string | null;
  onSelectNode: (nodeId: string) => void;
};

export function WorkflowCanvasSearchResults({
  t,
  searchQuery,
  searchMatches,
  selectedNodeId,
  onSelectNode,
}: SearchResultsProps) {
  if (searchQuery.trim().length === 0) return null;
  return (
    <div className="discovery-flow-search-results">
      {searchMatches.length === 0 ? (
        <span className="discovery-flow-search-results__empty">{t("flow.noSearchResults")}</span>
      ) : (
        <ul className="discovery-flow-search-results__list">
          {searchMatches.map((cn) => (
            <li key={cn.id}>
              <button
                type="button"
                className={
                  selectedNodeId === cn.id
                    ? "discovery-flow-search-results__item discovery-flow-search-results__item--active"
                    : "discovery-flow-search-results__item"
                }
                onClick={() => onSelectNode(cn.id)}
              >
                <span className="discovery-flow-search-results__name">{canvasNodeDisplayLabel(cn)}</span>
                <span className="discovery-flow-search-results__meta">{canvasNodeKindLabel(cn)}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
