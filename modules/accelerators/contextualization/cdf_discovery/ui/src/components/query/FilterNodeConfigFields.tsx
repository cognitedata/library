import { useAppSettings } from "../../context/AppSettingsContext";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import type { JsonObject } from "../../types/jsonConfig";
import { readFilters, mergeFilters } from "../../utils/filtersConfigModel";
import { QueryEditorTabs, useQueryEditorTabState, type QueryEditorTabDef } from "./QueryEditorTabs";
import { SourceViewFiltersSection } from "./SourceViewFiltersSection";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
};

const TAB_CONFIG = "config";
const TAB_FILTERS = "filters";

const TABS: QueryEditorTabDef[] = [
  { id: TAB_CONFIG, labelKey: "transform.query.tabConfig" },
  { id: TAB_FILTERS, labelKey: "transform.query.tabFilters" },
];

/** Editor for ``filter`` node ``data.config``. */
export function FilterNodeConfigFields({ value, onChange, fieldKey }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const filters = readFilters(value);
  const [activeTab, setActiveTab] = useQueryEditorTabState(fieldKey, TAB_CONFIG);

  return (
    <div className="transform-query-fields-wrap transform-query-fields">
      <QueryEditorTabs
        tabs={TABS}
        activeTab={activeTab}
        onActiveTabChange={setActiveTab}
        ariaLabel={t("transform.query.editorTabsAria")}
        panelIdPrefix={`filter-node-${fieldKey}`}
      >
        {activeTab === TAB_CONFIG ? (
          <div className="transform-query-fields__config">
            <p className="transform-query-hint transform-query-fields__intro">{t("transform.query.filterEditorIntro")}</p>
            <label className="transform-query-label transform-query-label--block">
              {t("transform.query.description")}
              <DeferredCommitInput
                className="gov-input"
                style={{ marginTop: "0.35rem" }}
                committedValue={String(value.description ?? "")}
                syncKey={`${fieldKey}-desc`}
                onCommit={(v) => patch({ description: v })}
                spellCheck={false}
                autoComplete="off"
              />
            </label>
          </div>
        ) : null}

        {activeTab === TAB_FILTERS ? (
          <SourceViewFiltersSection
            filters={filters}
            onFiltersChange={(next) => onChange(mergeFilters(value, next))}
            fieldKey={fieldKey}
            combineHintKey="transform.filters.nodeCombineHint"
          />
        ) : null}
      </QueryEditorTabs>
    </div>
  );
}
