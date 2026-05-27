import { useAppSettings } from "../../context/AppSettingsContext";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import type { JsonObject } from "../../types/jsonConfig";
import { readFilters, mergeFilters } from "../../utils/filtersConfigModel";
import { SourceViewFiltersSection } from "./SourceViewFiltersSection";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
};

/** Editor for ``filter`` node ``data.config``. */
export function FilterNodeConfigFields({ value, onChange, fieldKey }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const filters = readFilters(value);

  return (
    <div className="transform-query-fields-wrap transform-query-fields">
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
        <SourceViewFiltersSection
          filters={filters}
          onFiltersChange={(next) => onChange(mergeFilters(value, next))}
          fieldKey={fieldKey}
          combineHintKey="transform.filters.nodeCombineHint"
        />
      </div>
    </div>
  );
}
