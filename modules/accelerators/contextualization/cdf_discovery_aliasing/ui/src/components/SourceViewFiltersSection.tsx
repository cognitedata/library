import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n/types";
import type { JsonObject } from "../types/scopeConfig";
import {
  SourceViewFilterNodeEditor,
  emptyAnd,
  emptyLeaf,
  emptyNot,
  emptyOr,
} from "./SourceViewFiltersEditor";

type Props = {
  filters: JsonObject[];
  onFiltersChange: (next: JsonObject[]) => void;
  fieldKey: string;
  /** Defaults to ``sourceViews.filtersCombineHint`` (view query / DM list). */
  combineHintKey?: MessageKey;
  titleKey?: MessageKey;
};

/** Shared CDF filter DSL editor (query nodes, filter nodes, scope source views). */
export function SourceViewFiltersSection({
  filters,
  onFiltersChange,
  fieldKey,
  combineHintKey = "sourceViews.filtersCombineHint",
  titleKey = "sourceViews.filters",
}: Props) {
  const { t } = useAppSettings();

  return (
    <>
      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem", marginTop: "0.75rem" }}>
        {t(titleKey)}
      </h4>
      <p className="discovery-hint" style={{ marginTop: 0, marginBottom: "0.65rem", maxWidth: "56rem" }}>
        {t(combineHintKey)}
      </p>
      {filters.map((f, fi) => {
        const row = f && typeof f === "object" && !Array.isArray(f) ? f : emptyLeaf();
        return (
          <SourceViewFilterNodeEditor
            key={`svf-${fieldKey}-${fi}`}
            t={t}
            syncKeyPrefix={`${fieldKey}-${fi}`}
            value={row}
            onChange={(next) => {
              const fl = [...filters];
              fl[fi] = next;
              onFiltersChange(fl);
            }}
            onRemove={() => {
              onFiltersChange(filters.filter((_, j) => j !== fi));
            }}
          />
        );
      })}
      <div className="discovery-toolbar-inline" style={{ marginTop: 10, flexWrap: "wrap", gap: 8 }}>
        <button
          type="button"
          className="discovery-btn discovery-btn--sm"
          onClick={() => onFiltersChange([...filters, emptyLeaf()])}
        >
          {t("sourceViews.filterAddLeaf")}
        </button>
        <button
          type="button"
          className="discovery-btn discovery-btn--sm"
          onClick={() => onFiltersChange([...filters, emptyAnd()])}
        >
          {t("sourceViews.filterAddAnd")}
        </button>
        <button
          type="button"
          className="discovery-btn discovery-btn--sm"
          onClick={() => onFiltersChange([...filters, emptyOr()])}
        >
          {t("sourceViews.filterAddOr")}
        </button>
        <button
          type="button"
          className="discovery-btn discovery-btn--sm"
          onClick={() => onFiltersChange([...filters, emptyNot()])}
        >
          {t("sourceViews.filterAddNot")}
        </button>
      </div>
    </>
  );
}
