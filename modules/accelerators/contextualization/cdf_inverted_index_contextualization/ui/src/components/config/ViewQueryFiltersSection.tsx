import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  ViewQueryFilterNodeEditor,
  emptyAnd,
  emptyLeaf,
  emptyNot,
  emptyOr,
} from "./ViewQueryFiltersEditor";

type Props = {
  filters: JsonObject[];
  onFiltersChange: (next: JsonObject[]) => void;
  fieldKey: string;
};

export function ViewQueryFiltersSection({ filters, onFiltersChange, fieldKey }: Props) {
  const { t } = useAppSettings();

  return (
    <>
      <h5 className="idx-config-subsection__title">{t("config.indexFields.filters.title")}</h5>
      <p className="idx-config-hint">{t("config.indexFields.filters.hint")}</p>
      {filters.map((f, fi) => {
        const row = f && typeof f === "object" && !Array.isArray(f) ? f : emptyLeaf();
        return (
          <ViewQueryFilterNodeEditor
            key={`iqf-${fieldKey}-${fi}`}
            t={t}
            value={row}
            onChange={(next) => {
              const fl = [...filters];
              fl[fi] = next;
              onFiltersChange(fl);
            }}
            onRemove={() => onFiltersChange(filters.filter((_, j) => j !== fi))}
          />
        );
      })}
      <div className="idx-config-toolbar">
        <div />
        <div className="idx-config-toolbar__actions">
          <button type="button" className="idx-btn idx-btn--sm" onClick={() => onFiltersChange([...filters, emptyLeaf()])}>
            {t("config.indexFields.filters.addLeaf")}
          </button>
          <button type="button" className="idx-btn idx-btn--sm" onClick={() => onFiltersChange([...filters, emptyAnd()])}>
            {t("config.indexFields.filters.addAnd")}
          </button>
          <button type="button" className="idx-btn idx-btn--sm" onClick={() => onFiltersChange([...filters, emptyOr()])}>
            {t("config.indexFields.filters.addOr")}
          </button>
          <button type="button" className="idx-btn idx-btn--sm" onClick={() => onFiltersChange([...filters, emptyNot()])}>
            {t("config.indexFields.filters.addNot")}
          </button>
        </div>
      </div>
    </>
  );
}
