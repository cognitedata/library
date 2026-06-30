import { useAppSettings } from "../../context/AppSettingsContext";
import {
  emptyIndexFieldProperty,
  emptyIndexFieldView,
  type IndexFieldProperty,
  type IndexFieldView,
} from "../../types/invertedIndexConfig";
import { StringListInput } from "./StringListInput";
import { ViewQueryFiltersSection } from "./ViewQueryFiltersSection";
import { FormPanel } from "../shared/FormPanel";

type Props = {
  value: IndexFieldView[];
  onChange: (next: IndexFieldView[]) => void;
};

function PropertyRow({
  property,
  index,
  onChange,
  onRemove,
}: {
  property: IndexFieldProperty;
  index: number;
  onChange: (p: IndexFieldProperty) => void;
  onRemove: () => void;
}) {
  const { t } = useAppSettings();

  return (
    <article className="idx-config-card idx-config-card--nested">
      <div className="idx-config-card__header">
        <h5 className="idx-config-card__title">
          {t("config.indexFields.propertyCard", { index: String(index + 1) })}
        </h5>
        <button type="button" className="idx-btn idx-btn--sm idx-btn--danger" onClick={onRemove}>
          {t("config.indexFields.removeProperty")}
        </button>
      </div>
      <div className="idx-config-grid">
        <label className="idx-label">
          <span className="idx-label__caption">{t("config.indexFields.path")}</span>
          <input
            className="idx-input idx-input--mono"
            value={property.path}
            onChange={(e) => onChange({ ...property, path: e.target.value })}
          />
        </label>
        <label className="idx-label">
          <span className="idx-label__caption">{t("config.indexFields.sourceType")}</span>
          <select
            className="idx-select"
            value={property.sourceType}
            onChange={(e) =>
              onChange({
                ...property,
                sourceType: e.target.value === "file_metadata" ? "file_metadata" : "asset_metadata",
              })
            }
          >
            <option value="asset_metadata">{t("config.indexFields.sourceAsset")}</option>
            <option value="file_metadata">{t("config.indexFields.sourceFile")}</option>
          </select>
        </label>
        <label className="idx-label idx-config-grid__full">
          <span className="idx-label__caption">{t("config.indexFields.extractPattern")}</span>
          <input
            className="idx-input idx-input--mono"
            value={property.extractPattern}
            onChange={(e) => {
              const extractPattern = e.target.value;
              onChange({
                ...property,
                extractPattern,
                extractMode: extractPattern.trim() ? "regex" : "passthrough",
              });
            }}
          />
          <span className="idx-config-hint">{t("config.indexFields.extractPatternHint")}</span>
        </label>
      </div>
    </article>
  );
}

function ViewCard({
  view,
  index,
  onChange,
  onRemove,
}: {
  view: IndexFieldView;
  index: number;
  onChange: (v: IndexFieldView) => void;
  onRemove: () => void;
}) {
  const { t } = useAppSettings();

  return (
    <article className="idx-config-card">
      <div className="idx-config-card__header">
        <h4 className="idx-config-card__title">
          {t("config.indexFields.viewCard", { index: String(index + 1) })}
        </h4>
        <button type="button" className="idx-btn idx-btn--sm idx-btn--danger" onClick={onRemove}>
          {t("config.indexFields.removeView")}
        </button>
      </div>
      <div className="idx-config-grid">
        <label className="idx-label">
          {t("config.indexFields.view")}
          <input
            className="idx-input"
            value={view.view}
            onChange={(e) => onChange({ ...view, view: e.target.value })}
          />
        </label>
        <label className="idx-label">
          {t("config.indexFields.viewSpace")}
          <input
            className="idx-input idx-input--mono"
            value={view.viewSpace}
            onChange={(e) => onChange({ ...view, viewSpace: e.target.value })}
          />
        </label>
        <label className="idx-label">
          {t("config.indexFields.version")}
          <input
            className="idx-input"
            value={view.version}
            onChange={(e) => onChange({ ...view, version: e.target.value })}
          />
        </label>
        <label className="idx-label idx-config-grid__full">
          {t("config.indexFields.instanceSpaces")}
          <StringListInput
            value={view.instanceSpaces}
            onChange={(instanceSpaces) => onChange({ ...view, instanceSpaces })}
            placeholder={t("config.indexFields.instanceSpacesPlaceholder")}
            mono
          />
          <span className="idx-config-hint">{t("config.indexFields.instanceSpacesHint")}</span>
        </label>
      </div>
      <ViewQueryFiltersSection
        fieldKey={`view-${index}`}
        filters={view.filters}
        onFiltersChange={(filters) => onChange({ ...view, filters })}
      />
      <div className="idx-config-toolbar">
        <h5 className="idx-config-subsection__title" style={{ margin: 0 }}>
          {t("config.indexFields.properties")}
        </h5>
        <button
          type="button"
          className="idx-btn idx-btn--sm"
          onClick={() =>
            onChange({
              ...view,
              properties: [...view.properties, emptyIndexFieldProperty()],
            })
          }
        >
          {t("config.indexFields.addProperty")}
        </button>
      </div>
      {view.properties.map((p, i) => (
        <PropertyRow
          key={i}
          property={p}
          index={i}
          onChange={(next) => {
            const properties = [...view.properties];
            properties[i] = next;
            onChange({ ...view, properties });
          }}
          onRemove={() =>
            onChange({
              ...view,
              properties: view.properties.filter((_, j) => j !== i),
            })
          }
        />
      ))}
    </article>
  );
}

export function IndexFieldConfigEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();

  return (
    <FormPanel title={t("config.indexFields.title")} hint={t("config.indexFields.hint")}>
      <div className="idx-config-toolbar">
        <div />
        <button
          type="button"
          className="idx-btn idx-btn--primary"
          onClick={() => onChange([...value, emptyIndexFieldView()])}
        >
          {t("config.indexFields.addView")}
        </button>
      </div>
      {value.length === 0 ? (
        <div className="idx-empty-state">
          <p className="idx-empty-state__text">{t("config.indexFields.hint")}</p>
        </div>
      ) : null}
      <div className="idx-config-card-grid">
      {value.map((view, i) => (
        <ViewCard
          key={i}
          view={view}
          index={i}
          onChange={(next) => {
            const views = [...value];
            views[i] = next;
            onChange(views);
          }}
          onRemove={() => onChange(value.filter((_, j) => j !== i))}
        />
      ))}
      </div>
    </FormPanel>
  );
}
