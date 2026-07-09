import { useState } from "react";
import { useInvertedIndexT } from "../../hooks/useInvertedIndexT";
import {
  emptyIndexFieldProperty,
  emptyIndexFieldView,
  emptyScopePropertyOverride,
  type IndexFieldProperty,
  type IndexFieldView,
  type ScopeConfig,
  type ScopePropertyOverride,
} from "../../utils/invertedIndexConfig";
import { StringListInput } from "./StringListInput";
import { ViewQueryFiltersSection } from "./ViewQueryFiltersSection";
import { FormPanel } from "../shared/FormPanel";

type Props = {
  value: IndexFieldView[];
  onChange: (next: IndexFieldView[]) => void;
  scopeConfig?: ScopeConfig;
};

function propertyRuleKey(property: IndexFieldProperty): string {
  return `${property.path}\0${property.sourceType}`;
}

function summarizeMergeInheritance(
  defaults: IndexFieldProperty[],
  scoped: IndexFieldProperty[]
): { inherited: number; overridden: number; added: number } {
  const defaultKeys = new Set(defaults.map(propertyRuleKey));
  const scopedKeys = new Set(scoped.map(propertyRuleKey));
  let overridden = 0;
  for (const key of scopedKeys) {
    if (defaultKeys.has(key)) overridden += 1;
  }
  let added = 0;
  for (const key of scopedKeys) {
    if (!defaultKeys.has(key)) added += 1;
  }
  const inherited = defaults.length - overridden;
  return { inherited, overridden, added };
}

function buildScopeKeyPlaceholder(scopeConfig: ScopeConfig | undefined): string {
  const levels = scopeConfig?.levels ?? [];
  if (!levels.length) {
    return scopeConfig?.fallbackScopeKey?.trim() || "global";
  }
  const parts = levels.map((level, index) => {
    if (index === levels.length - 1) {
      return `${level}:*`;
    }
    return `${level}:example_${level}`;
  });
  return parts.join("|");
}

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
  const { t } = useInvertedIndexT();

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

function PropertyListEditor({
  properties,
  onChange,
}: {
  properties: IndexFieldProperty[];
  onChange: (next: IndexFieldProperty[]) => void;
}) {
  const { t } = useInvertedIndexT();

  return (
    <>
      {properties.map((p, i) => (
        <PropertyRow
          key={i}
          property={p}
          index={i}
          onChange={(next) => {
            const rows = [...properties];
            rows[i] = next;
            onChange(rows);
          }}
          onRemove={() => onChange(properties.filter((_, j) => j !== i))}
        />
      ))}
      <button
        type="button"
        className="idx-btn idx-btn--sm"
        onClick={() => onChange([...properties, emptyIndexFieldProperty()])}
      >
        {t("config.indexFields.addProperty")}
      </button>
    </>
  );
}

function detectAmbiguousScopeKeys(keys: string[]): string[] {
  const wildcardTier = (key: string): number => {
    const trimmed = key.trim();
    if (!trimmed || trimmed === "*") return 10_000;
    return trimmed.split("|").filter((part) => part.endsWith(":*") || part === "*").length;
  };
  const byTier = new Map<number, string[]>();
  for (const key of keys) {
    const tier = wildcardTier(key);
    const group = byTier.get(tier) ?? [];
    group.push(key);
    byTier.set(tier, group);
  }
  const ambiguous: string[] = [];
  for (const group of byTier.values()) {
    if (group.length > 1) ambiguous.push(...group);
  }
  return ambiguous;
}

function ScopePropertyOverrideCard({
  scopeKey,
  override,
  defaultProperties,
  onChange,
  onRemove,
}: {
  scopeKey: string;
  override: ScopePropertyOverride;
  defaultProperties: IndexFieldProperty[];
  onChange: (next: ScopePropertyOverride) => void;
  onRemove: () => void;
}) {
  const { t } = useInvertedIndexT();
  const [open, setOpen] = useState(true);
  const inheritance =
    override.mode === "merge"
      ? summarizeMergeInheritance(defaultProperties, override.properties)
      : null;

  return (
    <article className="idx-config-card idx-config-scope-override-card">
      <div className="idx-config-card__header">
        <button
          type="button"
          className="idx-config-card__toggle"
          onClick={() => setOpen((o) => !o)}
          aria-expanded={open}
        >
          <span className="idx-config-card__title idx-config-card__title--mono">{scopeKey}</span>
        </button>
        <button type="button" className="idx-btn idx-btn--sm idx-btn--danger" onClick={onRemove}>
          {t("config.indexFields.scopeOverrides.removeOverride")}
        </button>
      </div>
      {open ? (
        <div className="idx-config-scope-override-card__body">
          <div className="idx-config-grid idx-config-scope-override-card__mode-row">
            <label className="idx-label">
              <span className="idx-label__caption">{t("config.indexFields.scopeOverrides.mode")}</span>
              <select
                className="idx-select"
                value={override.mode}
                onChange={(e) =>
                  onChange({
                    ...override,
                    mode: e.target.value === "replace" ? "replace" : "merge",
                  })
                }
              >
                <option value="merge">{t("config.indexFields.scopeOverrides.modeMerge")}</option>
                <option value="replace">{t("config.indexFields.scopeOverrides.modeReplace")}</option>
              </select>
              <span className="idx-config-hint">
                {override.mode === "replace"
                  ? t("config.indexFields.scopeOverrides.modeReplaceHint")
                  : t("config.indexFields.scopeOverrides.modeMergeHint")}
              </span>
            </label>
          </div>
          {inheritance ? (
            <p className="idx-config-hint">
              {t("config.indexFields.scopeOverrides.inheritanceSummary", {
                inherited: String(inheritance.inherited),
                overridden: String(inheritance.overridden),
                added: String(inheritance.added),
              })}
            </p>
          ) : null}
          <PropertyListEditor
            properties={override.properties}
            onChange={(properties) => onChange({ ...override, properties })}
          />
        </div>
      ) : null}
    </article>
  );
}

function ScopeOverridesSection({
  view,
  scopeConfig,
  onChange,
}: {
  view: IndexFieldView;
  scopeConfig?: ScopeConfig;
  onChange: (next: IndexFieldView) => void;
}) {
  const { t } = useInvertedIndexT();
  const [newScopeKey, setNewScopeKey] = useState("");
  const scopeKeyPlaceholder = buildScopeKeyPlaceholder(scopeConfig);
  const ambiguousKeys = detectAmbiguousScopeKeys(Object.keys(view.propertiesByScope));

  const addOverride = () => {
    const key = newScopeKey.trim();
    if (!key || view.propertiesByScope[key]) return;
    onChange({
      ...view,
      propertiesByScope: {
        ...view.propertiesByScope,
        [key]: emptyScopePropertyOverride(),
      },
    });
    setNewScopeKey("");
  };

  return (
    <section className="idx-config-subsection">
      <h5 className="idx-config-subsection__title">{t("config.indexFields.scopeOverrides.title")}</h5>
      <p className="idx-pane__hint">{t("config.indexFields.scopeOverrides.hint")}</p>
      <p className="idx-config-hint">{t("config.indexFields.scopeOverrides.scopeKeyWildcardHint")}</p>
      <div className="idx-config-toolbar idx-config-scope-override-toolbar">
        <label className="idx-label idx-config-scope-override-toolbar__key">
          <span className="idx-label__caption">{t("config.indexFields.scopeOverrides.scopeKey")}</span>
          <input
            className="idx-input idx-input--mono"
            value={newScopeKey}
            placeholder={scopeKeyPlaceholder}
            onChange={(e) => setNewScopeKey(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                addOverride();
              }
            }}
          />
        </label>
        <button type="button" className="idx-btn idx-btn--sm idx-btn--primary" onClick={addOverride}>
          {t("config.indexFields.scopeOverrides.addOverride")}
        </button>
      </div>
      {ambiguousKeys.length > 0 ? (
        <p className="idx-config-warning" role="status">
          {t("config.indexFields.scopeOverrides.ambiguityWarning", {
            keys: ambiguousKeys.join(", "),
          })}
        </p>
      ) : null}
      {Object.keys(view.propertiesByScope).length === 0 ? (
        <p className="idx-pane__hint">{t("config.indexFields.scopeOverrides.noOverrides")}</p>
      ) : null}
      <div className="idx-config-card-grid">
        {Object.entries(view.propertiesByScope).map(([scopeKey, override]) => (
          <ScopePropertyOverrideCard
            key={scopeKey}
            scopeKey={scopeKey}
            override={override}
            defaultProperties={view.properties}
            onChange={(next) =>
              onChange({
                ...view,
                propertiesByScope: { ...view.propertiesByScope, [scopeKey]: next },
              })
            }
            onRemove={() => {
              const next = { ...view.propertiesByScope };
              delete next[scopeKey];
              onChange({ ...view, propertiesByScope: next });
            }}
          />
        ))}
      </div>
    </section>
  );
}

function ViewCard({
  view,
  index,
  scopeConfig,
  onChange,
  onRemove,
}: {
  view: IndexFieldView;
  index: number;
  scopeConfig?: ScopeConfig;
  onChange: (v: IndexFieldView) => void;
  onRemove: () => void;
}) {
  const { t } = useInvertedIndexT();

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
      </div>
      <PropertyListEditor
        properties={view.properties}
        onChange={(properties) => onChange({ ...view, properties })}
      />
      <ScopeOverridesSection view={view} scopeConfig={scopeConfig} onChange={onChange} />
    </article>
  );
}

export function IndexFieldConfigEditor({ value, onChange, scopeConfig }: Props) {
  const { t } = useInvertedIndexT();

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
            scopeConfig={scopeConfig}
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
