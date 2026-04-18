import { useEffect, useMemo, useState } from "react";
import YAML from "yaml";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import { withoutRegexpMatch } from "../utils/validationConfig";
import {
  SourceViewFilterNodeEditor,
  emptyAnd,
  emptyLeaf,
  emptyNot,
  emptyOr,
} from "./SourceViewFiltersEditor";
import { ValidationStructuredEditor } from "./ValidationStructuredEditor";
import { commaJoinSegments, splitCommaSegments } from "../utils/commaDelimited";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  value: unknown;
  onChange: (next: unknown) => void;
  /** When set, selects this row in the view list (e.g. flow canvas node double-click). */
  initialViewIndex?: number;
  /** Full scope document for match-rule refs (`confidence_match_rule_definitions` / sequences). */
  scopeDocument?: Record<string, unknown>;
  /** Scroll to this match rule in per-view validation (inline rules only). */
  initialFocusedMatchRuleName?: string;
};

function asViewList(v: unknown): JsonObject[] {
  if (!Array.isArray(v)) return [];
  return v.filter((x): x is JsonObject => x !== null && typeof x === "object" && !Array.isArray(x));
}

function emptyView(): JsonObject {
  return {
    view_external_id: "",
    view_space: "",
    view_version: "",
    entity_type: "",
    filters: [],
    include_properties: [],
  };
}

function viewListLabel(view: JsonObject, vi: number, t: TFn): string {
  const id = String(view.view_external_id ?? "").trim();
  if (id) return id;
  return t("sourceViews.unnamedView", { index: String(vi + 1) });
}

export function SourceViewsControls({
  value,
  onChange,
  initialViewIndex,
  scopeDocument,
  initialFocusedMatchRuleName,
}: Props) {
  const { t } = useAppSettings();
  const views = asViewList(value);
  const [selectedVi, setSelectedVi] = useState(0);

  useEffect(() => {
    setSelectedVi((sel) => {
      if (views.length === 0) return 0;
      return Math.min(Math.max(0, sel), views.length - 1);
    });
  }, [views.length]);

  useEffect(() => {
    if (initialViewIndex === undefined || views.length === 0) return;
    const clamped = Math.min(Math.max(0, initialViewIndex), views.length - 1);
    setSelectedVi(clamped);
  }, [initialViewIndex, views.length]);

  const setViews = (next: JsonObject[]) => onChange(next);

  const patchView = (i: number, patch: JsonObject) => {
    const next = views.map((v, j) => (j === i ? { ...v, ...patch } : v));
    setViews(next);
  };

  const setFilters = (vi: number, filters: JsonObject[]) => {
    patchView(vi, { filters });
  };

  const removeView = (vi: number) => {
    const next = views.filter((_, j) => j !== vi);
    setViews(next);
    setSelectedVi((sel) => {
      if (next.length === 0) return 0;
      if (vi < sel) return sel - 1;
      if (vi === sel) return Math.min(vi, next.length - 1);
      return sel;
    });
  };

  const addView = () => {
    const next = [...views, emptyView()];
    setViews(next);
    setSelectedVi(next.length - 1);
  };

  const includePropsToText = (v: unknown): string => {
    if (!Array.isArray(v)) return "";
    return commaJoinSegments(v.map((x) => String(x)));
  };

  const textToIncludeProps = (s: string): string[] => splitCommaSegments(s);

  const vi = selectedVi;
  const view = views[vi];
  const hasSelection = views.length > 0 && view != null;
  const includeKey = JSON.stringify(view?.include_properties ?? null);
  const [includeDraft, setIncludeDraft] = useState(() =>
    views.length > 0 && views[selectedVi]
      ? includePropsToText(views[selectedVi].include_properties)
      : ""
  );

  useEffect(() => {
    if (!view) return;
    setIncludeDraft(includePropsToText(view.include_properties));
  }, [vi, includeKey]);

  const validationKey = JSON.stringify(view?.validation ?? null);
  const validationObject = useMemo((): JsonObject => {
    const v = view?.validation;
    if (v !== null && typeof v === "object" && !Array.isArray(v)) return v as JsonObject;
    return {};
  }, [vi, validationKey]);

  const [validationYaml, setValidationYaml] = useState(() =>
    YAML.stringify(validationObject, { lineWidth: 0 })
  );
  const [validationError, setValidationError] = useState<string | null>(null);

  useEffect(() => {
    if (!view) return;
    const v = view.validation;
    const vo =
      v !== null && typeof v === "object" && !Array.isArray(v) ? (v as JsonObject) : {};
    setValidationYaml(YAML.stringify(vo, { lineWidth: 0 }));
    setValidationError(null);
  }, [vi, validationKey, view]);

  const commitStructuredValidation = (next: JsonObject) => {
    const cleaned = withoutRegexpMatch(next);
    setValidationYaml(YAML.stringify(cleaned, { lineWidth: 0 }));
    setValidationError(null);
    patchView(vi, { validation: cleaned });
  };

  const commitValidationYaml = () => {
    try {
      const parsed = YAML.parse(validationYaml);
      const vo =
        parsed !== null && typeof parsed === "object" && !Array.isArray(parsed) ? (parsed as JsonObject) : {};
      const cleaned = withoutRegexpMatch(vo);
      setValidationYaml(YAML.stringify(cleaned, { lineWidth: 0 }));
      setValidationError(null);
      patchView(vi, { validation: cleaned });
    } catch (e) {
      setValidationError(String(e));
    }
  };

  const clearValidationOverlay = () => {
    const next = views.map((v, j) => {
      if (j !== vi) return v;
      const copy = { ...v };
      delete copy.validation;
      return copy;
    });
    setViews(next);
    setValidationError(null);
  };

  return (
    <div className="kea-source-views">
      <div className="kea-toolbar-inline">
        <h3 className="kea-section-title" style={{ margin: 0 }}>
          {t("sourceViews.title")}
        </h3>
        <button type="button" className="kea-btn kea-btn--primary kea-btn--sm" onClick={addView}>
          {t("sourceViews.addView")}
        </button>
      </div>

      <div className="kea-source-views-split">
        <aside className="kea-source-views-sidebar">
          <p className="kea-artifact-list-title">{t("sourceViews.listTitle")}</p>
          <ul className="kea-source-views-list" role="listbox" aria-label={t("sourceViews.listAriaLabel")}>
            {views.map((v, i) => (
              <li key={`sv-${i}`} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedVi === i}
                  className={`kea-source-views-item${selectedVi === i ? " kea-source-views-item--active" : ""}`}
                  onClick={() => setSelectedVi(i)}
                >
                  {viewListLabel(v, i, t)}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <div className="kea-source-views-editor">
          {!hasSelection ? (
            <p className="kea-hint">{t("sourceViews.emptyEditor")}</p>
          ) : (
            <div className="kea-source-views-editor-inner">
              <div className="kea-toolbar-inline" style={{ marginBottom: "0.85rem" }}>
                <span className="kea-hint" style={{ margin: 0 }}>
                  #{vi + 1} — {viewListLabel(view, vi, t)}
                </span>
                <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={() => removeView(vi)}>
                  {t("sourceViews.removeView")}
                </button>
              </div>
              <div className="kea-loc-fields">
                <label className="kea-label">
                  {t("sourceViews.viewExternalId")}
                  <input
                    className="kea-input"
                    value={String(view.view_external_id ?? "")}
                    onChange={(e) => patchView(vi, { view_external_id: e.target.value })}
                  />
                </label>
                <label className="kea-label">
                  {t("sourceViews.viewSpace")}
                  <input
                    className="kea-input"
                    value={String(view.view_space ?? "")}
                    onChange={(e) => patchView(vi, { view_space: e.target.value })}
                  />
                </label>
                <label className="kea-label">
                  {t("sourceViews.viewVersion")}
                  <input
                    className="kea-input"
                    value={String(view.view_version ?? "")}
                    onChange={(e) => patchView(vi, { view_version: e.target.value })}
                  />
                </label>
                <label className="kea-label">
                  {t("sourceViews.entityType")}
                  <input
                    className="kea-input"
                    value={String(view.entity_type ?? "")}
                    onChange={(e) => patchView(vi, { entity_type: e.target.value })}
                  />
                </label>
                <label className="kea-label">
                  {t("sourceViews.batchSize")}
                  <input
                    className="kea-input"
                    type="number"
                    value={view.batch_size != null ? String(view.batch_size) : ""}
                    onChange={(e) => {
                      const val = e.target.value.trim();
                      patchView(vi, { batch_size: val === "" ? undefined : Number(val) });
                    }}
                  />
                </label>
                <label className="kea-label">
                  {t("sourceViews.instanceSpace")}
                  <input
                    className="kea-input"
                    value={String(view.instance_space ?? "")}
                    onChange={(e) => patchView(vi, { instance_space: e.target.value || undefined })}
                  />
                </label>
              </div>
              <label className="kea-label kea-label--block" style={{ marginTop: "0.75rem" }}>
                {t("sourceViews.includeProperties")}
                <span
                  className="kea-hint"
                  style={{ display: "block", marginBottom: "0.25rem", cursor: "help" }}
                  title={t("sourceViews.includePropsHint.tooltip")}
                >
                  {t("sourceViews.includePropsHint")}
                </span>
                <input
                  type="text"
                  className="kea-input"
                  value={includeDraft}
                  onChange={(e) => setIncludeDraft(e.target.value)}
                  onBlur={() => patchView(vi, { include_properties: textToIncludeProps(includeDraft) })}
                  spellCheck={false}
                  autoComplete="off"
                />
              </label>
              <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "0.75rem" }}>
                {t("sourceViews.filters")}
              </h4>
              <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.65rem", maxWidth: "56rem" }}>
                {t("sourceViews.filtersCombineHint")}
              </p>
              {(Array.isArray(view.filters) ? view.filters : []).map((f, fi) => {
                const row =
                  f && typeof f === "object" && !Array.isArray(f) ? (f as JsonObject) : emptyLeaf();
                return (
                  <SourceViewFilterNodeEditor
                    key={`f-${vi}-${fi}`}
                    t={t}
                    value={row}
                    onChange={(next) => {
                      const fl = [...(Array.isArray(view.filters) ? view.filters : [])];
                      fl[fi] = next;
                      setFilters(vi, fl as JsonObject[]);
                    }}
                    onRemove={() => {
                      const fl = [...(Array.isArray(view.filters) ? view.filters : [])];
                      fl.splice(fi, 1);
                      setFilters(vi, fl as JsonObject[]);
                    }}
                  />
                );
              })}
              <div className="kea-toolbar-inline" style={{ marginTop: 10, flexWrap: "wrap", gap: 8 }}>
                <button
                  type="button"
                  className="kea-btn kea-btn--sm"
                  onClick={() => {
                    const fl = [...(Array.isArray(view.filters) ? view.filters : []), emptyLeaf()];
                    setFilters(vi, fl as JsonObject[]);
                  }}
                >
                  {t("sourceViews.filterAddLeaf")}
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--sm"
                  onClick={() => {
                    const fl = [...(Array.isArray(view.filters) ? view.filters : []), emptyAnd()];
                    setFilters(vi, fl as JsonObject[]);
                  }}
                >
                  {t("sourceViews.filterAddAnd")}
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--sm"
                  onClick={() => {
                    const fl = [...(Array.isArray(view.filters) ? view.filters : []), emptyOr()];
                    setFilters(vi, fl as JsonObject[]);
                  }}
                >
                  {t("sourceViews.filterAddOr")}
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--sm"
                  onClick={() => {
                    const fl = [...(Array.isArray(view.filters) ? view.filters : []), emptyNot()];
                    setFilters(vi, fl as JsonObject[]);
                  }}
                >
                  {t("sourceViews.filterAddNot")}
                </button>
              </div>
              <div style={{ marginTop: "1.25rem" }}>
                <div className="kea-toolbar-inline" style={{ marginBottom: "0.5rem", flexWrap: "wrap", gap: 8 }}>
                  <h4 className="kea-section-title" style={{ fontSize: "0.95rem", margin: 0, flex: "1 1 auto" }}>
                    {t("sourceViews.perViewValidationTitle")}
                  </h4>
                  <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={clearValidationOverlay}>
                    {t("sourceViews.perViewValidationClear")}
                  </button>
                </div>
                <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.65rem", maxWidth: "56rem" }}>
                  {t("sourceViews.perViewValidationHint")}
                </p>
                <ValidationStructuredEditor
                  variant="keyExtraction"
                  value={validationObject}
                  onChange={commitStructuredValidation}
                  scopeDocument={scopeDocument}
                  initialFocusedMatchRuleName={initialFocusedMatchRuleName}
                />
                <details style={{ marginTop: "1rem" }}>
                  <summary style={{ cursor: "pointer", color: "var(--kea-text-muted)" }}>{t("validationEditor.advancedYaml")}</summary>
                  {validationError && <p className="kea-hint kea-hint--warn">{validationError}</p>}
                  <textarea
                    className="kea-textarea"
                    style={{ minHeight: 200, fontFamily: "ui-monospace, monospace", marginTop: "0.5rem" }}
                    value={validationYaml}
                    onChange={(e) => setValidationYaml(e.target.value)}
                    onBlur={commitValidationYaml}
                    spellCheck={false}
                  />
                </details>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
