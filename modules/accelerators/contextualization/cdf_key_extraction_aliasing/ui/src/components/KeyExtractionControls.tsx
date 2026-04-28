import { useEffect, useMemo, useState } from "react";
import YAML from "yaml";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { mergeDataWithValidation, splitDataByValidation } from "../utils/splitConfigData";
import { withoutRegexpMatch } from "../utils/validationConfig";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import { DiscoveryRulesStructuredEditor } from "./DiscoveryRulesStructuredEditor";
import { ValidationStructuredEditor } from "./ValidationStructuredEditor";

type EditorSub = "settings" | "rules" | "validation";

type Props = {
  value: unknown;
  onChange: (next: unknown) => void;
  /** Full workflow scope document (definitions + sequences live at root). */
  scopeDocument: Record<string, unknown>;
  /** Sub-tab on first mount (e.g. flow canvas double-click). */
  initialEditorSub?: EditorSub;
  /** Focus this discovery rule on the Rules tab (entity bucket + expand + scroll). */
  initialFocusedExtractionRuleName?: string;
  /** Scroll to this match rule on the Validation tab (inline rules only). */
  initialFocusedMatchRuleName?: string;
};

type KeyExtractionBlock = {
  externalId: string;
  config: { parameters: JsonObject; data: JsonObject };
};

function normalize(v: unknown): KeyExtractionBlock {
  if (v !== null && typeof v === "object" && !Array.isArray(v)) {
    const o = v as JsonObject;
    const cfg = o.config;
    const c =
      cfg !== null && typeof cfg === "object" && !Array.isArray(cfg)
        ? (cfg as JsonObject)
        : {};
    return {
      externalId: String(o.externalId ?? ""),
      config: {
        parameters: (c.parameters as JsonObject) ?? {},
        data: (c.data as JsonObject) ?? {},
      },
    };
  }
  return {
    externalId: "",
    config: { parameters: {}, data: {} },
  };
}

function coerceParam(s: string): unknown {
  const t = s.trim();
  if (t === "true") return true;
  if (t === "false") return false;
  if (t === "") return "";
  const n = Number(t);
  if (!Number.isNaN(n) && String(n) === t) return n;
  return t;
}

function editorSubtabClass(active: boolean): string {
  return `kea-tab${active ? " kea-tab--active" : ""}`;
}

export function KeyExtractionControls({
  value,
  onChange,
  scopeDocument,
  initialEditorSub,
  initialFocusedExtractionRuleName,
  initialFocusedMatchRuleName,
}: Props) {
  const { t } = useAppSettings();
  const [editorSub, setEditorSub] = useState<EditorSub>(() => initialEditorSub ?? "rules");
  const ke = useMemo(() => normalize(value), [value]);
  const params = (ke.config.parameters as JsonObject) ?? {};

  const [rulesYaml, setRulesYaml] = useState(() => {
    const { withoutValidation } = splitDataByValidation((ke.config.data as JsonObject) ?? {});
    return YAML.stringify(withoutValidation, { lineWidth: 0 });
  });
  const [validationYaml, setValidationYaml] = useState(() => {
    const { validation } = splitDataByValidation((ke.config.data as JsonObject) ?? {});
    return YAML.stringify(validation, { lineWidth: 0 });
  });
  const [rulesError, setRulesError] = useState<string | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [validationMergeHint, setValidationMergeHint] = useState<string | null>(null);

  const validationObject = useMemo(() => {
    const { validation } = splitDataByValidation((ke.config.data as JsonObject) ?? {});
    return validation;
  }, [value]);

  const rulesDataObject = useMemo(() => {
    const { withoutValidation } = splitDataByValidation((ke.config.data as JsonObject) ?? {});
    return withoutValidation;
  }, [value]);

  useEffect(() => {
    const data = (ke.config.data as JsonObject) ?? {};
    const { withoutValidation, validation } = splitDataByValidation(data);
    setRulesYaml(YAML.stringify(withoutValidation, { lineWidth: 0 }));
    setValidationYaml(YAML.stringify(validation, { lineWidth: 0 }));
    setRulesError(null);
    setValidationError(null);
    setValidationMergeHint(null);
  }, [value]);

  const push = (next: KeyExtractionBlock) => onChange(next);

  const rulesObjectForMerge = (): { rulesObj: JsonObject; parseFailed: boolean } => {
    try {
      const parsed = YAML.parse(rulesYaml);
      const rulesObj =
        parsed !== null && typeof parsed === "object" && !Array.isArray(parsed) ? (parsed as JsonObject) : {};
      setRulesError(null);
      return { rulesObj, parseFailed: false };
    } catch (e) {
      setRulesError(String(e));
      const data = (normalize(value).config.data as JsonObject) ?? {};
      return { rulesObj: splitDataByValidation(data).withoutValidation, parseFailed: true };
    }
  };

  const commitValidation = (validationObj: JsonObject) => {
    const { rulesObj, parseFailed } = rulesObjectForMerge();
    setValidationMergeHint(parseFailed ? t("validationEditor.rulesYamlInvalidMerge") : null);
    const cleaned = withoutRegexpMatch(validationObj);
    setValidationYaml(YAML.stringify(cleaned, { lineWidth: 0 }));
    setValidationError(null);
    const current = normalize(value);
    const data = mergeDataWithValidation(rulesObj, cleaned);
    push({ ...current, config: { ...current.config, data } });
  };

  const setExternalId = (externalId: string) => {
    push({ ...ke, externalId });
  };

  const setParamValue = (key: string, val: string) => {
    const p = { ...params } as Record<string, unknown>;
    p[key] = coerceParam(val);
    push({ ...ke, config: { ...ke.config, parameters: p } });
  };

  const renameParam = (oldKey: string, newKey: string) => {
    const nk = newKey.trim();
    if (nk === oldKey) return;
    const p = { ...params } as Record<string, unknown>;
    const val = p[oldKey];
    delete p[oldKey];
    if (nk) p[nk] = val;
    push({ ...ke, config: { ...ke.config, parameters: p } });
  };

  const addParam = () => {
    const p = { ...params } as Record<string, unknown>;
    let k = "new_key";
    let n = 0;
    while (k in p) {
      n += 1;
      k = `new_key_${n}`;
    }
    p[k] = "";
    push({ ...ke, config: { ...ke.config, parameters: p } });
  };

  const removeParam = (key: string) => {
    const p = { ...params } as Record<string, unknown>;
    delete p[key];
    push({ ...ke, config: { ...ke.config, parameters: p } });
  };

  const mergeAndPushConfigData = (withoutValidation: JsonObject) => {
    let validationObj: JsonObject;
    try {
      const parsed = YAML.parse(validationYaml);
      validationObj =
        parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
          ? (parsed as JsonObject)
          : {};
      setValidationError(null);
    } catch (e) {
      setValidationError(String(e));
      return;
    }
    validationObj = withoutRegexpMatch(validationObj);
    setValidationYaml(YAML.stringify(validationObj, { lineWidth: 0 }));
    const current = normalize(value);
    const data = mergeDataWithValidation(withoutValidation, validationObj);
    push({ ...current, config: { ...current.config, data } });
  };

  const commitRulesData = (nextWithoutValidation: JsonObject) => {
    setRulesYaml(YAML.stringify(nextWithoutValidation, { lineWidth: 0 }));
    setRulesError(null);
    mergeAndPushConfigData(nextWithoutValidation);
  };

  const commitConfigData = () => {
    const { rulesObj, parseFailed } = rulesObjectForMerge();
    setValidationMergeHint(parseFailed ? t("validationEditor.rulesYamlInvalidMerge") : null);
    mergeAndPushConfigData(rulesObj);
  };

  return (
    <div className="kea-key-discovery">
      <h3 className="kea-section-title">{t("keyExtraction.title")}</h3>
      <nav className="kea-tabs kea-editor-subtabs" role="tablist" aria-label={t("nav.subtabs")}>
        <button
          type="button"
          role="tab"
          aria-selected={editorSub === "rules"}
          className={editorSubtabClass(editorSub === "rules")}
          onClick={() => setEditorSub("rules")}
        >
          {t("editor.subtab.rules")}
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={editorSub === "validation"}
          className={editorSubtabClass(editorSub === "validation")}
          onClick={() => setEditorSub("validation")}
        >
          {t("editor.subtab.validation")}
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={editorSub === "settings"}
          className={editorSubtabClass(editorSub === "settings")}
          onClick={() => setEditorSub("settings")}
        >
          {t("editor.subtab.settings")}
        </button>
      </nav>
      {editorSub === "settings" && (
        <div role="tabpanel">
          <label className="kea-label kea-label--block">
            {t("keyExtraction.externalId")}
            <DeferredCommitInput
              className="kea-input"
              committedValue={String(ke.externalId ?? "")}
              onCommit={setExternalId}
            />
          </label>
          <h4 className="kea-section-title" style={{ fontSize: "0.95rem" }}>
            {t("keyExtraction.parameters")}
          </h4>
          {Object.entries(params).map(([k, v]) => (
            <div
              key={k}
              className="kea-filter-row"
              style={{ gridTemplateColumns: "minmax(8rem,1fr) minmax(8rem,1fr) auto", alignItems: "end" }}
            >
              <label className="kea-label">
                {t("forms.paramKey")}
                <input
                  className="kea-input"
                  defaultValue={k}
                  onBlur={(e) => renameParam(k, e.target.value)}
                />
              </label>
              <label className="kea-label">
                {t("forms.paramValue")}
                <input
                  className="kea-input"
                  value={stringifyVal(v)}
                  onChange={(e) => setParamValue(k, e.target.value)}
                />
              </label>
              <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={() => removeParam(k)}>
                {t("scope.remove")}
              </button>
            </div>
          ))}
          <button type="button" className="kea-btn kea-btn--sm" onClick={addParam}>
            {t("keyExtraction.addParam")}
          </button>
        </div>
      )}
      {editorSub === "rules" && (
        <div role="tabpanel">
          <h4 className="kea-section-title" style={{ fontSize: "0.95rem" }}>
            {t("keyExtraction.dataYaml")}
          </h4>
          <p className="kea-hint">{t("keyExtraction.dataYamlHint")}</p>
          {rulesError && <p className="kea-hint kea-hint--warn">{rulesError}</p>}
          <DiscoveryRulesStructuredEditor
            value={rulesDataObject}
            onChange={commitRulesData}
            scopeDocument={scopeDocument}
            initialFocusedRuleName={initialFocusedExtractionRuleName}
          />
          <details style={{ marginTop: "1rem" }}>
            <summary style={{ cursor: "pointer", color: "var(--kea-text-muted)" }}>{t("keyExtraction.advancedRulesYaml")}</summary>
            <textarea
              className="kea-textarea"
              style={{ minHeight: 200, fontFamily: "ui-monospace, monospace", marginTop: "0.5rem" }}
              value={rulesYaml}
              onChange={(e) => setRulesYaml(e.target.value)}
              onBlur={commitConfigData}
              spellCheck={false}
            />
          </details>
        </div>
      )}
      {editorSub === "validation" && (
        <div role="tabpanel">
          <h4 className="kea-section-title" style={{ fontSize: "0.95rem" }}>
            {t("keyExtraction.validationYaml")}
          </h4>
          <p className="kea-hint">{t("keyExtraction.validationYamlHint")}</p>
          {rulesError && <p className="kea-hint kea-hint--warn">{rulesError}</p>}
          {validationMergeHint && <p className="kea-hint kea-hint--warn">{validationMergeHint}</p>}
          <ValidationStructuredEditor
            variant="keyExtraction"
            value={validationObject}
            onChange={commitValidation}
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
              onBlur={commitConfigData}
              spellCheck={false}
            />
          </details>
        </div>
      )}
    </div>
  );
}

function stringifyVal(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "boolean" || typeof v === "number") return String(v);
  if (typeof v === "string") return v;
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}
