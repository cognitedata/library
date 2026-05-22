import { useEffect, useMemo, useState } from "react";
import YAML from "yaml";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { mergeDataWithValidation, splitDataByValidation } from "../utils/splitConfigData";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import { ValidationStructuredEditor } from "./ValidationStructuredEditor";

type EditorSub = "settings" | "validation";

type Props = {
  value: unknown;
  onChange: (next: unknown) => void;
  /** Full scope document for match-rule refs (definitions / sequences at root). */
  scopeDocument?: Record<string, unknown>;
  /** Sub-tab on first mount (e.g. flow canvas double-click). */
  initialEditorSub?: EditorSub;
  /** Scroll to this match rule on the Validation tab (inline rules only). */
  initialFocusedMatchRuleName?: string;
};

type AliasingBlock = {
  externalId: string;
  config: { parameters: JsonObject; data: JsonObject };
};

function normalize(v: unknown): AliasingBlock {
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
  return `discovery-tab${active ? " discovery-tab--active" : ""}`;
}

export function AliasingControls({
  value,
  onChange,
  scopeDocument,
  initialEditorSub,
  initialFocusedMatchRuleName,
}: Props) {
  const { t } = useAppSettings();
  const [editorSub, setEditorSub] = useState<EditorSub>(() =>
    initialEditorSub === "rules" ? "settings" : (initialEditorSub ?? "settings")
  );
  const al = useMemo(() => normalize(value), [value]);
  const params = (al.config.parameters as JsonObject) ?? {};

  const [rulesYaml, setRulesYaml] = useState(() => {
    const { withoutValidation } = splitDataByValidation((al.config.data as JsonObject) ?? {});
    return YAML.stringify(withoutValidation, { lineWidth: 0 });
  });
  const [validationYaml, setValidationYaml] = useState(() => {
    const { validation } = splitDataByValidation((al.config.data as JsonObject) ?? {});
    return YAML.stringify(validation, { lineWidth: 0 });
  });
  const [rulesError, setRulesError] = useState<string | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [validationMergeHint, setValidationMergeHint] = useState<string | null>(null);

  const validationObject = useMemo(() => {
    const { validation } = splitDataByValidation((al.config.data as JsonObject) ?? {});
    return validation;
  }, [value]);

  const rulesDataObject = useMemo(() => {
    const { withoutValidation } = splitDataByValidation((al.config.data as JsonObject) ?? {});
    return withoutValidation;
  }, [value]);

  useEffect(() => {
    const data = (al.config.data as JsonObject) ?? {};
    const { withoutValidation, validation } = splitDataByValidation(data);
    setRulesYaml(YAML.stringify(withoutValidation, { lineWidth: 0 }));
    setValidationYaml(YAML.stringify(validation, { lineWidth: 0 }));
    setRulesError(null);
    setValidationError(null);
    setValidationMergeHint(null);
  }, [value]);

  const push = (next: AliasingBlock) => onChange(next);

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
    setValidationYaml(YAML.stringify(validationObj, { lineWidth: 0 }));
    setValidationError(null);
    const current = normalize(value);
    const data = mergeDataWithValidation(rulesObj, validationObj);
    push({ ...current, config: { ...current.config, data } });
  };

  const setExternalId = (externalId: string) => {
    push({ ...al, externalId });
  };

  const setParamValue = (key: string, val: string) => {
    const p = { ...params } as Record<string, unknown>;
    p[key] = coerceParam(val);
    push({ ...al, config: { ...al.config, parameters: p } });
  };

  const renameParam = (oldKey: string, newKey: string) => {
    const nk = newKey.trim();
    if (nk === oldKey) return;
    const p = { ...params } as Record<string, unknown>;
    const val = p[oldKey];
    delete p[oldKey];
    if (nk) p[nk] = val;
    push({ ...al, config: { ...al.config, parameters: p } });
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
    push({ ...al, config: { ...al.config, parameters: p } });
  };

  const removeParam = (key: string) => {
    const p = { ...params } as Record<string, unknown>;
    delete p[key];
    push({ ...al, config: { ...al.config, parameters: p } });
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
    <div className="discovery-aliasing">
      <h3 className="discovery-section-title">{t("aliasing.title")}</h3>
      <nav className="discovery-tabs discovery-editor-subtabs" role="tablist" aria-label={t("nav.subtabs")}>
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
          <label className="discovery-label discovery-label--block">
            {t("aliasing.externalId")}
            <DeferredCommitInput
              className="discovery-input"
              committedValue={String(al.externalId ?? "")}
              onCommit={setExternalId}
            />
          </label>
          <h4 className="discovery-section-title" style={{ fontSize: "0.95rem" }}>
            {t("aliasing.parameters")}
          </h4>
          {Object.entries(params).map(([k, v]) => (
            <div
              key={k}
              className="discovery-filter-row discovery-filter-row--pair-wide discovery-filter-row--align-end"
            >
              <label className="discovery-label">
                {t("forms.paramKey")}
                <input
                  className="discovery-input"
                  defaultValue={k}
                  onBlur={(e) => renameParam(k, e.target.value)}
                />
              </label>
              <label className="discovery-label">
                {t("forms.paramValue")}
                <input
                  className="discovery-input"
                  value={stringifyVal(v)}
                  onChange={(e) => setParamValue(k, e.target.value)}
                />
              </label>
              <button type="button" className="discovery-btn discovery-btn--ghost discovery-btn--sm" onClick={() => removeParam(k)}>
                {t("scope.remove")}
              </button>
            </div>
          ))}
          <button type="button" className="discovery-btn discovery-btn--sm" onClick={addParam}>
            {t("aliasing.addParam")}
          </button>
        </div>
      )}
      {editorSub === "validation" && (
        <div role="tabpanel">
          <h4 className="discovery-section-title" style={{ fontSize: "0.95rem" }}>
            {t("aliasing.validationYaml")}
          </h4>
          <p className="discovery-hint">{t("aliasing.validationYamlHint")}</p>
          {rulesError && <p className="discovery-hint discovery-hint--warn">{rulesError}</p>}
          {validationMergeHint && <p className="discovery-hint discovery-hint--warn">{validationMergeHint}</p>}
          <ValidationStructuredEditor
            variant="aliasing"
            value={validationObject}
            onChange={commitValidation}
            scopeDocument={scopeDocument}
            initialFocusedMatchRuleName={initialFocusedMatchRuleName}
          />
          <details style={{ marginTop: "1rem" }}>
            <summary style={{ cursor: "pointer", color: "var(--discovery-text-muted)" }}>{t("validationEditor.advancedYaml")}</summary>
            {validationError && <p className="discovery-hint discovery-hint--warn">{validationError}</p>}
            <textarea
              className="discovery-textarea"
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
