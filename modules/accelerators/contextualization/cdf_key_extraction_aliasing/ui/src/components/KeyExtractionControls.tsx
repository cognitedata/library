import { useEffect, useMemo, useState } from "react";
import YAML from "yaml";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";

type Props = {
  value: unknown;
  onChange: (next: unknown) => void;
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

export function KeyExtractionControls({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const [editorSub, setEditorSub] = useState<"settings" | "rules">("rules");
  const ke = useMemo(() => normalize(value), [value]);
  const params = (ke.config.parameters as JsonObject) ?? {};

  const [dataYaml, setDataYaml] = useState(() =>
    YAML.stringify((ke.config.data as JsonObject) ?? {}, { lineWidth: 0 })
  );
  const [dataError, setDataError] = useState<string | null>(null);

  useEffect(() => {
    setDataYaml(YAML.stringify((ke.config.data as JsonObject) ?? {}, { lineWidth: 0 }));
    setDataError(null);
  }, [value]);

  const push = (next: KeyExtractionBlock) => onChange(next);

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

  const applyDataYaml = () => {
    try {
      const parsed = YAML.parse(dataYaml);
      const data =
        parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
          ? (parsed as JsonObject)
          : {};
      setDataError(null);
      const current = normalize(value);
      push({ ...current, config: { ...current.config, data } });
    } catch (e) {
      setDataError(String(e));
    }
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
            <input className="kea-input" value={String(ke.externalId ?? "")} onChange={(e) => setExternalId(e.target.value)} />
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
          {dataError && <p className="kea-hint kea-hint--warn">{dataError}</p>}
          <textarea
            className="kea-textarea"
            style={{ minHeight: 280, fontFamily: "ui-monospace, monospace" }}
            value={dataYaml}
            onChange={(e) => setDataYaml(e.target.value)}
            onBlur={applyDataYaml}
            spellCheck={false}
          />
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
