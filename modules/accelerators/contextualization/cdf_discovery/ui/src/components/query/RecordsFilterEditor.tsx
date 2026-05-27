import { useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";

type Props = {
  filter: JsonObject | null;
  onChange: (next: JsonObject | null) => void;
};

export function RecordsFilterEditor({ filter, onChange }: Props) {
  const { t } = useAppSettings();
  const [jsonText, setJsonText] = useState(() =>
    filter && Object.keys(filter).length ? JSON.stringify(filter, null, 2) : ""
  );
  const [parseError, setParseError] = useState<string | null>(null);

  const applyJson = () => {
    const raw = jsonText.trim();
    if (!raw) {
      setParseError(null);
      onChange(null);
      return;
    }
    try {
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        setParseError(t("transform.query.recordsFilterInvalid"));
        return;
      }
      setParseError(null);
      onChange(parsed as JsonObject);
    } catch {
      setParseError(t("transform.query.recordsFilterInvalid"));
    }
  };

  return (
    <div className="transform-records-filter">
      <p className="transform-query-hint">{t("transform.query.recordsFilterHint")}</p>
      <label className="transform-query-label transform-query-label--block">
        {t("transform.query.recordsFilterJson")}
        <textarea
          className="gov-input gov-input--mono"
          style={{ marginTop: "0.35rem", minHeight: "10rem", width: "100%" }}
          value={jsonText}
          onChange={(e) => setJsonText(e.target.value)}
          onBlur={applyJson}
          spellCheck={false}
        />
      </label>
      {parseError ? <p className="transform-query-error">{parseError}</p> : null}
    </div>
  );
}
