import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";

export type QueryScopeMode = "inherit" | "all" | "incremental";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function readQueryScopeMode(value: JsonObject): QueryScopeMode {
  const raw = String(value.query_scope_mode ?? "inherit").trim().toLowerCase();
  if (raw === "all" || raw === "incremental") return raw;
  return "inherit";
}

export function QueryScopeModeFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const mode = readQueryScopeMode(value);

  return (
    <label className="transform-query-label transform-query-label--block">
      {t("transform.query.scopeMode")}
      <select
        className="gov-input"
        style={{ marginTop: "0.35rem", display: "block" }}
        value={mode}
        onChange={(e) => {
          const next = e.target.value as QueryScopeMode;
          const updated = { ...value };
          if (next === "inherit") {
            delete updated.query_scope_mode;
          } else {
            updated.query_scope_mode = next;
          }
          onChange(updated);
        }}
      >
        <option value="inherit">{t("transform.query.scopeModeInherit")}</option>
        <option value="incremental">{t("transform.query.scopeModeIncremental")}</option>
        <option value="all">{t("transform.query.scopeModeAll")}</option>
      </select>
      <span className="transform-query-hint" style={{ display: "block", marginTop: "0.25rem" }}>
        {t("transform.query.scopeModeHint")}
      </span>
    </label>
  );
}
