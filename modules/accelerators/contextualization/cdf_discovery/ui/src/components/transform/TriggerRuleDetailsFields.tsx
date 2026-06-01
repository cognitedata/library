import { useEffect, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  triggerType: string;
};

type TriggerValueType = "string" | "number" | "boolean" | "json";

type RuleRow = {
  key: string;
  valueType: TriggerValueType;
  valueText: string;
};

function ruleObject(cfg: JsonObject): JsonObject {
  const raw = cfg.trigger_rule;
  if (raw && typeof raw === "object" && !Array.isArray(raw)) return raw as JsonObject;
  return {};
}

function valueTypeOf(value: unknown): TriggerValueType {
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "boolean";
  if (value && typeof value === "object") return "json";
  return "string";
}

function valueTextOf(value: unknown): string {
  if (value && typeof value === "object") return JSON.stringify(value, null, 2);
  return value == null ? "" : String(value);
}

function rowsFromRule(rule: JsonObject): RuleRow[] {
  return Object.entries(rule)
    .filter(([k]) => k !== "triggerType")
    .map(([key, value]) => ({
      key,
      valueType: valueTypeOf(value),
      valueText: valueTextOf(value),
    }));
}

function parseTypedValue(row: RuleRow): unknown {
  const raw = row.valueText.trim();
  if (!raw) return "";
  if (row.valueType === "number") {
    const n = Number(raw);
    if (Number.isFinite(n)) return n;
    return raw;
  }
  if (row.valueType === "boolean") return raw.toLowerCase() === "true";
  if (row.valueType === "json") {
    try {
      return JSON.parse(raw);
    } catch {
      return raw;
    }
  }
  return raw;
}

function mergeRowsIntoConfig(cfg: JsonObject, rows: RuleRow[], triggerType: string): JsonObject {
  const nextRule: JsonObject = { triggerType };
  for (const row of rows) {
    const key = row.key.trim();
    if (!key) continue;
    const val = parseTypedValue(row);
    if (val === "" || val == null) continue;
    nextRule[key] = val;
  }
  const next = { ...cfg };
  if (Object.keys(nextRule).length > 1) next.trigger_rule = nextRule;
  else delete next.trigger_rule;
  return next;
}

export function TriggerRuleDetailsFields({ value, onChange, triggerType }: Props) {
  const { t } = useAppSettings();
  const [rows, setRows] = useState<RuleRow[]>(() => rowsFromRule(ruleObject(value)));

  useEffect(() => {
    setRows(rowsFromRule(ruleObject(value)));
  }, [value.trigger_rule, triggerType]);

  const updateRow = (idx: number, patch: Partial<RuleRow>) => {
    const nextRows = rows.map((r, i) => (i === idx ? { ...r, ...patch } : r));
    setRows(nextRows);
    onChange(mergeRowsIntoConfig(value, nextRows, triggerType));
  };

  const removeRow = (idx: number) => {
    const nextRows = rows.filter((_, i) => i !== idx);
    setRows(nextRows);
    onChange(mergeRowsIntoConfig(value, nextRows, triggerType));
  };

  const addRow = () => {
    setRows([...rows, { key: "", valueType: "string", valueText: "" }]);
  };

  return (
    <div style={{ marginTop: "0.75rem" }}>
      <p className="transform-node-editor-modal__hint">{t("transform.config.triggerRuleDetailedHint")}</p>
      {rows.map((row, idx) => (
        <div
          key={`${row.key}-${idx}`}
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 8rem 1fr auto",
            gap: "0.5rem",
            alignItems: "start",
            marginBottom: "0.5rem",
          }}
        >
          <input
            className="gov-input"
            value={row.key}
            placeholder={t("transform.config.triggerRuleKey")}
            onChange={(e) => updateRow(idx, { key: e.target.value })}
            spellCheck={false}
          />
          <select
            className="gov-input"
            value={row.valueType}
            onChange={(e) => updateRow(idx, { valueType: e.target.value as TriggerValueType })}
          >
            <option value="string">{t("transform.config.triggerRuleTypeString")}</option>
            <option value="number">{t("transform.config.triggerRuleTypeNumber")}</option>
            <option value="boolean">{t("transform.config.triggerRuleTypeBoolean")}</option>
            <option value="json">{t("transform.config.triggerRuleTypeJson")}</option>
          </select>
          <textarea
            className="gov-input gov-input--mono"
            value={row.valueText}
            placeholder={t("transform.config.triggerRuleValue")}
            onChange={(e) => updateRow(idx, { valueText: e.target.value })}
            spellCheck={false}
            style={{ minHeight: "3rem" }}
          />
          <button type="button" className="disc-btn disc-btn--subtle" onClick={() => removeRow(idx)}>
            {t("transform.config.triggerRuleRemove")}
          </button>
        </div>
      ))}
      <button type="button" className="disc-btn" onClick={addRow}>
        {t("transform.config.triggerRuleAdd")}
      </button>
    </div>
  );
}
