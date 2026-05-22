import { useEffect, useState } from "react";
import YAML from "yaml";
import type { MessageKey } from "../../i18n/types";
import { DeferredCommitInput } from "../DeferredCommitTextField";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

/** Keys edited in the form; other keys (except deprecated) are preserved in `extra` per row. */
const FORM_KEYS = new Set([
  "field_name",
  "variable",
  "regex",
  "max_matches_per_field",
  "table_id",
  "required",
  "priority",
  "role",
  "max_length",
  "preprocessing",
]);

/** Removed from schema — stripped on emit so old configs lose these on save. */
const DEPRECATED_SOURCE_FIELD_KEYS = new Set([
  "field_type",
  "separator",
  "join_fields",
  "fixed_width",
]);

export type SourceFieldForm = {
  field_name: string;
  variable: string;
  regex: string;
  max_matches_per_field: string;
  table_id: string;
  required: boolean;
  priority: string;
  max_length: string;
  role: string;
  preprocessingCsv: string;
};

export type RowState = { form: SourceFieldForm; extra: Record<string, unknown> };

function defaultForm(): SourceFieldForm {
  return {
    field_name: "name",
    variable: "name",
    regex: "",
    max_matches_per_field: "",
    table_id: "",
    required: true,
    priority: "1",
    max_length: "500",
    role: "",
    preprocessingCsv: "trim",
  };
}

function defaultRowState(): RowState {
  return { form: defaultForm(), extra: {} };
}

function parseOne(raw: unknown): RowState {
  const o = raw !== null && typeof raw === "object" && !Array.isArray(raw) ? (raw as Record<string, unknown>) : {};
  const extra: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(o)) {
    if (!FORM_KEYS.has(k) && !DEPRECATED_SOURCE_FIELD_KEYS.has(k)) extra[k] = v;
  }

  let preprocessingCsv = "";
  const prep = o.preprocessing;
  if (Array.isArray(prep)) preprocessingCsv = prep.map(String).join(", ");
  else if (typeof prep === "string" && prep.trim()) preprocessingCsv = prep;

  const role = o.role != null && String(o.role).trim() !== "" ? String(o.role) : "";

  return {
    form: {
      field_name: String(o.field_name ?? ""),
      variable: o.variable != null ? String(o.variable) : "",
      regex: o.regex != null ? String(o.regex) : "",
      max_matches_per_field: o.max_matches_per_field != null ? String(o.max_matches_per_field) : "",
      table_id: o.table_id != null ? String(o.table_id) : "",
      required: o.required === true,
      priority: o.priority != null ? String(o.priority) : "1",
      max_length: o.max_length != null ? String(o.max_length) : "",
      role,
      preprocessingCsv,
    },
    extra,
  };
}

export function parseSourceFieldsToRows(yaml: string): RowState[] {
  try {
    const p = YAML.parse(yaml);
    const arr = Array.isArray(p) ? p : p != null && typeof p === "object" ? [p] : [];
    if (arr.length === 0) return [defaultRowState()];
    return arr.map(parseOne);
  } catch {
    return [defaultRowState()];
  }
}

function emitOne(row: RowState): Record<string, unknown> {
  const { form, extra } = row;
  const o: Record<string, unknown> = { ...extra };
  o.field_name = form.field_name;
  o.required = form.required;
  const pr = Number(form.priority);
  o.priority = Number.isFinite(pr) ? pr : 1;
  if (form.variable.trim()) o.variable = form.variable.trim();
  else delete o.variable;
  if (form.regex.trim()) o.regex = form.regex.trim();
  else delete o.regex;
  if (form.max_matches_per_field.trim()) {
    const mm = Number(form.max_matches_per_field);
    if (Number.isFinite(mm)) o.max_matches_per_field = mm;
  } else delete o.max_matches_per_field;
  if (form.table_id.trim()) o.table_id = form.table_id.trim();
  else delete o.table_id;
  if (form.max_length.trim()) {
    const ml = Number(form.max_length);
    if (Number.isFinite(ml)) o.max_length = ml;
  } else delete o.max_length;
  if (form.role.trim()) o.role = form.role.trim();
  else delete o.role;
  const steps = form.preprocessingCsv
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (steps.length) o.preprocessing = steps;
  else delete o.preprocessing;
  return o;
}

export function emitSourceFieldsYaml(rows: RowState[]): string {
  const arr = rows.map(emitOne);
  return YAML.stringify(arr, { lineWidth: 0 }) + "\n";
}

type Props = {
  /** Key extraction handler — reserved for future per-handler field UX. */
  handler: string;
  sourceFieldsYaml: string;
  onChange: (nextYaml: string) => void;
  t: TFn;
};

export function DiscoverySourceFieldsEditor({ handler: _handler, sourceFieldsYaml, onChange, t }: Props) {
  const [rows, setRows] = useState<RowState[]>(() => parseSourceFieldsToRows(sourceFieldsYaml));

  useEffect(() => {
    setRows(parseSourceFieldsToRows(sourceFieldsYaml));
  }, [sourceFieldsYaml]);

  const commit = (next: RowState[]) => {
    setRows(next);
    onChange(emitSourceFieldsYaml(next));
  };

  const updateRow = (index: number, patch: Partial<SourceFieldForm>) => {
    const next = [...rows];
    const cur = next[index];
    if (!cur) return;
    next[index] = { ...cur, form: { ...cur.form, ...patch } };
    commit(next);
  };

  const addRow = () => {
    commit([...rows, defaultRowState()]);
  };

  const removeRow = (index: number) => {
    if (rows.length <= 1) return;
    commit(rows.filter((_, i) => i !== index));
  };

  return (
    <div className="discovery-handler-fields discovery-source-fields">
      <div className="discovery-source-fields__toolbar" style={{ marginBottom: "0.5rem" }}>
        <button type="button" className="discovery-btn discovery-btn--sm" onClick={addRow}>
          {t("discoveryRules.sourceFields.addField")}
        </button>
      </div>
      {rows.map((row, i) => (
        <div
          key={i}
          className="discovery-source-fields__card"
          style={{
            border: "1px solid var(--discovery-border)",
            borderRadius: "var(--discovery-radius-sm)",
            padding: "0.75rem 0.85rem",
            marginBottom: "0.65rem",
            background: "var(--discovery-bg-elevated)",
          }}
        >
          <div className="discovery-field-group">
            <div className="discovery-filter-row discovery-filter-row--discovery-source-identity">
              <label className="discovery-label">
                {t("discoveryRules.sourceFields.fieldName")}
                <DeferredCommitInput
                  className="discovery-input"
                  committedValue={row.form.field_name}
                  syncKey={i}
                  onCommit={(v) => updateRow(i, { field_name: v })}
                />
              </label>
              <label className="discovery-label">
                {t("discoveryRules.sourceFields.variable")}
                <DeferredCommitInput
                  className="discovery-input"
                  placeholder={t("discoveryRules.sourceFields.variablePlaceholder")}
                  committedValue={row.form.variable}
                  syncKey={i}
                  onCommit={(v) => updateRow(i, { variable: v })}
                />
              </label>
            </div>
            <div className="discovery-discovery-source-actions">
              <label className="discovery-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", marginBottom: 0 }}>
                <input
                  type="checkbox"
                  checked={row.form.required}
                  onChange={(e) => updateRow(i, { required: e.target.checked })}
                />
                {t("discoveryRules.sourceFields.required")}
              </label>
              <button
                type="button"
                className="discovery-btn discovery-btn--ghost discovery-btn--sm discovery-discovery-source-actions__remove"
                disabled={rows.length <= 1}
                onClick={() => removeRow(i)}
              >
                {t("discoveryRules.sourceFields.removeField")}
              </button>
            </div>
          </div>
          <div className="discovery-field-group">
            <label className="discovery-label discovery-label--block">
              {t("discoveryRules.sourceFields.regex")}
              <input
                className="discovery-input"
                style={{ fontFamily: "ui-monospace, monospace" }}
                placeholder={t("discoveryRules.sourceFields.regexPlaceholder")}
                value={row.form.regex}
                onChange={(e) => updateRow(i, { regex: e.target.value })}
                spellCheck={false}
              />
            </label>
            <label className="discovery-label discovery-label--block" style={{ marginTop: "0.45rem" }}>
              {t("discoveryRules.sourceFields.maxMatchesPerField")}
              <input
                className="discovery-input"
                type="number"
                min={1}
                placeholder="100"
                style={{ maxWidth: "12rem" }}
                value={row.form.max_matches_per_field}
                onChange={(e) => updateRow(i, { max_matches_per_field: e.target.value })}
              />
            </label>
          </div>
          <div className="discovery-field-group">
            <div className="discovery-filter-row discovery-filter-row--discovery-source-numeric">
              <label className="discovery-label">
                {t("discoveryRules.sourceFields.priority")}
                <input
                  className="discovery-input"
                  type="number"
                  value={row.form.priority}
                  onChange={(e) => updateRow(i, { priority: e.target.value })}
                />
              </label>
              <label className="discovery-label">
                {t("discoveryRules.sourceFields.maxLength")}
                <input
                  className="discovery-input"
                  type="number"
                  min={0}
                  value={row.form.max_length}
                  onChange={(e) => updateRow(i, { max_length: e.target.value })}
                />
              </label>
              <label className="discovery-label">
                {t("discoveryRules.sourceFields.role")}
                <input
                  className="discovery-input"
                  autoComplete="off"
                  placeholder="—"
                  value={row.form.role}
                  onChange={(e) => updateRow(i, { role: e.target.value })}
                />
              </label>
            </div>
          </div>
          <div className="discovery-field-group">
            <label className="discovery-label discovery-label--block">
              {t("discoveryRules.sourceFields.tableId")}
              <input
                className="discovery-input"
                placeholder="—"
                value={row.form.table_id}
                onChange={(e) => updateRow(i, { table_id: e.target.value })}
              />
            </label>
          </div>
          <div className="discovery-field-group">
            <label className="discovery-label discovery-label--block" title={t("discoveryRules.sourceFields.preprocessingCsv.tooltip")}>
              {t("discoveryRules.sourceFields.preprocessingCsv")}
              <input
                className="discovery-input"
                placeholder="trim, lowercase"
                value={row.form.preprocessingCsv}
                onChange={(e) => updateRow(i, { preprocessingCsv: e.target.value })}
              />
            </label>
          </div>
        </div>
      ))}
    </div>
  );
}
