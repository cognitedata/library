import { useCallback, useMemo, useState, type DragEvent } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { confidenceMatchDefinitionIds } from "../utils/confidenceMatchDefinitionIds";
import { reorderListAtIndex } from "../utils/ruleListReorder";
import { isShorthandConfidenceMatchChain } from "../utils/confidenceMatchRuleNames";

type RefRow =
  | { kind: "rule"; ruleId: string }
  | { kind: "sequence"; sequenceId: string }
  | { kind: "parallel"; children: RefRow[] }
  | { kind: "sequential"; children: RefRow[] };

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  /** Scope document (definitions + sequences live at top level). */
  scopeDocument: Record<string, unknown>;
};

function parseRows(raw: unknown): RefRow[] {
  if (!Array.isArray(raw)) return [];
  const out: RefRow[] = [];
  for (const r of raw) {
    if (typeof r === "string") {
      const s = r.trim();
      if (s) out.push({ kind: "rule", ruleId: s });
      continue;
    }
    if (r && typeof r === "object" && !Array.isArray(r)) {
      const o = r as Record<string, unknown>;
      if (isShorthandConfidenceMatchChain(o)) {
        const k = Object.keys(o)[0]!;
        const tail = o[k] as unknown[];
        out.push({
          kind: "sequential",
          children: [{ kind: "rule", ruleId: String(k).trim() }, ...parseRows(tail)],
        });
        continue;
      }
      const hi = o.hierarchy;
      if (hi && typeof hi === "object" && !Array.isArray(hi)) {
        const h = hi as Record<string, unknown>;
        const rawCh = Array.isArray(h.children) ? h.children : [];
        const mode = String(h.mode ?? "ordered").toLowerCase();
        const concurrent = mode === "concurrent";
        out.push({
          kind: concurrent ? "parallel" : "sequential",
          children: parseRows(rawCh as unknown[]),
        });
        continue;
      }
      if (o.sequence != null && String(o.sequence).trim()) {
        out.push({ kind: "sequence", sequenceId: String(o.sequence).trim() });
        continue;
      }
      if (o.ref != null && String(o.ref).trim()) {
        out.push({ kind: "rule", ruleId: String(o.ref).trim() });
        continue;
      }
      if (o.name != null && String(o.name).trim()) {
        out.push({ kind: "rule", ruleId: String(o.name).trim() });
        continue;
      }
    }
  }
  return out;
}

function serializeRows(rows: RefRow[]): unknown[] {
  return rows.map((row) => {
    if (row.kind === "parallel") {
      return { hierarchy: { mode: "concurrent", children: serializeRows(row.children) } };
    }
    if (row.kind === "sequential") {
      return { hierarchy: { mode: "ordered", children: serializeRows(row.children) } };
    }
    return row.kind === "rule" ? row.ruleId : { sequence: row.sequenceId };
  });
}

function sequenceKeys(scope: Record<string, unknown>): string[] {
  const raw = scope.confidence_match_rule_sequences;
  if (raw !== null && typeof raw === "object" && !Array.isArray(raw)) {
    return Object.keys(raw as Record<string, unknown>).sort();
  }
  return [];
}

type RowEditorProps = {
  rows: RefRow[];
  setRows: (r: RefRow[]) => void;
  depth: number;
  defKeys: string[];
  seqKeys: string[];
};

function defaultLeafRow(defKeys: string[], seqKeys: string[]): RefRow {
  if (defKeys.length > 0) return { kind: "rule", ruleId: defKeys[0]! };
  if (seqKeys.length > 0) return { kind: "sequence", sequenceId: seqKeys[0]! };
  return { kind: "rule", ruleId: "" };
}

function RefRowsEditor({ rows, setRows, depth, defKeys, seqKeys }: RowEditorProps) {
  const { t } = useAppSettings();
  const [dragFrom, setDragFrom] = useState<number | null>(null);
  const [dragOver, setDragOver] = useState<number | null>(null);

  return (
    <>
      {rows.map((row, idx) => {
        const dropActive = dragOver === idx;
        const cardClass = [
          "kea-validation-rule",
          dropActive ? "kea-validation-rule--drop" : "",
          dragFrom === idx ? "kea-validation-rule--dragging" : "",
        ]
          .filter(Boolean)
          .join(" ");
        if (row.kind === "parallel" || row.kind === "sequential") {
          const isPar = row.kind === "parallel";
          return (
            <div
              key={`grp-${depth}-${idx}-${isPar ? "p" : "s"}`}
              className={cardClass}
              style={{
                border: "1px solid var(--kea-border)",
                borderRadius: "var(--kea-radius-sm)",
                padding: "0.75rem",
                marginBottom: "0.5rem",
                marginLeft: depth > 0 ? "1rem" : undefined,
                background: "var(--kea-surface-elevated, var(--kea-surface))",
              }}
            >
              <div className="kea-filter-row" style={{ marginBottom: "0.5rem", alignItems: "center" }}>
                <strong style={{ fontSize: "0.85rem" }}>
                  {isPar ? t("validationEditor.parallelGroup") : t("validationEditor.sequentialGroup")}
                </strong>
                <button
                  type="button"
                  className="kea-btn kea-btn--ghost kea-btn--sm"
                  style={{ marginLeft: "auto" }}
                  onClick={() => setRows(rows.filter((_, i) => i !== idx))}
                >
                  {t("validationEditor.rule.remove")}
                </button>
              </div>
              <RefRowsEditor
                rows={row.children}
                setRows={(nextChildren) => {
                  const next = [...rows];
                  next[idx] = isPar
                    ? { kind: "parallel", children: nextChildren }
                    : { kind: "sequential", children: nextChildren };
                  setRows(next);
                }}
                depth={depth + 1}
                defKeys={defKeys}
                seqKeys={seqKeys}
              />
              <button
                type="button"
                className="kea-btn kea-btn--sm"
                style={{ marginTop: "0.35rem" }}
                onClick={() => {
                  const next = [...rows];
                  const cur = next[idx];
                  if (cur.kind !== "parallel" && cur.kind !== "sequential") return;
                  next[idx] = {
                    ...cur,
                    children: [...cur.children, defaultLeafRow(defKeys, seqKeys)],
                  };
                  setRows(next);
                }}
              >
                {t("validationEditor.addMatchStep")}
              </button>
            </div>
          );
        }
        return (
          <div
            key={`row-${depth}-${idx}`}
            className={cardClass}
            data-kea-match-ref-rule={row.kind === "rule" && row.ruleId.trim() ? row.ruleId : undefined}
            style={{
              border: "1px solid var(--kea-border)",
              borderRadius: "var(--kea-radius-sm)",
              padding: "0.75rem",
              marginBottom: "0.5rem",
              marginLeft: depth > 0 ? "1rem" : undefined,
              background: "var(--kea-surface)",
            }}
            onDragOver={(e: DragEvent<HTMLDivElement>) => {
              e.preventDefault();
              e.dataTransfer.dropEffect = "move";
              setDragOver(idx);
            }}
            onDragLeave={(e) => {
              if (!e.currentTarget.contains(e.relatedTarget as Node | null)) {
                setDragOver(null);
              }
            }}
            onDrop={(e: DragEvent<HTMLDivElement>) => {
              e.preventDefault();
              const rawFrom = e.dataTransfer.getData("text/plain");
              const from = parseInt(rawFrom, 10);
              if (Number.isNaN(from) || from === idx) {
                setDragFrom(null);
                setDragOver(null);
                return;
              }
              setRows(reorderListAtIndex(rows, from, idx));
              setDragFrom(null);
              setDragOver(null);
            }}
          >
            <div
              className="kea-filter-row"
              style={{ gridTemplateColumns: "auto 1fr 1fr auto", gap: "0.5rem", alignItems: "end" }}
            >
              <span
                className="kea-drag-handle"
                draggable
                onDragStart={(e: DragEvent<HTMLSpanElement>) => {
                  e.dataTransfer.setData("text/plain", String(idx));
                  e.dataTransfer.effectAllowed = "move";
                  setDragFrom(idx);
                }}
                onDragEnd={() => {
                  setDragFrom(null);
                  setDragOver(null);
                }}
                aria-label={t("rulesEntity.dragHandle")}
                title={t("rulesEntity.dragHandle")}
              >
                <span className="kea-drag-handle__grip" aria-hidden>
                  ⋮⋮
                </span>
              </span>
              <label className="kea-label">
                {t("validationEditor.matchStepKind")}
                <select
                  className="kea-input"
                  value={row.kind}
                  onChange={(e) => {
                    const k = e.target.value as "rule" | "sequence" | "parallel" | "sequential";
                    const next = [...rows];
                    const leaf = defaultLeafRow(defKeys, seqKeys);
                    if (k === "rule") {
                      next[idx] = { kind: "rule", ruleId: defKeys[0] ?? "" };
                    } else if (k === "sequence") {
                      next[idx] = { kind: "sequence", sequenceId: seqKeys[0] ?? "" };
                    } else if (k === "parallel") {
                      next[idx] = { kind: "parallel", children: [leaf] };
                    } else {
                      next[idx] = { kind: "sequential", children: [leaf] };
                    }
                    setRows(next);
                  }}
                >
                  <option value="rule">{t("validationEditor.matchStepRule")}</option>
                  <option value="sequence">{t("validationEditor.matchStepSequence")}</option>
                  <option value="parallel">{t("validationEditor.parallelGroup")}</option>
                  <option value="sequential">{t("validationEditor.sequentialGroup")}</option>
                </select>
              </label>
              {row.kind === "rule" ? (
                <label className="kea-label">
                  {t("validationEditor.matchStepRuleId")}
                  <select
                    className="kea-input"
                    value={row.ruleId}
                    onChange={(e) => {
                      const next = [...rows];
                      next[idx] = { kind: "rule", ruleId: e.target.value };
                      setRows(next);
                    }}
                  >
                    <option value="">{t("validationEditor.matchStepSelectDefinition")}</option>
                    {defKeys.map((k) => (
                      <option key={k} value={k}>
                        {k}
                      </option>
                    ))}
                  </select>
                </label>
              ) : (
                <label className="kea-label">
                  {t("validationEditor.matchStepSequenceId")}
                  <select
                    className="kea-input"
                    value={row.sequenceId}
                    onChange={(e) => {
                      const next = [...rows];
                      next[idx] = { kind: "sequence", sequenceId: e.target.value };
                      setRows(next);
                    }}
                  >
                    <option value="">{t("validationEditor.matchStepSelectSequence")}</option>
                    {seqKeys.map((k) => (
                      <option key={k} value={k}>
                        {k}
                      </option>
                    ))}
                  </select>
                </label>
              )}
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                onClick={() => setRows(rows.filter((_, i) => i !== idx))}
              >
                {t("validationEditor.rule.remove")}
              </button>
            </div>
          </div>
        );
      })}
    </>
  );
}

export function MatchValidationRefsEditor({ value, onChange, scopeDocument }: Props) {
  const { t } = useAppSettings();
  const defKeys = useMemo(() => confidenceMatchDefinitionIds(scopeDocument), [scopeDocument]);
  const seqKeys = useMemo(() => sequenceKeys(scopeDocument), [scopeDocument]);

  const rows = useMemo(() => parseRows(value.validation_rules), [value.validation_rules]);

  const setRows = useCallback(
    (nextRows: RefRow[]) => {
      const next: JsonObject = { ...value, validation_rules: serializeRows(nextRows) as unknown[] };
      onChange(next);
    },
    [value, onChange]
  );

  return (
    <div className="kea-match-validation-refs">
      <p className="kea-hint" style={{ marginTop: "0" }}>
        {t("validationEditor.validationRulesHierarchyHint")}
      </p>

      <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "1rem" }}>
        {t("validationEditor.validationRulesSteps")}
      </h4>
      <p className="kea-hint">{t("validationEditor.validationRulesStepsHint")}</p>
      <p className="kea-hint" style={{ marginTop: "0.25rem" }}>
        {t("rulesEntity.dragReorderRules")}
      </p>

      <RefRowsEditor rows={rows} setRows={setRows} depth={0} defKeys={defKeys} seqKeys={seqKeys} />

      <button
        type="button"
        className="kea-btn kea-btn--sm"
        style={{ marginTop: "0.25rem" }}
        onClick={() => {
          setRows([...rows, defaultLeafRow(defKeys, seqKeys)]);
        }}
      >
        {t("validationEditor.addMatchStep")}
      </button>
    </div>
  );
}
