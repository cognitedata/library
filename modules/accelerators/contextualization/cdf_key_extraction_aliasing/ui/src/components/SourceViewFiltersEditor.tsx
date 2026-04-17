import { useEffect, useState } from "react";
import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import { commaJoinSegments, splitCommaSegments } from "../utils/commaDelimited";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

/** Operators supported by fn_dm_key_extraction / DM instances.list filters. */
export const FILTER_OPERATORS = [
  "EQUALS",
  "IN",
  "EXISTS",
  "CONTAINSALL",
  "CONTAINSANY",
  "SEARCH",
  "PREFIX",
  "RANGE",
] as const;

const FILTER_OPERATOR_SET = new Set<string>(FILTER_OPERATORS);

export function normalizeFilterOperator(raw: unknown): string {
  const s = String(raw ?? "EQUALS").trim().toUpperCase();
  return s || "EQUALS";
}

export function filterOperatorNeedsValues(operator: string): boolean {
  return normalizeFilterOperator(operator) !== "EXISTS";
}

export function emptyLeaf(): JsonObject {
  return {
    operator: "EQUALS",
    target_property: "",
    property_scope: "view",
    values: [],
  };
}

function emptyAnd(): JsonObject {
  return { and: [emptyLeaf()] };
}

function emptyOr(): JsonObject {
  return { or: [emptyLeaf()] };
}

function emptyNot(): JsonObject {
  return { not: emptyLeaf() };
}

function isGroupNode(n: JsonObject): "and" | "or" | "not" | null {
  if (Array.isArray(n.and)) return "and";
  if (Array.isArray(n.or)) return "or";
  if (n.not !== undefined && n.not !== null) return "not";
  return null;
}

type Props = {
  t: TFn;
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  onRemove?: () => void;
  depth?: number;
};

function valuesToText(vals: unknown): string {
  if (vals == null) return "";
  if (Array.isArray(vals)) return commaJoinSegments(vals.map((x) => String(x)));
  return String(vals);
}

function textToValues(s: string): unknown {
  const parts = splitCommaSegments(s);
  if (parts.length === 0) return [];
  if (parts.length === 1) {
    const p = parts[0];
    const n = Number(p);
    if (!Number.isNaN(n) && p === String(n)) return n;
    if (p === "true") return true;
    if (p === "false") return false;
    return p;
  }
  return parts;
}

function numOrUndef(s: string): number | undefined {
  const t = s.trim();
  if (t === "") return undefined;
  const n = Number(t);
  return Number.isNaN(n) ? undefined : n;
}

export function SourceViewFilterNodeEditor({ t, value, onChange, onRemove, depth = 0 }: Props) {
  const g = isGroupNode(value);
  const leafValuesSig = !g ? JSON.stringify((value as JsonObject).values ?? null) : "";
  const [valuesDraft, setValuesDraft] = useState(() => {
    if (isGroupNode(value)) return "";
    return valuesToText((value as JsonObject).values);
  });
  useEffect(() => {
    if (g) return;
    setValuesDraft(valuesToText((value as JsonObject).values));
  }, [g, leafValuesSig]);

  if (g === "and") {
    const arr = (Array.isArray(value.and) ? value.and : []) as JsonObject[];
    return (
      <div className="kea-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="kea-toolbar-inline" style={{ marginBottom: 6 }}>
          <span className="kea-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("sourceViews.filterGroupAnd")}
          </span>
          {onRemove ? (
            <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={onRemove}>
              {t("sourceViews.filterRemoveNode")}
            </button>
          ) : null}
        </div>
        {arr.map((child, i) => (
          <SourceViewFilterNodeEditor
            key={`and-${depth}-${i}`}
            t={t}
            depth={depth + 1}
            value={child && typeof child === "object" && !Array.isArray(child) ? child : emptyLeaf()}
            onChange={(next) => {
              const nextArr = [...arr];
              nextArr[i] = next;
              onChange({ and: nextArr });
            }}
            onRemove={() => {
              const nextArr = arr.filter((_, j) => j !== i);
              if (nextArr.length === 0) {
                onRemove?.();
                return;
              }
              onChange({ and: nextArr });
            }}
          />
        ))}
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          style={{ marginTop: 6 }}
          onClick={() => onChange({ and: [...arr, emptyLeaf()] })}
        >
          {t("sourceViews.filterAddToGroup")}
        </button>
      </div>
    );
  }

  if (g === "or") {
    const arr = (Array.isArray(value.or) ? value.or : []) as JsonObject[];
    return (
      <div className="kea-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="kea-toolbar-inline" style={{ marginBottom: 6 }}>
          <span className="kea-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("sourceViews.filterGroupOr")}
          </span>
          {onRemove ? (
            <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={onRemove}>
              {t("sourceViews.filterRemoveNode")}
            </button>
          ) : null}
        </div>
        {arr.map((child, i) => (
          <SourceViewFilterNodeEditor
            key={`or-${depth}-${i}`}
            t={t}
            depth={depth + 1}
            value={child && typeof child === "object" && !Array.isArray(child) ? child : emptyLeaf()}
            onChange={(next) => {
              const nextArr = [...arr];
              nextArr[i] = next;
              onChange({ or: nextArr });
            }}
            onRemove={() => {
              const nextArr = arr.filter((_, j) => j !== i);
              if (nextArr.length === 0) {
                onRemove?.();
                return;
              }
              onChange({ or: nextArr });
            }}
          />
        ))}
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          style={{ marginTop: 6 }}
          onClick={() => onChange({ or: [...arr, emptyLeaf()] })}
        >
          {t("sourceViews.filterAddToGroup")}
        </button>
      </div>
    );
  }

  if (g === "not") {
    const inner =
      value.not && typeof value.not === "object" && !Array.isArray(value.not)
        ? (value.not as JsonObject)
        : emptyLeaf();
    return (
      <div className="kea-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="kea-toolbar-inline" style={{ marginBottom: 6 }}>
          <span className="kea-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("sourceViews.filterGroupNot")}
          </span>
          {onRemove ? (
            <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={onRemove}>
              {t("sourceViews.filterRemoveNode")}
            </button>
          ) : null}
        </div>
        <SourceViewFilterNodeEditor
          t={t}
          depth={depth + 1}
          value={inner}
          onChange={(next) => onChange({ not: next })}
        />
      </div>
    );
  }

  /* leaf */
  const row = value;
  const op = normalizeFilterOperator(row.operator);
  const isRange = op === "RANGE";

  return (
    <div className="kea-filter-row">
      <label className="kea-label">
        {t("sourceViews.filterOperator")}
        <select
          className="kea-input"
          value={op}
          onChange={(e) => {
            const next = e.target.value;
            const patch: JsonObject = { operator: next };
            if (normalizeFilterOperator(next) === "EXISTS") {
              patch.values = [];
            }
            if (normalizeFilterOperator(next) === "RANGE") {
              patch.gt = undefined;
              patch.gte = undefined;
              patch.lt = undefined;
              patch.lte = undefined;
            }
            onChange({ ...row, ...patch });
          }}
        >
          {!FILTER_OPERATOR_SET.has(op) && op ? <option value={op}>{op}</option> : null}
          {FILTER_OPERATORS.map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      </label>
      <label className="kea-label">
        {t("sourceViews.filterTargetProperty")}
        <input
          className="kea-input"
          value={String(row.target_property ?? "")}
          onChange={(e) => onChange({ ...row, target_property: e.target.value })}
        />
      </label>
      <label className="kea-label">
        {t("sourceViews.filterPropertyScope")}
        <input
          className="kea-input"
          value={String(row.property_scope ?? "view")}
          onChange={(e) => onChange({ ...row, property_scope: e.target.value })}
        />
      </label>
      <label className="kea-label kea-label--block" style={{ gridColumn: "1 / -1", display: "flex", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked={Boolean(row.negate)}
          onChange={(e) => onChange({ ...row, negate: e.target.checked })}
        />
        {t("sourceViews.filterNegate")}
      </label>
      {isRange ? (
        <div style={{ gridColumn: "1 / -1", display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "0.5rem" }}>
          <label className="kea-label">
            {t("sourceViews.filterRangeGt")}
            <input
              className="kea-input"
              type="number"
              value={row.gt != null ? String(row.gt) : ""}
              onChange={(e) => onChange({ ...row, gt: numOrUndef(e.target.value) })}
            />
          </label>
          <label className="kea-label">
            {t("sourceViews.filterRangeGte")}
            <input
              className="kea-input"
              type="number"
              value={row.gte != null ? String(row.gte) : ""}
              onChange={(e) => onChange({ ...row, gte: numOrUndef(e.target.value) })}
            />
          </label>
          <label className="kea-label">
            {t("sourceViews.filterRangeLt")}
            <input
              className="kea-input"
              type="number"
              value={row.lt != null ? String(row.lt) : ""}
              onChange={(e) => onChange({ ...row, lt: numOrUndef(e.target.value) })}
            />
          </label>
          <label className="kea-label">
            {t("sourceViews.filterRangeLte")}
            <input
              className="kea-input"
              type="number"
              value={row.lte != null ? String(row.lte) : ""}
              onChange={(e) => onChange({ ...row, lte: numOrUndef(e.target.value) })}
            />
          </label>
        </div>
      ) : filterOperatorNeedsValues(op) ? (
        <label className="kea-label kea-label--block" style={{ gridColumn: "1 / -1" }}>
          {t("sourceViews.filterValues")}
          <input
            type="text"
            className="kea-input"
            value={valuesDraft}
            onChange={(e) => setValuesDraft(e.target.value)}
            onBlur={() => onChange({ ...row, values: textToValues(valuesDraft) })}
            spellCheck={false}
            autoComplete="off"
          />
        </label>
      ) : (
        <p className="kea-hint" style={{ gridColumn: "1 / -1", margin: 0 }}>
          {t("sourceViews.filterExistsNoValues")}
        </p>
      )}
      {onRemove ? (
        <button
          type="button"
          className="kea-btn kea-btn--ghost kea-btn--sm"
          style={{ gridColumn: "1 / -1" }}
          onClick={onRemove}
        >
          {t("sourceViews.filterRemoveNode")}
        </button>
      ) : null}
    </div>
  );
}

export { emptyAnd, emptyOr, emptyNot };
