import type { MessageKey } from "../../i18n/types";
import type { JsonObject } from "../../types/jsonConfig";
import { commaJoinSegments, splitCommaSegments } from "../../utils/commaDelimited";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

export const FILTER_OPERATORS = [
  "EQUALS",
  "IN",
  "EXISTS",
  "CONTAINSALL",
  "CONTAINSANY",
  "SEARCH",
  "PREFIX",
  "GT",
  "GTE",
  "LT",
  "LTE",
  "RANGE",
] as const;

const FILTER_OPERATOR_SET = new Set<string>(FILTER_OPERATORS);
const FILTER_COMPARISON_OPERATORS = new Set<string>(["GT", "GTE", "LT", "LTE"]);

const OPERATOR_ALIASES: Record<string, string> = {
  ">": "GT",
  ">=": "GTE",
  GE: "GTE",
  "<": "LT",
  "<=": "LTE",
  LE: "LTE",
};

export function normalizeFilterOperator(raw: unknown): string {
  const s = String(raw ?? "EQUALS").trim();
  if (s in OPERATOR_ALIASES) return OPERATOR_ALIASES[s]!;
  const up = s.toUpperCase();
  if (up === "CONTAINS_ALL") return "CONTAINSALL";
  if (up === "CONTAINS_ANY") return "CONTAINSANY";
  return up || "EQUALS";
}

function filterOperatorNeedsValues(operator: string): boolean {
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

export function emptyAnd(): JsonObject {
  return { and: [emptyLeaf()] };
}

export function emptyOr(): JsonObject {
  return { or: [emptyLeaf()] };
}

export function emptyNot(): JsonObject {
  return { not: emptyLeaf() };
}

function isGroupNode(n: JsonObject): "and" | "or" | "not" | null {
  if (Array.isArray(n.and)) return "and";
  if (Array.isArray(n.or)) return "or";
  if (n.not !== undefined && n.not !== null) return "not";
  return null;
}

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

function numToText(n: unknown): string {
  return n != null && n !== "" ? String(n) : "";
}

type NodeProps = {
  t: TFn;
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  onRemove?: () => void;
  depth?: number;
};

export function ViewQueryFilterNodeEditor({ t, value, onChange, onRemove, depth = 0 }: NodeProps) {
  const g = isGroupNode(value);

  if (g === "and") {
    const arr = (Array.isArray(value.and) ? value.and : []) as JsonObject[];
    return (
      <div className="idx-config-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="idx-config-toolbar">
          <span className="idx-config-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("config.indexFields.filters.groupAnd")}
          </span>
          {onRemove ? (
            <button type="button" className="idx-btn idx-btn--sm" onClick={onRemove}>
              {t("config.indexFields.filters.removeNode")}
            </button>
          ) : null}
        </div>
        {arr.map((child, i) => (
          <ViewQueryFilterNodeEditor
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
          className="idx-btn idx-btn--sm"
          style={{ marginTop: 6 }}
          onClick={() => onChange({ and: [...arr, emptyLeaf()] })}
        >
          {t("config.indexFields.filters.addToGroup")}
        </button>
      </div>
    );
  }

  if (g === "or") {
    const arr = (Array.isArray(value.or) ? value.or : []) as JsonObject[];
    return (
      <div className="idx-config-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="idx-config-toolbar">
          <span className="idx-config-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("config.indexFields.filters.groupOr")}
          </span>
          {onRemove ? (
            <button type="button" className="idx-btn idx-btn--sm" onClick={onRemove}>
              {t("config.indexFields.filters.removeNode")}
            </button>
          ) : null}
        </div>
        {arr.map((child, i) => (
          <ViewQueryFilterNodeEditor
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
          className="idx-btn idx-btn--sm"
          style={{ marginTop: 6 }}
          onClick={() => onChange({ or: [...arr, emptyLeaf()] })}
        >
          {t("config.indexFields.filters.addToGroup")}
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
      <div className="idx-config-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="idx-config-toolbar">
          <span className="idx-config-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("config.indexFields.filters.groupNot")}
          </span>
          {onRemove ? (
            <button type="button" className="idx-btn idx-btn--sm" onClick={onRemove}>
              {t("config.indexFields.filters.removeNode")}
            </button>
          ) : null}
        </div>
        <ViewQueryFilterNodeEditor
          t={t}
          depth={depth + 1}
          value={inner}
          onChange={(next) => onChange({ not: next })}
        />
      </div>
    );
  }

  const row = value;
  const op = normalizeFilterOperator(row.operator);
  const isRange = op === "RANGE";
  const isComparison = FILTER_COMPARISON_OPERATORS.has(op);

  return (
    <div className="idx-config-filter-row">
      <label className="idx-label">
        <span className="idx-label__caption">{t("config.indexFields.filters.operator")}</span>
        <select
          className="idx-select"
          value={op}
          onChange={(e) => {
            const nxt = normalizeFilterOperator(e.target.value);
            const { gt: _gt, gte: _gte, lt: _lt, lte: _lte, ...rowRest } = row;
            const patch: JsonObject = { ...rowRest, operator: nxt };
            if (nxt === "EXISTS") patch.values = [];
            if (nxt === "RANGE") {
              patch.gt = undefined;
              patch.gte = undefined;
              patch.lt = undefined;
              patch.lte = undefined;
            }
            onChange(patch);
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
      <label className="idx-label">
        <span className="idx-label__caption">{t("config.indexFields.filters.targetProperty")}</span>
        <input
          className="idx-input idx-input--mono"
          value={String(row.target_property ?? "")}
          onChange={(e) => onChange({ ...row, target_property: e.target.value })}
        />
      </label>
      <label className="idx-label">
        <span className="idx-label__caption">{t("config.indexFields.filters.propertyScope")}</span>
        <input
          className="idx-input idx-input--mono"
          value={String(row.property_scope ?? "view")}
          onChange={(e) => onChange({ ...row, property_scope: e.target.value })}
        />
      </label>
      <label className="idx-checkbox-label idx-config-grid__full">
        <input
          type="checkbox"
          checked={Boolean(row.negate)}
          onChange={(e) => onChange({ ...row, negate: e.target.checked })}
        />
        {t("config.indexFields.filters.negate")}
      </label>
      {isRange ? (
        <div className="idx-config-grid idx-config-grid__full">
          <label className="idx-label">
            <span className="idx-label__caption">{t("config.indexFields.filters.rangeGt")}</span>
            <input
              className="idx-input"
              type="number"
              value={numToText(row.gt)}
              onChange={(e) => onChange({ ...row, gt: numOrUndef(e.target.value) })}
            />
          </label>
          <label className="idx-label">
            <span className="idx-label__caption">{t("config.indexFields.filters.rangeGte")}</span>
            <input
              className="idx-input"
              type="number"
              value={numToText(row.gte)}
              onChange={(e) => onChange({ ...row, gte: numOrUndef(e.target.value) })}
            />
          </label>
          <label className="idx-label">
            <span className="idx-label__caption">{t("config.indexFields.filters.rangeLt")}</span>
            <input
              className="idx-input"
              type="number"
              value={numToText(row.lt)}
              onChange={(e) => onChange({ ...row, lt: numOrUndef(e.target.value) })}
            />
          </label>
          <label className="idx-label">
            <span className="idx-label__caption">{t("config.indexFields.filters.rangeLte")}</span>
            <input
              className="idx-input"
              type="number"
              value={numToText(row.lte)}
              onChange={(e) => onChange({ ...row, lte: numOrUndef(e.target.value) })}
            />
          </label>
        </div>
      ) : filterOperatorNeedsValues(op) ? (
        <label className="idx-label idx-config-grid__full">
          <span className="idx-label__caption">
            {isComparison
              ? t("config.indexFields.filters.comparisonValue")
              : t("config.indexFields.filters.values")}
          </span>
          <input
            className="idx-input idx-input--mono"
            type={isComparison ? "number" : "text"}
            value={valuesToText(row.values)}
            onChange={(e) => {
              const raw = e.target.value;
              let parsed: unknown;
              if (isComparison) {
                const trimmed = raw.trim();
                if (trimmed === "") parsed = [];
                else {
                  const n = Number(trimmed);
                  parsed = Number.isNaN(n) ? textToValues(raw) : n;
                }
              } else {
                parsed = textToValues(raw);
              }
              onChange({ ...row, values: parsed });
            }}
          />
        </label>
      ) : (
        <p className="idx-config-hint idx-config-grid__full">{t("config.indexFields.filters.existsNoValues")}</p>
      )}
      {onRemove ? (
        <button type="button" className="idx-btn idx-btn--sm idx-btn--danger idx-config-grid__full" onClick={onRemove}>
          {t("config.indexFields.filters.removeNode")}
        </button>
      ) : null}
    </div>
  );
}
