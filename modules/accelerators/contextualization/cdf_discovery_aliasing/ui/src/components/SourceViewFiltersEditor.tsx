import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import { commaJoinSegments, splitCommaSegments } from "../utils/commaDelimited";
import { DeferredCommitInput } from "./DeferredCommitTextField";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

/** Operators supported by discovery view query / DM instances.list filters. */
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

export const FILTER_COMPARISON_OPERATORS = new Set<string>(["GT", "GTE", "LT", "LTE"]);

const FILTER_OPERATOR_SET = new Set<string>(FILTER_OPERATORS);

/** Same aliases as ``source_view_filter_build.normalize_filter_operator``. */
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
  /** Stable prefix for deferred-input sync keys (e.g. node id + filter index). */
  syncKeyPrefix?: string;
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

function numToText(n: unknown): string {
  return n != null && n !== "" ? String(n) : "";
}

export function SourceViewFilterNodeEditor({
  t,
  value,
  onChange,
  onRemove,
  depth = 0,
  syncKeyPrefix = "svf",
}: Props) {
  const g = isGroupNode(value);

  if (g === "and") {
    const arr = (Array.isArray(value.and) ? value.and : []) as JsonObject[];
    return (
      <div className="discovery-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="discovery-toolbar-inline" style={{ marginBottom: 6 }}>
          <span className="discovery-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("sourceViews.filterGroupAnd")}
          </span>
          {onRemove ? (
            <button type="button" className="discovery-btn discovery-btn--ghost discovery-btn--sm" onClick={onRemove}>
              {t("sourceViews.filterRemoveNode")}
            </button>
          ) : null}
        </div>
        {arr.map((child, i) => {
          const childRow =
            child && typeof child === "object" && !Array.isArray(child) ? child : emptyLeaf();
          const childPrefix = `${syncKeyPrefix}-and-${depth}-${i}`;
          return (
            <SourceViewFilterNodeEditor
              key={`and-${depth}-${i}`}
              t={t}
              syncKeyPrefix={childPrefix}
              depth={depth + 1}
              value={childRow}
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
          );
        })}
        <button
          type="button"
          className="discovery-btn discovery-btn--sm"
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
      <div className="discovery-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="discovery-toolbar-inline" style={{ marginBottom: 6 }}>
          <span className="discovery-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("sourceViews.filterGroupOr")}
          </span>
          {onRemove ? (
            <button type="button" className="discovery-btn discovery-btn--ghost discovery-btn--sm" onClick={onRemove}>
              {t("sourceViews.filterRemoveNode")}
            </button>
          ) : null}
        </div>
        {arr.map((child, i) => {
          const childRow =
            child && typeof child === "object" && !Array.isArray(child) ? child : emptyLeaf();
          const childPrefix = `${syncKeyPrefix}-or-${depth}-${i}`;
          return (
            <SourceViewFilterNodeEditor
              key={`or-${depth}-${i}`}
              t={t}
              syncKeyPrefix={childPrefix}
              depth={depth + 1}
              value={childRow}
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
          );
        })}
        <button
          type="button"
          className="discovery-btn discovery-btn--sm"
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
      <div className="discovery-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="discovery-toolbar-inline" style={{ marginBottom: 6 }}>
          <span className="discovery-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("sourceViews.filterGroupNot")}
          </span>
          {onRemove ? (
            <button type="button" className="discovery-btn discovery-btn--ghost discovery-btn--sm" onClick={onRemove}>
              {t("sourceViews.filterRemoveNode")}
            </button>
          ) : null}
        </div>
        <SourceViewFilterNodeEditor
          t={t}
          syncKeyPrefix={`${syncKeyPrefix}-not-${depth}`}
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
  const isComparison = FILTER_COMPARISON_OPERATORS.has(op);
  const leafKey = `${syncKeyPrefix}-leaf-${depth}`;

  return (
    <div className="discovery-filter-row discovery-filter-row--source-leaf">
      <label className="discovery-label">
        {t("sourceViews.filterOperator")}
        <select
          className="discovery-input discovery-select"
          value={op}
          onChange={(e) => {
            const nxt = normalizeFilterOperator(e.target.value);
            const { gt: _gt, gte: _gte, lt: _lt, lte: _lte, ...rowRest } = row;
            const patch: JsonObject = { ...rowRest, operator: nxt };
            if (nxt === "EXISTS") {
              patch.values = [];
            } else if (nxt === "RANGE") {
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
      <label className="discovery-label">
        {t("sourceViews.filterTargetProperty")}
        <DeferredCommitInput
          className="discovery-input"
          committedValue={String(row.target_property ?? "")}
          syncKey={`${leafKey}-target-${op}`}
          onCommit={(v) => onChange({ ...row, target_property: v })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="discovery-label">
        {t("sourceViews.filterPropertyScope")}
        <DeferredCommitInput
          className="discovery-input"
          committedValue={String(row.property_scope ?? "view")}
          syncKey={`${leafKey}-scope-${op}`}
          onCommit={(v) => onChange({ ...row, property_scope: v })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="discovery-label discovery-label--block" style={{ gridColumn: "1 / -1", display: "flex", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked={Boolean(row.negate)}
          onChange={(e) => onChange({ ...row, negate: e.target.checked })}
        />
        {t("sourceViews.filterNegate")}
      </label>
      <div key={`filter-tpl-${op}`} style={{ display: "contents" }}>
        {isRange ? (
          <div className="discovery-filter-row--range-bounds" style={{ gridColumn: "1 / -1" }}>
            <label className="discovery-label">
              {t("sourceViews.filterRangeGt")}
              <DeferredCommitInput
                className="discovery-input"
                type="number"
                committedValue={numToText(row.gt)}
                syncKey={`${leafKey}-gt`}
                onCommit={(v) => onChange({ ...row, gt: numOrUndef(v) })}
              />
            </label>
            <label className="discovery-label">
              {t("sourceViews.filterRangeGte")}
              <DeferredCommitInput
                className="discovery-input"
                type="number"
                committedValue={numToText(row.gte)}
                syncKey={`${leafKey}-gte`}
                onCommit={(v) => onChange({ ...row, gte: numOrUndef(v) })}
              />
            </label>
            <label className="discovery-label">
              {t("sourceViews.filterRangeLt")}
              <DeferredCommitInput
                className="discovery-input"
                type="number"
                committedValue={numToText(row.lt)}
                syncKey={`${leafKey}-lt`}
                onCommit={(v) => onChange({ ...row, lt: numOrUndef(v) })}
              />
            </label>
            <label className="discovery-label">
              {t("sourceViews.filterRangeLte")}
              <DeferredCommitInput
                className="discovery-input"
                type="number"
                committedValue={numToText(row.lte)}
                syncKey={`${leafKey}-lte`}
                onCommit={(v) => onChange({ ...row, lte: numOrUndef(v) })}
              />
            </label>
          </div>
        ) : filterOperatorNeedsValues(op) ? (
          <label className="discovery-label discovery-label--block" style={{ gridColumn: "1 / -1" }}>
            {isComparison ? t("sourceViews.filterComparisonValue") : t("sourceViews.filterValues")}
            <DeferredCommitInput
              type={isComparison ? "number" : "text"}
              className="discovery-input"
              committedValue={valuesToText(row.values)}
              syncKey={`${leafKey}-values-${op}`}
              onCommit={(raw) => {
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
              spellCheck={false}
              autoComplete="off"
            />
          </label>
        ) : (
          <p className="discovery-hint" style={{ gridColumn: "1 / -1", margin: 0 }}>
            {t("sourceViews.filterExistsNoValues")}
          </p>
        )}
      </div>
      {onRemove ? (
        <button
          type="button"
          className="discovery-btn discovery-btn--ghost discovery-btn--sm"
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
