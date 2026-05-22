import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

/** Operators supported by ``fn_dm_join.engine.join_on_eval.eval_join_on``. */
export const JOIN_PREDICATE_OPERATORS = [
  "EQUALS",
  "IEQUALS",
  "STARTS_WITH",
  "ENDS_WITH",
  "CONTAINS",
] as const;

const JOIN_OP_SET = new Set<string>(JOIN_PREDICATE_OPERATORS);

export function normalizeJoinOperator(raw: unknown): string {
  const s = String(raw ?? "EQUALS").trim().toUpperCase();
  if (!s) return "EQUALS";
  if (s === "EQUALS_IGNORE_CASE") return "IEQUALS";
  return s;
}

export function emptyJoinLeaf(): JsonObject {
  return {
    operator: "EQUALS",
    left_property: "",
    right_property: "",
  };
}

export function emptyJoinAnd(): JsonObject {
  return { and: [emptyJoinLeaf()] };
}

export function emptyJoinOr(): JsonObject {
  return { or: [emptyJoinLeaf()] };
}

export function emptyJoinNot(): JsonObject {
  return { not: emptyJoinLeaf() };
}

export function defaultJoinOnRoot(): JsonObject {
  return {
    and: [
      {
        operator: "IEQUALS",
        left_property: "name",
        right_property: "raw_columns.name",
      },
    ],
  };
}

function isJoinGroupNode(n: JsonObject): "and" | "or" | "not" | null {
  if (Array.isArray(n.and)) return "and";
  if (Array.isArray(n.or)) return "or";
  if (n.not !== undefined && n.not !== null) return "not";
  return null;
}

/** True when ``join_on`` can be edited with the structured tree (no unknown top-level shape). */
export function joinOnStructuredEditable(raw: unknown): boolean {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return false;
  const n = raw as JsonObject;
  const g = isJoinGroupNode(n);
  if (g) {
    const keys = Object.keys(n).filter((k) => n[k] !== undefined);
    const allowed = g === "not" ? ["not"] : [g];
    if (keys.some((k) => !allowed.includes(k as "and" | "or" | "not"))) return false;
    if (g === "and" || g === "or") {
      const ch = n[g];
      if (!Array.isArray(ch) || ch.length === 0) return false;
    }
    return subtreeStructured(n);
  }
  return isJoinLeafNode(n);
}

function isJoinLeafNode(n: JsonObject): boolean {
  const keys = Object.keys(n);
  const allowedLeaf = new Set(["operator", "left_property", "right_property"]);
  if (keys.some((k) => !allowedLeaf.has(k))) return false;
  return true;
}

function subtreeStructured(n: JsonObject): boolean {
  const g = isJoinGroupNode(n);
  if (!g) return isJoinLeafNode(n);
  if (g === "not") {
    const inner = n.not;
    if (!inner || typeof inner !== "object" || Array.isArray(inner)) return false;
    return subtreeStructured(inner as JsonObject);
  }
  const arr = (n[g] as unknown[]) || [];
  for (const ch of arr) {
    if (!ch || typeof ch !== "object" || Array.isArray(ch)) return false;
    if (!subtreeStructured(ch as JsonObject)) return false;
  }
  return true;
}

type Props = {
  t: TFn;
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  onRemove?: () => void;
  depth?: number;
};

export function JoinOnPredicateEditor({ t, value, onChange, onRemove, depth = 0 }: Props) {
  const g = isJoinGroupNode(value);

  if (g === "and") {
    const arr = (Array.isArray(value.and) ? value.and : []) as JsonObject[];
    const safe = arr.length ? arr : [emptyJoinLeaf()];
    return (
      <div className="discovery-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="discovery-toolbar-inline" style={{ marginBottom: 6 }}>
          <span className="discovery-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("joinEditor.groupAnd")}
          </span>
          {onRemove ? (
            <button type="button" className="discovery-btn discovery-btn--ghost discovery-btn--sm" onClick={onRemove}>
              {t("sourceViews.filterRemoveNode")}
            </button>
          ) : null}
        </div>
        {safe.map((child, i) => (
          <JoinOnPredicateEditor
            key={`join-and-${depth}-${i}`}
            t={t}
            depth={depth + 1}
            value={child && typeof child === "object" && !Array.isArray(child) ? child : emptyJoinLeaf()}
            onChange={(next) => {
              const nextArr = [...safe];
              nextArr[i] = next;
              onChange({ and: nextArr });
            }}
            onRemove={() => {
              const nextArr = safe.filter((_, j) => j !== i);
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
          className="discovery-btn discovery-btn--sm"
          style={{ marginTop: 6 }}
          onClick={() => onChange({ and: [...safe, emptyJoinLeaf()] })}
        >
          {t("joinEditor.addToGroup")}
        </button>
      </div>
    );
  }

  if (g === "or") {
    const arr = (Array.isArray(value.or) ? value.or : []) as JsonObject[];
    const safe = arr.length ? arr : [emptyJoinLeaf()];
    return (
      <div className="discovery-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="discovery-toolbar-inline" style={{ marginBottom: 6 }}>
          <span className="discovery-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("joinEditor.groupOr")}
          </span>
          {onRemove ? (
            <button type="button" className="discovery-btn discovery-btn--ghost discovery-btn--sm" onClick={onRemove}>
              {t("sourceViews.filterRemoveNode")}
            </button>
          ) : null}
        </div>
        {safe.map((child, i) => (
          <JoinOnPredicateEditor
            key={`join-or-${depth}-${i}`}
            t={t}
            depth={depth + 1}
            value={child && typeof child === "object" && !Array.isArray(child) ? child : emptyJoinLeaf()}
            onChange={(next) => {
              const nextArr = [...safe];
              nextArr[i] = next;
              onChange({ or: nextArr });
            }}
            onRemove={() => {
              const nextArr = safe.filter((_, j) => j !== i);
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
          className="discovery-btn discovery-btn--sm"
          style={{ marginTop: 6 }}
          onClick={() => onChange({ or: [...safe, emptyJoinLeaf()] })}
        >
          {t("joinEditor.addToGroup")}
        </button>
      </div>
    );
  }

  if (g === "not") {
    const inner =
      value.not && typeof value.not === "object" && !Array.isArray(value.not)
        ? (value.not as JsonObject)
        : emptyJoinLeaf();
    return (
      <div className="discovery-filter-group" style={{ marginLeft: depth ? 12 : 0 }}>
        <div className="discovery-toolbar-inline" style={{ marginBottom: 6 }}>
          <span className="discovery-hint" style={{ margin: 0, fontWeight: 600 }}>
            {t("joinEditor.groupNot")}
          </span>
          {onRemove ? (
            <button type="button" className="discovery-btn discovery-btn--ghost discovery-btn--sm" onClick={onRemove}>
              {t("sourceViews.filterRemoveNode")}
            </button>
          ) : null}
        </div>
        <JoinOnPredicateEditor t={t} depth={depth + 1} value={inner} onChange={(next) => onChange({ not: next })} />
      </div>
    );
  }

  const row = value;
  const op = normalizeJoinOperator(row.operator);

  return (
    <div className="discovery-filter-row discovery-filter-row--join-leaf">
      <label className="discovery-label">
        {t("joinEditor.operator")}
        <select
          className="discovery-input"
          value={op}
          onChange={(e) => onChange({ ...row, operator: e.target.value })}
        >
          {!JOIN_OP_SET.has(op) && op ? <option value={op}>{op}</option> : null}
          {JOIN_PREDICATE_OPERATORS.map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      </label>
      <label className="discovery-label">
        {t("joinEditor.leftProperty")}
        <input
          className="discovery-input"
          value={String(row.left_property ?? "")}
          onChange={(e) => onChange({ ...row, left_property: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="discovery-label">
        {t("joinEditor.rightProperty")}
        <input
          className="discovery-input"
          value={String(row.right_property ?? "")}
          onChange={(e) => onChange({ ...row, right_property: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
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
