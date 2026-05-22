import { useEffect, useState } from "react";
import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import {
  JoinOnPredicateEditor,
  defaultJoinOnRoot,
  emptyJoinAnd,
  emptyJoinLeaf,
  emptyJoinNot,
  emptyJoinOr,
  joinOnStructuredEditable,
} from "./JoinOnPredicateEditor";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

function joinRootGroupKind(n: JsonObject): "and" | "or" | "not" | null {
  if (Array.isArray(n.and)) return "and";
  if (Array.isArray(n.or)) return "or";
  if (n.not !== undefined && n.not !== null) return "not";
  return null;
}

/** Append a leaf (or subtree) to the root ``and`` / ``or`` list, or wrap a bare root in ``and``. */
function appendJoinPredicate(root: JsonObject, leaf: JsonObject): JsonObject {
  const g = joinRootGroupKind(root);
  if (g === "and") {
    const arr = (Array.isArray(root.and) ? root.and : []) as JsonObject[];
    return { and: [...arr, leaf] };
  }
  if (g === "or") {
    const arr = (Array.isArray(root.or) ? root.or : []) as JsonObject[];
    return { or: [...arr, leaf] };
  }
  if (g === "not") {
    return { and: [root, leaf] };
  }
  return { and: [root, leaf] };
}

export function JoinNodeConfigFields({ t, value, onChange }: Props) {
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const joinType = String(value.join_type ?? "inner").trim().toLowerCase();

  const joinOnRawObj =
    value.join_on && typeof value.join_on === "object" && !Array.isArray(value.join_on)
      ? (value.join_on as JsonObject)
      : defaultJoinOnRoot();

  const structuredOk = joinOnStructuredEditable(joinOnRawObj);
  const canonical = JSON.stringify(joinOnRawObj, null, 2);
  const [joinOnRaw, setJoinOnRaw] = useState(canonical);
  const [jsonOverride, setJsonOverride] = useState(!structuredOk);

  useEffect(() => {
    setJoinOnRaw(canonical);
  }, [canonical]);

  useEffect(() => {
    if (!structuredOk) setJsonOverride(true);
  }, [structuredOk]);

  const showStructured = structuredOk && !jsonOverride;

  const setJoinOn = (next: JsonObject) => patch({ join_on: next });

  return (
    <div className="discovery-loc-fields">
      <label className="discovery-label discovery-label--block">
        Description
        <input
          className="discovery-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.description ?? "")}
          onChange={(e) => patch({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="discovery-label discovery-label--block">
        Join type
        <select
          className="discovery-select"
          style={{ marginTop: "0.35rem" }}
          value={joinType === "left" ? "left" : "inner"}
          onChange={(e) => patch({ join_type: e.target.value })}
        >
          <option value="inner">inner</option>
          <option value="left">left</option>
        </select>
      </label>
      <label className="discovery-label discovery-label--block">
        Right property prefix (optional)
        <input
          className="discovery-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.right_prefix ?? "")}
          onChange={(e) => patch({ right_prefix: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>

      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem", marginTop: "0.85rem" }}>
        {t("joinEditor.title")}
      </h4>
      <p className="discovery-hint" style={{ marginTop: 0, marginBottom: "0.65rem", maxWidth: "56rem" }}>
        {t("joinEditor.hint")}
      </p>

      {structuredOk ? (
        <div style={{ marginBottom: "0.75rem" }}>
          <p className="discovery-hint" style={{ margin: "0 0 0.35rem", fontWeight: 600 }}>
            {t("joinEditor.presetsTitle")}
          </p>
          <p className="discovery-hint" style={{ marginTop: 0, marginBottom: "0.45rem", maxWidth: "56rem" }}>
            {t("joinEditor.presetHint")}
          </p>
          <div className="discovery-toolbar-inline" style={{ flexWrap: "wrap", gap: 8 }}>
            <button
              type="button"
              className="discovery-btn discovery-btn--sm"
              onClick={() => {
                const leaf: JsonObject = {
                  operator: "IEQUALS",
                  left_property: "name",
                  right_property: "raw_columns.name",
                };
                setJoinOn(appendJoinPredicate(joinOnRawObj, leaf));
                setJsonOverride(false);
              }}
            >
              {t("joinEditor.presetNameRawName")}
            </button>
            <button
              type="button"
              className="discovery-btn discovery-btn--sm"
              onClick={() => {
                const leaf: JsonObject = {
                  operator: "IEQUALS",
                  left_property: "name",
                  right_property: "raw_columns.aliases",
                };
                setJoinOn(appendJoinPredicate(joinOnRawObj, leaf));
                setJsonOverride(false);
              }}
            >
              {t("joinEditor.presetNameRawAliases")}
            </button>
            <button
              type="button"
              className="discovery-btn discovery-btn--sm"
              onClick={() => {
                const leaf: JsonObject = {
                  operator: "EQUALS",
                  left_property: "externalId",
                  right_property: "raw_columns.name",
                };
                setJoinOn(appendJoinPredicate(joinOnRawObj, leaf));
                setJsonOverride(false);
              }}
            >
              {t("joinEditor.presetExternalIdRawName")}
            </button>
            <button
              type="button"
              className="discovery-btn discovery-btn--ghost discovery-btn--sm"
              onClick={() => {
                const d = defaultJoinOnRoot();
                setJoinOn(d);
                setJoinOnRaw(JSON.stringify(d, null, 2));
                setJsonOverride(false);
              }}
            >
              {t("joinEditor.applyDefaultPredicate")}
            </button>
          </div>
        </div>
      ) : null}

      {!structuredOk ? (
        <p className="discovery-hint" style={{ marginTop: 0, marginBottom: "0.5rem", maxWidth: "56rem" }}>
          {t("joinEditor.unsupportedShapeHint")}
        </p>
      ) : null}

      {structuredOk ? (
        <label className="discovery-label discovery-label--block" style={{ marginBottom: "0.5rem" }}>
          <input
            type="checkbox"
            checked={jsonOverride}
            onChange={(e) => setJsonOverride(e.target.checked)}
          />{" "}
          {t("joinEditor.advancedJson")}
        </label>
      ) : null}

      {showStructured ? (
        <>
          <JoinOnPredicateEditor t={t} value={joinOnRawObj} onChange={setJoinOn} depth={0} />
          <div className="discovery-toolbar-inline" style={{ marginTop: 10, flexWrap: "wrap", gap: 8 }}>
            <button
              type="button"
              className="discovery-btn discovery-btn--sm"
              onClick={() => {
                const g = joinRootGroupKind(joinOnRawObj);
                if (g === "and") {
                  const arr = (Array.isArray(joinOnRawObj.and) ? joinOnRawObj.and : []) as JsonObject[];
                  setJoinOn({ and: [...arr, emptyJoinLeaf()] });
                  return;
                }
                if (g === "or") {
                  const arr = (Array.isArray(joinOnRawObj.or) ? joinOnRawObj.or : []) as JsonObject[];
                  setJoinOn({ or: [...arr, emptyJoinLeaf()] });
                  return;
                }
                setJoinOn({ and: [joinOnRawObj, emptyJoinLeaf()] });
              }}
            >
              {t("joinEditor.addLeaf")}
            </button>
            <button
              type="button"
              className="discovery-btn discovery-btn--sm"
              onClick={() => {
                const g = joinRootGroupKind(joinOnRawObj);
                if (g === "and") {
                  const arr = (Array.isArray(joinOnRawObj.and) ? joinOnRawObj.and : []) as JsonObject[];
                  setJoinOn({ and: [...arr, emptyJoinAnd()] });
                  return;
                }
                if (g === "or") {
                  const arr = (Array.isArray(joinOnRawObj.or) ? joinOnRawObj.or : []) as JsonObject[];
                  setJoinOn({ or: [...arr, emptyJoinAnd()] });
                  return;
                }
                setJoinOn({ and: [joinOnRawObj, emptyJoinAnd()] });
              }}
            >
              {t("joinEditor.addAnd")}
            </button>
            <button
              type="button"
              className="discovery-btn discovery-btn--sm"
              onClick={() => {
                const g = joinRootGroupKind(joinOnRawObj);
                if (g === "and") {
                  const arr = (Array.isArray(joinOnRawObj.and) ? joinOnRawObj.and : []) as JsonObject[];
                  setJoinOn({ and: [...arr, emptyJoinOr()] });
                  return;
                }
                if (g === "or") {
                  const arr = (Array.isArray(joinOnRawObj.or) ? joinOnRawObj.or : []) as JsonObject[];
                  setJoinOn({ or: [...arr, emptyJoinOr()] });
                  return;
                }
                setJoinOn({ and: [joinOnRawObj, emptyJoinOr()] });
              }}
            >
              {t("joinEditor.addOr")}
            </button>
            <button
              type="button"
              className="discovery-btn discovery-btn--sm"
              onClick={() => {
                const g = joinRootGroupKind(joinOnRawObj);
                if (g === "and") {
                  const arr = (Array.isArray(joinOnRawObj.and) ? joinOnRawObj.and : []) as JsonObject[];
                  setJoinOn({ and: [...arr, emptyJoinNot()] });
                  return;
                }
                if (g === "or") {
                  const arr = (Array.isArray(joinOnRawObj.or) ? joinOnRawObj.or : []) as JsonObject[];
                  setJoinOn({ or: [...arr, emptyJoinNot()] });
                  return;
                }
                setJoinOn({ and: [joinOnRawObj, emptyJoinNot()] });
              }}
            >
              {t("joinEditor.addNot")}
            </button>
          </div>
        </>
      ) : null}

      {jsonOverride || !structuredOk ? (
        <>
          <button
            type="button"
            className="discovery-btn discovery-btn--ghost discovery-btn--sm"
            style={{ marginBottom: 8 }}
            onClick={() => {
              const d = defaultJoinOnRoot();
              setJoinOn(d);
              setJoinOnRaw(JSON.stringify(d, null, 2));
              setJsonOverride(false);
            }}
          >
            {t("joinEditor.resetTemplate")}
          </button>
          <label className="discovery-label discovery-label--block">
            join_on (JSON)
            <textarea
              className="discovery-textarea"
              spellCheck={false}
              style={{ marginTop: "0.35rem", minHeight: 140, fontFamily: "ui-monospace, monospace", fontSize: "0.8rem" }}
              value={joinOnRaw}
              onChange={(e) => {
                const raw = e.target.value;
                setJoinOnRaw(raw);
                try {
                  const parsed = JSON.parse(raw) as unknown;
                  if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
                    patch({ join_on: parsed as JsonObject });
                  }
                } catch {
                  /* incomplete JSON while typing */
                }
              }}
              onBlur={(e) => {
                try {
                  const parsed = JSON.parse(e.target.value) as unknown;
                  if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
                    patch({ join_on: parsed as JsonObject });
                    setJoinOnRaw(JSON.stringify(parsed, null, 2));
                    if (joinOnStructuredEditable(parsed as JsonObject)) setJsonOverride(false);
                  }
                } catch {
                  setJoinOnRaw(canonical);
                }
              }}
            />
          </label>
        </>
      ) : null}

      <p className="discovery-hint" style={{ marginTop: "0.35rem", maxWidth: "42rem" }}>
        {t("joinEditor.wireHandlesHint")}
      </p>
    </div>
  );
}
