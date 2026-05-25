import { useEffect, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  JoinOnPredicateEditor,
  defaultJoinOnRoot,
  emptyJoinAnd,
  emptyJoinLeaf,
  emptyJoinNot,
  emptyJoinOr,
  joinOnStructuredEditable,
} from "./JoinOnPredicateEditor";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

function joinRootGroupKind(n: JsonObject): "and" | "or" | "not" | null {
  if (Array.isArray(n.and)) return "and";
  if (Array.isArray(n.or)) return "or";
  if (n.not !== undefined && n.not !== null) return "not";
  return null;
}

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

export function EtlJoinNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const joinType = String(value.join_type ?? "inner").trim().toLowerCase();
  const enabled = value.enabled !== false;

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
    <div className="transform-node-editor-fields transform-join-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.join.canvasHint")}</p>

      <label className="gov-label gov-label--block">
        {t("transform.config.description")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.description ?? "")}
          onChange={(e) => patch({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>

      <label className="gov-label" style={{ marginTop: "0.75rem", display: "flex", alignItems: "center", gap: 8 }}>
        <input type="checkbox" checked={enabled} onChange={(e) => patch({ enabled: e.target.checked })} />
        {t("transform.join.enabledLabel")}
      </label>
      <p className="transform-node-editor-modal__hint">{t("transform.join.enabledHint")}</p>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transform.join.joinType")}
        <select
          className="gov-input"
          style={{ marginTop: "0.35rem", maxWidth: "12rem" }}
          value={joinType === "left" ? "left" : "inner"}
          onChange={(e) => patch({ join_type: e.target.value })}
        >
          <option value="inner">{t("transform.join.typeInner")}</option>
          <option value="left">{t("transform.join.typeLeft")}</option>
        </select>
      </label>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transform.join.rightPrefix")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.right_prefix ?? "")}
          onChange={(e) => patch({ right_prefix: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>

      <h4 className="transform-join-section-title">{t("transform.joinEditor.title")}</h4>
      <p className="transform-node-editor-modal__hint">{t("transform.joinEditor.hint")}</p>

      {structuredOk ? (
        <div style={{ marginBottom: "0.75rem" }}>
          <p className="transform-node-editor-modal__hint" style={{ margin: "0 0 0.35rem", fontWeight: 600 }}>
            {t("transform.joinEditor.presetsTitle")}
          </p>
          <p className="transform-node-editor-modal__hint">{t("transform.joinEditor.presetHint")}</p>
          <div className="transform-join-toolbar" style={{ flexWrap: "wrap", gap: 8 }}>
            <button
              type="button"
              className="disc-btn disc-btn--sm"
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
              {t("transform.joinEditor.presetNameRawName")}
            </button>
            <button
              type="button"
              className="disc-btn disc-btn--sm"
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
              {t("transform.joinEditor.presetNameRawAliases")}
            </button>
            <button
              type="button"
              className="disc-btn disc-btn--sm"
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
              {t("transform.joinEditor.presetExternalIdRawName")}
            </button>
            <button
              type="button"
              className="disc-btn disc-btn--ghost disc-btn--sm"
              onClick={() => {
                const d = defaultJoinOnRoot();
                setJoinOn(d);
                setJoinOnRaw(JSON.stringify(d, null, 2));
                setJsonOverride(false);
              }}
            >
              {t("transform.joinEditor.applyDefaultPredicate")}
            </button>
          </div>
        </div>
      ) : null}

      {!structuredOk ? (
        <p className="transform-node-editor-modal__hint">{t("transform.joinEditor.unsupportedShapeHint")}</p>
      ) : null}

      {structuredOk ? (
        <label className="gov-label gov-label--block" style={{ marginBottom: "0.5rem" }}>
          <input type="checkbox" checked={jsonOverride} onChange={(e) => setJsonOverride(e.target.checked)} />{" "}
          {t("transform.joinEditor.advancedJson")}
        </label>
      ) : null}

      {showStructured ? (
        <>
          <JoinOnPredicateEditor t={t} value={joinOnRawObj} onChange={setJoinOn} depth={0} />
          <div className="transform-join-toolbar" style={{ marginTop: 10, flexWrap: "wrap", gap: 8 }}>
            <button
              type="button"
              className="disc-btn disc-btn--sm"
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
              {t("transform.joinEditor.addLeaf")}
            </button>
            <button
              type="button"
              className="disc-btn disc-btn--sm"
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
              {t("transform.joinEditor.addAnd")}
            </button>
            <button
              type="button"
              className="disc-btn disc-btn--sm"
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
              {t("transform.joinEditor.addOr")}
            </button>
            <button
              type="button"
              className="disc-btn disc-btn--sm"
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
              {t("transform.joinEditor.addNot")}
            </button>
          </div>
        </>
      ) : null}

      {jsonOverride || !structuredOk ? (
        <>
          <button
            type="button"
            className="disc-btn disc-btn--ghost disc-btn--sm"
            style={{ marginBottom: 8 }}
            onClick={() => {
              const d = defaultJoinOnRoot();
              setJoinOn(d);
              setJoinOnRaw(JSON.stringify(d, null, 2));
              setJsonOverride(false);
            }}
          >
            {t("transform.joinEditor.resetTemplate")}
          </button>
          <label className="gov-label gov-label--block">
            join_on (JSON)
            <textarea
              className="gov-input"
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

      <p className="transform-node-editor-modal__hint">{t("transform.joinEditor.wireHandlesHint")}</p>
    </div>
  );
}
