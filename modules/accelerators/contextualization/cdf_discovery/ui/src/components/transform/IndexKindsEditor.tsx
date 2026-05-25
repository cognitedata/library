import { useCallback, useEffect, useMemo, useState } from "react";
import type { MessageKey } from "../../i18n/types";
import type { JsonObject } from "../../types/jsonConfig";
import {
  defaultIndexKindRow,
  indexKindsStructuredEditable,
  indexKindsToConfig,
  metadataIndexKeyPreset,
  parseIndexKindsJson,
  rowsFromIndexKinds,
  type IndexKindRow,
} from "../../utils/buildIndexNodeConfigModel";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  indexKinds: unknown;
  onChange: (indexKinds: JsonObject | undefined) => void;
};

function commitRows(rows: IndexKindRow[], onChange: Props["onChange"]) {
  onChange(indexKindsToConfig(rows));
}

export function IndexKindsEditor({ t, indexKinds, onChange }: Props) {
  const indexKindsSig = useMemo(() => JSON.stringify(indexKinds ?? null), [indexKinds]);
  const structuredOk = indexKindsStructuredEditable(indexKinds);
  const canonical = structuredOk
    ? JSON.stringify(indexKinds, null, 2)
    : indexKinds !== undefined && indexKinds !== null
      ? JSON.stringify(indexKinds, null, 2)
      : "";

  const [rows, setRows] = useState<IndexKindRow[]>([]);
  const [advancedJsonOpen, setAdvancedJsonOpen] = useState(!structuredOk);
  const [jsonText, setJsonText] = useState(canonical);
  const [jsonError, setJsonError] = useState<string | null>(null);

  useEffect(() => {
    setRows(rowsFromIndexKinds(indexKinds));
    setJsonText(canonical);
    setJsonError(null);
    if (!indexKindsStructuredEditable(indexKinds)) setAdvancedJsonOpen(true);
  }, [indexKindsSig, canonical, indexKinds]);

  const setRowsAndCommit = useCallback(
    (next: IndexKindRow[]) => {
      setRows(next);
      commitRows(next, onChange);
    },
    [onChange]
  );

  const onBlurAdvancedJson = () => {
    const parsed = parseIndexKindsJson(jsonText);
    if (parsed === null) {
      setJsonError(t("transform.buildIndex.jsonInvalid"));
      return;
    }
    setJsonError(null);
    setRows(parsed.length > 0 ? parsed : [defaultIndexKindRow()]);
    commitRows(parsed, onChange);
  };

  return (
    <div className="discovery-index-kinds-editor">
      <h4 className="transform-join-section-title">{t("transform.buildIndex.indexKindsSection")}</h4>
      <p className="transform-node-editor-modal__hint" style={{ marginTop: 0 }}>
        {t("transform.buildIndex.indexKindsHint")}
      </p>

      {!structuredOk ? (
        <p className="transform-node-editor-modal__hint">{t("transform.buildIndex.unsupportedShapeHint")}</p>
      ) : null}

      {structuredOk && !advancedJsonOpen ? (
        <>
          <div className="transform-join-toolbar" style={{ flexWrap: "wrap", gap: 8, marginBottom: "0.75rem" }}>
            <button
              type="button"
              className="disc-btn disc-btn--sm"
              onClick={() => setRowsAndCommit([...rows, defaultIndexKindRow()])}
            >
              {t("transform.buildIndex.addIndexKind")}
            </button>
            <button
              type="button"
              className="disc-btn disc-btn--ghost disc-btn--sm"
              onClick={() => setRowsAndCommit([metadataIndexKeyPreset()])}
            >
              {t("transform.buildIndex.presetMetadataIndexKey")}
            </button>
          </div>

          {rows.length === 0 ? (
            <p className="transform-node-editor-modal__hint">{t("transform.buildIndex.emptyHint")}</p>
          ) : null}

          {rows.map((row, kindIndex) => (
            <div
              key={`index-kind-row-${kindIndex}`}
              className="transform-join-filter-group"
              style={{ marginBottom: "0.65rem" }}
            >
              <div className="transform-join-toolbar" style={{ marginBottom: "0.5rem", flexWrap: "wrap", gap: 8 }}>
                <label className="gov-label" style={{ flex: "1 1 12rem", minWidth: "10rem", marginBottom: 0 }}>
                  {t("transform.buildIndex.indexKindLabel")}
                  <input
                    className="gov-input"
                    style={{ marginTop: "0.35rem", width: "100%" }}
                    value={row.kind}
                    onChange={(e) => {
                      const next = [...rows];
                      next[kindIndex] = { ...row, kind: e.target.value };
                      setRowsAndCommit(next);
                    }}
                    spellCheck={false}
                    autoComplete="off"
                    placeholder="metadata"
                  />
                </label>
                <button
                  type="button"
                  className="disc-btn disc-btn--ghost disc-btn--sm"
                  style={{ marginLeft: "auto" }}
                  onClick={() => setRowsAndCommit(rows.filter((_, i) => i !== kindIndex))}
                  aria-label={t("transform.buildIndex.removeIndexKind")}
                >
                  ×
                </button>
              </div>

              {row.properties.map((prop, propIndex) => (
                <div
                  key={`index-kind-${kindIndex}-prop-${propIndex}`}
                  style={{
                    display: "flex",
                    flexWrap: "wrap",
                    gap: "0.5rem",
                    alignItems: "flex-end",
                    marginBottom: "0.45rem",
                  }}
                >
                  <label className="gov-label" style={{ flex: "1 1 14rem", minWidth: "10rem", marginBottom: 0 }}>
                    {t("transform.buildIndex.sourcePropertyLabel")}
                    <input
                      className="gov-input"
                      style={{ marginTop: "0.35rem", width: "100%" }}
                      value={prop}
                      onChange={(e) => {
                        const next = [...rows];
                        const props = [...row.properties];
                        props[propIndex] = e.target.value;
                        next[kindIndex] = { ...row, properties: props };
                        setRowsAndCommit(next);
                      }}
                      spellCheck={false}
                      autoComplete="off"
                      placeholder="indexKey"
                    />
                  </label>
                  <button
                    type="button"
                    className="disc-btn disc-btn--ghost disc-btn--sm"
                    onClick={() => {
                      const next = [...rows];
                      const props = row.properties.filter((_, i) => i !== propIndex);
                      next[kindIndex] = { ...row, properties: props.length > 0 ? props : [""] };
                      setRowsAndCommit(next);
                    }}
                    aria-label={t("transform.buildIndex.removeSourceProperty")}
                  >
                    ×
                  </button>
                </div>
              ))}

              <button
                type="button"
                className="disc-btn disc-btn--sm"
                onClick={() => {
                  const next = [...rows];
                  next[kindIndex] = { ...row, properties: [...row.properties, ""] };
                  setRowsAndCommit(next);
                }}
              >
                {t("transform.buildIndex.addSourceProperty")}
              </button>
            </div>
          ))}
        </>
      ) : null}

      <div style={{ marginTop: "0.75rem" }}>
        {structuredOk ? (
          <button
            type="button"
            className="disc-btn disc-btn--ghost disc-btn--sm"
            onClick={() => {
              const nextOpen = !advancedJsonOpen;
              setAdvancedJsonOpen(nextOpen);
              if (nextOpen) {
                setJsonText(canonical);
                setJsonError(null);
              }
            }}
          >
            {advancedJsonOpen
              ? t("transform.buildIndex.jsonToggleHide")
              : t("transform.buildIndex.jsonToggleShow")}
          </button>
        ) : null}

        {advancedJsonOpen || !structuredOk ? (
          <>
            <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.35rem" }}>
              {t("transform.buildIndex.advancedJsonHint")}
            </p>
            <label className="gov-label gov-label--block">
              {t("transform.config.indexKinds")}
              <textarea
                className="gov-input"
                spellCheck={false}
                style={{
                  marginTop: "0.35rem",
                  minHeight: 140,
                  fontFamily: "ui-monospace, monospace",
                  fontSize: "0.8rem",
                }}
                value={jsonText}
                onChange={(e) => setJsonText(e.target.value)}
                onBlur={onBlurAdvancedJson}
              />
            </label>
            {jsonError ? (
              <p className="transform-node-editor-modal__hint" style={{ color: "var(--discovery-danger, #b00020)" }}>
                {jsonError}
              </p>
            ) : null}
          </>
        ) : null}
      </div>
    </div>
  );
}
