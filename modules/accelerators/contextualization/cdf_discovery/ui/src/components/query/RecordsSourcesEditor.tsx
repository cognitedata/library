import { useAppSettings } from "../../context/AppSettingsContext";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import type { JsonObject } from "../../types/jsonConfig";

type Props = {
  sources: JsonObject[];
  onChange: (next: JsonObject[]) => void;
  onCopyFromStream?: () => void;
};

function emptySource(): JsonObject {
  return { space: "", externalId: "", version: "" };
}

export function RecordsSourcesEditor({ sources, onChange, onCopyFromStream }: Props) {
  const { t } = useAppSettings();

  const patchRow = (index: number, patch: JsonObject) => {
    const next = sources.map((row, i) => (i === index ? { ...row, ...patch } : row));
    onChange(next);
  };

  return (
    <div className="transform-records-sources">
      <p className="transform-query-hint">{t("transform.query.recordsSourcesHint")}</p>
      {onCopyFromStream ? (
        <button type="button" className="gov-btn gov-btn--secondary" style={{ marginBottom: "0.5rem" }} onClick={onCopyFromStream}>
          {t("transform.query.recordsSourcesCopyFromStream")}
        </button>
      ) : null}
      {sources.map((row, index) => (
        <div key={index} className="transform-records-sources__row" style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", marginBottom: "0.5rem" }}>
          <label className="transform-query-label">
            {t("transform.query.recordsSourceSpace")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={String(row.space ?? "")}
              syncKey={`src-space-${index}`}
              onCommit={(v) => patchRow(index, { space: v })}
              spellCheck={false}
              autoComplete="off"
            />
          </label>
          <label className="transform-query-label">
            {t("transform.query.recordsSourceExternalId")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={String(row.externalId ?? row.external_id ?? "")}
              syncKey={`src-ext-${index}`}
              onCommit={(v) => patchRow(index, { externalId: v })}
              spellCheck={false}
              autoComplete="off"
            />
          </label>
          <label className="transform-query-label">
            {t("transform.query.recordsSourceVersion")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={String(row.version ?? "")}
              syncKey={`src-ver-${index}`}
              onCommit={(v) => patchRow(index, { version: v })}
              spellCheck={false}
              autoComplete="off"
            />
          </label>
          <button
            type="button"
            className="gov-btn gov-btn--secondary"
            onClick={() => onChange(sources.filter((_, i) => i !== index))}
            aria-label={t("transform.query.recordsSourceRemove")}
          >
            {t("transform.query.recordsSourceRemove")}
          </button>
        </div>
      ))}
      <button type="button" className="gov-btn gov-btn--secondary" onClick={() => onChange([...sources, emptySource()])}>
        {t("transform.query.recordsSourceAdd")}
      </button>
    </div>
  );
}
