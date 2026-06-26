import { useState } from "react";
import { redactForDisplay } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import { OperationResultPanel } from "./OperationResultPanel";

type Props = {
  onSelectRow: (row: unknown) => void;
};

export function FileContextPane({ onSelectRow }: Props) {
  const { t } = useAppSettings();
  const [fileId, setFileId] = useState("");
  const [fileSpace, setFileSpace] = useState("cdf_cdm");
  const [scopeKey, setScopeKey] = useState("");
  const scoreOp = useOperationRun();
  const deltasOp = useOperationRun();
  const listOp = useOperationRun();
  const anyLoading = scoreOp.loading || deltasOp.loading || listOp.loading;

  const body = () => ({
    file_external_id: fileId.trim(),
    file_space: fileSpace.trim() || "cdf_cdm",
    match_scope_key: scopeKey.trim() || undefined,
  });

  const runScore = () => {
    if (!fileId.trim()) return;
    void scoreOp.run("/api/inverted-index/score/stream", body());
  };

  const runDeltas = () => {
    if (!fileId.trim()) return;
    void deltasOp.run("/api/inverted-index/deltas/stream", body());
  };

  const runList = () => {
    if (!fileId.trim()) return;
    void listOp.run("/api/inverted-index/list-by-file/stream", { ...body(), limit: 5000 });
  };

  const deltasResult =
    deltasOp.result && typeof deltasOp.result === "object"
      ? (deltasOp.result as { missing_tags: unknown[]; pattern_feedback: unknown[] })
      : null;
  const entries = Array.isArray(listOp.result) ? listOp.result : [];

  return (
    <div className="idx-pane">
      <h2 className="idx-pane__title">{t("fileContext.title")}</h2>
      <p className="idx-pane__hint">{t("fileContext.hint")}</p>
      <div className="idx-field-row">
        <label className="idx-label">
          {t("fileContext.fileId")}
          <input
            className="idx-input"
            value={fileId}
            onChange={(e) => setFileId(e.target.value)}
            placeholder={t("fileContext.fileIdPlaceholder")}
          />
        </label>
        <label className="idx-label">
          {t("fileContext.fileSpace")}
          <input className="idx-input" value={fileSpace} onChange={(e) => setFileSpace(e.target.value)} />
        </label>
        <label className="idx-label">
          {t("fileContext.scopeKey")}
          <input
            className="idx-input"
            value={scopeKey}
            onChange={(e) => setScopeKey(e.target.value)}
            placeholder={t("fileContext.scopeKeyPlaceholder")}
          />
        </label>
      </div>
      <div className="idx-field-row">
        <button type="button" className="idx-btn" disabled={anyLoading} onClick={runScore}>
          {scoreOp.loading ? t("ops.running") : t("fileContext.runScore")}
        </button>
        {scoreOp.loading ? (
          <button type="button" className="idx-btn" onClick={scoreOp.cancel}>
            {t("ops.cancel")}
          </button>
        ) : null}
        <button type="button" className="idx-btn" disabled={anyLoading} onClick={runDeltas}>
          {deltasOp.loading ? t("ops.running") : t("fileContext.runDeltas")}
        </button>
        {deltasOp.loading ? (
          <button type="button" className="idx-btn" onClick={deltasOp.cancel}>
            {t("ops.cancel")}
          </button>
        ) : null}
        <button type="button" className="idx-btn" disabled={anyLoading} onClick={runList}>
          {listOp.loading ? t("ops.running") : t("fileContext.runList")}
        </button>
        {listOp.loading ? (
          <button type="button" className="idx-btn" onClick={listOp.cancel}>
            {t("ops.cancel")}
          </button>
        ) : null}
      </div>
      <div className="idx-file-sections">
        <section>
          <h3 className="idx-file-section__title">{t("fileContext.score")}</h3>
          <OperationResultPanel
            loading={scoreOp.loading}
            cancelled={scoreOp.cancelled}
            error={scoreOp.error}
            result={scoreOp.result}
            log={scoreOp.log}
            showConsole
          />
        </section>
        <section>
          <h3 className="idx-file-section__title">{t("fileContext.deltas")}</h3>
          <OperationResultPanel
            loading={deltasOp.loading}
            cancelled={deltasOp.cancelled}
            error={deltasOp.error}
            result={
              deltasResult
                ? {
                    missing_tags_count: deltasResult.missing_tags.length,
                    pattern_feedback_count: deltasResult.pattern_feedback.length,
                  }
                : null
            }
            log={deltasOp.log}
            showConsole
          />
          {deltasResult ? (
            <>
              <h4>{t("fileContext.missingTags")}</h4>
              <pre className="idx-json-pre">
                {JSON.stringify(redactForDisplay(deltasResult.missing_tags), null, 2)}
              </pre>
              <h4>{t("fileContext.patternFeedback")}</h4>
              <pre className="idx-json-pre">
                {JSON.stringify(redactForDisplay(deltasResult.pattern_feedback), null, 2)}
              </pre>
            </>
          ) : null}
        </section>
        <section>
          <h3 className="idx-file-section__title">{t("fileContext.entries")}</h3>
          <OperationResultPanel
            loading={listOp.loading}
            cancelled={listOp.cancelled}
            error={listOp.error}
            result={listOp.result ? { entry_count: entries.length } : null}
            log={listOp.log}
            showConsole
          />
          {entries.length > 0 ? (
            <div className="idx-table-wrap">
              <table className="idx-table">
                <thead>
                  <tr>
                    <th>term</th>
                    <th>source_type</th>
                    <th>reference</th>
                  </tr>
                </thead>
                <tbody>
                  {entries.map((entry, i) => {
                    const row = entry as Record<string, unknown>;
                    return (
                      <tr key={i} onClick={() => onSelectRow(entry)}>
                        <td>{String(row.normalized_term ?? row.term ?? "")}</td>
                        <td>{String(row.source_type ?? "")}</td>
                        <td>{String(row.reference_external_id ?? "")}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}
        </section>
      </div>
    </div>
  );
}
