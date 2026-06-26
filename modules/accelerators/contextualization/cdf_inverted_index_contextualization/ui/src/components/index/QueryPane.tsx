import { useState } from "react";
import { parseCsvList, redactForDisplay } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import { OperationResultPanel } from "./OperationResultPanel";
import { OperationRunControls } from "./OperationRunControls";

type Props = {
  onSelectRow: (row: unknown) => void;
};

export function QueryPane({ onSelectRow }: Props) {
  const { t } = useAppSettings();
  const [termsRaw, setTermsRaw] = useState("");
  const [scopeKeysRaw, setScopeKeysRaw] = useState("");
  const [sourceTypesRaw, setSourceTypesRaw] = useState("");
  const [minConfidence, setMinConfidence] = useState(0);
  const [allScopes, setAllScopes] = useState(false);
  const [reuseOnly, setReuseOnly] = useState(false);
  const [hitsOnly, setHitsOnly] = useState(false);
  const { loading, cancelled, error, result, log, run, cancel } = useOperationRun();

  const handleRun = () => {
    const terms = parseCsvList(termsRaw);
    if (!terms.length) return;
    void run("/api/inverted-index/query/stream", {
      terms,
      all_scopes: allScopes,
      match_scope_keys: parseCsvList(scopeKeysRaw),
      source_types: parseCsvList(sourceTypesRaw),
      min_confidence: minConfidence,
      reuse_only: reuseOnly,
      hits_only: hitsOnly,
    });
  };

  const hits = Array.isArray(result)
    ? result
    : result && typeof result === "object" && "hits" in result
      ? (result as { hits: unknown[] }).hits
      : [];

  const summaryResult =
    result && typeof result === "object" && !Array.isArray(result)
      ? {
          scopes_queried: (result as { scopes_queried?: unknown }).scopes_queried,
          terms_queried: (result as { terms_queried?: unknown }).terms_queried,
          hit_count: hits.length,
          reuse_metrics: (result as { reuse_metrics?: unknown }).reuse_metrics,
        }
      : { hit_count: hits.length };

  return (
    <div className="idx-pane">
      <h2 className="idx-pane__title">{t("query.title")}</h2>
      <p className="idx-pane__hint">{t("query.hint")}</p>
      <div className="idx-field-row">
        <label className="idx-label">
          {t("query.terms")}
          <input
            className="idx-input"
            value={termsRaw}
            onChange={(e) => setTermsRaw(e.target.value)}
            placeholder={t("query.termsPlaceholder")}
          />
        </label>
        <label className="idx-label">
          {t("query.scopeKeys")}
          <input
            className="idx-input"
            value={scopeKeysRaw}
            onChange={(e) => setScopeKeysRaw(e.target.value)}
            placeholder={t("query.scopeKeysPlaceholder")}
            disabled={allScopes}
          />
        </label>
        <label className="idx-label">
          {t("query.sourceTypes")}
          <input
            className="idx-input"
            value={sourceTypesRaw}
            onChange={(e) => setSourceTypesRaw(e.target.value)}
            placeholder={t("query.sourceTypesPlaceholder")}
          />
        </label>
        <label className="idx-label">
          {t("query.minConfidence")}
          <input
            className="idx-input"
            type="number"
            min={0}
            max={1}
            step={0.05}
            value={minConfidence}
            onChange={(e) => setMinConfidence(Number(e.target.value))}
          />
        </label>
      </div>
      <div className="idx-field-row">
        <label className="idx-checkbox-label">
          <input type="checkbox" checked={allScopes} onChange={(e) => setAllScopes(e.target.checked)} />
          {t("query.allScopes")}
        </label>
        <label className="idx-checkbox-label">
          <input type="checkbox" checked={reuseOnly} onChange={(e) => setReuseOnly(e.target.checked)} />
          {t("query.reuseOnly")}
        </label>
        <label className="idx-checkbox-label">
          <input type="checkbox" checked={hitsOnly} onChange={(e) => setHitsOnly(e.target.checked)} />
          {t("query.hitsOnly")}
        </label>
        <OperationRunControls
          loading={loading}
          onRun={handleRun}
          onCancel={cancel}
          runLabelKey="query.run"
        />
      </div>
      <OperationResultPanel
        loading={loading}
        cancelled={cancelled}
        error={error}
        result={summaryResult}
        log={log}
        showConsole
      />
      {result && !Array.isArray(result) && typeof result === "object" && "reuse_metrics" in result ? (
        <div>
          <h3 className="idx-file-section__title">{t("query.reuseMetrics")}</h3>
          <pre className="idx-json-pre">
            {JSON.stringify(redactForDisplay((result as { reuse_metrics: unknown }).reuse_metrics), null, 2)}
          </pre>
        </div>
      ) : null}
      <h3 className="idx-file-section__title">{t("query.hits")}</h3>
      {hits.length === 0 && !loading ? <p className="idx-pane__hint">{t("query.noHits")}</p> : null}
      {hits.length > 0 ? (
        <div className="idx-table-wrap">
          <table className="idx-table">
            <thead>
              <tr>
                <th>term</th>
                <th>source_type</th>
                <th>reference</th>
                <th>confidence</th>
              </tr>
            </thead>
            <tbody>
              {hits.map((hit, i) => {
                const row = hit as Record<string, unknown>;
                return (
                  <tr key={i} onClick={() => onSelectRow(hit)}>
                    <td>{String(row.normalized_term ?? row.term ?? "")}</td>
                    <td>{String(row.source_type ?? "")}</td>
                    <td>{String(row.reference_external_id ?? "")}</td>
                    <td>{String(row.confidence ?? "")}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}
