import { useMemo, useState } from "react";
import { formatTagReuseAuditResult, parseCsvList } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import { OperationResultPanel } from "./OperationResultPanel";
import { OperationRunControls } from "./OperationRunControls";

type Props = {
  onSelectRow: (row: unknown) => void;
};

export function TagReusePane({ onSelectRow }: Props) {
  const { t } = useAppSettings();
  const [allScopes, setAllScopes] = useState(true);
  const [scopeKeysRaw, setScopeKeysRaw] = useState("");
  const [minScopeCount, setMinScopeCount] = useState(2);
  const [limit, setLimit] = useState(5000);
  const [validationError, setValidationError] = useState<string | null>(null);
  const { loading, cancelled, error, result, log, run, cancel } = useOperationRun();

  const auditResult =
    result && typeof result === "object" && !Array.isArray(result)
      ? (result as Record<string, unknown>)
      : null;

  const rows = useMemo(() => {
    if (!auditResult) return [];
    const reuse = auditResult.reuse_metrics;
    if (reuse && typeof reuse === "object" && "by_term" in reuse) {
      const byTerm = (reuse as { by_term: unknown }).by_term;
      if (Array.isArray(byTerm)) return byTerm;
    }
    const candidates = ["tags", "results", "reused_tags", "entries"];
    for (const key of candidates) {
      const val = auditResult[key];
      if (Array.isArray(val)) return val;
    }
    return [];
  }, [auditResult]);

  const scopesScanned = Number(auditResult?.scopes_scanned ?? 0);
  const showSingleScopeHint =
    !loading && auditResult != null && scopesScanned > 0 && scopesScanned < minScopeCount;

  const summaryResult = useMemo(() => formatTagReuseAuditResult(result), [result]);

  const handleRun = () => {
    const scopeKeys = parseCsvList(scopeKeysRaw);
    if (!allScopes && scopeKeys.length === 0) {
      setValidationError(t("tagReuse.scopeRequired"));
      return;
    }
    setValidationError(null);
    void run("/api/inverted-index/tag-reuse-audit/stream", {
      all_scopes: allScopes,
      match_scope_keys: scopeKeys,
      min_scope_count: minScopeCount,
      limit,
    });
  };

  const displayError = validationError ?? error;

  return (
    <div className="idx-pane">
      <h2 className="idx-pane__title">{t("tagReuse.title")}</h2>
      <p className="idx-pane__hint">{t("tagReuse.hint")}</p>
      <div className="idx-field-row">
        <label className="idx-checkbox-label">
          <input type="checkbox" checked={allScopes} onChange={(e) => setAllScopes(e.target.checked)} />
          {t("tagReuse.allScopes")}
        </label>
        <label className="idx-label">
          {t("tagReuse.scopeKeys")}
          <input
            className="idx-input"
            value={scopeKeysRaw}
            onChange={(e) => setScopeKeysRaw(e.target.value)}
            placeholder={t("tagReuse.scopeKeysPlaceholder")}
            disabled={allScopes}
          />
        </label>
        <label className="idx-label">
          {t("tagReuse.minScopeCount")}
          <input
            className="idx-input"
            type="number"
            min={2}
            value={minScopeCount}
            onChange={(e) => setMinScopeCount(Number(e.target.value))}
          />
        </label>
        <label className="idx-label">
          {t("tagReuse.limit")}
          <input
            className="idx-input"
            type="number"
            min={1}
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value))}
          />
        </label>
        <OperationRunControls
          loading={loading}
          onRun={handleRun}
          onCancel={cancel}
          runLabelKey="tagReuse.run"
        />
      </div>
      <OperationResultPanel
        loading={loading}
        cancelled={cancelled}
        error={displayError}
        result={summaryResult}
        log={log}
        showConsole
      />
      <h3 className="idx-file-section__title">{t("tagReuse.results")}</h3>
      {showSingleScopeHint ? (
        <p className="idx-pane__hint">{t("tagReuse.singleScopeHint")}</p>
      ) : null}
      {rows.length === 0 && auditResult && !loading ? (
        <p className="idx-pane__hint">{t("tagReuse.noResults")}</p>
      ) : null}
      {rows.length > 0 ? (
        <div className="idx-table-wrap">
          <table className="idx-table">
            <thead>
              <tr>
                <th>term</th>
                <th>scopes</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => {
                const r = row as Record<string, unknown>;
                const scopes = r.scope_keys ?? r.scopes ?? r.match_scope_keys;
                return (
                  <tr key={i} onClick={() => onSelectRow(row)}>
                    <td>{String(r.normalized_term ?? r.term ?? "")}</td>
                    <td>{Array.isArray(scopes) ? scopes.join(", ") : String(scopes ?? "")}</td>
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
