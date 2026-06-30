import { useMemo, useState } from "react";
import { formatTagReuseAuditResult, parseCsvList } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import { TAG_REUSE_METRICS } from "../../utils/metricDefs";
import { ActionsBar } from "../shared/ActionsBar";
import { DataTable } from "../shared/DataTable";
import { EditorPage } from "../shared/EditorPage";
import { EditorWorkflowLayout } from "../shared/EditorWorkflowLayout";
import { FieldGroup } from "../shared/FieldGroup";
import { FormPanel } from "../shared/FormPanel";
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
  const metricsData =
    summaryResult && typeof summaryResult === "object"
      ? (summaryResult as Record<string, unknown>)
      : null;

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
    <EditorPage title={t("tagReuse.title")} hint={t("tagReuse.hint")}>
      <EditorWorkflowLayout
        parameters={
          <FormPanel title={t("ops.panel.parameters")} hint={t("tagReuse.panel.filters")}>
            <ActionsBar sticky>
              <OperationRunControls
                loading={loading}
                onRun={handleRun}
                onCancel={cancel}
                runLabelKey="tagReuse.run"
              />
            </ActionsBar>
            <FieldGroup>
              <div className="idx-checkbox-group idx-checkbox-group--inline">
                <label className="idx-checkbox-label">
                  <input type="checkbox" checked={allScopes} onChange={(e) => setAllScopes(e.target.checked)} />
                  {t("tagReuse.allScopes")}
                </label>
              </div>
              <div className="idx-field-row">
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
              </div>
            </FieldGroup>
          </FormPanel>
        }
        results={
          <FormPanel title={t("ops.panel.results")} className="idx-panel--output">
            <OperationResultPanel
              loading={loading}
              cancelled={cancelled}
              error={displayError}
              result={summaryResult}
              log={log}
              showConsole
              metrics={TAG_REUSE_METRICS}
              metricsData={metricsData}
              rawResult={result}
            >
              <section className="idx-result-section">
                <h4 className="idx-result-section__title">{t("tagReuse.results")}</h4>
                {showSingleScopeHint ? (
                  <p className="idx-pane__hint">{t("tagReuse.singleScopeHint")}</p>
                ) : null}
                <DataTable
                  columns={[
                    {
                      id: "term",
                      headerKey: "table.term",
                      render: (row) => String(row.normalized_term ?? row.term ?? ""),
                    },
                    {
                      id: "scopes",
                      headerKey: "table.scopes",
                      render: (row) => {
                        const scopes = row.scope_keys ?? row.scopes ?? row.match_scope_keys;
                        return Array.isArray(scopes) ? scopes.join(", ") : String(scopes ?? "");
                      },
                    },
                  ]}
                  rows={rows as Record<string, unknown>[]}
                  onRowClick={onSelectRow}
                  emptyMessage={auditResult && !loading ? t("tagReuse.noResults") : undefined}
                />
              </section>
            </OperationResultPanel>
          </FormPanel>
        }
      />
    </EditorPage>
  );
}
