import { useMemo, useState } from "react";
import { parseCsvList } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import { QUERY_SUMMARY_METRICS, REUSE_METRICS } from "../../utils/metricDefs";
import { queryByTermRows, queryHits, queryReuseMetrics } from "../../utils/resultViews";
import { ActionsBar } from "../shared/ActionsBar";
import { DataTable } from "../shared/DataTable";
import { EditorPage } from "../shared/EditorPage";
import { EditorWorkflowLayout } from "../shared/EditorWorkflowLayout";
import { FieldGroup } from "../shared/FieldGroup";
import { FormPanel } from "../shared/FormPanel";
import { MetricSummary } from "../shared/MetricSummary";
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

  const hits = queryHits(result);
  const reuseMetrics = queryReuseMetrics(result);
  const byTermRows = queryByTermRows(result);

  const summaryResult = useMemo(() => {
    if (!result || typeof result !== "object" || Array.isArray(result)) {
      return { hit_count: hits.length };
    }
    return {
      scopes_queried: (result as { scopes_queried?: unknown }).scopes_queried,
      terms_queried: (result as { terms_queried?: unknown }).terms_queried,
      hit_count: hits.length,
    };
  }, [result, hits.length]);

  const reuseSummary = reuseMetrics
    ? {
        ...reuseMetrics,
        by_term_count: Array.isArray(reuseMetrics.by_term) ? (reuseMetrics.by_term as unknown[]).length : 0,
      }
    : null;

  return (
    <EditorPage title={t("query.title")} hint={t("query.hint")}>
      <EditorWorkflowLayout
        parameters={
          <FormPanel title={t("ops.panel.parameters")}>
            <ActionsBar sticky>
              <OperationRunControls loading={loading} onRun={handleRun} onCancel={cancel} runLabelKey="query.run" />
            </ActionsBar>
            <FieldGroup label={t("query.panel.search")}>
              <label className="idx-label">
                {t("query.terms")}
                <input
                  className="idx-input"
                  value={termsRaw}
                  onChange={(e) => setTermsRaw(e.target.value)}
                  placeholder={t("query.termsPlaceholder")}
                />
              </label>
            </FieldGroup>
            <FieldGroup label={t("query.panel.scope")}>
              <div className="idx-field-row">
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
            </FieldGroup>
            <FieldGroup label={t("query.panel.output")}>
              <div className="idx-checkbox-group">
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
              </div>
            </FieldGroup>
          </FormPanel>
        }
        results={
          <FormPanel title={t("ops.panel.results")} className="idx-panel--output">
            <OperationResultPanel
              loading={loading}
              cancelled={cancelled}
              error={error}
              result={summaryResult}
              log={log}
              showConsole
              metrics={QUERY_SUMMARY_METRICS}
              metricsData={summaryResult}
              rawResult={result}
            >
              {reuseSummary ? (
                <section className="idx-result-section">
                  <h4 className="idx-result-section__title">{t("query.reuseMetrics")}</h4>
                  <MetricSummary data={reuseSummary} metrics={REUSE_METRICS} />
                </section>
              ) : null}
              {byTermRows.length > 0 ? (
                <section className="idx-result-section">
                  <h4 className="idx-result-section__title">{t("query.reuseByTerm")}</h4>
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
                          const scopes = row.scope_keys ?? row.scopes;
                          return Array.isArray(scopes) ? scopes.join(", ") : String(scopes ?? "");
                        },
                      },
                      {
                        id: "count",
                        headerKey: "table.scopeCount",
                        render: (row) => {
                          const scopes = row.scope_keys ?? row.scopes;
                          const count = row.scope_count ?? (Array.isArray(scopes) ? scopes.length : "");
                          return String(count);
                        },
                      },
                    ]}
                    rows={byTermRows}
                    onRowClick={onSelectRow}
                  />
                </section>
              ) : null}
              <section className="idx-result-section">
                <h4 className="idx-result-section__title">{t("query.hits")}</h4>
                <DataTable
                  columns={[
                    {
                      id: "term",
                      headerKey: "table.term",
                      render: (row) => String(row.normalized_term ?? row.term ?? ""),
                    },
                    {
                      id: "source",
                      headerKey: "table.sourceType",
                      render: (row) => String(row.source_type ?? ""),
                    },
                    {
                      id: "ref",
                      headerKey: "table.reference",
                      render: (row) => String(row.reference_external_id ?? ""),
                    },
                    {
                      id: "conf",
                      headerKey: "table.confidence",
                      render: (row) =>
                        row.confidence != null && row.confidence !== "" ? (
                          <span className="idx-confidence-badge">{String(row.confidence)}</span>
                        ) : (
                          "—"
                        ),
                    },
                  ]}
                  rows={hits}
                  onRowClick={onSelectRow}
                  emptyMessage={!loading ? t("query.noHits") : undefined}
                />
              </section>
            </OperationResultPanel>
          </FormPanel>
        }
      />
    </EditorPage>
  );
}
