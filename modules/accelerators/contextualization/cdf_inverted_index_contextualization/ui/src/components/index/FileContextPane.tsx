import { useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import { asHitRows } from "../../utils/resultViews";
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

type FileContextTab = "score" | "deltas" | "entries";

export function FileContextPane({ onSelectRow }: Props) {
  const { t } = useAppSettings();
  const [fileId, setFileId] = useState("");
  const [fileSpace, setFileSpace] = useState("cdf_cdm");
  const [scopeKey, setScopeKey] = useState("");
  const [activeTab, setActiveTab] = useState<FileContextTab>("score");
  const scoreOp = useOperationRun();
  const deltasOp = useOperationRun();
  const listOp = useOperationRun();

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
  const entries = Array.isArray(listOp.result) ? asHitRows(listOp.result) : [];
  const missingTags = deltasResult ? asHitRows(deltasResult.missing_tags) : [];
  const patternFeedback = deltasResult ? asHitRows(deltasResult.pattern_feedback) : [];

  const activeOp =
    activeTab === "score" ? scoreOp : activeTab === "deltas" ? deltasOp : listOp;

  const runActive = () => {
    if (activeTab === "score") runScore();
    else if (activeTab === "deltas") runDeltas();
    else runList();
  };

  const runLabelKey =
    activeTab === "score"
      ? "fileContext.runScore"
      : activeTab === "deltas"
        ? "fileContext.runDeltas"
        : "fileContext.runList";

  const tabTitleKey =
    activeTab === "score"
      ? "fileContext.score"
      : activeTab === "deltas"
        ? "fileContext.deltas"
        : "fileContext.entries";

  return (
    <EditorPage title={t("fileContext.title")} hint={t("fileContext.hint")}>
      <EditorWorkflowLayout
        parameters={
          <FormPanel title={t("ops.panel.parameters")} hint={t("fileContext.panel.file")}>
            <ActionsBar sticky>
              <OperationRunControls
                loading={activeOp.loading}
                onRun={runActive}
                onCancel={activeOp.cancel}
                runLabelKey={runLabelKey}
              />
            </ActionsBar>
            <FieldGroup>
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
            </FieldGroup>
          </FormPanel>
        }
        between={
          <nav className="idx-subtabs idx-subtabs--workflow" aria-label={t("fileContext.tabsLabel")}>
            {(["score", "deltas", "entries"] as const).map((tab) => (
              <button
                key={tab}
                type="button"
                className={`idx-subtab${activeTab === tab ? " idx-subtab--active" : ""}`}
                aria-current={activeTab === tab ? "page" : undefined}
                onClick={() => setActiveTab(tab)}
              >
                {t(`fileContext.tab.${tab}`)}
              </button>
            ))}
          </nav>
        }
        results={
          <FormPanel title={t(tabTitleKey)} className="idx-panel--output">
            {activeTab === "score" ? (
              <OperationResultPanel
                loading={scoreOp.loading}
                cancelled={scoreOp.cancelled}
                error={scoreOp.error}
                result={scoreOp.result}
                log={scoreOp.log}
                showConsole
                rawResult={scoreOp.result}
              />
            ) : null}
            {activeTab === "deltas" ? (
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
                rawResult={deltasOp.result}
              >
                <section className="idx-result-section">
                  <h4 className="idx-result-section__title">{t("fileContext.missingTags")}</h4>
                  <DataTable
                    columns={[
                      { id: "term", headerKey: "table.term", render: (row) => String(row.normalized_term ?? row.term ?? "") },
                      { id: "source", headerKey: "table.sourceType", render: (row) => String(row.source_type ?? "") },
                    ]}
                    rows={missingTags}
                    onRowClick={onSelectRow}
                  />
                </section>
                <section className="idx-result-section">
                  <h4 className="idx-result-section__title">{t("fileContext.patternFeedback")}</h4>
                  <DataTable
                    columns={[
                      { id: "term", headerKey: "table.term", render: (row) => String(row.normalized_term ?? row.term ?? "") },
                      { id: "source", headerKey: "table.sourceType", render: (row) => String(row.source_type ?? "") },
                    ]}
                    rows={patternFeedback}
                    onRowClick={onSelectRow}
                  />
                </section>
              </OperationResultPanel>
            ) : null}
            {activeTab === "entries" ? (
              <OperationResultPanel
                loading={listOp.loading}
                cancelled={listOp.cancelled}
                error={listOp.error}
                result={listOp.result ? { entry_count: entries.length } : null}
                log={listOp.log}
                showConsole
                metrics={[{ key: "entry_count", labelKey: "metrics.hitCount" }]}
                rawResult={listOp.result}
              >
                <DataTable
                  columns={[
                    { id: "term", headerKey: "table.term", render: (row) => String(row.normalized_term ?? row.term ?? "") },
                    { id: "source", headerKey: "table.sourceType", render: (row) => String(row.source_type ?? "") },
                    { id: "ref", headerKey: "table.reference", render: (row) => String(row.reference_external_id ?? "") },
                  ]}
                  rows={entries}
                  onRowClick={onSelectRow}
                />
              </OperationResultPanel>
            ) : null}
          </FormPanel>
        }
      />
    </EditorPage>
  );
}
