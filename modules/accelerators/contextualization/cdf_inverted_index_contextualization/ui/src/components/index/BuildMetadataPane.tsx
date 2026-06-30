import { useId, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import { ActionsBar } from "../shared/ActionsBar";
import { EditorPage } from "../shared/EditorPage";
import { EditorWorkflowLayout } from "../shared/EditorWorkflowLayout";
import { FieldGroup } from "../shared/FieldGroup";
import { FormPanel } from "../shared/FormPanel";
import { BuildOperationOutput } from "./BuildOperationOutput";
import { DryRunToggle } from "./OperationResultPanel";
import { OperationRunControls } from "./OperationRunControls";

export function BuildMetadataPane() {
  const { t } = useAppSettings();
  const baseId = useId();
  const [dryRun, setDryRun] = useState(true);
  const [filterUpdatedAfter, setFilterUpdatedAfter] = useState("");
  const [batchSize, setBatchSize] = useState("");
  const [progressInterval, setProgressInterval] = useState("100");
  const { loading, cancelled, error, result, log, run, cancel } = useOperationRun();

  const filterUpdatedId = `${baseId}-filter-updated`;
  const batchSizeId = `${baseId}-batch-size`;
  const progressIntervalId = `${baseId}-progress-interval`;

  const handleRun = () => {
    void run("/api/inverted-index/build/metadata/stream", {
      dry_run: dryRun,
      filter_updated_after: filterUpdatedAfter.trim() || undefined,
      batch_size: batchSize.trim() ? Number(batchSize) : undefined,
      progress_interval: progressInterval.trim() ? Number(progressInterval) : undefined,
    });
  };

  return (
    <EditorPage title={t("buildMetadata.title")} hint={t("buildMetadata.hint")}>
      <EditorWorkflowLayout
        parameters={
          <FormPanel title={t("ops.panel.parameters")} hint={t("buildMetadata.panel.options")}>
            <ActionsBar sticky>
              <DryRunToggle checked={dryRun} onChange={setDryRun} />
              <OperationRunControls loading={loading} onRun={handleRun} onCancel={cancel} />
            </ActionsBar>
            <FieldGroup label={t("buildMetadata.panel.scan")}>
              <div className="idx-field-row">
                <label className="idx-label idx-label--wide" htmlFor={filterUpdatedId}>
                  <span className="idx-label__caption">{t("buildMetadata.filterUpdatedAfter")}</span>
                  <input
                    id={filterUpdatedId}
                    className="idx-input idx-input--mono"
                    value={filterUpdatedAfter}
                    onChange={(e) => setFilterUpdatedAfter(e.target.value)}
                    placeholder={t("buildMetadata.filterUpdatedAfterPlaceholder")}
                  />
                  <span className="idx-field-hint">{t("buildMetadata.filterUpdatedAfterHint")}</span>
                </label>
              </div>
            </FieldGroup>
            <FieldGroup label={t("buildMetadata.panel.performance")}>
              <div className="idx-field-row">
                <label className="idx-label idx-label--wide" htmlFor={batchSizeId}>
                  <span className="idx-label__caption">{t("buildMetadata.batchSize")}</span>
                  <input
                    id={batchSizeId}
                    className="idx-input"
                    type="number"
                    min={1}
                    step={100}
                    value={batchSize}
                    onChange={(e) => setBatchSize(e.target.value)}
                    placeholder={t("buildMetadata.batchSizePlaceholder")}
                  />
                  <span className="idx-field-hint">{t("buildMetadata.batchSizeHint")}</span>
                </label>
                <label className="idx-label idx-label--wide" htmlFor={progressIntervalId}>
                  <span className="idx-label__caption">{t("buildMetadata.progressInterval")}</span>
                  <input
                    id={progressIntervalId}
                    className="idx-input"
                    type="number"
                    min={0}
                    step={50}
                    value={progressInterval}
                    onChange={(e) => setProgressInterval(e.target.value)}
                  />
                  <span className="idx-field-hint">{t("buildMetadata.progressIntervalHint")}</span>
                </label>
              </div>
            </FieldGroup>
          </FormPanel>
        }
        results={
          <FormPanel title={t("ops.panel.results")} className="idx-panel--output">
            <BuildOperationOutput
              loading={loading}
              cancelled={cancelled}
              error={error}
              result={result}
              log={log}
            />
          </FormPanel>
        }
      />
    </EditorPage>
  );
}
