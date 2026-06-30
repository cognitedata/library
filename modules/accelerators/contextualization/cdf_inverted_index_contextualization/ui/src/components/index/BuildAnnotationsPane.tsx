import { useState } from "react";
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

export function BuildAnnotationsPane() {
  const { t } = useAppSettings();
  const [dryRun, setDryRun] = useState(true);
  const [fileId, setFileId] = useState("");
  const [detectionMode, setDetectionMode] = useState<"all" | "pattern" | "standard">("all");
  const { loading, cancelled, error, result, log, run, cancel } = useOperationRun();

  const handleRun = () => {
    void run("/api/inverted-index/build/annotations/stream", {
      dry_run: dryRun,
      file_external_id: fileId.trim() || undefined,
      detection_mode: detectionMode,
    });
  };

  return (
    <EditorPage title={t("buildAnnotations.title")} hint={t("buildAnnotations.hint")}>
      <EditorWorkflowLayout
        parameters={
          <FormPanel title={t("ops.panel.parameters")} hint={t("buildAnnotations.panel.filters")}>
            <ActionsBar sticky>
              <DryRunToggle checked={dryRun} onChange={setDryRun} />
              <OperationRunControls loading={loading} onRun={handleRun} onCancel={cancel} />
            </ActionsBar>
            <FieldGroup>
              <div className="idx-field-row">
                <label className="idx-label">
                  {t("buildAnnotations.fileId")}
                  <input
                    className="idx-input"
                    value={fileId}
                    onChange={(e) => setFileId(e.target.value)}
                    placeholder={t("buildAnnotations.fileIdPlaceholder")}
                  />
                </label>
                <label className="idx-label">
                  {t("buildAnnotations.detectionMode")}
                  <select
                    className="idx-select"
                    value={detectionMode}
                    onChange={(e) => setDetectionMode(e.target.value as typeof detectionMode)}
                  >
                    <option value="all">{t("buildAnnotations.detectionAll")}</option>
                    <option value="pattern">{t("buildAnnotations.detectionPattern")}</option>
                    <option value="standard">{t("buildAnnotations.detectionStandard")}</option>
                  </select>
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
