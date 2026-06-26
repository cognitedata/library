import { useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
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
    <div className="idx-pane">
      <h2 className="idx-pane__title">{t("buildAnnotations.title")}</h2>
      <p className="idx-pane__hint">{t("buildAnnotations.hint")}</p>
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
        <DryRunToggle checked={dryRun} onChange={setDryRun} />
        <OperationRunControls loading={loading} onRun={handleRun} onCancel={cancel} />
      </div>
      <BuildOperationOutput
        loading={loading}
        cancelled={cancelled}
        error={error}
        result={result}
        log={log}
      />
    </div>
  );
}
