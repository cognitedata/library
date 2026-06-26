import { useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import { BuildOperationOutput } from "./BuildOperationOutput";
import { DryRunToggle } from "./OperationResultPanel";
import { OperationRunControls } from "./OperationRunControls";

export function BuildMetadataPane() {
  const { t } = useAppSettings();
  const [dryRun, setDryRun] = useState(true);
  const { loading, cancelled, error, result, log, run, cancel } = useOperationRun();

  const handleRun = () => {
    void run("/api/inverted-index/build/metadata/stream", { dry_run: dryRun });
  };

  return (
    <div className="idx-pane">
      <h2 className="idx-pane__title">{t("buildMetadata.title")}</h2>
      <p className="idx-pane__hint">{t("buildMetadata.hint")}</p>
      <div className="idx-field-row">
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
