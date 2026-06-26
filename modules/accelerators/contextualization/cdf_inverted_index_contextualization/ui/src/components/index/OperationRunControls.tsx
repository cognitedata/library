import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n/types";

type OperationRunControlsProps = {
  loading: boolean;
  onRun: () => void;
  onCancel: () => void;
  runLabelKey?: MessageKey;
  runningLabelKey?: MessageKey;
};

export function OperationRunControls({
  loading,
  onRun,
  onCancel,
  runLabelKey = "ops.run",
  runningLabelKey = "ops.running",
}: OperationRunControlsProps) {
  const { t } = useAppSettings();

  return (
    <>
      {loading ? (
        <button type="button" className="idx-btn" onClick={onCancel}>
          {t("ops.cancel")}
        </button>
      ) : null}
      <button type="button" className="idx-btn idx-btn--primary" disabled={loading} onClick={onRun}>
        {loading ? t(runningLabelKey) : t(runLabelKey)}
      </button>
    </>
  );
}
