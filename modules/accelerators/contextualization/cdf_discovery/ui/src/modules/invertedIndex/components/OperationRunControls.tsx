import { useInvertedIndexT } from "../hooks/useInvertedIndexT";

type OperationRunControlsProps = {
  loading: boolean;
  onRun: () => void;
  onCancel: () => void;
  runLabelKey?: string;
  runningLabelKey?: string;
};

export function OperationRunControls({
  loading,
  onRun,
  onCancel,
  runLabelKey = "ops.run",
  runningLabelKey = "ops.running",
}: OperationRunControlsProps) {
  const { t } = useInvertedIndexT();

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
