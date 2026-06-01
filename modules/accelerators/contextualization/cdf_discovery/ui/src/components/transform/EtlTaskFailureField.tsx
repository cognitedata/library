import type { MessageKey } from "../../i18n/types";

export const TASK_FAILURE_SKIP = "skipTask";
export const TASK_FAILURE_ABORT = "abortWorkflow";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  value: string;
  onChange: (onFailure: string) => void;
};

export function EtlTaskFailureField({ t, value, onChange }: Props) {
  const selected =
    String(value || "").trim() === TASK_FAILURE_ABORT ? TASK_FAILURE_ABORT : TASK_FAILURE_SKIP;

  return (
    <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
      {t("transform.config.onFailure")}
      <select
        className="gov-input"
        style={{ marginTop: "0.35rem", display: "block" }}
        value={selected}
        onChange={(e) => onChange(e.target.value)}
        title={t("transform.config.onFailureHint")}
      >
        <option value={TASK_FAILURE_SKIP}>{t("transform.config.onFailureSkipTask")}</option>
        <option value={TASK_FAILURE_ABORT}>{t("transform.config.onFailureAbortWorkflow")}</option>
      </select>
      <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.35rem" }}>
        {t("transform.config.onFailureHint")}
      </p>
    </label>
  );
}
