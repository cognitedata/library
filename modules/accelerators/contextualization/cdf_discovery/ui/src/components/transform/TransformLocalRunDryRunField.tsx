import type { MessageKey } from "../../i18n/types";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  dryRun: boolean;
  onDryRunChange: (value: boolean) => void;
  disabled?: boolean;
};

/** Dry-run checkbox for transform local run toolbars. */
export function TransformLocalRunDryRunField({ t, dryRun, onDryRunChange, disabled = false }: Props) {
  return (
    <label className="transform-flow-toolbar__run-scope transform-flow-toolbar__dry-run">
      <input
        type="checkbox"
        checked={dryRun}
        onChange={(e) => onDryRunChange(e.target.checked)}
        disabled={disabled}
        title={t("transform.toolbar.dryRunHint")}
        aria-label={t("transform.toolbar.dryRun")}
      />
      <span className="transform-flow-toolbar__run-scope-label">{t("transform.toolbar.dryRun")}</span>
    </label>
  );
}
