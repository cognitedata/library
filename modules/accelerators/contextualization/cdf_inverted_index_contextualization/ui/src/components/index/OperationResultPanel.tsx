import { useAppSettings } from "../../context/AppSettingsContext";
import { redactForDisplay } from "../../api";
import { OperationConsole } from "./OperationConsole";

type Props = {
  loading: boolean;
  cancelled?: boolean;
  error: string | null;
  result: unknown | null;
  log?: string;
  showConsole?: boolean;
  showResult?: boolean;
};

export function OperationResultPanel({
  loading,
  cancelled = false,
  error,
  result,
  log = "",
  showConsole = false,
  showResult = true,
}: Props) {
  const { t } = useAppSettings();

  return (
    <div className="idx-operation-output">
      {showConsole ? <OperationConsole log={log} loading={loading} /> : null}
      {loading && !showConsole ? <p className="idx-pane__hint">{t("ops.running")}</p> : null}
      {cancelled ? (
        <div className="idx-operation-result">
          <p className="idx-operation-result__status">{t("ops.cancelled")}</p>
        </div>
      ) : null}
      {error ? (
        <div className="idx-operation-result">
          <p className="idx-operation-result__status idx-operation-result__status--error">
            {t("ops.error")}: {error}
          </p>
        </div>
      ) : null}
      {!loading && showResult && result != null ? (
        <div className="idx-operation-result">
          <p className="idx-operation-result__status idx-operation-result__status--ok">{t("ops.result")}</p>
          <pre className="idx-json-pre">{JSON.stringify(redactForDisplay(result), null, 2)}</pre>
        </div>
      ) : null}
    </div>
  );
}

export function DryRunToggle({
  checked,
  onChange,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
}) {
  const { t } = useAppSettings();
  return (
    <label className="idx-checkbox-label">
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
      {t("ops.dryRun")}
    </label>
  );
}
