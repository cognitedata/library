import type { ReactNode } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { OperationConsole } from "../index/OperationConsole";
import { CollapsibleJson } from "../shared/CollapsibleJson";
import { MetricSummary } from "../shared/MetricSummary";
import { OperationStatus } from "../shared/OperationStatus";
import type { MessageKey } from "../../i18n";

type MetricDef = { key: string; labelKey: MessageKey };

type Props = {
  loading: boolean;
  cancelled?: boolean;
  error: string | null;
  result: unknown | null;
  log?: string;
  showConsole?: boolean;
  showResult?: boolean;
  metrics?: MetricDef[];
  metricsData?: Record<string, unknown> | null;
  rawResult?: unknown;
  children?: ReactNode;
};

export function OperationResultPanel({
  loading,
  cancelled = false,
  error,
  result,
  log = "",
  showConsole = false,
  showResult = true,
  metrics,
  metricsData,
  rawResult,
  children,
}: Props) {
  const { t } = useAppSettings();
  const displayMetrics = metricsData ?? (result && typeof result === "object" && !Array.isArray(result)
    ? (result as Record<string, unknown>)
    : null);
  const raw = rawResult ?? result;
  const hasStructured = Boolean(children) || Boolean(metrics?.length && displayMetrics);

  return (
    <div className="idx-operation-output">
      {showConsole ? <OperationConsole log={log} loading={loading} /> : null}
      {!showConsole && loading ? <p className="idx-pane__hint">{t("ops.running")}</p> : null}
      <OperationStatus
        loading={loading}
        cancelled={cancelled}
        error={error}
        hasResult={!loading && !cancelled && !error && (result != null || Boolean(children))}
      />
      {!loading && showResult ? (
        <>
          {metrics?.length ? <MetricSummary data={displayMetrics} metrics={metrics} /> : null}
          {children}
          {hasStructured && raw != null ? <CollapsibleJson data={raw} /> : null}
          {!hasStructured && result != null ? <CollapsibleJson data={raw} defaultOpen /> : null}
        </>
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
    <label className="idx-checkbox-label" title={t("ops.dryRunHint")}>
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
      {t("ops.dryRun")}
    </label>
  );
}
