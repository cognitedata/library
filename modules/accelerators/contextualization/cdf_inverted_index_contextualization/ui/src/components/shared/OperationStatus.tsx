import { useAppSettings } from "../../context/AppSettingsContext";

type Props = {
  loading: boolean;
  cancelled?: boolean;
  error: string | null;
  hasResult?: boolean;
};

export function OperationStatus({ loading, cancelled = false, error, hasResult = false }: Props) {
  const { t } = useAppSettings();

  if (loading) {
    return <div className="idx-status-banner idx-status-banner--running">{t("ops.running")}</div>;
  }
  if (cancelled) {
    return <div className="idx-status-banner idx-status-banner--cancelled">{t("ops.cancelled")}</div>;
  }
  if (error) {
    return (
      <div className="idx-status-banner idx-status-banner--error">
        {t("ops.error")}: {error}
      </div>
    );
  }
  if (hasResult) {
    return <div className="idx-status-banner idx-status-banner--ok">{t("ops.result")}</div>;
  }
  return null;
}
