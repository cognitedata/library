import { useAppSettings } from "../../context/AppSettingsContext";

type Props = {
  dirty: boolean;
  loading: boolean;
  saving: boolean;
  error: string | null;
  onReload: () => void;
  onSave: () => void;
  onMirror?: () => void;
};

export function GovernanceToolbar({
  dirty,
  loading,
  saving,
  error,
  onReload,
  onSave,
  onMirror,
}: Props) {
  const { t } = useAppSettings();
  return (
    <div className="gov-config-toolbar disc-gov-pane-actions" role="toolbar">
      <button
        type="button"
        className="gov-btn gov-btn--ghost gov-btn--sm"
        disabled={loading}
        onClick={onReload}
      >
        {t("governance.reload")}
      </button>
      <button
        type="button"
        className="gov-btn gov-btn--primary gov-btn--sm"
        disabled={!dirty || saving}
        onClick={onSave}
      >
        {saving ? t("governance.saving") : t("governance.save")}
      </button>
      {onMirror && (
        <button type="button" className="gov-btn gov-btn--sm" disabled={loading} onClick={onMirror}>
          {t("governance.mirror")}
        </button>
      )}
      {dirty && (
        <span className="gov-hint gov-hint--warn" role="status">
          {t("governance.unsaved")}
        </span>
      )}
      {error && <span className="gov-hint gov-hint--warn">{error}</span>}
    </div>
  );
}
