import { useAppSettings } from "../../context/AppSettingsContext";
import { redactForDisplay } from "../../api";

type Props = {
  detail: unknown | null;
};

export function PropertiesPanel({ detail }: Props) {
  const { t } = useAppSettings();
  return (
    <aside className="idx-properties-pane">
      <div className="idx-properties-pane__header">
        <span>{t("properties.title")}</span>
      </div>
      <div className="idx-properties-pane__body">
        {detail == null ? (
          <p className="idx-pane__hint">{t("properties.empty")}</p>
        ) : (
          <pre className="idx-json-pre">{JSON.stringify(redactForDisplay(detail), null, 2)}</pre>
        )}
      </div>
    </aside>
  );
}
