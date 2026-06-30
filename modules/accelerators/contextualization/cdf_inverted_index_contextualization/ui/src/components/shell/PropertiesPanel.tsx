import { useAppSettings } from "../../context/AppSettingsContext";
import { redactForDisplay } from "../../api";
import type { MessageKey } from "../../i18n";
import { rowSummaryFields } from "../../utils/resultViews";
import { CollapsibleJson } from "../shared/CollapsibleJson";

type Props = {
  detail: unknown | null;
  collapsed?: boolean;
  onToggleCollapse?: () => void;
};

export function PropertiesPanel({ detail, collapsed = false, onToggleCollapse }: Props) {
  const { t } = useAppSettings();
  const summary = detail != null ? rowSummaryFields(detail) : [];

  return (
    <aside className={`idx-properties-pane${collapsed ? " idx-properties-pane--collapsed" : ""}`}>
      <div className="idx-properties-pane__header">
        <span>{t("properties.title")}</span>
        {onToggleCollapse ? (
          <button
            type="button"
            className="idx-btn idx-btn--sm idx-btn--ghost"
            onClick={onToggleCollapse}
            aria-expanded={!collapsed}
            aria-label={collapsed ? t("properties.expand") : t("properties.collapse")}
          >
            {collapsed ? "▴" : "▾"}
          </button>
        ) : null}
      </div>
      {!collapsed ? (
        <div className="idx-properties-pane__body">
          {detail == null ? (
            <p className="idx-pane__hint">{t("properties.empty")}</p>
          ) : (
            <>
              {summary.length > 0 ? (
                <dl className="idx-properties-summary">
                  {summary.map((field) => (
                    <div key={field.labelKey} className="idx-properties-summary__row">
                      <dt className="idx-properties-summary__label">
                        {t(field.labelKey as MessageKey)}
                      </dt>
                      <dd className="idx-properties-summary__value">{field.value}</dd>
                    </div>
                  ))}
                </dl>
              ) : null}
              <CollapsibleJson data={redactForDisplay(detail)} />
            </>
          )}
        </div>
      ) : null}
    </aside>
  );
}
