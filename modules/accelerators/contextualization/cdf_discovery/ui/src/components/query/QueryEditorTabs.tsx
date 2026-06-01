import { useEffect, useState, type ReactNode } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";

/** Resets to ``defaultTab`` when the owning node (``fieldKey``) changes. */
export function useQueryEditorTabState(
  fieldKey: string,
  defaultTab: string
): readonly [string, (id: string) => void] {
  const [activeTab, setActiveTab] = useState(defaultTab);
  useEffect(() => {
    setActiveTab(defaultTab);
  }, [fieldKey, defaultTab]);
  return [activeTab, setActiveTab] as const;
}

export type QueryEditorTabDef = {
  id: string;
  labelKey: MessageKey;
};

type Props = {
  tabs: QueryEditorTabDef[];
  activeTab: string;
  onActiveTabChange: (id: string) => void;
  ariaLabel: string;
  panelIdPrefix: string;
  children: ReactNode;
};

/** Tab bar for transform query node editors (config / properties / preview). */
export function QueryEditorTabs({
  tabs,
  activeTab,
  onActiveTabChange,
  ariaLabel,
  panelIdPrefix,
  children,
}: Props) {
  const { t } = useAppSettings();

  return (
    <div className="transform-query-editor-tabs">
      <div className="transform-query-editor-tabs__bar-wrap">
        <div role="tablist" aria-label={ariaLabel} className="transform-query-editor-tabs__bar">
          {tabs.map((tab) => {
            const selected = activeTab === tab.id;
            return (
              <button
                key={tab.id}
                type="button"
                role="tab"
                id={`${panelIdPrefix}-tab-${tab.id}`}
                aria-selected={selected}
                aria-controls={`${panelIdPrefix}-panel-${tab.id}`}
                tabIndex={selected ? 0 : -1}
                className={`transform-query-editor-tabs__tab${selected ? " transform-query-editor-tabs__tab--active" : ""}`}
                onClick={() => onActiveTabChange(tab.id)}
              >
                {t(tab.labelKey)}
              </button>
            );
          })}
        </div>
      </div>
      <div
        role="tabpanel"
        id={`${panelIdPrefix}-panel-${activeTab}`}
        aria-labelledby={`${panelIdPrefix}-tab-${activeTab}`}
        className="transform-query-editor-tabs__panel"
      >
        {children}
      </div>
    </div>
  );
}
