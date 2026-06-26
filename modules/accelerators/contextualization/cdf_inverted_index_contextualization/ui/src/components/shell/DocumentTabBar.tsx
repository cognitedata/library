import { useCallback, useMemo, useState, type DragEvent } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { IndexDocumentTab } from "../../types/indexWorkspace";

const TAB_INDEX_MIME = "application/x-idx-tab-index";

type Props = {
  tabs: IndexDocumentTab[];
  activeId: string | null;
  onSelect: (id: string) => void;
  onClose: (id: string) => void;
  onReorder: (fromIndex: number, toIndex: number) => void;
};

export function DocumentTabBar({ tabs, activeId, onSelect, onClose, onReorder }: Props) {
  const { t } = useAppSettings();
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [overIndex, setOverIndex] = useState<number | null>(null);
  const [filter, setFilter] = useState("");
  const normalizedFilter = filter.trim().toLowerCase();
  const visibleTabs = useMemo(
    () =>
      tabs
        .map((tab, index) => ({ tab, index }))
        .filter(({ tab }) =>
          normalizedFilter ? tab.label.toLowerCase().includes(normalizedFilter) : true
        ),
    [normalizedFilter, tabs]
  );

  const clearDrag = useCallback(() => {
    setDragIndex(null);
    setOverIndex(null);
  }, []);

  const handleDragStart = useCallback((e: DragEvent<HTMLDivElement>, index: number) => {
    if ((e.target as HTMLElement).closest(".idx-tab-close")) {
      e.preventDefault();
      return;
    }
    setDragIndex(index);
    setOverIndex(index);
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData(TAB_INDEX_MIME, String(index));
  }, []);

  const handleDrop = useCallback(
    (e: DragEvent<HTMLDivElement>, toIndex: number) => {
      e.preventDefault();
      const raw = e.dataTransfer.getData(TAB_INDEX_MIME);
      const fromIndex = raw === "" ? dragIndex : Number.parseInt(raw, 10);
      if (fromIndex == null || Number.isNaN(fromIndex) || fromIndex === toIndex) {
        clearDrag();
        return;
      }
      const fromSourceIndex = visibleTabs[fromIndex]?.index;
      const toSourceIndex = visibleTabs[toIndex]?.index;
      if (fromSourceIndex == null || toSourceIndex == null) {
        clearDrag();
        return;
      }
      onReorder(fromSourceIndex, toSourceIndex);
      clearDrag();
    },
    [clearDrag, dragIndex, onReorder, visibleTabs]
  );

  return (
    <div className="idx-tabbar">
      <div className="idx-tabbar__filter">
        <input
          type="search"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder={t("tabs.filterPlaceholder")}
          aria-label={t("tabs.filterLabel")}
        />
      </div>
      <div className="idx-tablist" role="tablist" aria-label={t("tabs.tablist")}>
        {visibleTabs.length === 0 ? (
          <span className="idx-tabpanel-empty">{t("tabs.filterNoResults")}</span>
        ) : (
          visibleTabs.map(({ tab }, visibleIndex) => (
            <div
              key={tab.id}
              className="idx-tab"
              data-active={tab.id === activeId}
              role="presentation"
              draggable
              onDragStart={(e) => handleDragStart(e, visibleIndex)}
              onDragOver={(e) => {
                e.preventDefault();
                setOverIndex(visibleIndex);
              }}
              onDrop={(e) => handleDrop(e, visibleIndex)}
              onDragEnd={clearDrag}
              style={overIndex === visibleIndex && dragIndex !== visibleIndex ? { opacity: 0.7 } : undefined}
            >
              <button
                type="button"
                role="tab"
                aria-selected={tab.id === activeId}
                onClick={() => onSelect(tab.id)}
              >
                {tab.label}
              </button>
              <button
                type="button"
                className="idx-tab-close"
                aria-label={t("tabs.closeNamed", { name: tab.label })}
                onClick={() => onClose(tab.id)}
              >
                ×
              </button>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
