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

  if (tabs.length === 0) return null;

  return (
    <div className={`idx-tab-bar-wrap${dragIndex != null ? " idx-tab-bar-wrap--dragging" : ""}`}>
      <label className="idx-tab-filter">
        <input
          type="search"
          className="idx-input idx-tab-filter__input"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder={t("tabs.filterPlaceholder")}
          aria-label={t("tabs.filterLabel")}
        />
      </label>
      <div
        className={`idx-tab-bar${dragIndex != null ? " idx-tab-bar--dragging" : ""}`}
        role="tablist"
        aria-label={t("tabs.tablist")}
      >
        {visibleTabs.map(({ tab }, visibleIndex) => (
          <div
            key={tab.id}
            className={`idx-tab${tab.id === activeId ? " idx-tab--active" : ""}${
              dragIndex === visibleIndex ? " idx-tab--dragging" : ""
            }${overIndex === visibleIndex && dragIndex !== visibleIndex ? " idx-tab--drag-over" : ""}`}
            role="presentation"
            draggable
            onDragStart={(e) => handleDragStart(e, visibleIndex)}
            onDragOver={(e) => {
              e.preventDefault();
              setOverIndex(visibleIndex);
            }}
            onDrop={(e) => handleDrop(e, visibleIndex)}
            onDragEnd={clearDrag}
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
        ))}
      </div>
      {visibleTabs.length === 0 ? (
        <div className="idx-tab-filter-empty" role="status" aria-live="polite">
          {t("tabs.filterNoResults")}
        </div>
      ) : null}
    </div>
  );
}
