import {
  useCallback,
  useMemo,
  useRef,
  useState,
  type DragEvent,
  type KeyboardEvent,
} from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { DocumentTab } from "../types/discoveryNodes";
import { documentTabButtonId, documentTabPanelIdForTab } from "./documentTabIds";
import { IconTabMaximize, IconTabMinimize } from "./DocumentTabBarIcons";

const TAB_INDEX_MIME = "application/x-disc-tab-index";

type Props = {
  tabs: DocumentTab[];
  activeId: string | null;
  onSelect: (id: string) => void;
  onClose: (id: string) => void;
  onReorder: (fromIndex: number, toIndex: number) => void;
  fullscreen?: {
    open: boolean;
    onToggle: () => void;
    disabled?: boolean;
  };
};

export function DocumentTabBar({ tabs, activeId, onSelect, onClose, onReorder, fullscreen }: Props) {
  const { t } = useAppSettings();
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [overIndex, setOverIndex] = useState<number | null>(null);
  const [filter, setFilter] = useState("");
  const tablistRef = useRef<HTMLDivElement>(null);
  const normalizedFilter = filter.trim().toLowerCase();
  const visibleTabs = useMemo(
    () =>
      tabs
        .map((tab, index) => ({ tab, index }))
        .filter(({ tab }) => (normalizedFilter ? tab.label.toLowerCase().includes(normalizedFilter) : true)),
    [normalizedFilter, tabs]
  );

  const clearDrag = useCallback(() => {
    setDragIndex(null);
    setOverIndex(null);
  }, []);

  const handleDragStart = useCallback((e: DragEvent<HTMLDivElement>, index: number) => {
    if ((e.target as HTMLElement).closest(".disc-tab-close")) {
      e.preventDefault();
      return;
    }
    setDragIndex(index);
    setOverIndex(index);
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData(TAB_INDEX_MIME, String(index));
    if (e.currentTarget instanceof HTMLElement) {
      e.dataTransfer.setDragImage(e.currentTarget, 24, 16);
    }
  }, []);

  const handleDragOver = useCallback((e: DragEvent<HTMLDivElement>, index: number) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    setOverIndex(index);
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

  const focusTabAt = useCallback((index: number) => {
    const el = tablistRef.current?.querySelector<HTMLButtonElement>(
      `[role="tab"]:nth-of-type(${index + 1})`
    );
    el?.focus();
  }, []);

  const onTabListKeyDown = useCallback(
    (e: KeyboardEvent<HTMLDivElement>) => {
      if (visibleTabs.length === 0) return;
      const currentIndex = visibleTabs.findIndex(({ tab }) => tab.id === activeId);
      const idx = currentIndex >= 0 ? currentIndex : 0;
      if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
        e.preventDefault();
        const delta = e.key === "ArrowRight" ? 1 : -1;
        const next = (idx + delta + visibleTabs.length) % visibleTabs.length;
        onSelect(visibleTabs[next].tab.id);
        focusTabAt(next);
      } else if (e.key === "Home") {
        e.preventDefault();
        onSelect(visibleTabs[0].tab.id);
        focusTabAt(0);
      } else if (e.key === "End") {
        e.preventDefault();
        onSelect(visibleTabs[visibleTabs.length - 1].tab.id);
        focusTabAt(visibleTabs.length - 1);
      }
    },
    [activeId, focusTabAt, onSelect, visibleTabs]
  );

  if (tabs.length === 0) return null;

  return (
    <div className={`disc-tab-bar-wrap${dragIndex != null ? " disc-tab-bar-wrap--dragging" : ""}`}>
      <label className="disc-tab-filter">
        <input
          type="search"
          className="disc-input disc-tab-filter__input"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder={t("tabs.filterPlaceholder")}
          aria-label={t("tabs.filterLabel")}
        />
      </label>
      <div
        ref={tablistRef}
        className={`disc-tab-bar${dragIndex != null ? " disc-tab-bar--dragging" : ""}`}
        role="tablist"
        aria-label={t("tabs.tablist")}
        tabIndex={0}
        onKeyDown={onTabListKeyDown}
      >
        {visibleTabs.map(({ tab, index: sourceIndex }, index) => (
          <div
            key={tab.id}
            role="presentation"
            className={`disc-tab-item${tab.id === activeId ? " disc-tab-item--active" : ""}${
              dragIndex === index ? " disc-tab-item--dragging" : ""
            }${overIndex === index && dragIndex !== index ? " disc-tab-item--drag-over" : ""}`}
            draggable
            onDragStart={(e) => handleDragStart(e, index)}
            onDragOver={(e) => handleDragOver(e, index)}
            onDragLeave={() => setOverIndex((prev) => (prev === index ? null : prev))}
            onDrop={(e) => handleDrop(e, index)}
            onDragEnd={clearDrag}
          >
            <button
              type="button"
              role="tab"
              id={documentTabButtonId(tab.id)}
              aria-selected={tab.id === activeId}
              aria-controls={documentTabPanelIdForTab(tab.id)}
              tabIndex={tab.id === activeId ? 0 : -1}
              className="disc-tab"
              onClick={() => onSelect(tab.id)}
            >
              <span className="disc-tab__label">{tab.label}</span>
            </button>
            <button
              type="button"
              className="disc-tab-close"
              aria-label={t("tabs.closeNamed", { name: tab.label })}
              onClick={(e) => {
                e.stopPropagation();
                onClose(tab.id);
              }}
            >
              ×
            </button>
            <div className="disc-tab-reorder" role="group" aria-label={t("tabs.reorderGroup", { name: tab.label })}>
              <button
                type="button"
                className="disc-tab-reorder__btn"
                disabled={index === 0}
                aria-label={t("tabs.moveLeft")}
                onClick={() => onReorder(sourceIndex, visibleTabs[index - 1].index)}
              >
                ‹
              </button>
              <button
                type="button"
                className="disc-tab-reorder__btn"
                disabled={index === visibleTabs.length - 1}
                aria-label={t("tabs.moveRight")}
                onClick={() => onReorder(sourceIndex, visibleTabs[index + 1].index)}
              >
                ›
              </button>
            </div>
          </div>
        ))}
      </div>
      {visibleTabs.length === 0 ? (
        <div className="disc-tab-filter-empty" role="status" aria-live="polite">
          {t("tabs.filterNoResults")}
        </div>
      ) : null}
      {fullscreen ? (
        <div className="disc-tab-bar-actions">
          <button
            type="button"
            className="disc-btn disc-tab-bar-actions__btn disc-tab-bar-actions__btn--icon"
            disabled={fullscreen.disabled}
            title={fullscreen.open ? t("tabs.exitFullscreen") : t("tabs.fullscreenTooltip")}
            aria-label={fullscreen.open ? t("tabs.exitFullscreen") : t("tabs.fullscreen")}
            aria-pressed={fullscreen.open}
            onClick={fullscreen.onToggle}
          >
            {fullscreen.open ? <IconTabMinimize /> : <IconTabMaximize />}
          </button>
        </div>
      ) : null}
    </div>
  );
}
