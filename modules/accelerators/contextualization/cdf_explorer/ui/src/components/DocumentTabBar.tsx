import { useCallback, useState, type DragEvent } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { DocumentTab } from "../types/explorerNodes";

const TAB_INDEX_MIME = "application/x-exp-tab-index";

type Props = {
  tabs: DocumentTab[];
  activeId: string | null;
  onSelect: (id: string) => void;
  onClose: (id: string) => void;
  onReorder: (fromIndex: number, toIndex: number) => void;
};

export function DocumentTabBar({ tabs, activeId, onSelect, onClose, onReorder }: Props) {
  const { t } = useAppSettings();
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [overIndex, setOverIndex] = useState<number | null>(null);

  const clearDrag = useCallback(() => {
    setDragIndex(null);
    setOverIndex(null);
  }, []);

  const handleDragStart = useCallback((e: DragEvent<HTMLDivElement>, index: number) => {
    if ((e.target as HTMLElement).closest(".exp-tab-close")) {
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
      onReorder(fromIndex, toIndex);
      clearDrag();
    },
    [clearDrag, dragIndex, onReorder]
  );

  if (tabs.length === 0) return null;

  return (
    <div
      className={`exp-tab-bar${dragIndex != null ? " exp-tab-bar--dragging" : ""}`}
      role="tablist"
    >
      {tabs.map((tab, index) => (
        <div
          key={tab.id}
          role="presentation"
          className={`exp-tab-item${tab.id === activeId ? " exp-tab-item--active" : ""}${
            dragIndex === index ? " exp-tab-item--dragging" : ""
          }${overIndex === index && dragIndex !== index ? " exp-tab-item--drag-over" : ""}`}
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
            aria-selected={tab.id === activeId}
            className="exp-tab"
            onClick={() => onSelect(tab.id)}
          >
            {tab.label}
            <span
              className="exp-tab-close"
              role="button"
              tabIndex={0}
              aria-label={t("tabs.close")}
              onClick={(e) => {
                e.stopPropagation();
                onClose(tab.id);
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.stopPropagation();
                  onClose(tab.id);
                }
              }}
            >
              ×
            </span>
          </button>
        </div>
      ))}
    </div>
  );
}
