import type { DragEvent } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";

export type DraggablePanel = "tree" | "properties";

const PANEL_MIME = "application/x-cdf-discovery-panel";

type Props = {
  panel: DraggablePanel;
  labelKey?: MessageKey;
  onDragStart?: (panel: DraggablePanel) => void;
  onDragEnd?: () => void;
};

export function panelDragMimeType(): string {
  return PANEL_MIME;
}

export function readPanelDragData(dataTransfer: DataTransfer): DraggablePanel | null {
  const raw = dataTransfer.getData(PANEL_MIME) || dataTransfer.getData("text/plain");
  return raw === "tree" || raw === "properties" ? raw : null;
}

export function PanelDragHandle({ panel, labelKey = "layout.dragHandle", onDragStart, onDragEnd }: Props) {
  const { t } = useAppSettings();

  const handleDragStart = (e: DragEvent<HTMLSpanElement>) => {
    e.dataTransfer.setData(PANEL_MIME, panel);
    e.dataTransfer.setData("text/plain", panel);
    e.dataTransfer.effectAllowed = "move";
    onDragStart?.(panel);
  };

  return (
    <span
      className="disc-panel-drag-handle"
      draggable
      role="button"
      tabIndex={0}
      aria-label={t(labelKey)}
      title={t(labelKey)}
      onDragStart={handleDragStart}
      onDragEnd={() => onDragEnd?.()}
      onMouseDown={(e) => e.stopPropagation()}
    >
      <span className="disc-panel-drag-handle__grip" aria-hidden>
        ⋮⋮
      </span>
    </span>
  );
}
