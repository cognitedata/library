import type { DragEvent, KeyboardEvent } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";

export type DraggablePanel = "tree" | "properties";

const PANEL_MIME = "application/x-cdf-discovery-panel";

type Props = {
  panel: DraggablePanel;
  labelKey?: MessageKey;
  /** Hamburger trigger to focus when using keyboard on the drag handle. */
  dockMenuTriggerId?: string;
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

export function PanelDragHandle({
  panel,
  labelKey = "layout.dragHandle",
  dockMenuTriggerId,
  onDragStart,
  onDragEnd,
}: Props) {
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
      title={`${t(labelKey)}. ${t("layout.dragHandle.keyboardHint")}`}
      onDragStart={handleDragStart}
      onDragEnd={() => onDragEnd?.()}
      onMouseDown={(e) => e.stopPropagation()}
      onKeyDown={(e: KeyboardEvent<HTMLSpanElement>) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          const focusId =
            dockMenuTriggerId ??
            (panel === "tree" ? "disc-tree-panel-menu-trigger" : "disc-properties-panel-menu-trigger");
          document.getElementById(focusId)?.focus();
        }
      }}
    >
      <span className="disc-panel-drag-handle__grip" aria-hidden>
        ⋮⋮
      </span>
    </span>
  );
}
