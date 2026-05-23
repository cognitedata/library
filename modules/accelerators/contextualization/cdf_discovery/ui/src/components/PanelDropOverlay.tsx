import { useCallback, useState, type CSSProperties, type DragEvent } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { PropertiesPanelDock, TreePanelSide } from "../hooks/useDiscoveryPanelLayout";
import { readPanelDragData, type DraggablePanel } from "./PanelDragHandle";

type Props = {
  dragging: DraggablePanel | null;
  treeSide: TreePanelSide;
  treeWidth: number;
  onDropTree: (side: TreePanelSide) => void;
  onDropProperties: (dock: PropertiesPanelDock) => void;
};

type DropZoneProps = {
  zoneId: string;
  label: string;
  style: CSSProperties;
  dragging: DraggablePanel;
  accept: DraggablePanel;
  activeZone: string | null;
  onActivate: (zoneId: string | null) => void;
  onDrop: () => void;
};

function DropZone({ zoneId, label, style, dragging, accept, activeZone, onActivate, onDrop }: DropZoneProps) {
  const canAccept = dragging === accept;

  const allowDrop = useCallback(
    (e: DragEvent<HTMLDivElement>) => {
      if (!canAccept) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = "move";
      onActivate(zoneId);
    },
    [canAccept, onActivate, zoneId]
  );

  const handleDragLeave = useCallback(
    (e: DragEvent<HTMLDivElement>) => {
      if (e.currentTarget.contains(e.relatedTarget as Node | null)) return;
      onActivate(null);
    },
    [onActivate]
  );

  return (
    <div
      className={`disc-panel-drop-zone disc-panel-drop-zone--ready${activeZone === zoneId ? " disc-panel-drop-zone--active" : ""}`}
      style={style}
      onDragOver={allowDrop}
      onDragEnter={allowDrop}
      onDragLeave={handleDragLeave}
      onDrop={(e) => {
        e.preventDefault();
        const dropped = readPanelDragData(e.dataTransfer) ?? dragging;
        if (dropped !== accept) return;
        onDrop();
        onActivate(null);
      }}
    >
      <span className="disc-panel-drop-zone__label">{label}</span>
    </div>
  );
}

export function PanelDropOverlay({ dragging, treeSide, treeWidth, onDropTree, onDropProperties }: Props) {
  const { t } = useAppSettings();
  const [activeZone, setActiveZone] = useState<string | null>(null);

  if (!dragging) return null;

  const sideWidth = Math.max(treeWidth, 200);

  if (dragging === "tree") {
    return (
      <div className="disc-panel-drop-overlay" aria-live="polite">
        <DropZone
          zoneId="tree-left"
          label={t("layout.dropZone.treeLeft")}
          style={{ left: 0, top: 0, bottom: 0, width: "32%" }}
          dragging={dragging}
          accept="tree"
          activeZone={activeZone}
          onActivate={setActiveZone}
          onDrop={() => onDropTree("left")}
        />
        <DropZone
          zoneId="tree-right"
          label={t("layout.dropZone.treeRight")}
          style={{ right: 0, top: 0, bottom: 0, width: "32%" }}
          dragging={dragging}
          accept="tree"
          activeZone={activeZone}
          onActivate={setActiveZone}
          onDrop={() => onDropTree("right")}
        />
      </div>
    );
  }

  const underTreeStyle: CSSProperties =
    treeSide === "left"
      ? { left: 0, bottom: 0, width: sideWidth, height: "38%" }
      : { right: 0, bottom: 0, width: sideWidth, height: "38%" };

  const bottomStyle: CSSProperties = {
    left: sideWidth,
    right: sideWidth,
    bottom: 0,
    height: "28%",
  };

  const rightStyle: CSSProperties = { right: 0, top: 0, bottom: 0, width: sideWidth };

  return (
    <div className="disc-panel-drop-overlay" aria-live="polite">
      <DropZone
        zoneId="props-under-tree"
        label={t("layout.dropZone.propsUnderTree")}
        style={underTreeStyle}
        dragging={dragging}
        accept="properties"
        activeZone={activeZone}
        onActivate={setActiveZone}
        onDrop={() => onDropProperties("left-bottom")}
      />
      <DropZone
        zoneId="props-bottom"
        label={t("layout.dropZone.propsBottom")}
        style={bottomStyle}
        dragging={dragging}
        accept="properties"
        activeZone={activeZone}
        onActivate={setActiveZone}
        onDrop={() => onDropProperties("bottom")}
      />
      <DropZone
        zoneId="props-right"
        label={t("layout.dropZone.propsRight")}
        style={rightStyle}
        dragging={dragging}
        accept="properties"
        activeZone={activeZone}
        onActivate={setActiveZone}
        onDrop={() => onDropProperties("right")}
      />
    </div>
  );
}
