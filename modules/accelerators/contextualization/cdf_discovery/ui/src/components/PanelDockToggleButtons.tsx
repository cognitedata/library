import type { ReactNode } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { PropertiesPanelDock, TreePanelSide } from "../hooks/useDiscoveryPanelLayout";
import { PanelDockMenuItem, PanelMenuSection } from "./PanelHeaderActions";

const iconStroke = 2;

function DockToggleIcon({ children }: { children: ReactNode }) {
  return (
    <svg
      className="disc-panel-dock-menu-item__svg"
      width={18}
      height={18}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
    >
      {children}
    </svg>
  );
}

function TreeDockLeftIcon() {
  return (
    <DockToggleIcon>
      <rect x="4" y="5" width="6" height="14" rx="1" fill="currentColor" opacity="0.35" />
      <rect x="12" y="5" width="8" height="14" rx="1" stroke="currentColor" strokeWidth={iconStroke} />
    </DockToggleIcon>
  );
}

function TreeDockRightIcon() {
  return (
    <DockToggleIcon>
      <rect x="4" y="5" width="8" height="14" rx="1" stroke="currentColor" strokeWidth={iconStroke} />
      <rect x="14" y="5" width="6" height="14" rx="1" fill="currentColor" opacity="0.35" />
    </DockToggleIcon>
  );
}

function PropsDockUnderTreeIcon() {
  return (
    <DockToggleIcon>
      <rect x="4" y="5" width="7" height="8" rx="1" fill="currentColor" opacity="0.35" />
      <rect x="4" y="14" width="7" height="5" rx="1" stroke="currentColor" strokeWidth={iconStroke} />
      <rect x="13" y="5" width="7" height="14" rx="1" stroke="currentColor" strokeWidth={iconStroke} />
    </DockToggleIcon>
  );
}

function PropsDockBottomIcon() {
  return (
    <DockToggleIcon>
      <rect x="4" y="5" width="16" height="9" rx="1" stroke="currentColor" strokeWidth={iconStroke} />
      <rect x="4" y="15" width="16" height="4" rx="1" fill="currentColor" opacity="0.35" />
    </DockToggleIcon>
  );
}

function PropsDockRightIcon() {
  return (
    <DockToggleIcon>
      <rect x="4" y="5" width="9" height="14" rx="1" stroke="currentColor" strokeWidth={iconStroke} />
      <rect x="14" y="5" width="6" height="14" rx="1" fill="currentColor" opacity="0.35" />
    </DockToggleIcon>
  );
}

type TreeProps = {
  treeSide: TreePanelSide;
  onDockTree: (side: TreePanelSide) => void;
};

export function TreePanelDockMenu({ treeSide, onDockTree }: TreeProps) {
  const { t } = useAppSettings();
  return (
    <PanelMenuSection labelKey="layout.keyboardDock.tree">
      <div
        id="disc-tree-dock-toggles"
        className="disc-panel-dock-menu-items"
        role="group"
        aria-label={t("layout.keyboardDock.tree")}
        tabIndex={-1}
      >
      <PanelDockMenuItem
        checked={treeSide === "left"}
        labelKey="layout.dropZone.treeLeft"
        icon={<TreeDockLeftIcon />}
        onSelect={() => onDockTree("left")}
      />
      <PanelDockMenuItem
        checked={treeSide === "right"}
        labelKey="layout.dropZone.treeRight"
        icon={<TreeDockRightIcon />}
        onSelect={() => onDockTree("right")}
      />
      </div>
    </PanelMenuSection>
  );
}

type PropertiesProps = {
  propertiesDock: PropertiesPanelDock;
  onDockProperties: (dock: PropertiesPanelDock) => void;
};

export function PropertiesPanelDockMenu({ propertiesDock, onDockProperties }: PropertiesProps) {
  const { t } = useAppSettings();
  return (
    <PanelMenuSection labelKey="layout.keyboardDock.properties">
      <div
        id="disc-properties-dock-toggles"
        className="disc-panel-dock-menu-items"
        role="group"
        aria-label={t("layout.keyboardDock.properties")}
        tabIndex={-1}
      >
        <PanelDockMenuItem
          checked={propertiesDock === "left-bottom"}
          labelKey="layout.dropZone.propsUnderTree"
          icon={<PropsDockUnderTreeIcon />}
          onSelect={() => onDockProperties("left-bottom")}
        />
        <PanelDockMenuItem
          checked={propertiesDock === "bottom"}
          labelKey="layout.dropZone.propsBottom"
          icon={<PropsDockBottomIcon />}
          onSelect={() => onDockProperties("bottom")}
        />
        <PanelDockMenuItem
          checked={propertiesDock === "right"}
          labelKey="layout.dropZone.propsRight"
          icon={<PropsDockRightIcon />}
          onSelect={() => onDockProperties("right")}
        />
      </div>
    </PanelMenuSection>
  );
}
