import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useId,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";

export function panelHeaderMenuTriggerId(menuId: string): string {
  return `${menuId}-trigger`;
}

const PanelMenuCloseContext = createContext<(() => void) | null>(null);

function ChevronCollapseIcon({ collapsed }: { collapsed: boolean }) {
  return (
    <svg
      className="disc-panel-header-actions__chevron"
      width={18}
      height={18}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
      data-collapsed={collapsed ? "true" : "false"}
    >
      <path
        d="M6 9l6 6 6-6"
        stroke="currentColor"
        strokeWidth={2}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function MoreIcon() {
  return (
    <svg
      className="disc-panel-header-actions__more-icon"
      width={18}
      height={18}
      viewBox="0 0 24 24"
      fill="currentColor"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
    >
      <circle cx="12" cy="5" r="1.75" />
      <circle cx="12" cy="12" r="1.75" />
      <circle cx="12" cy="19" r="1.75" />
    </svg>
  );
}

type PanelHeaderActionsProps = {
  menuId: string;
  menuLabelKey: MessageKey;
  collapsed: boolean;
  collapseLabelKey: MessageKey;
  expandLabelKey: MessageKey;
  onToggleCollapse: () => void;
  children: ReactNode;
};

/** Panel title-bar actions: collapse chevron + overflow menu (dock layout, etc.). */
export function PanelHeaderActions({
  menuId,
  menuLabelKey,
  collapsed,
  collapseLabelKey,
  expandLabelKey,
  onToggleCollapse,
  children,
}: PanelHeaderActionsProps) {
  const { t } = useAppSettings();
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerId = panelHeaderMenuTriggerId(menuId);
  const popoverId = useId();
  const close = useCallback(() => setOpen(false), []);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (e: PointerEvent) => {
      if (!rootRef.current?.contains(e.target as Node)) close();
    };
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") close();
    };
    window.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [open, close]);

  return (
    <div className="disc-panel-header-actions" ref={rootRef}>
      <button
        type="button"
        className="disc-btn disc-btn--sm disc-panel-header-actions__collapse"
        aria-expanded={!collapsed}
        aria-controls={undefined}
        title={collapsed ? t(expandLabelKey) : t(collapseLabelKey)}
        aria-label={collapsed ? t(expandLabelKey) : t(collapseLabelKey)}
        onClick={onToggleCollapse}
      >
        <ChevronCollapseIcon collapsed={collapsed} />
      </button>
      <div className="disc-panel-header-actions__menu">
        <button
          id={triggerId}
          type="button"
          className="disc-btn disc-btn--sm disc-panel-header-actions__menu-trigger"
          aria-expanded={open}
          aria-haspopup="menu"
          aria-controls={open ? popoverId : undefined}
          title={t("layout.panelMenu.open")}
          aria-label={t(menuLabelKey)}
          onClick={() => setOpen((v) => !v)}
        >
          <MoreIcon />
        </button>
        {open ? (
          <PanelMenuCloseContext.Provider value={close}>
            <div id={popoverId} className="disc-panel-header-actions__popover" role="menu">
              {children}
            </div>
          </PanelMenuCloseContext.Provider>
        ) : null}
      </div>
    </div>
  );
}

type PanelDockMenuItemProps = {
  checked: boolean;
  labelKey: MessageKey;
  icon: ReactNode;
  onSelect: () => void;
};

/** Exclusive dock target in a panel overflow menu (`menuitemradio`). */
export function PanelDockMenuItem({ checked, labelKey, icon, onSelect }: PanelDockMenuItemProps) {
  const { t } = useAppSettings();
  const closeMenu = useContext(PanelMenuCloseContext);
  return (
    <button
      type="button"
      className="disc-panel-dock-menu-item"
      role="menuitemradio"
      aria-checked={checked}
      onClick={() => {
        onSelect();
        closeMenu?.();
      }}
    >
      <span className="disc-panel-dock-menu-item__icon" aria-hidden>
        {icon}
      </span>
      <span className="disc-panel-dock-menu-item__label">{t(labelKey)}</span>
    </button>
  );
}

type PanelMenuSectionProps = {
  labelKey: MessageKey;
  children: ReactNode;
};

export function PanelMenuSection({ labelKey, children }: PanelMenuSectionProps) {
  const { t } = useAppSettings();
  return (
    <div className="disc-panel-header-actions__section" role="presentation">
      <span className="disc-panel-header-actions__section-label">{t(labelKey)}</span>
      <div className="disc-panel-header-actions__section-items" role="group">
        {children}
      </div>
    </div>
  );
}
