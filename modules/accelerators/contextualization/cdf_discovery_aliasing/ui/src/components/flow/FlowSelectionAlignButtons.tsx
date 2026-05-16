import type { ReactNode } from "react";
import type { MessageKey } from "../../i18n";
import type { AlignFlowSelectionMode } from "./alignSelectedNodes";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  disabled: boolean;
  onAlign: (mode: AlignFlowSelectionMode) => void;
};

const iconStroke = 2;

function AlignLeftIcon() {
  return (
    <svg
      className="kea-flow-align-icon"
      width={18}
      height={18}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
    >
      <path
        d="M4 4v16M8 7h12M8 12h8M8 17h10"
        stroke="currentColor"
        strokeWidth={iconStroke}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function AlignRightIcon() {
  return (
    <svg
      className="kea-flow-align-icon"
      width={18}
      height={18}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
    >
      <path
        d="M20 4v16M4 7h16M8 12h12M6 17h14"
        stroke="currentColor"
        strokeWidth={iconStroke}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function AlignTopIcon() {
  return (
    <svg
      className="kea-flow-align-icon"
      width={18}
      height={18}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
    >
      <path
        d="M4 4h16M7 8v12M12 8v8M17 8v6"
        stroke="currentColor"
        strokeWidth={iconStroke}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function AlignBottomIcon() {
  return (
    <svg
      className="kea-flow-align-icon"
      width={18}
      height={18}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
    >
      <path
        d="M4 20h16M7 8v12M12 12v8M17 14v6"
        stroke="currentColor"
        strokeWidth={iconStroke}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

/** Horizontal bars centered on a vertical guide (align to vertical axis). */
function AlignCenterHorizontalIcon() {
  return (
    <svg
      className="kea-flow-align-icon"
      width={18}
      height={18}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
    >
      <path
        d="M12 4v16M6 7h12M8 12h8M7 17h10"
        stroke="currentColor"
        strokeWidth={iconStroke}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

/** Vertical bars centered on a horizontal guide (align to horizontal axis). */
function AlignCenterVerticalIcon() {
  return (
    <svg
      className="kea-flow-align-icon"
      width={18}
      height={18}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
    >
      <path
        d="M4 12h16M8 9v6M12 6v12M16 8v8"
        stroke="currentColor"
        strokeWidth={iconStroke}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export function FlowSelectionAlignButtons({ t, disabled, onAlign }: Props) {
  const mk = (mode: AlignFlowSelectionMode, labelKey: MessageKey, icon: ReactNode) => (
    <button
      key={mode}
      type="button"
      className="kea-btn kea-btn--sm kea-flow-toolbar__align-btn"
      disabled={disabled}
      title={t(labelKey)}
      aria-label={t(labelKey)}
      onClick={() => onAlign(mode)}
    >
      {icon}
    </button>
  );

  return (
    <div className="kea-flow-toolbar__align" role="group" aria-label={t("flow.alignSelectionGroup")}>
      {mk("left", "flow.alignLeft", <AlignLeftIcon />)}
      {mk("centerHorizontal", "flow.alignCenterHorizontal", <AlignCenterHorizontalIcon />)}
      {mk("right", "flow.alignRight", <AlignRightIcon />)}
      {mk("top", "flow.alignTop", <AlignTopIcon />)}
      {mk("centerVertical", "flow.alignCenterVertical", <AlignCenterVerticalIcon />)}
      {mk("bottom", "flow.alignBottom", <AlignBottomIcon />)}
    </div>
  );
}
