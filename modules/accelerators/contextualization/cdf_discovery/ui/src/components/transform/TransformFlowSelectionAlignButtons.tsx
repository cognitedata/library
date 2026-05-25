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
      className="transform-flow-align-icon"
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
      className="transform-flow-align-icon"
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
      className="transform-flow-align-icon"
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
      className="transform-flow-align-icon"
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

function AlignCenterHorizontalIcon() {
  return (
    <svg
      className="transform-flow-align-icon"
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

function AlignCenterVerticalIcon() {
  return (
    <svg
      className="transform-flow-align-icon"
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

export function TransformFlowSelectionAlignButtons({ t, disabled, onAlign }: Props) {
  const mk = (mode: AlignFlowSelectionMode, labelKey: MessageKey, icon: ReactNode) => (
    <button
      key={mode}
      type="button"
      className="disc-btn disc-btn--sm transform-flow-layout-panel__align-btn"
      disabled={disabled}
      title={t(labelKey)}
      aria-label={t(labelKey)}
      onClick={() => onAlign(mode)}
    >
      {icon}
    </button>
  );

  return (
    <div
      className="transform-flow-layout-panel__align"
      role="group"
      aria-label={t("transform.align.selectionGroup")}
    >
      {mk("left", "transform.align.left", <AlignLeftIcon />)}
      {mk("centerHorizontal", "transform.align.centerHorizontal", <AlignCenterHorizontalIcon />)}
      {mk("right", "transform.align.right", <AlignRightIcon />)}
      {mk("top", "transform.align.top", <AlignTopIcon />)}
      {mk("centerVertical", "transform.align.centerVertical", <AlignCenterVerticalIcon />)}
      {mk("bottom", "transform.align.bottom", <AlignBottomIcon />)}
    </div>
  );
}
