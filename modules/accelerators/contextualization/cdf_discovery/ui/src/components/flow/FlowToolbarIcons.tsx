import type { ReactNode } from "react";

const STROKE = 2;

type IconProps = { className?: string };

function Svg({
  className,
  children,
}: {
  className?: string;
  children: ReactNode;
}) {
  return (
    <svg
      className={className ?? "flow-toolbar-icon"}
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

export function FlowToolbarUndoIcon({ className }: IconProps) {
  return (
    <Svg className={className}>
      <path
        d="M9 7H5v4M5 11c2.5-4 8-5 11-2s3 8-1 11-6 4-10 1"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Svg>
  );
}

export function FlowToolbarRedoIcon({ className }: IconProps) {
  return (
    <Svg className={className}>
      <path
        d="M15 7h4v4M19 11c-2.5-4-8-5-11-2s-3 8 1 11 6 4 10-1"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Svg>
  );
}

export function FlowToolbarFitViewIcon({ className }: IconProps) {
  return (
    <Svg className={className}>
      <path
        d="M9 3H5a2 2 0 0 0-2 2v4M15 3h4a2 2 0 0 1 2 2v4M9 21H5a2 2 0 0 1-2-2v-4M15 21h4a2 2 0 0 0 2-2v-4"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M12 8v8M8 12h8"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
      />
    </Svg>
  );
}

export function FlowToolbarAutoLayoutIcon({ className }: IconProps) {
  return (
    <Svg className={className}>
      <circle cx="6" cy="6" r="2" stroke="currentColor" strokeWidth={STROKE} />
      <circle cx="18" cy="6" r="2" stroke="currentColor" strokeWidth={STROKE} />
      <circle cx="12" cy="18" r="2" stroke="currentColor" strokeWidth={STROKE} />
      <path
        d="M7.5 7.5 10 14M16.5 7.5 14 14M8.5 7.5h7"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
      />
    </Svg>
  );
}

export function FlowToolbarRefreshIcon({ className }: IconProps) {
  return (
    <Svg className={className}>
      <path
        d="M21 12a9 9 0 1 1-2.64-6.36M21 3v6h-6"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Svg>
  );
}

export function FlowToolbarOpenExternalIcon({ className }: IconProps) {
  return (
    <Svg className={className}>
      <path
        d="M14 5h5v5M10 14 19 5M15 9l-6 6"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Svg>
  );
}

export function FlowToolbarHandleLrIcon({ className }: IconProps) {
  return (
    <Svg className={className}>
      <path
        d="M5 12h12M14 9l3 3-3 3"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Svg>
  );
}

export function FlowToolbarHandleTbIcon({ className }: IconProps) {
  return (
    <Svg className={className}>
      <path
        d="M12 5v12M9 14l3 3 3-3"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Svg>
  );
}

export function FlowToolbarEdgeStyleIcon({ className }: IconProps) {
  return (
    <Svg className={className}>
      <path
        d="M4 16c4-8 12-8 16 0"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
      />
    </Svg>
  );
}

export function FlowToolbarLayoutMethodIcon({ className }: IconProps) {
  return (
    <Svg className={className}>
      <path
        d="M6 4v16M6 8h12M6 12h8M6 16h10"
        stroke="currentColor"
        strokeWidth={STROKE}
        strokeLinecap="round"
      />
    </Svg>
  );
}

type FlowToolbarIconButtonProps = {
  label: string;
  onClick: () => void;
  disabled?: boolean;
  active?: boolean;
  primary?: boolean;
  children: ReactNode;
};

/** Icon-only toolbar control with accessible name. */
export function FlowToolbarIconButton({
  label,
  onClick,
  disabled = false,
  active = false,
  primary = false,
  children,
}: FlowToolbarIconButtonProps) {
  return (
    <button
      type="button"
      className={[
        "disc-btn",
        "disc-btn--sm",
        "flow-toolbar-icon-btn",
        active ? "flow-toolbar-icon-btn--active" : "",
        primary ? "disc-btn--primary" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      title={label}
      aria-label={label}
      disabled={disabled}
      aria-pressed={active ? true : undefined}
      onClick={onClick}
    >
      {children}
    </button>
  );
}

type FlowToolbarGroupProps = {
  label: string;
  children: ReactNode;
  className?: string;
};

export function FlowToolbarGroup({ label, children, className }: FlowToolbarGroupProps) {
  return (
    <div
      className={["flow-toolbar-group", className].filter(Boolean).join(" ")}
      role="group"
      aria-label={label}
    >
      {children}
    </div>
  );
}

export function FlowToolbarSeparator() {
  return <span className="flow-toolbar-sep" aria-hidden="true" />;
}

type FlowToolbarSelectProps = {
  label: string;
  value: string;
  disabled?: boolean;
  onChange: (value: string) => void;
  icon: ReactNode;
  children: ReactNode;
};

/** Compact select with leading icon (settings group). */
export function FlowToolbarSelect({
  label,
  value,
  disabled = false,
  onChange,
  icon,
  children,
}: FlowToolbarSelectProps) {
  return (
    <label className="flow-toolbar-select">
      <span className="flow-toolbar-select__icon" aria-hidden="true">
        {icon}
      </span>
      <select
        className="gov-input flow-toolbar-select__input"
        value={value}
        aria-label={label}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
      >
        {children}
      </select>
    </label>
  );
}
