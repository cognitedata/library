import type { ReactNode } from "react";

/** Inline SVGs for grid/query pagination (currentColor). */

const stroke = {
  fill: "none" as const,
  stroke: "currentColor",
  strokeWidth: 2,
  strokeLinecap: "round" as const,
  strokeLinejoin: "round" as const,
};

function Icon({ className, children }: { className?: string; children: ReactNode }) {
  return (
    <svg className={className} width={16} height={16} viewBox="0 0 24 24" aria-hidden {...stroke}>
      {children}
    </svg>
  );
}

export function IconPaginationFirst({ className }: { className?: string }) {
  return (
    <Icon className={className}>
      <path d="m11 17-5-5 5-5" />
      <path d="m18 17-5-5 5-5" />
    </Icon>
  );
}

export function IconPaginationPrev({ className }: { className?: string }) {
  return (
    <Icon className={className}>
      <path d="m15 18-6-6 6-6" />
    </Icon>
  );
}

export function IconPaginationNext({ className }: { className?: string }) {
  return (
    <Icon className={className}>
      <path d="m9 18 6-6-6-6" />
    </Icon>
  );
}

export function IconPaginationLast({ className }: { className?: string }) {
  return (
    <Icon className={className}>
      <path d="m6 17 5-5-5-5" />
      <path d="m13 17 5-5-5-5" />
    </Icon>
  );
}
