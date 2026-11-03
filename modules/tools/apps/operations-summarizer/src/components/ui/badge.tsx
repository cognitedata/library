import { Badge as AuraBadge } from "@cognite/aura/components";
import type { ComponentProps, ReactNode } from "react";
import { cn } from "@/lib/utils";

/** shadcn-era names mapped to Aura Badge API */
type LegacyBadgeVariant =
  "default" | "secondary" | "destructive" | "outline" | "ghost";

type StatusBadgeVariant = "success" | "warning" | "error";

type AppBadgeVariant = LegacyBadgeVariant | StatusBadgeVariant;

type BadgeProps = Omit<
  ComponentProps<typeof AuraBadge>,
  "variant" | "outline"
> & {
  variant?: AppBadgeVariant;
  children?: ReactNode;
};

function toAuraBadgeProps(
  variant: AppBadgeVariant | undefined,
): Pick<ComponentProps<typeof AuraBadge>, "variant" | "outline"> {
  if (variant === "success" || variant === "warning" || variant === "error") {
    return { variant };
  }
  switch (variant) {
    case "secondary":
      return { variant: "secondary" };
    case "destructive":
      return { variant: "error" };
    case "outline":
      return { variant: "secondary", outline: true };
    case "ghost":
      return { variant: "gray", outline: true };
    default:
      // Aura 1 dropped Badge variant "default". Omit it so the primitive uses its own default.
      return {};
  }
}

function Badge({ variant, className, children, ...props }: BadgeProps) {
  const aura = toAuraBadgeProps(variant);
  // Aura's Badge treats falsy React children as "empty" (e.g. numeric 0 for scores).
  const safeChildren = children === 0 ? "0" : children;
  return (
    <AuraBadge
      {...aura}
      {...props}
      className={cn(
        // Aura uses overflow-hidden; long labels (page counts) and taller text-sm
        // content were clipped after the Aura migration.
        "overflow-visible",
        // Slightly more padding than Aura default (px-1) for multi-word labels.
        "px-1.5",
        variant === "secondary" &&
          "!border-transparent !bg-[var(--secondary)] !text-[var(--secondary-foreground)]",
        className,
      )}
    >
      {safeChildren}
    </AuraBadge>
  );
}

export { Badge };
