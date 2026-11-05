import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@cognite/aura/components";
import { IconMoon, IconSun } from "@tabler/icons-react";
import { type LucideIcon, Menu, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { NavigationPage } from "@/contexts/NavigationContext";
import { cn } from "@/lib/utils";

export interface NavigationMenuItem {
  id: NavigationPage;
  label: string;
  icon: LucideIcon;
}

interface NavigationMobileMenuProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  items: NavigationMenuItem[];
  currentPage: NavigationPage;
  onNavigate: (page: NavigationPage) => void;
}

export function NavigationMobileMenu({
  open,
  onOpenChange,
  items,
  currentPage,
  onNavigate,
}: NavigationMobileMenuProps) {
  return (
    <DropdownMenu open={open} onOpenChange={onOpenChange}>
      <DropdownMenuTrigger
        render={
          <Button
            type="button"
            variant="outline"
            size="icon"
            className="md:hidden"
            aria-label={open ? "Close navigation menu" : "Open navigation menu"}
          />
        }
      >
        {open ? (
          <X className="size-5" aria-hidden />
        ) : (
          <Menu className="size-5" aria-hidden />
        )}
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-48">
        {items.map((item) => {
          const isActive = currentPage === item.id;
          const Icon = item.icon;
          return (
            <DropdownMenuItem
              key={item.id}
              className={cn(
                "underline-offset-4 decoration-foreground decoration-2",
                isActive ? "underline" : "hover:underline",
              )}
              onClick={() => onNavigate(item.id)}
              aria-current={isActive ? "page" : undefined}
            >
              <Icon className="size-4" aria-hidden />
              {item.label}
            </DropdownMenuItem>
          );
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

interface NavigationThemeControlProps {
  isDark: boolean;
  onLight: () => void;
  onDark: () => void;
}

export function NavigationThemeControl({
  isDark,
  onLight,
  onDark,
}: NavigationThemeControlProps) {
  return (
    <fieldset
      aria-label="Color theme"
      className="m-0 flex min-w-0 items-center gap-1 rounded-full border border-border bg-muted/50 p-1"
    >
      <Tooltip>
        <TooltipTrigger
          render={
            <Button
              type="button"
              variant={!isDark ? "secondary" : "ghost"}
              size="icon-sm"
              className="rounded-full"
              onClick={onLight}
              aria-label="Use light theme"
              aria-pressed={!isDark}
            />
          }
        >
          <IconSun className="size-5" aria-hidden />
        </TooltipTrigger>
        <TooltipContent side="bottom">Light mode</TooltipContent>
      </Tooltip>
      <Tooltip>
        <TooltipTrigger
          render={
            <Button
              type="button"
              variant={isDark ? "secondary" : "ghost"}
              size="icon-sm"
              className="rounded-full"
              onClick={onDark}
              aria-label="Use dark theme"
              aria-pressed={isDark}
            />
          }
        >
          <IconMoon className="size-5" aria-hidden />
        </TooltipTrigger>
        <TooltipContent side="bottom">Dark mode</TooltipContent>
      </Tooltip>
    </fieldset>
  );
}
