/**
 * Navigation Component
 *
 * Main navigation header for the application.
 * Composed from Aura primitives per the design skill (Button, DropdownMenu, Tooltip, Separator).
 * Aura Topbar is not used from npm exports; layout uses semantic tokens only.
 */

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  Separator,
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@cognite/aura/components";
import { IconMoon, IconSun } from "@tabler/icons-react";
import { FileText, Menu, Settings, TestTube2, X } from "lucide-react";
import { useEffect, useState } from "react";
import CogniteLogoBlack from "@/assets/Cognite_Logomark_Black.svg";
import CogniteLogoWhite from "@/assets/Cognite_Logomark_White.svg";
import { Button } from "@/components/ui/button";
import { useNavigation } from "@/contexts/NavigationContext";
import { cn } from "@/lib/utils";

type Page = "summaries" | "tests" | "settings";

export function Navigation() {
  const { currentPage, navigate } = useNavigation();
  const [isDark, setIsDark] = useState(() => {
    if (typeof window !== "undefined") {
      return document.documentElement.classList.contains("dark");
    }
    return false;
  });
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  useEffect(() => {
    const root = document.documentElement;
    root.classList.toggle("dark", isDark);
    root.style.colorScheme = isDark ? "dark" : "light";
  }, [isDark]);

  const setLightTheme = () => setIsDark(false);
  const setDarkTheme = () => setIsDark(true);

  const primaryNavItems: {
    id: Exclude<Page, "settings">;
    label: string;
    icon: typeof FileText;
  }[] = [
    { id: "summaries", label: "Summaries", icon: FileText },
    { id: "tests", label: "Tests", icon: TestTube2 },
  ];

  const mobileNavItems: { id: Page; label: string; icon: typeof FileText }[] = [
    ...primaryNavItems,
    { id: "settings", label: "Settings", icon: Settings },
  ];

  const handleNavigation = (page: Page) => {
    navigate(page);
    setMobileMenuOpen(false);
  };

  return (
    <TooltipProvider delayDuration={300}>
      <header className="sticky top-0 z-50 border-b border-border bg-background">
        <div className="flex h-14 items-center justify-between gap-3 px-5">
          <div className="flex min-w-0 flex-1 items-center gap-6">
            <Button
              type="button"
              variant="ghost"
              className="h-9 shrink-0 px-1"
              onClick={() => handleNavigation("summaries")}
              aria-label="Go to Summaries"
            >
              <img
                src={isDark ? CogniteLogoWhite : CogniteLogoBlack}
                alt=""
                className="h-8 w-auto"
              />
            </Button>

            <nav
              className="hidden min-w-0 items-center gap-1 md:flex"
              aria-label="Main"
            >
              {primaryNavItems.map((item) => {
                const isActive = currentPage === item.id;
                const Icon = item.icon;
                return (
                  <Button
                    type="button"
                    key={item.id}
                    variant="ghost"
                    className={cn(
                      "gap-2 underline-offset-4 decoration-foreground decoration-2",
                      isActive ? "underline" : "hover:underline",
                    )}
                    onClick={() => handleNavigation(item.id)}
                    aria-current={isActive ? "page" : undefined}
                  >
                    <Icon className="size-4" aria-hidden />
                    {item.label}
                  </Button>
                );
              })}
            </nav>
          </div>

          <div className="flex shrink-0 items-center gap-2 md:gap-3">
            <Separator orientation="vertical" className="hidden h-6 md:block" />

            <Tooltip>
              <TooltipTrigger
                render={
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    className={cn(
                      "hidden md:inline-flex underline-offset-4 decoration-foreground decoration-2",
                      currentPage === "settings"
                        ? "underline"
                        : "hover:underline",
                    )}
                    onClick={() => handleNavigation("settings")}
                    aria-label="Settings"
                    aria-current={
                      currentPage === "settings" ? "page" : undefined
                    }
                  />
                }
              >
                <Settings className="size-5" aria-hidden />
              </TooltipTrigger>
              <TooltipContent side="bottom">Settings</TooltipContent>
            </Tooltip>

            <DropdownMenu
              open={mobileMenuOpen}
              onOpenChange={setMobileMenuOpen}
            >
              <DropdownMenuTrigger
                render={
                  <Button
                    type="button"
                    variant="outline"
                    size="icon"
                    className="md:hidden"
                    aria-label={
                      mobileMenuOpen
                        ? "Close navigation menu"
                        : "Open navigation menu"
                    }
                  />
                }
              >
                {mobileMenuOpen ? (
                  <X className="size-5" aria-hidden />
                ) : (
                  <Menu className="size-5" aria-hidden />
                )}
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="min-w-48">
                {mobileNavItems.map((item) => {
                  const isActive = currentPage === item.id;
                  const Icon = item.icon;
                  return (
                    <DropdownMenuItem
                      key={item.id}
                      className={cn(
                        "underline-offset-4 decoration-foreground decoration-2",
                        isActive ? "underline" : "hover:underline",
                      )}
                      onClick={() => handleNavigation(item.id)}
                      aria-current={isActive ? "page" : undefined}
                    >
                      <Icon className="size-4" aria-hidden />
                      {item.label}
                    </DropdownMenuItem>
                  );
                })}
              </DropdownMenuContent>
            </DropdownMenu>

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
                      onClick={setLightTheme}
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
                      onClick={setDarkTheme}
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
          </div>
        </div>
      </header>
    </TooltipProvider>
  );
}
