/**
 * Navigation Component
 *
 * Main navigation header for the application.
 * Composed from Aura primitives per the design skill (Button, DropdownMenu, Separator).
 * Aura Topbar is not used from npm exports; layout uses semantic tokens only.
 */

import { Separator, TooltipProvider } from "@cognite/aura/components";
import { FileText, TestTube2 } from "lucide-react";
import { useEffect, useState } from "react";
import CogniteLogoBlack from "@/assets/Cognite_Logomark_Black.svg";
import CogniteLogoWhite from "@/assets/Cognite_Logomark_White.svg";
import { Button } from "@/components/ui/button";
import {
  type NavigationPage,
  useNavigation,
} from "@/contexts/NavigationContext";
import { cn } from "@/lib/utils";
import {
  NavigationMobileMenu,
  NavigationThemeControl,
} from "./navigation-theme-menu";

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

  const navItems: {
    id: NavigationPage;
    label: string;
    icon: typeof FileText;
  }[] = [
    { id: "summaries", label: "Summaries", icon: FileText },
    { id: "tests", label: "Tests", icon: TestTube2 },
  ];

  const handleNavigation = (page: NavigationPage) => {
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
              {navItems.map((item) => {
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

            <NavigationMobileMenu
              open={mobileMenuOpen}
              onOpenChange={setMobileMenuOpen}
              items={navItems}
              currentPage={currentPage}
              onNavigate={handleNavigation}
            />

            <NavigationThemeControl
              isDark={isDark}
              onLight={setLightTheme}
              onDark={setDarkTheme}
            />
          </div>
        </div>
      </header>
    </TooltipProvider>
  );
}
