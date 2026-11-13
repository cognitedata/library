/**
 * App header — Aura Button/Separator; compact ghost icon toggles theme; primary nav is text-style.
 * Topbar layout: sticky bar, border-b, px-6, toolbar gap-3.
 */

import { Separator } from "@cognite/aura/components";
import { Moon, Sun } from "lucide-react";
import { useState } from "react";
import CogniteLogoBlack from "@/assets/Cognite_Logomark_Black.svg";
import CogniteLogoWhite from "@/assets/Cognite_Logomark_White.svg";
import { Button } from "@/components/ui";
import { useNavigation } from "@/contexts/NavigationContext";
import { cn } from "@/utils";
import { TemplateCanvasInstructionsDialog } from "./TemplateCanvasInstructionsDialog";

export function Navigation() {
	const { currentPage, navigate } = useNavigation();
	const [isDark, setIsDark] = useState(() =>
		typeof document !== "undefined"
			? document.documentElement.classList.contains("dark")
			: false
	);

	const toggleTheme = () => {
		const next = !isDark;
		setIsDark(next);
		document.documentElement.classList.toggle("dark", next);
		localStorage.setItem("theme", next ? "dark" : "light");
	};

	const navItems = [
		{ id: "work-order-selection" as const, label: "Work package generator" },
	];

	return (
		<header className="sticky top-0 z-50 border-b border-border bg-background">
			<div
				className={cn(
					"mx-auto flex h-14 w-full max-w-[2400px] items-center justify-between gap-4",
					"px-6 mobile:px-4"
				)}
			>
				<div className="flex min-w-0 flex-1 items-center gap-3">
					<Button
						type="button"
						variant="ghost"
						className="h-9 shrink-0 gap-2 px-2 has-[>svg]:px-2"
						onClick={() => navigate("work-order-selection")}
						aria-label="Cognite, go to work package generator"
					>
						<img
							src={isDark ? CogniteLogoWhite : CogniteLogoBlack}
							alt=""
							className="h-7 w-auto"
						/>
					</Button>

					<Separator orientation="vertical" className="hidden h-6 sm:block" />

					<nav
						className="flex shrink-0 items-center gap-1"
						aria-label="Primary navigation"
					>
						{navItems.map((item) => {
							const isActive = currentPage === item.id;
							return (
								<Button
									type="button"
									variant="ghost"
									key={item.id}
									onClick={() => navigate(item.id)}
									aria-current={isActive ? "page" : undefined}
								>
									{item.label}
								</Button>
							);
						})}
					</nav>
				</div>

				<div className="flex shrink-0 items-center gap-1">
					<TemplateCanvasInstructionsDialog />
					<Button
						type="button"
						variant="ghost"
						size="icon-sm"
						onClick={toggleTheme}
						className="shrink-0 text-muted-foreground hover:text-foreground"
						aria-label={isDark ? "Switch to light mode" : "Switch to dark mode"}
						aria-pressed={isDark}
					>
						{isDark ? (
							<Sun className="size-4" aria-hidden />
						) : (
							<Moon className="size-4" aria-hidden />
						)}
					</Button>
				</div>
			</div>
		</header>
	);
}
