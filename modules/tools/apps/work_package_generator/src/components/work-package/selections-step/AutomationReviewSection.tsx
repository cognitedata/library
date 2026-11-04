import { LayoutTemplate, ListTree } from "lucide-react";
import { Button } from "@/components/ui";

interface AutomationReviewSectionProps {
	templateDisplayName: string | null;
	filled: number;
	total: number;
	emptySlotCount: number;
	onContinueToCreate: () => void;
	onGoToTemplate?: () => void;
	onGoToResources?: () => void;
}

export function AutomationReviewSection({
	templateDisplayName,
	filled,
	total,
	emptySlotCount,
	onContinueToCreate,
	onGoToTemplate,
	onGoToResources,
}: AutomationReviewSectionProps) {
	return (
		<section
			className="rounded-xl border border-primary/25 bg-primary/5 p-4 shadow-sm space-y-3"
			aria-label="Review AI suggestions before creating the canvas"
		>
			<h4 className="text-sm font-semibold text-foreground">
				Review before you create
			</h4>
			<p className="text-xs text-muted-foreground leading-relaxed">
				The steps above ran automatically. Confirm what was chosen below, or
				jump back to adjust the template or resources.
			</p>
			<ul className="text-xs space-y-2 text-foreground">
				<li className="flex gap-2">
					<LayoutTemplate
						className="h-4 w-4 shrink-0 text-muted-foreground mt-0.5"
						aria-hidden
					/>
					<span>
						<span className="font-medium">Template: </span>
						{templateDisplayName ?? "—"}
					</span>
				</li>
				<li className="flex gap-2">
					<ListTree
						className="h-4 w-4 shrink-0 text-muted-foreground mt-0.5"
						aria-hidden
					/>
					<span>
						<span className="font-medium">Canvas slots: </span>
						{total === 0
							? "None on this template — you can create the canvas without slot picks."
							: `${filled} of ${total} filled${
									emptySlotCount > 0
										? ` (${emptySlotCount} with no automatic match)`
										: ""
								}`}
					</span>
				</li>
			</ul>
			{emptySlotCount > 0 && total > 0 && (
				<p className="text-xs text-amber-800 dark:text-amber-200/90 bg-amber-500/10 border border-amber-500/20 rounded-md px-2.5 py-2 leading-relaxed">
					Some slots have no suggested resource.{" "}
					{onGoToResources ? (
						<>
							Open <strong className="font-medium">Resources</strong> to check
							data, then return here before creating the canvas if you need
							those slots filled.
						</>
					) : (
						<>
							Check the available resources and return here before creating the
							canvas if you need those slots filled.
						</>
					)}
				</p>
			)}
			<div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap sm:items-center">
				<Button
					type="button"
					size="default"
					className="w-full sm:w-auto"
					onClick={onContinueToCreate}
				>
					Continue to Create
				</Button>
				<div className="flex flex-wrap gap-2 justify-center sm:justify-start">
					{onGoToTemplate && (
						<Button
							type="button"
							variant="outline"
							size="sm"
							className="text-xs"
							onClick={onGoToTemplate}
						>
							Edit template
						</Button>
					)}
					{onGoToResources && total > 0 && (
						<Button
							type="button"
							variant="outline"
							size="sm"
							className="text-xs"
							onClick={onGoToResources}
						>
							Open Resources
						</Button>
					)}
				</div>
			</div>
		</section>
	);
}
