import { RotateCcw } from "lucide-react";
import { Button } from "@/components/ui";

export function TemplateSelectionAlerts({
	templatesCount,
	templatesErrorMessage,
	onRetryTemplates,
	slotsError,
	onRetrySlots,
}: {
	templatesCount: number;
	templatesErrorMessage?: string | null;
	onRetryTemplates?: () => void;
	slotsError?: boolean;
	onRetrySlots?: () => void;
}) {
	if (templatesCount === 0) return null;

	return (
		<>
			{templatesErrorMessage && onRetryTemplates && (
				<div
					className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive"
					role="alert"
				>
					<span className="leading-snug">
						Could not refresh the template list. Showing the last loaded
						templates.
					</span>
					<Button
						type="button"
						variant="outline"
						size="sm"
						className="h-7 text-xs border-destructive/40 text-destructive hover:bg-destructive/10"
						onClick={onRetryTemplates}
					>
						<RotateCcw className="h-3 w-3 mr-1" />
						Retry
					</Button>
				</div>
			)}
			{slotsError && onRetrySlots && (
				<div
					className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive"
					role="alert"
				>
					<span>Could not load slot details for one or more templates.</span>
					<Button
						type="button"
						variant="outline"
						size="sm"
						className="h-7 text-xs border-destructive/40 text-destructive hover:bg-destructive/10"
						onClick={onRetrySlots}
					>
						<RotateCcw className="h-3 w-3 mr-1" />
						Retry slots
					</Button>
				</div>
			)}
		</>
	);
}
