import { Check } from "lucide-react";
import { Badge, Button } from "@/components/ui";

export function SelectionProgress({
	filled,
	total,
	isRunningAiSelection,
	onRerunAi,
}: {
	filled: number;
	total: number;
	isRunningAiSelection: boolean;
	onRerunAi?: () => void;
}) {
	if (total <= 0) return null;

	return (
		<>
			{onRerunAi && !isRunningAiSelection && (
				<Button type="button" variant="outline" size="sm" onClick={onRerunAi}>
					Re-run AI
				</Button>
			)}
			<div className="flex items-center gap-2 text-xs text-muted-foreground">
				<span>
					{filled} of {total} canvas slot{total !== 1 ? "s" : ""} filled
				</span>
				{filled === total && filled > 0 && (
					<Badge
						variant="success"
						className="text-[10px] px-1.5 py-0 bg-green-600 text-white hover:bg-green-600"
					>
						<Check className="h-2.5 w-2.5" />
						All filled
					</Badge>
				)}
				{filled > 0 && filled < total && (
					<Badge variant="secondary" className="text-[10px] px-1.5 py-0">
						Partial
					</Badge>
				)}
			</div>
		</>
	);
}
