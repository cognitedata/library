import { Button } from "@/components/ui";
import {
	cn,
	type WorkflowRecoveryHint,
	type WorkflowRecoveryStep,
} from "@/utils";

export function WorkflowRecoveryLinks({
	hints,
	isSuccess,
	onNavigate,
}: {
	hints: WorkflowRecoveryHint[];
	isSuccess: boolean;
	onNavigate: (step: WorkflowRecoveryStep) => void;
}) {
	if (hints.length === 0) return null;

	return (
		<section
			className={cn(
				"mt-3 rounded-lg border p-3 space-y-2",
				isSuccess
					? "border-amber-200 bg-amber-50/90 dark:border-amber-800 dark:bg-amber-950/50"
					: "border-amber-900/15 bg-amber-50/50 dark:border-amber-700/40 dark:bg-amber-950/30"
			)}
			aria-label="Jump to workflow steps to fix issues"
		>
			<p className="text-xs font-medium text-foreground">
				{isSuccess
					? "Some slots still need a resource — open a step to fix:"
					: "Fix issues faster — open a step:"}
			</p>
			<ul className="flex flex-col gap-1 m-0 p-0 list-none">
				{hints.map((h) => (
					<li key={`${h.step}-${h.label}`}>
						<Button
							type="button"
							variant="ghost"
							className="h-auto min-h-0 py-0.5 px-0 text-left text-xs font-normal text-primary whitespace-normal justify-start underline-offset-2"
							onClick={() => onNavigate(h.step)}
						>
							{h.label}
						</Button>
					</li>
				))}
			</ul>
		</section>
	);
}
