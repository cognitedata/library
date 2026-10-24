import { Activity, RotateCcw } from "lucide-react";
import { Button, Loader } from "@/components/ui";

export function ResourcesStepStatus({
	isLoading,
	errorMessage,
	onRetry,
}: {
	isLoading: boolean;
	errorMessage?: string | null;
	onRetry?: () => void;
}) {
	return (
		<div className="p-3 space-y-3">
			<h3 className="text-sm font-semibold flex items-center gap-2">
				<Activity className="h-4 w-4" />
				Available Resources
			</h3>
			{isLoading ? (
				<div className="flex items-center gap-2 py-10 justify-center text-xs text-muted-foreground">
					<Loader size={16} />
					Loading resources for this work order and asset…
				</div>
			) : (
				<div
					className="flex flex-col items-center gap-3 py-10 px-3 text-center rounded-lg border border-destructive/30 bg-destructive/5"
					role="alert"
				>
					<p className="text-xs text-destructive max-w-md leading-snug">
						{errorMessage}
					</p>
					{onRetry && (
						<Button
							type="button"
							variant="outline"
							size="sm"
							className="text-xs border-destructive/40 text-destructive hover:bg-destructive/10"
							onClick={onRetry}
						>
							<RotateCcw className="h-3.5 w-3.5 mr-1.5" />
							Retry
						</Button>
					)}
				</div>
			)}
		</div>
	);
}
