import { RotateCcw } from "lucide-react";
import { Button, Input, Loader } from "@/components/ui";

export function CanvasNameField({
	canvasName,
	onCanvasNameChange,
	isCreating,
	isCheckingExists,
	existingCanvasLookupErrorMessage,
	onRetryExistingCanvasLookup,
	hasExistingCanvas,
}: {
	canvasName: string;
	onCanvasNameChange: (value: string) => void;
	isCreating: boolean;
	isCheckingExists: boolean;
	existingCanvasLookupErrorMessage?: string | null;
	onRetryExistingCanvasLookup?: () => void;
	hasExistingCanvas: boolean;
}) {
	return (
		<div className="bg-background rounded-xl p-4 mb-4 border">
			<label
				htmlFor="canvas-name-input"
				className="text-xs font-medium text-muted-foreground mb-2 block"
			>
				Canvas Name
			</label>
			<Input
				id="canvas-name-input"
				type="text"
				value={canvasName}
				onChange={(e) => onCanvasNameChange(e.target.value)}
				placeholder="Enter canvas name..."
				className="h-11 px-4 text-sm"
				disabled={isCreating}
			/>
			{isCheckingExists && (
				<span className="text-xs text-muted-foreground flex items-center gap-1 mt-2">
					<Loader size={12} />
					Checking for existing canvas...
				</span>
			)}
			{existingCanvasLookupErrorMessage && !isCheckingExists && (
				<div
					className="mt-2 flex flex-col gap-2 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive"
					role="alert"
				>
					<p className="leading-snug">{existingCanvasLookupErrorMessage}</p>
					{onRetryExistingCanvasLookup && (
						<Button
							type="button"
							variant="outline"
							size="sm"
							className="self-start"
							onClick={onRetryExistingCanvasLookup}
						>
							<RotateCcw className="h-3.5 w-3.5 shrink-0" />
							Retry check
						</Button>
					)}
				</div>
			)}
			{hasExistingCanvas && !isCheckingExists && (
				<p className="text-xs text-amber-600 dark:text-amber-400 mt-2">
					A canvas with this name already exists
				</p>
			)}
			{!canvasName.trim() && !isCheckingExists && (
				<p className="text-xs text-muted-foreground mt-2">
					Canvas name is required to save
				</p>
			)}
		</div>
	);
}
