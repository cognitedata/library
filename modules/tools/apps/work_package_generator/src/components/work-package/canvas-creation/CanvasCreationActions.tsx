import type { CanvasNodeDef } from "@dune-canvas/canvas-preview-lib";
import { Plus } from "lucide-react";
import { Button, Loader } from "@/components/ui";
import { CanvasDraftPreview } from "../CanvasDraftPreview";

export function CanvasCreationActions({
	isReady,
	draftPreview,
	isSavedSuccessfully,
	onCreateAnotherWorkPackage,
	onSave,
	isCreating,
	canvasName,
}: {
	isReady: boolean;
	draftPreview: {
		data: CanvasNodeDef[] | undefined;
		error: Error | null;
		isFetching: boolean;
		isPending: boolean;
		isPlaceholderData: boolean;
	};
	isSavedSuccessfully: boolean;
	onCreateAnotherWorkPackage?: () => void;
	onSave: () => void;
	isCreating: boolean;
	canvasName: string;
}) {
	return (
		<>
			{isReady && (
				<div className="mb-4">
					<p className="text-xs font-medium text-muted-foreground mb-2">
						Preview before saving
					</p>
					<p className="text-xs text-muted-foreground mb-2">
						Read-only view of the canvas from your template and selections. Save
						to create or update it in CDF.
					</p>
					<div className="rounded-xl border overflow-hidden bg-muted/20">
						<CanvasDraftPreview
							nodes={draftPreview.data}
							isLoading={
								draftPreview.isPending ||
								(draftPreview.isFetching && draftPreview.isPlaceholderData)
							}
							error={draftPreview.error}
						/>
					</div>
				</div>
			)}

			<div className="flex flex-col gap-2">
				<div className="flex flex-row gap-3 mobile:flex-col">
					{isSavedSuccessfully && onCreateAnotherWorkPackage ? (
						<Button
							onClick={onCreateAnotherWorkPackage}
							className="flex-1 h-12"
							size="lg"
						>
							<Plus className="h-5 w-5" />
							<span className="ml-2">Create another work package</span>
						</Button>
					) : (
						<Button
							onClick={onSave}
							disabled={isCreating || !isReady || !canvasName.trim()}
							className="flex-1 h-12"
							size="lg"
						>
							{isCreating ? <Loader size={18} /> : <Plus className="h-5 w-5" />}
							<span className="ml-2">
								{isCreating ? "Saving..." : "Save to Fusion"}
							</span>
						</Button>
					)}
				</div>
			</div>

			{!isReady && !isCreating && (
				<div className="mt-4 flex items-center justify-center gap-2 text-sm text-muted-foreground">
					<Loader size={14} />
					<span>Preparing workflow...</span>
				</div>
			)}
		</>
	);
}
