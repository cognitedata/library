import { Plus, RefreshCw } from "lucide-react";
import {
	Button,
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogTitle,
} from "@/components/ui";

export function ExistingCanvasSaveChoiceDialog({
	open,
	onOpenChange,
	onUpdateExisting,
	onCreateNew,
	isActionPending,
	canvasName,
}: {
	open: boolean;
	onOpenChange: (open: boolean) => void;
	onUpdateExisting: () => void;
	onCreateNew: () => void;
	isActionPending: boolean;
	canvasName: string;
}) {
	const handleUpdate = () => {
		onUpdateExisting();
		onOpenChange(false);
	};

	const handleCreateNew = () => {
		onCreateNew();
		onOpenChange(false);
	};

	return (
		<Dialog open={open} onOpenChange={onOpenChange}>
			<DialogContent
				className="max-w-md"
				showCloseButton={!isActionPending}
				closeButtonLabel="Close"
			>
				<div className="mb-4 min-w-0 pr-8">
					<DialogTitle className="text-base font-semibold">
						Canvas already exists
					</DialogTitle>
					<DialogDescription className="mt-2 text-sm text-muted-foreground">
						A canvas for this work order already exists. Update it or create a
						new one?
					</DialogDescription>
				</div>
				<p className="mb-4 text-xs text-muted-foreground">
					Updating replaces the existing canvas in Fusion with your current
					template, selections, work order, and asset. Creating new saves an
					additional canvas with a new link.
				</p>
				<DialogFooter>
					<Button
						type="button"
						variant="outline"
						disabled={isActionPending}
						className="sm:min-w-24"
						onClick={() => onOpenChange(false)}
					>
						Cancel
					</Button>
					<Button
						type="button"
						variant="outline"
						disabled={isActionPending || !canvasName.trim()}
						className="sm:min-w-32"
						onClick={handleCreateNew}
					>
						<Plus className="h-4 w-4 mr-1.5" />
						Create new
					</Button>
					<Button
						type="button"
						disabled={isActionPending || !canvasName.trim()}
						className="sm:min-w-36"
						onClick={handleUpdate}
					>
						<RefreshCw className="h-4 w-4 mr-1.5" />
						Update existing
					</Button>
				</DialogFooter>
			</DialogContent>
		</Dialog>
	);
}
