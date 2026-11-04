import { Wrench } from "lucide-react";
import { CanvasNameField } from "@/components/work-package/canvas-creation/CanvasNameField";
import { CanvasResultCard } from "@/components/work-package/canvas-creation/CanvasResultCard";
import { ExistingCanvasSaveChoiceDialog } from "@/components/work-package/canvas-creation/ExistingCanvasSaveChoiceDialog";
import type { CanvasResult } from "@/components/work-package/canvas-creation/types";
import { useCanvasCreationSectionViewModel } from "@/hooks/useCanvasCreationSectionViewModel";
import type { AiSelections, TemplateField } from "@/types";
import type { WorkflowRecoveryStep } from "@/utils";
import { CanvasCreationActions } from "./canvas-creation/CanvasCreationActions";

interface CanvasCreationSectionProps {
	canvasName: string;
	onCanvasNameChange: (value: string) => void;
	isCreating: boolean;
	isCheckingExists: boolean;
	/** When duplicate-name lookup fails */
	existingCanvasLookupErrorMessage?: string | null;
	onRetryExistingCanvasLookup?: () => void;
	hasExistingCanvas: boolean;
	isReady: boolean;
	templateFields: TemplateField[];
	aiSelections: AiSelections;
	/** Jump to Template (1), Resources (2), or Selections (3) from the result card. */
	onNavigateToWorkflowStep?: (step: WorkflowRecoveryStep) => void;
	canvasResult?: CanvasResult;
	onCreateCanvas: () => void;
	onUpdateCanvas: () => void;
	/** After a successful save, start a new work package from the beginning. */
	onCreateAnotherWorkPackage?: () => void;
}

export function CanvasCreationSection({
	canvasName,
	onCanvasNameChange,
	isCreating,
	isCheckingExists,
	existingCanvasLookupErrorMessage = null,
	onRetryExistingCanvasLookup,
	hasExistingCanvas,
	isReady,
	templateFields,
	aiSelections,
	onNavigateToWorkflowStep,
	canvasResult,
	onCreateCanvas,
	onUpdateCanvas,
	onCreateAnotherWorkPackage,
}: CanvasCreationSectionProps) {
	const {
		draftPreview,
		existingSaveDialogOpen,
		setExistingSaveDialogOpen,
		isSavedSuccessfully,
		recoveryHints,
		subtitle,
		handleSaveClick,
	} = useCanvasCreationSectionViewModel({
		isReady,
		hasExistingCanvas,
		canvasResult,
		templateFields,
		aiSelections,
		onCreateCanvas,
	});

	return (
		<div className="bg-gradient-to-br from-primary/5 via-primary/10 to-primary/5 rounded-2xl border border-primary/20 p-6 mobile:p-4">
			<div className="flex items-center gap-3 mb-4">
				<div className="p-2 bg-primary rounded-lg">
					<Wrench className="h-5 w-5 text-primary-foreground" />
				</div>
				<div>
					<h2 className="font-semibold text-lg">Generate Canvas</h2>
					<p className="text-xs text-muted-foreground">{subtitle}</p>
				</div>
			</div>

			<CanvasNameField
				canvasName={canvasName}
				onCanvasNameChange={onCanvasNameChange}
				isCreating={isCreating}
				isCheckingExists={isCheckingExists}
				existingCanvasLookupErrorMessage={existingCanvasLookupErrorMessage}
				onRetryExistingCanvasLookup={onRetryExistingCanvasLookup}
				hasExistingCanvas={hasExistingCanvas}
			/>

			<CanvasCreationActions
				isReady={isReady}
				draftPreview={draftPreview}
				isSavedSuccessfully={isSavedSuccessfully}
				onCreateAnotherWorkPackage={onCreateAnotherWorkPackage}
				onSave={handleSaveClick}
				isCreating={isCreating}
				canvasName={canvasName}
			/>

			<ExistingCanvasSaveChoiceDialog
				open={existingSaveDialogOpen}
				onOpenChange={setExistingSaveDialogOpen}
				onUpdateExisting={onUpdateCanvas}
				onCreateNew={onCreateCanvas}
				isActionPending={isCreating}
				canvasName={canvasName}
			/>

			{canvasResult && (
				<CanvasResultCard
					result={canvasResult}
					canvasName={canvasName}
					recoveryHints={recoveryHints}
					onNavigateToWorkflowStep={onNavigateToWorkflowStep}
				/>
			)}
		</div>
	);
}
