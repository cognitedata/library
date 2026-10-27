import type { WorkPackageGeneratorPageViewModel } from "@/hooks/useWorkPackageGeneratorPage";
import { CanvasCreationSection } from "../CanvasCreationSection";
import { SelectionsStep } from "../SelectionsStep";

export function SelectionsStepPanel({
	vm,
}: {
	vm: WorkPackageGeneratorPageViewModel;
}) {
	return (
		<SelectionsStep
			workOrder={vm.workOrder}
			templateCanvasId={vm.templateCanvasId}
			templateFields={vm.templateFields}
			aiSelections={vm.aiSelections}
			candidatesByType={vm.resources}
			isRunningAiSelection={vm.isRunningAiSelection}
			isLoadingFields={vm.isLoadingFields}
			templateFieldsErrorMessage={vm.templateFieldsErrorMessage}
			onRetryTemplateFields={vm.onRetryTemplateFields}
			templateDisplayName={vm.selectedTemplateName}
			showAutomationReview={vm.isReady && !vm.automationReviewAcknowledged}
			onContinueToCreate={vm.onContinueToCreate}
			onGoToTemplate={vm.onGoToTemplate}
			onGoToResources={vm.onGoToResources}
		/>
	);
}

export function CanvasCreationStepPanel({
	vm,
}: {
	vm: WorkPackageGeneratorPageViewModel;
}) {
	return (
		<CanvasCreationSection
			canvasName={vm.canvasName}
			onCanvasNameChange={vm.setCanvasName}
			isCreating={vm.isCreatingCanvas}
			isCheckingExists={vm.isCheckingExists}
			existingCanvasLookupErrorMessage={vm.existingCanvasLookupErrorMessage}
			onRetryExistingCanvasLookup={vm.onRetryExistingCanvasLookup}
			hasExistingCanvas={vm.hasExistingCanvas}
			isReady={vm.isReady}
			templateFields={vm.templateFields}
			aiSelections={vm.aiSelections}
			onNavigateToWorkflowStep={vm.onNavigateToWorkflowStep}
			canvasResult={vm.canvasResult}
			onCreateCanvas={vm.onCreateCanvas}
			onUpdateCanvas={vm.onUpdateCanvas}
			onCreateAnotherWorkPackage={vm.onCreateAnotherWorkPackage}
		/>
	);
}
