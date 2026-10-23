import type { WorkPackageGeneratorPageViewModel } from "@/hooks/useWorkPackageGeneratorPage";
import { ResourcesStep } from "../ResourcesStep";
import { TemplateSelectionStep } from "../TemplateSelectionStep";

export function TemplateSelectionStepPanel({
	vm,
}: {
	vm: WorkPackageGeneratorPageViewModel;
}) {
	return (
		<TemplateSelectionStep
			templates={vm.templates}
			templateCanvasId={vm.templateCanvasId}
			aiSelectedCanvasId={vm.aiSelectedCanvasId}
			slotsByTemplate={vm.slotsByTemplate}
			isLoadingSlots={vm.isLoadingSlots}
			slotsError={vm.slotsError}
			onRetrySlots={vm.onRetrySlots}
			isLoadingTemplates={vm.isLoadingTemplates}
			templatesErrorMessage={vm.templatesErrorMessage}
			onRetryTemplates={vm.onRetryTemplates}
			isRunningAiCanvasRecommendation={vm.isRunningAiCanvasRecommendation}
			onSelectTemplate={vm.onSelectTemplate}
			onContinueFromTemplate={vm.onContinueFromTemplate}
		/>
	);
}

export function ResourcesStepPanel({
	vm,
}: {
	vm: WorkPackageGeneratorPageViewModel;
}) {
	return (
		<ResourcesStep
			resources={vm.resources}
			resourceConfigs={vm.resourceConfigs}
			isLoadingResources={vm.isLoadingResources}
			resourcesErrorMessage={vm.resourcesErrorMessage}
			onRetryResources={vm.onRetryResources}
			onContinueFromResources={vm.onContinueFromResources}
			onRemoveResource={vm.onRemoveResource}
		/>
	);
}
