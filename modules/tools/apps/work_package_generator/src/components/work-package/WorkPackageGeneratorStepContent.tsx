import type { WorkPackageGeneratorPageViewModel } from "@/hooks/useWorkPackageGeneratorPage";
import {
	CanvasCreationStepPanel,
	SelectionsStepPanel,
} from "./step-content/SelectionsAndCanvasSteps";
import {
	ResourcesStepPanel,
	TemplateSelectionStepPanel,
} from "./step-content/TemplateAndResourcesSteps";
import {
	WorkOrderDetailStepPanel,
	WorkOrderSearchStep,
} from "./step-content/WorkOrderSteps";

type Props = {
	vm: WorkPackageGeneratorPageViewModel;
};

/**
 * Renders the active workflow step panel for the work package generator page.
 */
export function WorkPackageGeneratorStepContent({ vm }: Props) {
	const { activeStep, workOrder } = vm;

	return (
		<>
			{activeStep === 0 && !workOrder && <WorkOrderSearchStep vm={vm} />}
			{activeStep === 0 && workOrder && <WorkOrderDetailStepPanel vm={vm} />}
			{activeStep === 1 && <TemplateSelectionStepPanel vm={vm} />}
			{activeStep === 2 && <ResourcesStepPanel vm={vm} />}
			{activeStep === 3 && <SelectionsStepPanel vm={vm} />}
			{activeStep === 4 && <CanvasCreationStepPanel vm={vm} />}
		</>
	);
}
