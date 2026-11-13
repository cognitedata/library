/**
 * Work Package Generator Page
 * Two-column layout: vertical pill nav on the left, active step content on the right.
 */

import {
	WorkPackageGeneratorLayout,
	WorkPackageGeneratorStepContent,
} from "@/components/work-package";
import { useWorkPackageGeneratorPage } from "@/hooks";

export function WorkPackageGenerator() {
	const vm = useWorkPackageGeneratorPage();

	return (
		<WorkPackageGeneratorLayout
			isMobileLayout={vm.isMobileLayout}
			error={vm.error}
			onDismissError={vm.onDismissError}
			isAnyLoading={vm.isAnyLoading}
			loadingLabel={vm.loadingLabel}
			stepperWorkflowStep={vm.stepperWorkflowStep}
			activeStep={vm.activeStep}
			onStepClick={vm.onStepClick}
			stepperMessage={vm.stepperMessage}
		>
			<WorkPackageGeneratorStepContent vm={vm} />
		</WorkPackageGeneratorLayout>
	);
}
