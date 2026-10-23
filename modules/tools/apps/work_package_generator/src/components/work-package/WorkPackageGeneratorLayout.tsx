import type { ReactNode } from "react";
import { ErrorAlert } from "./ErrorAlert";
import type { WorkflowStepIndex } from "./WorkflowStepper";
import { WorkflowStepper } from "./WorkflowStepper";

export type WorkPackageGeneratorLayoutProps = {
	isMobileLayout: boolean;
	error: string | null;
	onDismissError: () => void;
	isAnyLoading: boolean;
	loadingLabel: string;
	stepperWorkflowStep: number;
	activeStep: WorkflowStepIndex;
	onStepClick: (step: WorkflowStepIndex) => void;
	stepperMessage: string;
	children: ReactNode;
};

/**
 * Shell for the work package generator: global alerts, stepper rail, and main panel.
 */
export function WorkPackageGeneratorLayout({
	isMobileLayout,
	error,
	onDismissError,
	isAnyLoading,
	loadingLabel,
	stepperWorkflowStep,
	activeStep,
	onStepClick,
	stepperMessage,
	children,
}: WorkPackageGeneratorLayoutProps) {
	const px = isMobileLayout ? "px-4" : "px-6";
	const py = isMobileLayout ? "py-5" : "py-8";
	const mb = isMobileLayout ? "mb-4" : "mb-6";
	const gap = isMobileLayout ? "gap-4" : "gap-6";
	const contentP = isMobileLayout ? "p-4" : "p-6";
	const rowDirection = isMobileLayout ? "flex-col" : "flex-row";
	const asideClasses = isMobileLayout
		? "shrink-0 w-full rounded-xl border border-border bg-muted/40 p-3"
		: "flex w-40 max-w-full shrink-0 flex-col self-start overflow-y-auto rounded-xl border border-border bg-muted/40 p-3";

	return (
		<div className="flex min-h-0 flex-1 flex-col overflow-hidden bg-background">
			<div
				className={`flex min-h-0 flex-1 flex-col overflow-hidden ${px} ${py}`}
			>
				<div
					className={`mx-auto flex min-h-0 w-full max-w-7xl flex-col ${isMobileLayout ? "flex-1" : ""}`}
				>
					{(error || isAnyLoading) && (
						<div className={`${mb} shrink-0 space-y-3`}>
							{error && (
								<ErrorAlert message={error} onDismiss={onDismissError} />
							)}
							{isAnyLoading && (
								<div
									className="w-full space-y-2"
									role="status"
									aria-live="polite"
									aria-busy="true"
								>
									<div
										className="h-1.5 w-full overflow-hidden rounded-full bg-muted"
										aria-hidden
									>
										<div className="h-full w-1/4 rounded-full bg-primary animate-loading-bar-slide" />
									</div>
									<p className="truncate text-xs text-muted-foreground">
										{loadingLabel}
									</p>
								</div>
							)}
						</div>
					)}

					<div
						className={`flex min-h-0 flex-1 ${rowDirection} ${gap} overflow-hidden`}
					>
						<aside className={asideClasses}>
							<WorkflowStepper
								workflowStep={stepperWorkflowStep}
								activeStep={activeStep}
								onStepClick={onStepClick}
								message={stepperMessage}
								orientation={isMobileLayout ? "horizontal" : "vertical"}
							/>
						</aside>

						<div
							className={`min-h-0 min-w-0 flex-1 overflow-y-auto rounded-xl border border-border bg-card shadow-sm ${contentP}`}
						>
							{children}
						</div>
					</div>
				</div>
			</div>
		</div>
	);
}
