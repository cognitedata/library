/**
 * Workflow Progress Pills
 * Horizontal (default) or vertical pill navigation for workflow steps.
 */

import { useAppConfig } from "@/app-config";
import { cn } from "@/utils";
import { WorkflowStep } from "./WorkflowStepPill";

export type WorkflowStepIndex = 0 | 1 | 2 | 3 | 4;

/** Map internal workflow step (0–6) to display step index (0–4). */
export function workflowStepToIndex(workflowStep: number): WorkflowStepIndex {
	if (workflowStep <= 1) return 0;
	if (workflowStep <= 3) return 1;
	if (workflowStep <= 4) return 2;
	if (workflowStep <= 5) return 3;
	return 4;
}

interface WorkflowStepperProps {
	/** Internal workflow step 0–6 (from workflowStatus.step). */
	workflowStep: number;
	onStepClick?: (step: WorkflowStepIndex) => void;
	/** Which pill is visually active/selected (used in vertical mode). */
	activeStep?: WorkflowStepIndex;
	message?: string;
	className?: string;
	/** Stack pills vertically for a sidebar layout. */
	orientation?: "horizontal" | "vertical";
}

export function WorkflowStepper({
	workflowStep,
	onStepClick,
	activeStep,
	message,
	className,
	orientation = "horizontal",
}: WorkflowStepperProps) {
	const isVertical = orientation === "vertical";
	const stepLabels = useAppConfig().ui.stepperLabels;

	return (
		<div className={cn("w-full", className)}>
			<ol
				className={cn(
					isVertical
						? "flex flex-col gap-1"
						: "flex items-center gap-1.5 flex-wrap"
				)}
				aria-label="Workflow steps"
			>
				{stepLabels.map((label, index) => (
					<WorkflowStep
						key={label}
						label={label}
						index={index}
						stepIndex={index as WorkflowStepIndex}
						workflowStep={workflowStep}
						onStepClick={onStepClick}
						activeStep={activeStep}
						orientation={orientation}
					/>
				))}
			</ol>

			{message && (
				<p
					className={cn(
						"text-[11px] text-muted-foreground",
						isVertical ? "mt-3 px-1 leading-relaxed" : "mt-2 px-0.5"
					)}
				>
					{message}
				</p>
			)}
		</div>
	);
}
