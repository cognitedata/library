import { Check } from "lucide-react";
import { cn } from "@/utils";
import { type WorkflowStepIndex, workflowStepToIndex } from "./WorkflowStepper";

interface WorkflowStepProps {
	label: string;
	index: number;
	stepIndex: WorkflowStepIndex;
	workflowStep: number;
	onStepClick?: (step: WorkflowStepIndex) => void;
	activeStep?: WorkflowStepIndex;
	orientation: "horizontal" | "vertical";
}

export function WorkflowStep({
	label,
	index,
	stepIndex,
	workflowStep,
	onStepClick,
	activeStep,
	orientation,
}: WorkflowStepProps) {
	const isVertical = orientation === "vertical";
	const completed = isStepCompleted(index, workflowStep);
	const isCurrent = isStepCurrent(index, workflowStep);
	const isActive =
		activeStep !== undefined ? activeStep === stepIndex : isCurrent;

	return (
		<li key={stepIndex} className={isVertical ? "w-full" : undefined}>
			<button
				type="button"
				onClick={() => onStepClick?.(stepIndex)}
				aria-current={isActive ? "step" : undefined}
				aria-label={`${label}${completed ? ", completed" : isCurrent ? ", current step" : ", pending"}`}
				className={cn(
					"inline-flex items-center gap-2 text-xs font-medium transition-all",
					"focus:outline-none focus-visible:ring-2 focus-visible:ring-ring",
					isVertical
						? "w-full rounded-lg px-3 py-2 justify-start text-left"
						: "rounded-full px-3 py-1.5",
					// Vertical active state (selected pill)
					isVertical &&
						isActive &&
						"bg-primary text-primary-foreground shadow-sm",
					// Vertical completed-but-not-active
					isVertical &&
						completed &&
						!isActive &&
						"bg-primary/12 text-primary hover:bg-primary/20 cursor-pointer",
					// Vertical pending
					isVertical &&
						!completed &&
						!isActive &&
						"text-muted-foreground hover:bg-muted cursor-pointer",
					// Horizontal active
					!isVertical &&
						isCurrent &&
						!completed &&
						"bg-primary text-primary-foreground shadow-sm cursor-pointer",
					// Horizontal completed
					!isVertical &&
						completed &&
						"bg-primary/15 text-primary hover:bg-primary/25 cursor-pointer",
					// Horizontal pending
					!isVertical &&
						!isCurrent &&
						!completed &&
						"bg-muted text-muted-foreground cursor-default"
				)}
			>
				{/* Step indicator */}
				<span
					className={cn(
						"flex shrink-0 items-center justify-center rounded-full text-[10px] font-bold leading-none",
						isVertical ? "h-5 w-5" : "h-3.5 w-3.5",
						isVertical && isActive && "bg-white/20",
						isVertical && completed && !isActive && "bg-primary/20",
						isVertical && !completed && !isActive && "bg-muted-foreground/15",
						!isVertical && "hidden"
					)}
					aria-hidden
				>
					{isVertical &&
						(completed ? <Check className="h-3 w-3" /> : index + 1)}
				</span>

				{/* Horizontal-only leading icon */}
				{!isVertical &&
					(completed ? (
						<Check className="h-3 w-3 shrink-0" aria-hidden />
					) : (
						<span
							className="text-[10px] font-bold leading-none shrink-0"
							aria-hidden
						>
							{index + 1}
						</span>
					))}

				{label}
			</button>
		</li>
	);
}

/** Step i is completed when workflow (0-6) has passed it. */
function isStepCompleted(stepIndex: number, workflowStep: number): boolean {
	if (stepIndex <= 0) return workflowStep >= 2;
	if (stepIndex === 1) return workflowStep >= 4;
	if (stepIndex === 2) return workflowStep >= 5;
	if (stepIndex === 3) return workflowStep >= 6;
	return false;
}

/** Step i is the current in-progress step. */
function isStepCurrent(stepIndex: number, workflowStep: number): boolean {
	return workflowStepToIndex(workflowStep) === stepIndex;
}
