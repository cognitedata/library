import {
	createContext,
	type ReactNode,
	useCallback,
	useContext,
	useMemo,
	useState,
} from "react";
import type { WorkflowStep } from "../types";

interface NavigationContextType {
	currentPage: WorkflowStep;
	navigate: (page: WorkflowStep) => void;
	goToNext: () => void;
	goToPrevious: () => void;
	canGoNext: boolean;
	canGoPrevious: boolean;
}

const NavigationContext = createContext<NavigationContextType | undefined>(
	undefined
);

const WORKFLOW_STEPS: WorkflowStep[] = [
	"work-order-selection",
	"canvas-selection",
	"canvas-generation",
	"ai-selection",
	"canvas-creation",
];

export function NavigationProvider({ children }: { children: ReactNode }) {
	const [currentPage, setCurrentPage] = useState<WorkflowStep>(
		"work-order-selection"
	);

	const navigate = useCallback((page: WorkflowStep) => {
		setCurrentPage(page);
	}, []);

	const currentStepIndex = WORKFLOW_STEPS.indexOf(currentPage);

	const goToNext = useCallback(() => {
		const nextIndex = currentStepIndex + 1;
		if (nextIndex < WORKFLOW_STEPS.length) {
			setCurrentPage(WORKFLOW_STEPS[nextIndex]);
		}
	}, [currentStepIndex]);

	const goToPrevious = useCallback(() => {
		const previousIndex = currentStepIndex - 1;
		if (previousIndex >= 0) {
			setCurrentPage(WORKFLOW_STEPS[previousIndex]);
		}
	}, [currentStepIndex]);

	const canGoNext = currentStepIndex < WORKFLOW_STEPS.length - 1;
	const canGoPrevious = currentStepIndex > 0;

	const ctxValue = useMemo(
		() => ({
			currentPage,
			navigate,
			goToNext,
			goToPrevious,
			canGoNext,
			canGoPrevious,
		}),
		[currentPage, navigate, goToNext, goToPrevious, canGoNext, canGoPrevious]
	);

	return (
		<NavigationContext.Provider value={ctxValue}>
			{children}
		</NavigationContext.Provider>
	);
}

export function useNavigation() {
	const context = useContext(NavigationContext);
	if (context === undefined) {
		throw new Error("useNavigation must be used within a NavigationProvider");
	}
	return context;
}
