import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { WorkPackageGenerator } from "./WorkPackageGenerator";

// vi.mock: page hook is not injected via context; stub isolates layout wiring from orchestration.
vi.mock("@/hooks", () => ({
	useWorkPackageGeneratorPage: vi.fn(() => ({
		isMobileLayout: false,
		error: null,
		onDismissError: vi.fn(),
		isAnyLoading: false,
		loadingLabel: "",
		stepperWorkflowStep: 0,
		activeStep: 0,
		onStepClick: vi.fn(),
		stepperMessage: "",
	})),
}));

// vi.mock: child layout components are heavy; stubs verify page composition only.
vi.mock("@/components/work-package", () => ({
	WorkPackageGeneratorLayout: ({ children }: { children: React.ReactNode }) => (
		<div data-testid="layout">{children}</div>
	),
	WorkPackageGeneratorStepContent: () => (
		<div data-testid="step-content">Step Content</div>
	),
}));

describe("WorkPackageGenerator", () => {
	it("renders the layout with step content", () => {
		render(<WorkPackageGenerator />);

		expect(screen.getByTestId("layout")).toBeInTheDocument();
		expect(screen.getByTestId("step-content")).toBeInTheDocument();
	});
});
