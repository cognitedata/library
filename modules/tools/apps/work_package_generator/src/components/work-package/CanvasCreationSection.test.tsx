import { act, fireEvent, render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_CANVAS_CONFIG } from "@/__mocks__/app-config";
import { useWorkPackageStore } from "@/store";
import { copyToClipboard } from "@/utils/clipboard";
import { CanvasCreationSection } from "./CanvasCreationSection";

// vi.mock: clipboard is a browser API wrapper (not DI); stub avoids real clipboard access.
vi.mock("@/utils/clipboard", () => ({
	copyToClipboard: vi.fn(() => Promise.resolve()),
}));

// vi.mock: useCanvasDraftPreview is not injected; stub isolates create-step UI from preview fetching.
vi.mock("@/hooks/useCanvasDraftPreview", () => ({
	useCanvasDraftPreview: vi.fn(() => ({
		data: [],
		isPending: false,
		isFetching: false,
		isPlaceholderData: false,
		error: null,
	})),
}));

// vi.mock: CanvasDraftPreview is covered in its own suite; stub keeps this test on create-step actions.
vi.mock("./CanvasDraftPreview", () => ({
	CanvasDraftPreview: () => <div data-testid="canvas-draft-preview" />,
}));

const baseValidation = {
	canvasExists: true,
	itemsCount: 1,
	expectedItemsCount: 1,
	itemsMatchExpected: true,
	canvasId: "c1",
	errors: [] as string[],
};

describe("CanvasCreationSection", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		vi.mocked(copyToClipboard).mockResolvedValue(undefined);
		useWorkPackageStore.setState({
			workOrder: null,
			asset: null,
			templateCanvasId: "",
			aiSelections: {},
			config: DEFAULT_CANVAS_CONFIG,
		});
	});

	it("shows preparing copy when not ready", () => {
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady={false}
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
			/>
		);
		expect(
			screen.getByText(/Preparing canvas generation/i)
		).toBeInTheDocument();
		expect(screen.getByText(/Preparing workflow/i)).toBeInTheDocument();
	});

	it("shows ready subtitle before save", () => {
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
			/>
		);
		expect(
			screen.getByText(/Review preview, then save to Fusion/i)
		).toBeInTheDocument();
	});

	it("shows checking state and wires canvas name input", () => {
		const onChange = vi.fn();
		render(
			<CanvasCreationSection
				canvasName="Start"
				onCanvasNameChange={onChange}
				isCreating={false}
				isCheckingExists
				hasExistingCanvas={false}
				isReady={false}
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
			/>
		);
		expect(
			screen.getByText(/Checking for existing canvas/i)
		).toBeInTheDocument();
		const input = screen.getByLabelText(/Canvas Name/i);
		fireEvent.change(input, { target: { value: "Startx" } });
		expect(onChange).toHaveBeenCalledWith("Startx");
	});

	it("shows lookup error and retry", () => {
		const onRetry = vi.fn();
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				existingCanvasLookupErrorMessage="Lookup failed"
				onRetryExistingCanvasLookup={onRetry}
				hasExistingCanvas={false}
				isReady={false}
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
			/>
		);
		expect(screen.getByRole("alert")).toHaveTextContent("Lookup failed");
		fireEvent.click(screen.getByRole("button", { name: /Retry check/i }));
		expect(onRetry).toHaveBeenCalledTimes(1);
	});

	it("disables Save to Fusion when not ready", () => {
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady={false}
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
			/>
		);
		expect(
			screen.getByRole("button", { name: /Save to Fusion/i })
		).toBeDisabled();
	});

	it("enables Save to Fusion when ready and not creating", () => {
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
			/>
		);
		expect(
			screen.getByRole("button", { name: /Save to Fusion/i })
		).not.toBeDisabled();
	});

	it("calls onCreateCanvas when no existing canvas", () => {
		const onCreate = vi.fn();
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={onCreate}
				onUpdateCanvas={vi.fn()}
			/>
		);
		fireEvent.click(screen.getByRole("button", { name: /Save to Fusion/i }));
		expect(onCreate).toHaveBeenCalledTimes(1);
	});

	it("opens duplicate dialog and routes update vs create", () => {
		const onCreate = vi.fn();
		const onUpdate = vi.fn();
		render(
			<CanvasCreationSection
				canvasName="Dup"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas
				isReady
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={onCreate}
				onUpdateCanvas={onUpdate}
			/>
		);
		expect(
			screen.getByText(/A canvas with this name already exists/i)
		).toBeInTheDocument();
		fireEvent.click(screen.getByRole("button", { name: /Save to Fusion/i }));
		const dialog = screen.getByRole("dialog");
		expect(
			within(dialog).getByText(/Canvas already exists/i)
		).toBeInTheDocument();
		fireEvent.click(
			within(dialog).getByRole("button", { name: /Update existing/i })
		);
		expect(onUpdate).toHaveBeenCalledTimes(1);
		fireEvent.click(screen.getByRole("button", { name: /Save to Fusion/i }));
		const dialog2 = screen.getByRole("dialog");
		fireEvent.click(
			within(dialog2).getByRole("button", { name: /^Create new$/i })
		);
		expect(onCreate).toHaveBeenCalled();
	});

	it("disables name input while creating", () => {
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
			/>
		);
		expect(screen.getByLabelText(/Canvas Name/i)).toBeDisabled();
		expect(screen.getByRole("button", { name: /Saving/i })).toBeInTheDocument();
	});

	it("renders success result card", () => {
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
				canvasResult={{
					canvasId: "c1",
					canvasLink: "https://fusion.example/c",
					validation: { ...baseValidation, success: true },
					isUpdate: false,
				}}
			/>
		);
		expect(
			screen.getByText(/Canvas created successfully/i)
		).toBeInTheDocument();
		expect(
			screen.getByRole("link", { name: /Open in Fusion/i })
		).toHaveAttribute("href", "https://fusion.example/c");
		expect(
			screen.getByRole("button", { name: /Email link/i })
		).toBeInTheDocument();
	});

	it("shows Create another work package after save and calls handler", () => {
		const onCreateAnother = vi.fn();
		const onCreate = vi.fn();
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={onCreate}
				onUpdateCanvas={vi.fn()}
				onCreateAnotherWorkPackage={onCreateAnother}
				canvasResult={{
					canvasId: "c1",
					canvasLink: "https://fusion.example/c",
					validation: { ...baseValidation, success: true },
					isUpdate: false,
				}}
			/>
		);
		expect(
			screen.queryByRole("button", { name: /Save to Fusion/i })
		).not.toBeInTheDocument();
		const button = screen.getByRole("button", {
			name: /Create another work package/i,
		});
		expect(button).not.toBeDisabled();
		fireEvent.click(button);
		expect(onCreateAnother).toHaveBeenCalledTimes(1);
		expect(onCreate).not.toHaveBeenCalled();
	});

	it("copies canvas link and resets copied label after timeout", async () => {
		vi.useFakeTimers();
		const link = "https://fusion.example/canvas-xyz";
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
				canvasResult={{
					canvasId: "c1",
					canvasLink: link,
					validation: { ...baseValidation, success: true },
					isUpdate: false,
				}}
			/>
		);
		await act(async () => {
			fireEvent.click(screen.getByRole("button", { name: /Copy link/i }));
		});
		expect(copyToClipboard).toHaveBeenCalledWith(link);
		expect(screen.getByText(/Copied!/i)).toBeInTheDocument();
		await act(async () => {
			vi.advanceTimersByTime(2000);
		});
		expect(screen.queryByText(/Copied!/i)).not.toBeInTheDocument();
		vi.useRealTimers();
	});

	it("renders warning result when validation fails", () => {
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady
				templateFields={[]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
				canvasResult={{
					canvasId: "c1",
					canvasLink: "https://fusion.example/c",
					validation: {
						...baseValidation,
						success: false,
						itemsMatchExpected: false,
						itemsCount: 0,
						expectedItemsCount: 2,
						errors: ["slot a"],
					},
					isUpdate: true,
				}}
			/>
		);
		expect(screen.getByText(/couldn’t fully verify it/i)).toBeInTheDocument();
		expect(
			screen.getByText(/fewer items than we expected/i)
		).toBeInTheDocument();
	});

	it("invokes onNavigateToWorkflowStep from recovery hints when provided", () => {
		const onNav = vi.fn();
		render(
			<CanvasCreationSection
				canvasName="N"
				onCanvasNameChange={vi.fn()}
				isCreating={false}
				isCheckingExists={false}
				hasExistingCanvas={false}
				isReady
				templateFields={[
					{
						type: "rel",
						label: "Main asset",
						viewExternalId: "CogniteAsset",
						viewSpace: "cdf_cdm",
						viewVersion: "v1",
					},
				]}
				aiSelections={{}}
				onCreateCanvas={vi.fn()}
				onUpdateCanvas={vi.fn()}
				onNavigateToWorkflowStep={onNav}
				canvasResult={{
					canvasId: "c1",
					canvasLink: "https://fusion.example/c",
					validation: {
						...baseValidation,
						success: false,
						errors: ["e1"],
					},
					isUpdate: false,
				}}
			/>
		);
		const fixLink = screen.getByRole("button", {
			name: /review Selections/i,
		});
		fireEvent.click(fixLink);
		expect(onNav).toHaveBeenCalledWith(3);
	});
});
