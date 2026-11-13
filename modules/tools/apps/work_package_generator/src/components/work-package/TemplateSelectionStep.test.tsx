import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { TemplateSlot } from "@/hooks";
import type { TemplateCanvas } from "@/types";
import { TemplateSelectionStep } from "./TemplateSelectionStep";

const templates: TemplateCanvas[] = [
	{
		externalId: "t1",
		space: "s",
		name: "template_pump",
		visibility: "public",
	},
	{
		externalId: "t2",
		space: "s",
		name: "template_valve",
		visibility: "public",
	},
];

const slots: TemplateSlot[] = [
	{
		label: "Main asset",
		viewExternalId: "CogniteAsset",
		category: "asset",
		populatedBy: "auto",
	},
	{
		label: "Doc",
		viewExternalId: "CogniteFile",
		category: "file",
		populatedBy: "ai",
	},
];

function renderStep(
	overrides: Partial<Parameters<typeof TemplateSelectionStep>[0]> = {}
) {
	const onSelectTemplate = vi.fn();
	const onContinueFromTemplate = vi.fn();
	render(
		<TemplateSelectionStep
			templates={templates}
			templateCanvasId="t1"
			aiSelectedCanvasId="t1"
			slotsByTemplate={{ t1: slots, t2: [] }}
			isLoadingSlots={false}
			isLoadingTemplates={false}
			isRunningAiCanvasRecommendation={false}
			onSelectTemplate={onSelectTemplate}
			onContinueFromTemplate={onContinueFromTemplate}
			{...overrides}
		/>
	);
	return { onSelectTemplate, onContinueFromTemplate };
}

describe("TemplateSelectionStep", () => {
	it("renders templates, AI pick badge, slots, and continue", () => {
		const { onSelectTemplate, onContinueFromTemplate } = renderStep();
		expect(screen.getByText("2 templates")).toBeInTheDocument();
		expect(screen.getByText("AI Pick")).toBeInTheDocument();
		expect(screen.getByText("Main asset")).toBeInTheDocument();
		expect(screen.getByText("auto")).toBeInTheDocument();
		expect(screen.getByText("AI")).toBeInTheDocument();
		expect(screen.getByText("No slots")).toBeInTheDocument();

		fireEvent.click(screen.getByRole("button", { name: /template_valve/i }));
		expect(onSelectTemplate).toHaveBeenCalledWith("t2");

		fireEvent.click(
			screen.getByRole("button", { name: /Continue to resources/i })
		);
		expect(onContinueFromTemplate).toHaveBeenCalledTimes(1);
	});

	it("shows loading copy while templates or AI recommendation run", () => {
		renderStep({ isLoadingTemplates: true });
		expect(screen.getByText("Loading templates...")).toBeInTheDocument();
	});

	it("shows AI analyzing copy while recommendation is running", () => {
		renderStep({ isRunningAiCanvasRecommendation: true });
		expect(screen.getByText("AI analyzing templates...")).toBeInTheDocument();
	});

	it("shows empty and error states", () => {
		const onRetryTemplates = vi.fn();
		renderStep({
			templates: [],
			templateCanvasId: "",
			slotsByTemplate: {},
			templatesErrorMessage: "Could not load templates",
			onRetryTemplates,
		});
		expect(screen.getByRole("alert")).toHaveTextContent(
			"Could not load templates"
		);
		fireEvent.click(screen.getByRole("button", { name: /^Retry$/i }));
		expect(onRetryTemplates).toHaveBeenCalledTimes(1);
	});

	it("shows no templates found when the list is empty without an error", () => {
		renderStep({
			templates: [],
			templateCanvasId: "",
			slotsByTemplate: {},
		});
		expect(screen.getByText("No templates found")).toBeInTheDocument();
	});

	it("shows stale-list and slot retry banners", () => {
		const onRetryTemplates = vi.fn();
		const onRetrySlots = vi.fn();
		renderStep({
			templatesErrorMessage: "stale",
			onRetryTemplates,
			slotsError: true,
			onRetrySlots,
			templateCanvasId: "t2",
			aiSelectedCanvasId: "t1",
		});
		fireEvent.click(screen.getByRole("button", { name: /Retry$/i }));
		fireEvent.click(screen.getByRole("button", { name: /Retry slots/i }));
		expect(onRetryTemplates).toHaveBeenCalledTimes(1);
		expect(onRetrySlots).toHaveBeenCalledTimes(1);
		expect(screen.getByText("Selected")).toBeInTheDocument();
	});

	it("disables continue and shows a hint when no template is selected", () => {
		renderStep({ templateCanvasId: "" });
		expect(
			screen.getByRole("button", { name: /Continue to resources/i })
		).toBeDisabled();
		expect(
			screen.getByText("Select a template to continue.")
		).toBeInTheDocument();
	});

	it("shows slot loading placeholders", () => {
		renderStep({
			slotsByTemplate: {},
			isLoadingSlots: true,
		});
		expect(screen.getAllByText("Loading...").length).toBeGreaterThan(0);
	});
});
