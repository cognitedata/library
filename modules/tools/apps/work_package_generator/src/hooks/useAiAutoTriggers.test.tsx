import { renderHook as renderHookBase, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { BASE_TEMPLATE_ID } from "@/__mocks__/app-config";
import { AppConfigProvider } from "@/app-config";
import type { AiSelections } from "@/types";
import { useAiAutoTriggers } from "./useAiAutoTriggers";

function renderHook<Result, Props>(
	hook: (props: Props) => Result,
	options?: Parameters<typeof renderHookBase<Result, Props>>[1]
) {
	return renderHookBase(hook, {
		...options,
		wrapper: ({ children }: { children: ReactNode }) => (
			<AppConfigProvider>{children}</AppConfigProvider>
		),
	});
}

const base = {
	workflowAutomationEnabled: true,
	templateStepConfirmed: true,
	resourcesStepConfirmed: true,
	workOrder: { externalId: "wo1", space: "s" },
	asset: { assetExternalId: "a1", space: "s" },
	templates: [{ externalId: "t1" }],
	resources: {
		file: [] as {
			externalId: string;
			space: string;
			properties: Record<string, unknown>;
		}[],
	},
	templateCanvasId: "tc1" as string | null,
	templateFields: [
		{ type: "file" as const, label: "Doc", viewExternalId: "CogniteFile" },
	],
	aiSelections: {} as AiSelections,
	aiSelectedCanvasId: null as string | null,
	isLoadingResources: false,
	isLoadingTemplates: false,
	isLoadingFields: false,
	isRunningAiCanvasRecommendation: false,
	isRunningAiSelection: false,
};

describe("useAiAutoTriggers", () => {
	it("runs canvas recommendation when prerequisites met", async () => {
		const runAiCanvasRecommendation = vi.fn();
		const runAiSelection = vi.fn();
		renderHook(() =>
			useAiAutoTriggers({
				...base,
				runAiCanvasRecommendation,
				runAiSelection,
			})
		);
		await waitFor(() => expect(runAiCanvasRecommendation).toHaveBeenCalled());
	});

	it("runs AI selection when template ready and selections empty", async () => {
		const runAiCanvasRecommendation = vi.fn();
		const runAiSelection = vi.fn();
		renderHook(() =>
			useAiAutoTriggers({
				...base,
				aiSelectedCanvasId: "already",
				runAiCanvasRecommendation,
				runAiSelection,
			})
		);
		await waitFor(() => expect(runAiSelection).toHaveBeenCalled());
	});

	it("does not run AI selection until template step is confirmed", async () => {
		const runAiCanvasRecommendation = vi.fn();
		const runAiSelection = vi.fn();
		renderHook(() =>
			useAiAutoTriggers({
				...base,
				templateStepConfirmed: false,
				aiSelectedCanvasId: "already",
				runAiCanvasRecommendation,
				runAiSelection,
			})
		);
		await new Promise((r) => setTimeout(r, 50));
		expect(runAiSelection).not.toHaveBeenCalled();
	});

	it("does not run canvas recommendation when only All Resources is available", async () => {
		const runAiCanvasRecommendation = vi.fn();
		const runAiSelection = vi.fn();
		renderHook(() =>
			useAiAutoTriggers({
				...base,
				templates: [{ externalId: BASE_TEMPLATE_ID }],
				runAiCanvasRecommendation,
				runAiSelection,
			})
		);
		await new Promise((r) => setTimeout(r, 50));
		expect(runAiCanvasRecommendation).not.toHaveBeenCalled();
	});

	it("does not run AI selection until resources step is confirmed", async () => {
		const runAiCanvasRecommendation = vi.fn();
		const runAiSelection = vi.fn();
		renderHook(() =>
			useAiAutoTriggers({
				...base,
				resourcesStepConfirmed: false,
				aiSelectedCanvasId: "already",
				runAiCanvasRecommendation,
				runAiSelection,
			})
		);
		await new Promise((r) => setTimeout(r, 50));
		expect(runAiSelection).not.toHaveBeenCalled();
	});
});
