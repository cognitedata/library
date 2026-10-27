import { act, renderHook } from "@testing-library/react";
import { createElement, type ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CanvasResult } from "@/components/work-package/canvas-creation/types";
import {
	CanvasCreationSectionViewModelContext,
	type CanvasCreationSectionViewModelContextType,
	useCanvasCreationSectionViewModel,
} from "./useCanvasCreationSectionViewModel";

const stubDraftPreview = {
	data: [],
	isPending: false,
	isFetching: false,
	isPlaceholderData: false,
	error: null,
};

function wrapper({ children }: { children: ReactNode }) {
	const deps: CanvasCreationSectionViewModelContextType = {
		useCanvasDraftPreview: () => stubDraftPreview,
	};
	return createElement(
		CanvasCreationSectionViewModelContext.Provider,
		{ value: deps },
		children
	);
}

const savedResult: CanvasResult = {
	canvasId: "c1",
	canvasLink: "https://example.test/c",
	validation: {
		success: true,
		canvasExists: true,
		itemsCount: 1,
		expectedItemsCount: 1,
		itemsMatchExpected: true,
		canvasId: "c1",
		errors: [],
	},
	isUpdate: false,
};

describe(useCanvasCreationSectionViewModel.name, () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	it("should call onCreateCanvas when saving with no existing canvas", () => {
		const onCreateCanvas = vi.fn();
		const { result } = renderHook(
			() =>
				useCanvasCreationSectionViewModel({
					isReady: true,
					hasExistingCanvas: false,
					templateFields: [],
					aiSelections: {},
					onCreateCanvas,
				}),
			{ wrapper }
		);

		act(() => {
			result.current.handleSaveClick();
		});

		expect(onCreateCanvas).toHaveBeenCalledTimes(1);
		expect(result.current.existingSaveDialogOpen).toBe(false);
	});

	it("should open the existing-save dialog instead of creating when a canvas already exists", () => {
		const onCreateCanvas = vi.fn();
		const { result } = renderHook(
			() =>
				useCanvasCreationSectionViewModel({
					isReady: true,
					hasExistingCanvas: true,
					templateFields: [],
					aiSelections: {},
					onCreateCanvas,
				}),
			{ wrapper }
		);

		act(() => {
			result.current.handleSaveClick();
		});

		expect(onCreateCanvas).not.toHaveBeenCalled();
		expect(result.current.existingSaveDialogOpen).toBe(true);
	});

	it("should close the existing-save dialog when the duplicate flag clears", () => {
		const { result, rerender } = renderHook(
			({ hasExistingCanvas }) =>
				useCanvasCreationSectionViewModel({
					isReady: true,
					hasExistingCanvas,
					templateFields: [],
					aiSelections: {},
					onCreateCanvas: vi.fn(),
				}),
			{ initialProps: { hasExistingCanvas: true }, wrapper }
		);

		act(() => {
			result.current.handleSaveClick();
		});
		expect(result.current.existingSaveDialogOpen).toBe(true);

		rerender({ hasExistingCanvas: false });
		expect(result.current.existingSaveDialogOpen).toBe(false);
	});

	it("should mark a successful save and expose recovery hints only after a result", () => {
		const { result, rerender } = renderHook(
			({ canvasResult }: { canvasResult?: CanvasResult }) =>
				useCanvasCreationSectionViewModel({
					isReady: true,
					hasExistingCanvas: false,
					canvasResult,
					templateFields: [],
					aiSelections: {},
					onCreateCanvas: vi.fn(),
				}),
			{
				initialProps: { canvasResult: undefined as CanvasResult | undefined },
				wrapper,
			}
		);

		expect(result.current.isSavedSuccessfully).toBe(false);
		expect(result.current.recoveryHints).toEqual([]);

		rerender({ canvasResult: savedResult });
		expect(result.current.isSavedSuccessfully).toBe(true);
		expect(result.current.subtitle).toMatch(/open in Fusion/i);
	});
});
