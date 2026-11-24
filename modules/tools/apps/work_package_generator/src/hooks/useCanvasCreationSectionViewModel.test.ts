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

	it("C08: does not save while the name check is running, and opens the dialog when a recheck finds a canvas", async () => {
		const onCreateCanvas = vi.fn();
		const recheckCanvasName = vi.fn().mockResolvedValue(true);
		const { result, rerender } = renderHook(
			({ isCheckingExists }: { isCheckingExists: boolean }) =>
				useCanvasCreationSectionViewModel({
					isReady: true,
					hasExistingCanvas: false,
					templateFields: [],
					aiSelections: {},
					onCreateCanvas,
					isCheckingExists,
					recheckCanvasName,
				}),
			{ initialProps: { isCheckingExists: true }, wrapper }
		);

		act(() => {
			result.current.handleSaveClick();
		});
		expect(onCreateCanvas).not.toHaveBeenCalled();
		expect(recheckCanvasName).not.toHaveBeenCalled();

		rerender({ isCheckingExists: false });
		await act(async () => {
			result.current.handleSaveClick();
		});
		expect(recheckCanvasName).toHaveBeenCalledTimes(1);
		expect(onCreateCanvas).not.toHaveBeenCalled();
		expect(result.current.existingSaveDialogOpen).toBe(true);
	});

	it("C11: treats a saved canvas as successful without post-write validation", () => {
		const failedValidation: CanvasResult = {
			...savedResult,
			validation: {
				...savedResult.validation,
				success: false,
				itemsMatchExpected: false,
				itemsCount: 0,
				expectedItemsCount: 3,
				errors: ["Item count mismatch: expected 3, found 0"],
			},
		};
		const { result } = renderHook(
			() =>
				useCanvasCreationSectionViewModel({
					isReady: true,
					hasExistingCanvas: false,
					canvasResult: failedValidation,
					templateFields: [],
					aiSelections: {},
					onCreateCanvas: vi.fn(),
				}),
			{ wrapper }
		);

		expect(result.current.isSavedSuccessfully).toBe(true);
		expect(result.current.recoveryHints).toEqual([]);
		expect(result.current.subtitle).toMatch(/open in Fusion/i);
		expect(result.current.subtitle).not.toMatch(/verification/i);
	});
});
