import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WorkOrder } from "@/types";
import {
	UseCanvasNameFormContext,
	useCanvasNameForm,
} from "./useCanvasNameForm";

const useFindCanvasesByName = vi.fn();

const workOrder: WorkOrder = {
	externalId: "wo-1",
	space: "s",
	name: "Pump overhaul",
};

describe("useCanvasNameForm", () => {
	const createCanvas = vi.fn();
	const refetch = vi.fn();

	beforeEach(() => {
		vi.clearAllMocks();
		vi.useFakeTimers();
		useFindCanvasesByName.mockReturnValue({
			data: [],
			isLoading: false,
			error: null,
			refetch,
		});
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	function renderForm(
		props: Parameters<typeof useCanvasNameForm>[0] = {
			workOrder,
			createCanvas,
		}
	) {
		return renderHook(() => useCanvasNameForm(props), {
			wrapper: ({ children }: { children: ReactNode }) => (
				<UseCanvasNameFormContext.Provider value={{ useFindCanvasesByName }}>
					{children}
				</UseCanvasNameFormContext.Provider>
			),
		});
	}

	it("defaults the canvas name from the work order and creates with that name", () => {
		const { result } = renderForm();
		expect(result.current.canvasName).toBe("Canvas for Pump overhaul");
		expect(result.current.hasExistingCanvas).toBe(false);

		act(() => {
			result.current.onCreateCanvas();
		});
		expect(createCanvas).toHaveBeenCalledWith({
			canvasName: "Canvas for Pump overhaul",
		});
	});

	it("falls back to externalId when the work order has no name", () => {
		const { result } = renderForm({
			workOrder: { externalId: "wo-x", space: "s" },
			createCanvas,
		});
		expect(result.current.canvasName).toBe("Canvas for wo-x");
	});

	it("debounces name changes before lookup and updates when a match exists", async () => {
		useFindCanvasesByName.mockReturnValue({
			data: [{ externalId: "existing", name: "Canvas for Pump overhaul" }],
			isLoading: false,
			error: null,
			refetch,
		});
		const { result } = renderForm();
		expect(result.current.hasExistingCanvas).toBe(true);

		act(() => {
			result.current.onUpdateCanvas();
		});
		expect(createCanvas).toHaveBeenCalledWith({
			canvasName: "Canvas for Pump overhaul",
			existingCanvasId: "existing",
		});

		act(() => {
			result.current.setCanvasName("Renamed");
		});
		act(() => {
			vi.advanceTimersByTime(500);
		});
		expect(useFindCanvasesByName).toHaveBeenCalled();
	});

	it("does not update when no existing canvas is found", () => {
		const { result } = renderForm();
		act(() => {
			result.current.onUpdateCanvas();
		});
		expect(createCanvas).not.toHaveBeenCalled();
	});

	it("exposes lookup errors, retry, and reset", () => {
		useFindCanvasesByName.mockReturnValue({
			data: [],
			isLoading: false,
			error: new Error("lookup failed"),
			refetch,
		});
		const { result } = renderForm();
		expect(result.current.existingCanvasLookupErrorMessage).toBeTruthy();

		act(() => {
			result.current.onRetryExistingCanvasLookup();
			result.current.resetCanvasNameForm();
		});
		expect(refetch).toHaveBeenCalledTimes(1);
		expect(result.current.canvasName).toBe("");
	});
});
