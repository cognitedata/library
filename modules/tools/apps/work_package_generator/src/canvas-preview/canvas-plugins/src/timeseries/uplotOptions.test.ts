import { describe, expect, it, vi } from "vitest";
import type { TimeseriesNodeData } from "./types";
import { buildUPlotOptions } from "./uplotOptions";

function params(data: TimeseriesNodeData) {
	return {
		width: 200,
		height: 100,
		color: "#000",
		label: "",
		canvasScaleRef: { current: 2 },
		dataRef: { current: data },
		onDataChangeRef: { current: vi.fn() },
		setCursorIdx: vi.fn(),
		setCursorLeft: vi.fn(),
		setCursorTop: vi.fn(),
	};
}

describe("buildUPlotOptions", () => {
	it("scales mouse events and ignores document targets", () => {
		const options = buildUPlotOptions(
			params({
				series: [{ x: 1, y: 2 }],
				startDate: "2026-01-01",
				endDate: "2026-02-01",
			})
		);
		const bind = options.cursor?.bind?.mousedown;
		expect(bind).toBeTypeOf("function");
		const handler = vi.fn();
		const wrapped = bind?.(
			{} as never,
			{ getBoundingClientRect: () => ({ left: 10, top: 20 }) } as HTMLElement,
			handler
		);
		wrapped?.({ clientX: 30, clientY: 40 } as MouseEvent);
		expect(handler).toHaveBeenCalled();

		const passthrough = bind?.({} as never, document.body, handler);
		const event = { clientX: 1, clientY: 2 } as MouseEvent;
		Object.defineProperty(document.body, "getBoundingClientRect", {
			value: undefined,
			configurable: true,
		});
		passthrough?.(event);
	});

	it("writes a date range from a drag selection", () => {
		const onDataChange = vi.fn();
		const data: TimeseriesNodeData = {
			source: {
				type: "fdm",
				instance: {
					space: "s",
					externalId: "e",
					viewSpace: "v",
					viewExternalId: "CogniteTimeSeries",
				},
			},
		};
		const options = buildUPlotOptions({
			...params(data),
			minY: 0,
			maxY: 10,
			onDataChangeRef: { current: onDataChange },
		});
		const setSelect = options.hooks?.setSelect?.[0];
		const plot = {
			select: { left: 0, width: 2, top: 0, height: 0 },
			posToVal: (_value: number) => 0,
			setSelect: vi.fn(),
			data: [[1], [2]],
			cursor: { idx: null },
			valToPos: () => 0,
			bbox: { left: 0, top: 0 },
		};
		setSelect?.(plot as never);
		plot.select.width = 20;
		plot.posToVal = (value: number) => value;
		setSelect?.(plot as never);
		expect(onDataChange).toHaveBeenCalled();
	});

	it("clears the cursor when the index is out of range", () => {
		const setCursorIdx = vi.fn();
		const options = buildUPlotOptions({
			...params({ series: [] }),
			setCursorIdx,
		});
		const setCursor = options.hooks?.setCursor?.[0];
		setCursor?.({
			cursor: { idx: -1 },
			data: [[1], [2]],
			valToPos: () => 1,
			bbox: { left: 10, top: 10 },
		} as never);
		expect(setCursorIdx).toHaveBeenCalledWith(null);
		setCursor?.({
			cursor: { idx: 0 },
			data: [[1], [2]],
			valToPos: () => 4,
			bbox: { left: 10, top: 8 },
		} as never);
		expect(setCursorIdx).toHaveBeenCalledWith(0);
	});
});
