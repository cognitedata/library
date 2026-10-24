import type React from "react";

import type uPlot from "uplot";

import { FONT_FAMILY } from "./constants";
import type { TimeseriesNodeData } from "./types";
import { hasSource } from "./types";

export interface BuildUPlotOptionsParams {
	width: number;
	height: number;
	color: string;
	label: string;
	minY?: number;
	maxY?: number;
	canvasScaleRef: React.RefObject<number>;
	dataRef: React.RefObject<TimeseriesNodeData>;
	onDataChangeRef: React.RefObject<(data: TimeseriesNodeData) => void>;
	setCursorIdx: (idx: number | null) => void;
	setCursorLeft: (left: number) => void;
	setCursorTop: (top: number) => void;
}

/**
 * Build the full uPlot options object.
 *
 * The canvas framework applies CSS transform: scale(canvasScale) to all nodes.
 * uPlot uses getBoundingClientRect() for mouse tracking, which returns screen-space
 * coordinates (scaled). But its internal coordinate system uses the configured
 * logical width/height. We fix this mismatch via cursor.bind: intercept mouse events
 * and scale clientX/clientY from screen-space to logical-space before uPlot sees them.
 */
export function buildUPlotOptions({
	width,
	height,
	color,
	label,
	minY,
	maxY,
	canvasScaleRef,
	dataRef,
	onDataChangeRef,
	setCursorIdx,
	setCursorLeft,
	setCursorTop,
}: BuildUPlotOptionsParams): uPlot.Options {
	// Scale a mouse event's clientX/clientY from screen-space to logical-space.
	// During drag selection, uPlot passes `document` as targ for the mouseup
	// handler. Since document has no getBoundingClientRect, we pass the event
	// through unmodified in that case (the drag-end only needs to fire, not
	// to carry precise coordinates).
	const scaleEvent = (e: MouseEvent, targ: HTMLElement): MouseEvent => {
		const s = canvasScaleRef.current;
		if (s === 1 || typeof targ.getBoundingClientRect !== "function") return e;
		const rect = targ.getBoundingClientRect();
		const lx = (e.clientX - rect.left) / s + rect.left;
		const ly = (e.clientY - rect.top) / s + rect.top;
		return new Proxy(e, {
			get(t, p) {
				if (p === "clientX") return lx;
				if (p === "clientY") return ly;
				const v = Reflect.get(t, p);
				return typeof v === "function" ? v.bind(t) : v;
			},
		});
	};

	// Wrap uPlot's event handlers to inject scaled coordinates
	const scaledBind = (
		_self: uPlot,
		targ: HTMLElement,
		handler: (e: MouseEvent) => void
	): ((e: MouseEvent) => null) => {
		return (e: MouseEvent) => {
			handler(scaleEvent(e, targ));
			return null;
		};
	};

	return {
		width,
		height,
		padding: [8, 12, 0, 0],
		legend: { show: false },
		cursor: {
			show: true,
			x: true,
			y: false,
			drag: { x: true, y: false, setScale: false },
			points: {
				size: 8,
				fill: color,
				stroke: "#fff",
				width: 2,
			},
			bind: {
				mousedown: scaledBind,
				mousemove: scaledBind,
				mouseup: scaledBind,
				click: scaledBind,
				dblclick: scaledBind,
				mouseenter: scaledBind,
				mouseleave: scaledBind,
			},
		},
		select: {
			show: true,
			left: 0,
			top: 0,
			width: 0,
			height: 0,
		},
		hooks: {
			setSelect: [
				(u: uPlot) => {
					if (u.select.width < 4) return; // ignore tiny drags
					const minSec = u.posToVal(u.select.left, "x");
					const maxSec = u.posToVal(u.select.left + u.select.width, "x");
					if (maxSec - minSec < 1) return;

					const newStart = new Date(minSec * 1000).toISOString();
					const newEnd = new Date(maxSec * 1000).toISOString();
					const d = dataRef.current;

					if (hasSource(d)) {
						onDataChangeRef.current({
							...d,
							startDate: newStart,
							endDate: newEnd,
							source: {
								...d.source,
								...(d.source.type === "fdm"
									? { startDate: newStart, endDate: newEnd }
									: {}),
							},
						});
					} else {
						onDataChangeRef.current({
							...d,
							startDate: newStart,
							endDate: newEnd,
						});
					}

					// Clear the selection rectangle
					u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false);
				},
			],
			setCursor: [
				(u: uPlot) => {
					const idx = u.cursor.idx;
					if (idx == null || idx < 0 || idx >= u.data[0].length) {
						setCursorIdx(null);
						return;
					}
					const px = u.valToPos(u.data[0][idx] as number, "x");
					const val = u.data[1][idx] as number;
					const py = u.valToPos(val, "y");
					// valToPos returns positions relative to the inner plotting area.
					// Add the plot area offset so coordinates are relative to the chart
					// container (accounts for y-axis label width and top padding).
					const dpr = window.devicePixelRatio || 1;
					setCursorIdx(idx);
					setCursorLeft(px + u.bbox.left / dpr);
					setCursorTop(py + u.bbox.top / dpr);
				},
			],
		},
		scales: {
			x: {
				time: true,
				// When a date range is set, use it as the visible x-axis range so
				// axis labels match the requested period (months for 1y, days for 1m, etc.)
				// instead of auto-fitting to where data points happen to cluster.
				...(() => {
					const d = dataRef.current;
					if (d.startDate && d.endDate) {
						const lo = new Date(d.startDate).getTime() / 1000;
						const hi = new Date(d.endDate).getTime() / 1000;
						if (hi > lo) return { range: [lo, hi] as uPlot.Range.MinMax };
					}
					return {};
				})(),
			},
			y: {
				auto: true,
				...(minY !== undefined && maxY !== undefined
					? { range: [minY, maxY] as uPlot.Range.MinMax }
					: {}),
			},
		},
		axes: [
			{
				stroke: "#999",
				grid: { stroke: "rgba(0,0,0,0.06)", width: 1 },
				ticks: { stroke: "rgba(0,0,0,0.1)", width: 1, size: 4 },
				font: `11px ${FONT_FAMILY}`,
				gap: 4,
			},
			{
				stroke: "#999",
				grid: { stroke: "rgba(0,0,0,0.06)", width: 1 },
				ticks: { stroke: "rgba(0,0,0,0.1)", width: 1, size: 4 },
				font: `11px ${FONT_FAMILY}`,
				size: 50,
				gap: 4,
				values: (_u: uPlot, vals: number[]) => vals.map((v) => v.toFixed(1)),
			},
		],
		series: [
			{},
			{
				stroke: color,
				width: 1.5,
				label: label || "Value",
			},
		],
	};
}
