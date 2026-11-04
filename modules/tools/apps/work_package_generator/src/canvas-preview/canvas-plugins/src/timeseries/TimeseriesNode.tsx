/**
 * Timeseries Chart Node Component
 * Interactive chart powered by uPlot with:
 * - Crosshair cursor with timestamp display
 * - Hover tooltip showing Min/Max/Average/Count stats
 * - Drag-to-zoom horizontal selection for date range changes
 * - Date range stored on node data, synced with toolbar controls
 *
 * Active/Passive Mode:
 * - Passive (default): overlay blocks pointer events so canvas panning works normally
 * - Active (click on selected node): overlay removed, uPlot receives events
 * - Flow: 1st click selects → 2nd click activates → click outside / Escape deactivates
 *
 * Flexible Data Sources:
 * - Direct: series data provided inline (filtered client-side by date range)
 * - FDM: data referenced via source property (date range triggers refetch)
 */

import type React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import uPlot from "uplot";

import "uplot/dist/uPlot.min.css";
import type {
	NodeComponentProps,
	TimeseriesDataPoint,
	TimeseriesSource,
} from "@dune-canvas/canvas-lib";
import { useResolvedTimeseries } from "@dune-canvas/canvas-lib";

import {
	DRAG_THRESHOLD,
	FONT_FAMILY,
	HEADER_HEIGHT,
	stateContainerStyle,
	TIMESTAMP_FULL_WIDTH,
	TIMESTAMP_HALF_WIDTH,
	TOOLTIP_GAP,
	TOOLTIP_WIDTH,
} from "./constants";
import { computeStats, formatTimestamp } from "./helpers";
import type { TimeseriesNodeData } from "./types";
import { hasDirectSeries, hasSource } from "./types";
import { buildUPlotOptions } from "./uplotOptions";

// Re-export types for backward compatibility
export type {
	TimeseriesDataPoint,
	TimeseriesNodeData,
	TimeseriesNodeDataDirect,
	TimeseriesNodeDataSource,
} from "./types";

// ============================================================================
// Main Component
// ============================================================================

export const TimeseriesNode: React.FC<
	NodeComponentProps<TimeseriesNodeData>
> = ({
	data,
	size,
	onDataChange,
	canvasScale,
	isCanvasPanning,
	isFocused,
	isSelected,
}) => {
	const containerRef = useRef<HTMLDivElement>(null);
	const chartRef = useRef<uPlot | null>(null);

	// ---- Active/Passive mode ----
	// Passive (default): overlay blocks pointer events so canvas panning works normally.
	// Active (after click on selected node): overlay removed, uPlot receives events for cursor & drag-to-zoom.
	// Flow: 1st click selects node → 2nd click enters active mode → click outside / Escape deactivates.
	const [isActive, setIsActive] = useState(false);

	// Deactivate when node loses focus or selection (user clicked elsewhere on canvas)
	useEffect(() => {
		if (!isFocused && !isSelected) setIsActive(false);
	}, [isFocused, isSelected]);

	// Deactivate on Escape key
	useEffect(() => {
		if (!isActive) return;
		const handleKeyDown = (e: KeyboardEvent) => {
			if (e.key === "Escape") setIsActive(false);
		};
		window.addEventListener("keydown", handleKeyDown);
		return () => window.removeEventListener("keydown", handleKeyDown);
	}, [isActive]);

	// Hover state for the passive-mode overlay hint text
	const [isOverlayHovered, setIsOverlayHovered] = useState(false);

	// Activate chart interaction on click (only when node is already selected).
	// Track selection state and mouse position at mouseDown so we can distinguish:
	//   1st click (selects node) → don't activate
	//   2nd click (node already selected) → activate
	//   drag (mouse moved > threshold) → don't activate (user is repositioning the node)
	const wasSelectedRef = useRef(false);
	const mouseDownPosRef = useRef<{ x: number; y: number } | null>(null);
	const handleOverlayMouseDown = useCallback(
		(e: React.MouseEvent) => {
			wasSelectedRef.current = !!isSelected;
			mouseDownPosRef.current = { x: e.clientX, y: e.clientY };
		},
		[isSelected]
	);
	const handleOverlayClick = useCallback((e: React.MouseEvent) => {
		const start = mouseDownPosRef.current;
		if (!start || !wasSelectedRef.current) return;
		const dx = e.clientX - start.x;
		const dy = e.clientY - start.y;
		if (dx * dx + dy * dy > DRAG_THRESHOLD * DRAG_THRESHOLD) return; // was a drag
		e.stopPropagation();
		setIsActive(true);
	}, []);

	// ---- Cursor state for tooltip overlay ----
	const [cursorIdx, setCursorIdx] = useState<number | null>(null);
	const [cursorLeft, setCursorLeft] = useState<number>(0);
	const [cursorTop, setCursorTop] = useState<number>(0);

	// Track whether a drag originated inside the chart area.
	// Only stop mousemove propagation for chart-initiated drags (selection),
	// not for drags that started on the header (node move).
	const chartDraggingRef = useRef(false);

	// Refs for callbacks/values to avoid stale closures inside uPlot hooks
	const onDataChangeRef = useRef(onDataChange);
	const dataRef = useRef(data);
	const canvasScaleRef = useRef(canvasScale);
	useEffect(() => {
		onDataChangeRef.current = onDataChange;
	}, [onDataChange]);
	useEffect(() => {
		dataRef.current = data;
	}, [data]);
	useEffect(() => {
		canvasScaleRef.current = canvasScale;
	}, [canvasScale]);

	// Move the uPlot crosshair off-screen and clear React tooltip state
	const clearChartCursor = useCallback(() => {
		if (chartRef.current) {
			chartRef.current.setCursor({ left: -10, top: -10 });
		}
		setCursorIdx(null);
	}, []);

	// Clear cursor when the canvas is panning (right-click drag or touchpad scroll).
	// The chart moves under the mouse but no mouse event fires, so the cursor would
	// be stale. The canvas framework now signals panning for both pointer and wheel pan.
	useEffect(() => {
		if (isCanvasPanning) clearChartCursor();
	}, [isCanvasPanning, clearChartCursor]);

	// Clear cursor and tooltips when deactivating (switching to passive mode)
	useEffect(() => {
		if (!isActive) clearChartCursor();
	}, [isActive, clearChartCursor]);

	// ---- Data resolution ----

	const source: TimeseriesSource | undefined = hasSource(data)
		? data.source
		: undefined;
	const {
		series: resolvedSeries,
		isLoading,
		error,
	} = useResolvedTimeseries(source);

	// Raw series from either direct data or resolved source
	const rawSeries: TimeseriesDataPoint[] = hasDirectSeries(data)
		? data.series
		: resolvedSeries;

	// Apply date range filter (mainly for direct data; FDM handles it server-side)
	const series = useMemo(() => {
		if (rawSeries.length === 0) return rawSeries;
		if (!data.startDate && !data.endDate) return rawSeries;

		const startMs = data.startDate
			? new Date(data.startDate).getTime()
			: -Infinity;
		const endMs = data.endDate ? new Date(data.endDate).getTime() : Infinity;

		return rawSeries.filter((p) => p.x >= startMs && p.x <= endMs);
	}, [rawSeries, data.startDate, data.endDate]);

	// ---- Stats for tooltip card ----

	const stats = useMemo(() => computeStats(series), [series]);

	// ---- Convert to uPlot columnar format (timestamps in seconds) ----
	// uPlot handles large datasets (150K+ points) natively and efficiently,
	// so we pass the full series without downsampling. No progressive fidelity needed.

	const uplotData = useMemo((): uPlot.AlignedData => {
		if (series.length === 0) return [[], []];
		const xs: number[] = new Array(series.length);
		const ys: number[] = new Array(series.length);
		for (let i = 0; i < series.length; i++) {
			xs[i] = series[i].x / 1000; // ms -> seconds for uPlot
			ys[i] = series[i].y;
		}
		return [xs, ys];
	}, [series]);

	// ---- Chart lifecycle ----

	// Build uPlot options (stable except when visual config changes)
	const color = data.color || "#3366ff";
	const label = data.label || "";

	// Create and mount the uPlot instance. Recreated when data or visual config changes.
	// Canvas zoom/pan is handled purely by CSS + cursor.bind, so canvasScale and
	// isCanvasPanning are intentionally NOT dependencies.
	useEffect(() => {
		const el = containerRef.current;
		if (!el || uplotData[0].length === 0) return;

		// Destroy previous instance
		if (chartRef.current) {
			chartRef.current.destroy();
			chartRef.current = null;
		}

		const opts = buildUPlotOptions({
			width: size.width,
			height: Math.max(50, size.height - HEADER_HEIGHT - 2), // -2 for borders
			color,
			label,
			minY: data.minY,
			maxY: data.maxY,
			canvasScaleRef,
			dataRef,
			onDataChangeRef,
			setCursorIdx,
			setCursorLeft,
			setCursorTop,
		});

		const chart = new uPlot(opts, uplotData, el);
		chartRef.current = chart;

		return () => {
			chart.destroy();
			chartRef.current = null;
		};
	}, [uplotData, size.width, size.height, color, label, data.minY, data.maxY]);

	// ---- Tooltip data ----
	// Cursor positions from uPlot are already in logical space (thanks to cursor.bind scaling)
	const cursorTimestamp =
		cursorIdx != null ? (uplotData[0][cursorIdx] as number) * 1000 : null;
	const cursorValue =
		cursorIdx != null ? (uplotData[1][cursorIdx] as number) : null;
	// Per-bucket stats for the hovered point (when data has server-side aggregates)
	const cursorPoint = cursorIdx != null ? series[cursorIdx] : null;
	const cursorBucketStats =
		cursorPoint?.min !== undefined
			? {
					min: cursorPoint.min!.toFixed(3),
					max: cursorPoint.max!.toFixed(3),
					average: cursorPoint.y.toFixed(3),
					count: cursorPoint.count ?? 0,
				}
			: null;

	// ---- Loading state ----
	if (isLoading) {
		return (
			<div style={stateContainerStyle}>
				<div style={{ color: "#666", fontSize: 14 }}>Loading timeseries...</div>
				{data.label && (
					<div style={{ color: "#999", fontSize: 12 }}>{data.label}</div>
				)}
			</div>
		);
	}

	// ---- Error state ----
	if (error) {
		return (
			<div
				style={{
					...stateContainerStyle,
					backgroundColor: "#fff5f5",
					border: "1px solid #ffcccc",
				}}
			>
				<div style={{ color: "#cc0000", fontSize: 14 }}>Error loading data</div>
				<div
					style={{
						color: "#666",
						fontSize: 11,
						textAlign: "center",
						wordBreak: "break-word",
						maxWidth: "100%",
					}}
				>
					{error.message}
				</div>
			</div>
		);
	}

	// ---- Empty state ----
	if (series.length === 0) {
		return (
			<div style={stateContainerStyle}>
				<div style={{ color: "#666", fontSize: 14 }}>No data available</div>
				{data.label && (
					<div style={{ color: "#999", fontSize: 12 }}>{data.label}</div>
				)}
			</div>
		);
	}

	return (
		<div
			style={{
				width: "100%",
				height: "100%",
				backgroundColor: "#fff",
				border: isActive ? "1px solid #3366ff" : "1px solid #ddd",
				overflow: "hidden",
				position: "relative",
				display: "flex",
				flexDirection: "column",
				transition: "border-color 0.15s ease",
			}}
		>
			{/* Header bar — acts as drag handle for the node.
          Mouse events propagate through to the canvas framework so dragging
          this area moves the node on the canvas. */}
			<div
				style={{
					height: HEADER_HEIGHT,
					minHeight: HEADER_HEIGHT,
					display: "flex",
					alignItems: "center",
					padding: "0 10px",
					cursor: "grab",
					borderBottom: "1px solid #eee",
					backgroundColor: "#fafafa",
					userSelect: "none",
				}}
			>
				<div
					style={{
						fontSize: 12,
						fontWeight: 600,
						color: "#555",
						fontFamily: FONT_FAMILY,
						overflow: "hidden",
						textOverflow: "ellipsis",
						whiteSpace: "nowrap",
					}}
				>
					{data.label || "Timeseries"}
				</div>
			</div>

			{/* Chart area — interactive only in active mode. mousemove is stopped only
          when the drag started inside the chart (for selection), not when it started
          on the header (for node move). In passive mode, pointerEvents are disabled
          so canvas panning works normally over the chart. */}
			<div
				ref={containerRef}
				style={{
					flex: 1,
					minHeight: 0,
					position: "relative",
					zIndex: 1,
					backgroundColor: "#fff",
					pointerEvents: isActive ? "auto" : "none",
				}}
				onMouseDown={() => {
					chartDraggingRef.current = true;
				}}
				onMouseUp={() => {
					chartDraggingRef.current = false;
				}}
				onMouseLeave={() => {
					chartDraggingRef.current = false;
				}}
				onMouseMove={(e) => {
					if (chartDraggingRef.current) e.stopPropagation();
				}}
			/>

			{/* Interaction gate overlay — blocks chart events in passive mode.
          Click to activate chart interaction when node is already selected.
          Flow: 1st click selects the node, 2nd click enters active mode.
          Shown only when not active; transparent so the chart is still visible. */}
			{!isActive && (
				<div
					onMouseDown={handleOverlayMouseDown}
					onClick={handleOverlayClick}
					onMouseEnter={() => setIsOverlayHovered(true)}
					onMouseLeave={() => setIsOverlayHovered(false)}
					style={{
						position: "absolute",
						left: 0,
						right: 0,
						top: HEADER_HEIGHT,
						bottom: 0,
						zIndex: 5,
						cursor: isSelected ? "pointer" : "grab",
						display: "flex",
						alignItems: "center",
						justifyContent: "center",
					}}
				>
					{/* Hint text — visible on hover when node is selected */}
					{isSelected && (
						<div
							style={{
								padding: "4px 10px",
								borderRadius: 4,
								backgroundColor: "rgba(0, 0, 0, 0.06)",
								color: "rgba(0, 0, 0, 0.35)",
								fontSize: 11,
								fontFamily: FONT_FAMILY,
								pointerEvents: "none",
								userSelect: "none",
								opacity: isOverlayHovered ? 1 : 0,
								transition: "opacity 0.15s ease",
							}}
						>
							Click to interact
						</div>
					)}
				</div>
			)}

			{/* Tooltip card — "smart slide": always to the side of the cursor, never overlapping the data point.
          Hidden for very small containers where it would dominate the chart area. */}
			{size.width >= 250 &&
				size.height >= 180 &&
				cursorIdx != null &&
				cursorValue != null &&
				(cursorBucketStats || stats) &&
				(() => {
					const displayStats = cursorBucketStats || stats;
					// Vertical: center on the data point, clamp to stay within node bounds
					const tooltipTop = Math.max(
						HEADER_HEIGHT + 4,
						Math.min(cursorTop + HEADER_HEIGHT - 50, size.height - 110)
					);
					// Horizontal: always to the side, never overlapping the cursor
					const spaceOnRight = size.width - cursorLeft - TOOLTIP_GAP;
					const tooltipLeft =
						spaceOnRight >= TOOLTIP_WIDTH
							? cursorLeft + TOOLTIP_GAP
							: Math.max(4, cursorLeft - TOOLTIP_GAP - TOOLTIP_WIDTH);

					return (
						<div
							style={{
								position: "absolute",
								top: tooltipTop,
								left: tooltipLeft,
								backgroundColor: "#fff",
								border: "1px solid #e0e0e0",
								borderRadius: 6,
								padding: "6px 10px",
								boxShadow: "0 2px 8px rgba(0,0,0,0.1)",
								zIndex: 10,
								pointerEvents: "none",
								fontFamily: FONT_FAMILY,
								fontSize: 12,
								lineHeight: "20px",
								minWidth: 100,
								color: "#333",
							}}
						>
							<div>Min: {displayStats!.min}</div>
							<div>Max: {displayStats!.max}</div>
							<div>Average: {displayStats!.average}</div>
							<div>Count: {displayStats!.count}</div>
						</div>
					);
				})()}

			{/* Timestamp tooltip above the x-axis, clamped to stay within bounds.
          Hidden for very small containers where it would overlap the chart data. */}
			{size.width >= 200 &&
				size.height >= 140 &&
				cursorTimestamp != null &&
				cursorValue != null && (
					<div
						style={{
							position: "absolute",
							bottom: 32,
							left: Math.max(
								4,
								Math.min(
									cursorLeft - TIMESTAMP_HALF_WIDTH,
									size.width - TIMESTAMP_FULL_WIDTH
								)
							),
							backgroundColor: "rgba(30, 30, 30, 0.9)",
							color: "#fff",
							padding: "3px 10px",
							borderRadius: 4,
							fontSize: 11,
							fontFamily: FONT_FAMILY,
							whiteSpace: "nowrap",
							zIndex: 10,
							pointerEvents: "none",
						}}
					>
						{formatTimestamp(cursorTimestamp)}
					</div>
				)}
		</div>
	);
};
