/**
 * Timeseries Placeholder — lightweight sparkline (no uPlot dependency)
 * Shown when the node is virtualized (outside viewport or near-culled).
 */

import {
	downsample,
	type TimeseriesDataPoint,
	type TimeseriesSource,
	useProgressiveFidelity,
	useResolvedTimeseries,
} from "@dune-canvas/canvas-lib";
import type React from "react";
import { useMemo } from "react";

import { stateContainerStyle } from "./constants";
import type { TimeseriesNodeData } from "./types";
import { hasDirectSeries, hasSource } from "./types";

const MAX_PLACEHOLDER_POINTS = 150;

const placeholderContainerStyle: React.CSSProperties = {
	...stateContainerStyle,
	flexDirection: "row",
	gap: 0,
	padding: 0,
};

export const TimeseriesPlaceholder: React.FC<{
	data: TimeseriesNodeData;
	size: { width: number; height: number };
}> = ({ data, size }) => {
	const source: TimeseriesSource | undefined = hasSource(data)
		? data.source
		: undefined;
	const {
		series: resolvedSeries,
		isLoading,
		error,
	} = useResolvedTimeseries(source);

	const series: TimeseriesDataPoint[] = hasDirectSeries(data)
		? data.series
		: resolvedSeries;

	const { dataPointMultiplier, fidelity, downsampleMode } =
		useProgressiveFidelity({
			maxZoomMultiplier: 3,
		});

	const sparklinePath = useMemo(() => {
		if (series.length === 0) return "";

		const fidelityMultiplier = fidelity === "high" ? 1 : 0.5;
		const targetPoints = Math.min(
			series.length,
			Math.min(
				50 * dataPointMultiplier * fidelityMultiplier,
				MAX_PLACEHOLDER_POINTS
			)
		);
		const points = downsample(
			series,
			Math.max(5, targetPoints),
			downsampleMode
		);
		const xMin = points[0].x;
		const xMax = points[points.length - 1].x;
		const xRange = xMax - xMin || 1;

		let yMin = Infinity;
		let yMax = -Infinity;
		for (const p of points) {
			if (p.y < yMin) yMin = p.y;
			if (p.y > yMax) yMax = p.y;
		}
		const yRange = yMax - yMin || 1;

		const padding = 10;
		const width = size.width - padding * 2;
		const height = size.height - padding * 2;

		let path = "";
		for (let i = 0; i < points.length; i++) {
			const x = padding + ((points[i].x - xMin) / xRange) * width;
			const y = padding + ((yMax - points[i].y) / yRange) * height;
			path += i === 0 ? `M ${x} ${y}` : ` L ${x} ${y}`;
		}

		return path;
	}, [series, size, dataPointMultiplier, fidelity, downsampleMode]);

	if (isLoading) {
		return (
			<div style={{ ...placeholderContainerStyle }}>
				<div style={{ color: "#999", fontSize: 10 }}>Loading...</div>
			</div>
		);
	}

	if (error) {
		return (
			<div
				style={{
					...placeholderContainerStyle,
					backgroundColor: "#fff5f5",
					border: "1px solid #ffcccc",
				}}
			>
				<div style={{ color: "#cc0000", fontSize: 10 }}>Error</div>
			</div>
		);
	}

	if (series.length === 0) {
		return (
			<div style={placeholderContainerStyle}>
				<div style={{ color: "#999", fontSize: 10 }}>No data</div>
			</div>
		);
	}

	return (
		<div style={placeholderContainerStyle}>
			<svg
				width="100%"
				height="100%"
				viewBox={`0 0 ${size.width} ${size.height}`}
			>
				<path
					d={sparklinePath}
					fill="none"
					stroke={data.color || "#0066ff"}
					strokeWidth="2"
					strokeOpacity="0.5"
				/>
			</svg>
			<div
				style={{
					position: "absolute",
					bottom: 5,
					right: 5,
					fontSize: 10,
					color: "#999",
				}}
			>
				{series.length} points
			</div>
		</div>
	);
};
