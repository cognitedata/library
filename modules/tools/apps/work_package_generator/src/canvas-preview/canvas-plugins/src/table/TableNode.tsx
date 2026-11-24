/**
 * Table Node Component
 * Renders a data table
 *
 * Supports two data modes:
 * - Direct: columns/rows provided inline
 * - Source-based: data fetched from FDM instance
 *
 * Auto-resizes to fit content when the data changes.
 */

import type {
	NodeComponentProps,
	TableColumn,
	TableSource,
} from "@dune-canvas/canvas-lib";
import { useResolvedTableData } from "@dune-canvas/canvas-lib";
import type React from "react";
import { useEffect, useLayoutEffect, useMemo, useRef } from "react";

// ============================================================================
// Types
// ============================================================================

// Re-export TableColumn from canvas-lib for backwards compatibility
export type { TableColumn } from "@dune-canvas/canvas-lib";

/**
 * Configuration for a single property's visibility and display order
 * in the Property Selector.
 */
export interface PropertyConfig {
	/** Original property key from the data source */
	key: string;
	/** Human-readable display label */
	label: string;
	/** Whether the property is visible in the table */
	visible: boolean;
	/** Display order index (lower = higher in the table) */
	index: number;
}

/**
 * Table node data - supports two modes:
 * 1. Direct data: provide columns and rows directly
 * 2. Source-based: provide a source that will be resolved to columns/rows
 */
export interface TableNodeData {
	/** Data source (FDM or direct) - if provided, takes precedence over columns/rows */
	source?: TableSource;
	/** Direct columns (used when no source is provided) */
	columns?: TableColumn[];
	/** Direct rows (used when no source is provided) */
	rows?: Record<string, string | number>[];
	title?: string;
	/** Hide the column headers (useful for key-value tables) */
	hideHeader?: boolean;
	/** Hide the footer row count */
	hideFooter?: boolean;
	/** Property visibility and ordering configuration (from Property Selector) */
	propertyConfig?: PropertyConfig[];
}

// Minimum padding around the table content
const PADDING = 2;

// ============================================================================
// Component
// ============================================================================

export const TableNode: React.FC<NodeComponentProps<TableNodeData>> = ({
	data,
	size,
	onSizeChange,
}) => {
	const { source, title, hideHeader = false, hideFooter = false } = data;

	// Resolve data from source (if provided)
	const resolved = useResolvedTableData(source, { enabled: !!source });

	// Use resolved data if source is provided, otherwise use direct data
	const columns = useMemo(
		() => (source ? resolved.columns : (data.columns ?? [])),
		[source, resolved.columns, data.columns]
	);
	const allRows = useMemo(
		() => (source ? resolved.rows : (data.rows ?? [])),
		[source, resolved.rows, data.rows]
	);
	const isLoading = source ? resolved.isLoading : false;
	const error = source ? resolved.error : null;

	// Use the first column key to identify the property name in each row
	const propertyColKey = columns[0]?.key;

	// Apply property filtering/reordering if propertyConfig is set
	const rows = useMemo(() => {
		if (
			!data.propertyConfig ||
			data.propertyConfig.length === 0 ||
			!propertyColKey
		)
			return allRows;

		// Build a map of visible property keys to their display order
		const visibleKeys = new Map(
			data.propertyConfig
				.filter((p) => p.visible)
				.sort((a, b) => a.index - b.index)
				.map((p, i) => [p.key, i])
		);

		return allRows
			.filter((row) => visibleKeys.has(String(row[propertyColKey])))
			.sort(
				(a, b) =>
					(visibleKeys.get(String(a[propertyColKey])) ?? 0) -
					(visibleKeys.get(String(b[propertyColKey])) ?? 0)
			);
	}, [allRows, data.propertyConfig, propertyColKey]);

	const containerRef = useRef<HTMLDivElement>(null);
	const tableRef = useRef<HTMLTableElement>(null);
	const hasResizedRef = useRef(false);

	// Store onSizeChange in a ref to avoid it being a dependency in the effect
	const onSizeChangeRef = useRef(onSizeChange);
	onSizeChangeRef.current = onSizeChange;

	// Measure and resize to fit content
	useLayoutEffect(() => {
		if (!containerRef.current || !tableRef.current || !onSizeChangeRef.current)
			return;
		if (isLoading) return; // Don't resize while loading

		const container = containerRef.current;
		const table = tableRef.current;

		// Get the title height if present
		const titleEl = container.querySelector(
			"[data-table-title]"
		) as HTMLElement | null;
		const titleHeight = titleEl ? titleEl.offsetHeight : 0;

		// Get the footer height
		const footerEl = container.querySelector(
			"[data-table-footer]"
		) as HTMLElement | null;
		const footerHeight = footerEl ? footerEl.offsetHeight : 0;

		// Calculate the ideal size to show all content
		// Use scrollWidth/scrollHeight to get the full content size including overflow
		const tableWidth = table.scrollWidth;
		const tableHeight = table.scrollHeight;

		// Add border and padding
		const idealWidth = tableWidth + PADDING;
		const idealHeight = tableHeight + titleHeight + footerHeight + PADDING;

		// Only resize if there's a significant difference (> 5px) to avoid infinite loops
		// Also check hasResizedRef to prevent re-triggering after we've already resized
		const widthDiff = Math.abs(idealWidth - size.width);
		const heightDiff = Math.abs(idealHeight - size.height);

		if (!hasResizedRef.current && (widthDiff > 5 || heightDiff > 5)) {
			onSizeChangeRef.current({
				width: Math.max(idealWidth, 200), // Respect minimum
				height: Math.max(idealHeight, 100),
			});
			hasResizedRef.current = true;
		}
	}, [columns, rows, title, size.width, size.height, isLoading]);

	// Reset resize flag when data changes significantly
	useEffect(() => {
		hasResizedRef.current = false;
	}, [columns.length, rows.length, data.propertyConfig]);

	// Show loading state
	if (isLoading) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					backgroundColor: "#fff",
					border: "1px solid #ddd",
					display: "flex",
					flexDirection: "column",
					alignItems: "center",
					justifyContent: "center",
					color: "#666",
				}}
			>
				<div style={{ fontSize: 14 }}>Loading...</div>
			</div>
		);
	}

	// Show error state
	if (error) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					backgroundColor: "#fff",
					border: "1px solid #ddd",
					display: "flex",
					flexDirection: "column",
					alignItems: "center",
					justifyContent: "center",
					color: "#d32f2f",
					padding: 16,
					overflow: "hidden",
					boxSizing: "border-box",
				}}
			>
				<div style={{ fontSize: 14, fontWeight: 500 }}>Error loading data</div>
				<div
					style={{
						fontSize: 12,
						marginTop: 8,
						color: "#666",
						textAlign: "center",
						wordBreak: "break-word",
						overflow: "hidden",
						textOverflow: "ellipsis",
						maxWidth: "100%",
					}}
				>
					{error.message}
				</div>
			</div>
		);
	}

	return (
		<div
			ref={containerRef}
			style={{
				width: "100%",
				height: "100%",
				backgroundColor: "#fff",
				border: "1px solid #ddd",
				display: "flex",
				flexDirection: "column",
				overflow: "hidden",
			}}
		>
			{/* Title */}
			{title && (
				<div
					data-table-title
					style={{
						padding: "12px 16px",
						borderBottom: "1px solid #e9ecef",
						backgroundColor: "#f8f9fa",
						flexShrink: 0,
					}}
				>
					<div
						style={{
							display: "flex",
							alignItems: "flex-start",
							gap: 10,
						}}
					>
						{/* Icon */}
						<div
							style={{
								width: 20,
								height: 20,
								borderRadius: "50%",
								border: "2px solid #4dabf7",
								flexShrink: 0,
								marginTop: 2,
							}}
						/>
						{/* Title text */}
						<div
							style={{
								fontWeight: 600,
								fontSize: 14,
								color: "#212529",
								lineHeight: 1.4,
								wordBreak: "break-word",
							}}
						>
							{title}
						</div>
					</div>
				</div>
			)}

			{/* Table */}
			<div
				style={{
					flex: 1,
					overflow: "hidden", // Changed from 'auto' - no scrolling, content should fit
				}}
			>
				<table
					ref={tableRef}
					style={{
						width: "max-content", // Changed from '100%' to get natural width
						minWidth: "100%",
						borderCollapse: "collapse",
						fontSize: 12,
					}}
				>
					{!hideHeader && (
						<thead>
							<tr>
								{columns.map((col) => (
									<th
										key={col.key}
										style={{
											padding: "8px 12px",
											textAlign: "left",
											borderBottom: "2px solid #eee",
											backgroundColor: "#fafafa",
											fontWeight: 600,
											position: "sticky",
											top: 0,
											width: col.width,
											color: "#333",
										}}
									>
										{col.label}
									</th>
								))}
							</tr>
						</thead>
					)}
					<tbody>
						{rows.map((row, rowIndex) => (
							<tr
								key={rowIndex}
								style={{
									backgroundColor: "#fff",
								}}
							>
								{columns.map((col, colIndex) => (
									<td
										key={col.key}
										style={{
											padding: "10px 16px",
											borderBottom:
												rowIndex < rows.length - 1
													? "1px dotted #dee2e6"
													: "none",
											color: colIndex === 0 ? "#495057" : "#212529",
											fontWeight: colIndex === 0 ? 500 : 400,
											textAlign: colIndex === 0 ? "left" : "right",
											verticalAlign: "top",
										}}
									>
										{row[col.key] ?? "—"}
									</td>
								))}
							</tr>
						))}
					</tbody>
				</table>
			</div>

			{/* Footer */}
			{!hideFooter && (
				<div
					data-table-footer
					style={{
						padding: "4px 12px",
						borderTop: "1px solid #eee",
						fontSize: 10,
						color: "#999",
						backgroundColor: "#fafafa",
						flexShrink: 0,
					}}
				>
					{rows.length} rows × {columns.length} columns
				</div>
			)}
		</div>
	);
};

// ============================================================================
// Placeholder
// ============================================================================

export const TablePlaceholder: React.FC<{
	data: TableNodeData;
	size: { width: number; height: number };
}> = ({ data }) => {
	const { source, title } = data;

	// Get columns/rows from source or direct data
	const columns =
		source?.type === "direct" ? source.columns : (data.columns ?? []);
	const rows = source?.type === "direct" ? source.rows : (data.rows ?? []);

	// For FDM source, show loading placeholder
	const isLoading = source?.type === "fdm";

	return (
		<div
			style={{
				width: "100%",
				height: "100%",
				backgroundColor: "#f9f9f9",
				border: "1px solid #ddd",
				display: "flex",
				flexDirection: "column",
				overflow: "hidden",
			}}
		>
			{/* Title skeleton */}
			{title && (
				<div
					style={{
						padding: "8px 12px",
						borderBottom: "1px solid #eee",
						fontWeight: 600,
						fontSize: 13,
						backgroundColor: "#fafafa",
					}}
				>
					{title}
				</div>
			)}

			{/* Table skeleton */}
			<div style={{ flex: 1, padding: 12 }}>
				{/* Header row */}
				<div style={{ display: "flex", gap: 8, marginBottom: 8 }}>
					{(columns.length > 0 ? columns : [{}, {}]).slice(0, 4).map((_, i) => (
						<div
							key={i}
							style={{
								flex: 1,
								height: 12,
								backgroundColor: "#ddd",
								borderRadius: 2,
							}}
						/>
					))}
				</div>

				{/* Data rows */}
				{Array.from({ length: isLoading ? 5 : Math.min(rows.length, 5) }).map(
					(_, i) => (
						<div
							key={i}
							style={{
								display: "flex",
								gap: 8,
								marginBottom: 6,
							}}
						>
							{(columns.length > 0 ? columns : [{}, {}])
								.slice(0, 4)
								.map((_, j) => (
									<div
										key={j}
										style={{
											flex: 1,
											height: 10,
											backgroundColor: "#e5e5e5",
											borderRadius: 2,
										}}
									/>
								))}
						</div>
					)
				)}
			</div>

			{/* Footer */}
			<div
				style={{
					padding: "4px 12px",
					borderTop: "1px solid #eee",
					fontSize: 10,
					color: "#999",
					backgroundColor: "#fafafa",
				}}
			>
				{isLoading
					? "Loading..."
					: `${rows.length} rows × ${columns.length} columns`}
			</div>
		</div>
	);
};
