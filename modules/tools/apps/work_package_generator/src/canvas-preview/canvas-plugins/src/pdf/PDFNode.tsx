/**
 * PDF Node Component
 * Renders a PDF document using pdf.js via the shared PDFRenderService.
 *
 * Features:
 * - Shared worker pool (limits concurrent renders based on CPU cores)
 * - Priority queue (visible PDFs render first)
 * - Progressive fidelity (quality scales with zoom level)
 * - Document annotations overlay (CDM annotations from P&IDs)
 *
 * Flexible Data Sources:
 * - Direct: URL provided inline
 * - FDM: file URL resolved from FDM instance reference (CogniteFile)
 */

import type {
	FileSource,
	FlexibleFileData,
	NodeComponentProps,
} from "@dune-canvas/canvas-lib";
import {
	stableScale,
	useDocumentAnnotations,
	useProgressiveFidelity,
	useResolvedFile,
} from "@dune-canvas/canvas-lib";
import { useSignals } from "@preact/signals-react/runtime";
import { useQuery } from "@tanstack/react-query";
import type React from "react";
import { useCallback, useEffect, useId, useRef, useState } from "react";

import { DocumentAnnotationOverlay } from "./DocumentAnnotationOverlay";
import {
	getPageDimensions,
	PDFRenderService,
	type RenderPriority,
} from "./PDFRenderService";

// Maximum quality multiplier to avoid excessive memory usage
const MAX_QUALITY_SCALE = 4;

// Threshold for re-rendering (10% change triggers update)
const QUALITY_CHANGE_THRESHOLD = 0.1;

// ============================================================================
// Types
// ============================================================================

/**
 * Legacy data format with direct URL
 */
export interface PDFNodeDataDirect {
	url: string;
	page?: number;
}

/**
 * Combined data type supporting both direct URL and flexible source
 * When `source` is present, URL is resolved via useResolvedFile hook
 * When `url` is present, URL is used directly
 */
export type PDFNodeData = PDFNodeDataDirect | FlexibleFileData;

/**
 * Type guard to check if data has direct URL
 */
function hasDirectUrl(data: PDFNodeData): data is PDFNodeDataDirect {
	return "url" in data && typeof (data as PDFNodeDataDirect).url === "string";
}

/**
 * Type guard to check if data has source for resolution
 */
function hasSource(data: PDFNodeData): data is FlexibleFileData {
	return "source" in data && (data as FlexibleFileData).source !== undefined;
}

// ============================================================================
// Component
// ============================================================================

export const PDFNode: React.FC<NodeComponentProps<PDFNodeData>> = ({
	data,
	size,
	isCanvasPanning,
	onSizeChange,
	rotation = 0,
	canvasScale = 1,
}) => {
	// Enable signal tracking for this component (required without babel transform)
	useSignals();

	// Generate stable ID for this PDF node instance
	const instanceId = useId();
	const canvasRef = useRef<HTMLCanvasElement>(null);

	// Track render state
	const [renderStage, setRenderStage] = useState<
		"idle" | "loading" | "rendering" | "complete"
	>("idle");
	const [error, setError] = useState<string | null>(null);
	const hasRenderedOnceRef = useRef(false);
	const lastRenderedQualityRef = useRef<number>(1);
	const lastRenderedPageRef = useRef<number>(1);
	const cancelRenderRef = useRef<(() => void) | null>(null);

	// Store onSizeChange in a ref to avoid it being a dependency in the effect
	const onSizeChangeRef = useRef(onSizeChange);
	onSizeChangeRef.current = onSizeChange;

	// Resolve data source if using FDM reference
	const source: FileSource | undefined = hasSource(data)
		? data.source
		: undefined;
	const {
		url: resolvedUrl,
		isLoading: isResolvingUrl,
		error: resolveError,
	} = useResolvedFile(source);

	// Get URL: either directly from data or from resolved source
	const url: string | null = hasDirectUrl(data) ? data.url : resolvedUrl;

	// Get page number from data (handling both data formats)
	const page = hasDirectUrl(data)
		? data.page
		: (data as FlexibleFileData).currentPage;
	const currentPage = page ?? 1;

	// Fetch document dimensions for annotation positioning
	const { data: pageDimensions } = useQuery({
		queryKey: ["pdf-dimensions", url, currentPage],
		queryFn: async () => {
			if (!url) return null;
			return getPageDimensions(url, currentPage);
		},
		enabled: !!url,
		staleTime: 10 * 60 * 1000, // 10 minutes
	});

	// Fetch document annotations (only for FDM-referenced files)
	const { annotations } = useDocumentAnnotations(source, currentPage, {
		enabled: source?.type === "fdm",
	});

	// Use progressive fidelity for quality-based rendering
	// Note: We use isInteracting from the hook (signal-based) rather than isCanvasPanning prop
	// to avoid race conditions where the prop might be stale when signals update
	const { shouldReduceFidelity, dataPointMultiplier, isInteracting } =
		useProgressiveFidelity({ maxZoomMultiplier: MAX_QUALITY_SCALE });

	// Read stableScale directly to ensure component subscribes to zoom-end updates
	// This is needed because the hook's derived values might not trigger re-renders properly
	const currentStableScale = stableScale.value;

	const qualityScale = Math.min(dataPointMultiplier, MAX_QUALITY_SCALE);

	// Determine render priority based on interaction state
	const getPriority = useCallback((): RenderPriority => {
		if (shouldReduceFidelity) return "low";
		return "high";
	}, [shouldReduceFidelity]);

	// Queue a render through the service
	const queueRender = useCallback(() => {
		const canvas = canvasRef.current;
		if (!canvas || !url) return;

		// Check if page changed
		const pageChanged = currentPage !== lastRenderedPageRef.current;

		// Don't re-render if quality hasn't changed significantly
		const qualityChangeRatio = qualityScale / lastRenderedQualityRef.current;
		const significantQualityChange =
			qualityChangeRatio > 1 + QUALITY_CHANGE_THRESHOLD ||
			qualityChangeRatio < 1 - QUALITY_CHANGE_THRESHOLD;

		// Skip render only if quality hasn't changed significantly AND page is the same
		if (
			hasRenderedOnceRef.current &&
			!significantQualityChange &&
			!pageChanged
		) {
			return;
		}

		// Cancel any pending render
		if (cancelRenderRef.current) {
			cancelRenderRef.current();
		}

		// Calculate scale: base scale to fit container * quality multiplier
		// Assume standard PDF page width of ~612 points (8.5" at 72dpi)
		const baseScale = size.width / 612;
		const renderScale = baseScale * qualityScale;

		// Update stage
		if (hasRenderedOnceRef.current) {
			setRenderStage("rendering"); // Show as upgrading
		} else {
			setRenderStage("loading");
		}
		setError(null);

		// Queue the render
		cancelRenderRef.current = PDFRenderService.queueRender({
			id: instanceId,
			url: url,
			page: page ?? 1,
			canvas,
			scale: renderScale,
			priority: getPriority(),
			onProgress: (stage) => {
				setRenderStage(stage);
			},
			onComplete: (success, err) => {
				if (success) {
					lastRenderedQualityRef.current = qualityScale;
					lastRenderedPageRef.current = currentPage;
					hasRenderedOnceRef.current = true;
					setRenderStage("complete");
				} else if (err) {
					setError(err.message);
					setRenderStage("idle");
				}
			},
		});
	}, [
		instanceId,
		url,
		page,
		currentPage,
		size.width,
		qualityScale,
		getPriority,
	]);

	// Trigger render when relevant props change
	useEffect(() => {
		// Don't render while interacting (panning or zooming)
		// We use signal-based isInteracting to avoid race conditions with stale props
		if (isInteracting) return;
		// Don't render while resolving URL
		if (isResolvingUrl || !url) return;

		queueRender();
		// currentStableScale ensures we re-render when zoom stabilizes (even if qualityScale hasn't changed much)
	}, [
		url,
		page,
		size.width,
		size.height,
		isInteracting,
		qualityScale,
		currentStableScale,
		queueRender,
		isResolvingUrl,
	]);

	// Reset state when URL changes (new PDF loaded)
	useEffect(() => {
		hasRenderedOnceRef.current = false;
		lastRenderedQualityRef.current = 1;
		setRenderStage("idle");
		setError(null);
	}, [url]);

	// Cleanup on unmount
	useEffect(() => {
		return () => {
			if (cancelRenderRef.current) {
				cancelRenderRef.current();
			}
		};
	}, []);

	// Get label from data (for flexible data format)
	const label = hasSource(data) ? data.label : undefined;

	// Show loading state while resolving URL from FDM
	if (isResolvingUrl) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					backgroundColor: "#f9f9f9",
					border: "1px solid #ddd",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					flexDirection: "column",
					gap: 8,
				}}
			>
				<div style={{ color: "#666", fontSize: 14 }}>Loading file...</div>
				{label && <div style={{ color: "#999", fontSize: 12 }}>{label}</div>}
			</div>
		);
	}

	// Show error state from URL resolution
	if (resolveError) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					backgroundColor: "#fff5f5",
					border: "1px solid #ffcccc",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					flexDirection: "column",
					gap: 8,
					padding: 16,
				}}
			>
				<div style={{ color: "#cc0000", fontSize: 14 }}>Error loading file</div>
				<div style={{ color: "#666", fontSize: 11, textAlign: "center" }}>
					{resolveError.message}
				</div>
			</div>
		);
	}

	// Show empty state when no URL available
	if (!url) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					backgroundColor: "#f9f9f9",
					border: "1px solid #ddd",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					flexDirection: "column",
					gap: 8,
				}}
			>
				<div style={{ color: "#666", fontSize: 14 }}>No file URL available</div>
				{label && <div style={{ color: "#999", fontSize: 12 }}>{label}</div>}
			</div>
		);
	}

	// Show full loading screen only if we have no content yet
	const showFullLoading =
		renderStage === "loading" && !hasRenderedOnceRef.current;
	// Show canvas if we have rendered at least once (even if currently upgrading)
	const showCanvas =
		hasRenderedOnceRef.current || (renderStage === "complete" && !error);
	// Show upgrading indicator when improving quality
	const showUpgrading =
		hasRenderedOnceRef.current &&
		(renderStage === "loading" || renderStage === "rendering");

	return (
		<div
			style={{
				position: "relative",
				width: "100%",
				height: "100%",
				backgroundColor: "#fff",
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				overflow: "hidden",
				border: "1px solid #ddd",
				userSelect: "none",
				WebkitUserSelect: "none",
			}}
		>
			{/* Full loading screen - only shown on initial load */}
			{showFullLoading && (
				<div style={{ color: "#666", fontSize: 14 }}>Loading PDF...</div>
			)}

			{/* Error message */}
			{error && (
				<div style={{ color: "red", fontSize: 12, padding: 10 }}>
					Error: {error}
				</div>
			)}

			{/* PDF canvas - shown once we have any content */}
			<canvas
				ref={canvasRef}
				style={{
					// Scale down the high-resolution canvas to fit the container
					// This gives us sharp rendering when zoomed in
					maxWidth: "100%",
					maxHeight: "100%",
					width: size.width,
					height: size.height,
					objectFit: "contain",
					display: showCanvas ? "block" : "none",
				}}
			/>

			{/* Document annotations overlay */}
			{showCanvas && pageDimensions && annotations.length > 0 && (
				<DocumentAnnotationOverlay
					annotations={annotations}
					containerWidth={size.width}
					containerHeight={size.height}
					documentWidth={pageDimensions.width}
					documentHeight={pageDimensions.height}
					rotation={((rotation % 360) + 360) % 360}
					canvasScale={canvasScale}
				/>
			)}

			{/* Quality upgrade spinner - shown in top-right while improving quality */}
			{showUpgrading && (
				<div
					style={{
						position: "absolute",
						top: 8,
						right: 8,
						display: "flex",
						alignItems: "center",
						justifyContent: "center",
						width: 24,
						height: 24,
						backgroundColor: "rgba(255, 255, 255, 0.9)",
						borderRadius: 4,
						boxShadow: "0 1px 3px rgba(0, 0, 0, 0.1)",
					}}
				>
					<svg
						width={16}
						height={16}
						viewBox="0 0 24 24"
						fill="none"
						stroke="#666"
						strokeWidth="2"
						strokeLinecap="round"
						style={{
							animation: "spin 1s linear infinite",
						}}
					>
						<path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83" />
					</svg>
				</div>
			)}

			{/* CSS for spinner animation */}
			<style>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
		</div>
	);
};

// ============================================================================
// Placeholder
// ============================================================================

export const PDFPlaceholder: React.FC<{
	data: PDFNodeData;
	size: { width: number; height: number };
}> = ({ data, size }) => {
	// Resolve data source if using FDM reference
	const source: FileSource | undefined = hasSource(data)
		? data.source
		: undefined;
	const { isLoading, error } = useResolvedFile(source);

	// Get label from data (for flexible data format)
	const label = hasSource(data) ? data.label : undefined;

	// Show loading indicator for placeholders
	if (isLoading) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					backgroundColor: "#f9f9f9",
					border: "1px solid #ddd",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
				}}
			>
				<div style={{ color: "#999", fontSize: 10 }}>Loading...</div>
			</div>
		);
	}

	// Show error indicator
	if (error) {
		return (
			<div
				style={{
					width: "100%",
					height: "100%",
					backgroundColor: "#fff5f5",
					border: "1px solid #ffcccc",
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
				}}
			>
				<div style={{ color: "#cc0000", fontSize: 10 }}>Error</div>
			</div>
		);
	}

	return (
		<div
			style={{
				width: "100%",
				height: "100%",
				backgroundColor: "#f5f5f5",
				border: "1px solid #ddd",
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				flexDirection: "column",
				gap: 8,
			}}
		>
			<svg
				width={48}
				height={48}
				viewBox="0 0 24 24"
				fill="none"
				stroke="#999"
				strokeWidth="1.5"
			>
				<path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
				<polyline points="14,2 14,8 20,8" />
				<line x1="16" y1="13" x2="8" y2="13" />
				<line x1="16" y1="17" x2="8" y2="17" />
				<polyline points="10,9 9,9 8,9" />
			</svg>
			<span style={{ color: "#999", fontSize: 12 }}>
				{label || "PDF Document"}
			</span>
		</div>
	);
};
