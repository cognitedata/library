/**
 * Document Annotation Overlay
 *
 * Renders CDM annotations on top of PDF documents.
 * Annotations are non-interactive visual elements that highlight
 * detected links to assets, files, timeseries, etc.
 *
 * Features:
 * - Color coding by resource type (matches fusion colors)
 * - Hover effects for visibility
 * - Rotates with the document
 * - Non-selectable, non-draggable, non-movable
 */

import type {
	AnnotationResourceType,
	DocumentAnnotation,
} from "@dune-canvas/canvas-lib";
import type React from "react";
import { useCallback, useState } from "react";

// ============================================================================
// Annotation Colors (exact values from @cognite/cogs.js-v10)
// ============================================================================

/**
 * Color configuration for each resource type
 * Exact colors from fusion's getResourceTypeAnnotationColor using cogs design tokens
 */
const ANNOTATION_COLORS: Record<
	AnnotationResourceType,
	{ stroke: string; hoverFill: string }
> = {
	asset: {
		stroke: "rgb(212, 106, 226)", // --cogs-decorative--purple--400
		hoverFill: "rgba(212, 106, 226, 0.15)",
	},
	file: {
		stroke: "rgb(255, 135, 70)", // --cogs-decorative--orange--400
		hoverFill: "rgba(255, 135, 70, 0.15)",
	},
	timeSeries: {
		stroke: "rgb(164, 178, 252)", // --cogs-decorative--blue--300
		hoverFill: "rgba(164, 178, 252, 0.15)",
	},
	sequence: {
		stroke: "rgb(255, 220, 127)", // --cogs-decorative--yellow--300
		hoverFill: "rgba(255, 220, 127, 0.15)",
	},
	event: {
		stroke: "rgb(253, 81, 144)", // --cogs-decorative--pink--400
		hoverFill: "rgba(253, 81, 144, 0.15)",
	},
	diagram: {
		stroke: "rgb(76, 175, 80)", // --cogs-decorative--green--400
		hoverFill: "rgba(76, 175, 80, 0.15)",
	},
	unknown: {
		stroke: "rgb(89, 89, 89)", // --cogs-decorative--grayscale--700
		hoverFill: "rgba(89, 89, 89, 0.15)",
	},
};

/**
 * Get annotation style based on resource type and hover state
 * Stroke width is scaled inversely with canvas zoom to maintain consistent visual thickness
 */
function getAnnotationStyle(
	resourceType: AnnotationResourceType,
	isHovered: boolean,
	canvasScale: number
) {
	// Inverse scale stroke width so it appears as ~2 physical pixels regardless of zoom
	// Clamp to reasonable range to avoid too thin or too thick
	const strokeWidth = Math.min(4, Math.max(1, 2 / canvasScale));

	const colors = ANNOTATION_COLORS[resourceType] ?? ANNOTATION_COLORS.unknown;

	return {
		stroke: colors.stroke,
		// Only show fill on hover - transparent otherwise
		fill: isHovered ? colors.hoverFill : "none",
		strokeWidth,
	};
}

// ============================================================================
// Types
// ============================================================================

export interface DocumentAnnotationOverlayProps {
	/** Annotations to render (coordinates are normalized 0-1) */
	annotations: DocumentAnnotation[];
	/** Container size in pixels */
	containerWidth: number;
	containerHeight: number;
	/** Original PDF document dimensions (for aspect ratio calculation) */
	documentWidth: number;
	documentHeight: number;
	/** Rotation angle in degrees (0, 90, 180, 270) */
	rotation?: number;
	/** Canvas zoom scale - used to keep stroke width consistent when zoomed out */
	canvasScale?: number;
	/** Optional click handler for annotations */
	onAnnotationClick?: (annotation: DocumentAnnotation) => void;
}

// ============================================================================
// Component
// ============================================================================

export const DocumentAnnotationOverlay: React.FC<
	DocumentAnnotationOverlayProps
> = ({
	annotations,
	containerWidth,
	containerHeight,
	documentWidth,
	documentHeight,
	rotation = 0,
	canvasScale = 1,
	onAnnotationClick,
}) => {
	const [hoveredId, setHoveredId] = useState<string | null>(null);

	const handleMouseEnter = useCallback((id: string) => {
		setHoveredId(id);
	}, []);

	const handleMouseLeave = useCallback(() => {
		setHoveredId(null);
	}, []);

	const handleClick = useCallback(
		(e: React.MouseEvent, annotation: DocumentAnnotation) => {
			e.stopPropagation();
			onAnnotationClick?.(annotation);
		},
		[onAnnotationClick]
	);

	// Calculate the actual rendered PDF size and position within the container
	// The PDF uses object-fit: contain and is centered, so we need to calculate
	// where it actually renders
	const docAspectRatio = documentWidth / documentHeight;
	const containerAspectRatio = containerWidth / containerHeight;

	let renderedWidth: number;
	let renderedHeight: number;
	let offsetX: number;
	let offsetY: number;

	if (docAspectRatio > containerAspectRatio) {
		// PDF is wider relative to container - width constrained
		renderedWidth = containerWidth;
		renderedHeight = containerWidth / docAspectRatio;
		offsetX = 0;
		offsetY = (containerHeight - renderedHeight) / 2;
	} else {
		// PDF is taller relative to container - height constrained
		renderedHeight = containerHeight;
		renderedWidth = containerHeight * docAspectRatio;
		offsetX = (containerWidth - renderedWidth) / 2;
		offsetY = 0;
	}

	// Transform annotation coordinates based on rotation
	// Note: Annotation coordinates are NORMALIZED (0-1 range) relative to PDF dimensions
	const transformAnnotation = (annotation: DocumentAnnotation) => {
		const { x, y, width, height } = annotation;

		// The annotation coordinates are normalized (0-1) relative to the unrotated document
		// Scale to actual rendered size and add offset for centering
		switch (rotation) {
			case 90:
				// Rotate 90 degrees clockwise
				return {
					x: offsetX + (1 - y - height) * renderedWidth,
					y: offsetY + x * renderedHeight,
					width: height * renderedWidth,
					height: width * renderedHeight,
				};
			case 180:
				// Rotate 180 degrees
				return {
					x: offsetX + (1 - x - width) * renderedWidth,
					y: offsetY + (1 - y - height) * renderedHeight,
					width: width * renderedWidth,
					height: height * renderedHeight,
				};
			case 270:
				// Rotate 270 degrees clockwise (or 90 counter-clockwise)
				return {
					x: offsetX + y * renderedWidth,
					y: offsetY + (1 - x - width) * renderedHeight,
					width: height * renderedWidth,
					height: width * renderedHeight,
				};
			default:
				// No rotation - scale normalized coords to rendered size and add offset
				return {
					x: offsetX + x * renderedWidth,
					y: offsetY + y * renderedHeight,
					width: width * renderedWidth,
					height: height * renderedHeight,
				};
		}
	};

	if (annotations.length === 0) {
		return null;
	}

	return (
		<svg
			style={{
				position: "absolute",
				top: 0,
				left: 0,
				width: "100%",
				height: "100%",
				pointerEvents: "none", // Allow clicks to pass through to PDF
				overflow: "visible",
			}}
			viewBox={`0 0 ${containerWidth} ${containerHeight}`}
		>
			{annotations.map((annotation) => {
				const isHovered = hoveredId === annotation.id;
				const style = getAnnotationStyle(
					annotation.resourceType,
					isHovered,
					canvasScale
				);
				const transformed = transformAnnotation(annotation);

				return (
					<rect
						key={annotation.id}
						x={transformed.x}
						y={transformed.y}
						width={transformed.width}
						height={transformed.height}
						fill={style.fill}
						stroke={style.stroke}
						strokeWidth={style.strokeWidth}
						rx={1}
						ry={1}
						style={{
							pointerEvents: "auto", // Re-enable pointer events on the rect
							cursor: "pointer",
						}}
						onMouseEnter={() => handleMouseEnter(annotation.id)}
						onMouseLeave={handleMouseLeave}
						onClick={(e) => handleClick(e, annotation)}
					>
						{/* Tooltip on hover */}
						{annotation.text && <title>{annotation.text}</title>}
					</rect>
				);
			})}
		</svg>
	);
};
