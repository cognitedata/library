/**
 * Canvas Adapter Mappers
 * Functions for mapping between API types and internal node types
 */

import type {
	FontSize,
	LabelAnnotationProps,
	PolylineAnnotationProps,
	ShapeAnnotationProps,
	StickyAnnotationProps,
	TextAnnotationProps,
} from "./annotationTypes";
import {
	DEFAULT_ANNOTATION_HEIGHT,
	DEFAULT_ANNOTATION_WIDTH,
	DEFAULT_FONT_SIZE,
	DEFAULT_LABEL_FONT_SIZE,
	MIN_TEXT_HEIGHT,
	MIN_TEXT_WIDTH,
	TEXT_CHAR_WIDTH_RATIO,
	TEXT_LINE_HEIGHT_RATIO,
} from "./constants";

// ============================================================================
// Shape Annotation Defaults
// ============================================================================

/** Default fill color for shape annotations */
export const DEFAULT_SHAPE_FILL_COLOR = "#9CA3AF"; // gray-400

/** Default stroke color for shape annotations */
export const DEFAULT_SHAPE_STROKE_COLOR = "#6B7280"; // gray-500

/** Default stroke width for shape annotations */
export const DEFAULT_SHAPE_STROKE_WIDTH = 2;

/** Maps specific annotation types to shape; shapeAnnotation uses props.shapeType */
const SHAPE_BY_ANNOTATION_TYPE: Partial<
	Record<string, "rectangle" | "ellipse">
> = {
	rectangleAnnotation: "rectangle",
	ellipseAnnotation: "ellipse",
};

// ============================================================================
// Node Type Mapping
// ============================================================================

/**
 * Maps annotation type from API to internal plugin node type
 */
export function mapAnnotationTypeToNodeType(annotationType: string): string {
	switch (annotationType) {
		case "textAnnotation":
			return "text";
		case "stickyAnnotation":
			return "sticky";
		case "labelAnnotation":
			return "label";
		case "commentAnnotation":
			return "comment";
		case "shapeAnnotation":
		case "rectangleAnnotation":
		case "ellipseAnnotation":
		case "lineAnnotation":
			return "shape";
		case "polylineAnnotation":
			return "polyline";
		case "svgAnnotation":
			return "svg";
		case "imageAnnotation":
			return "image-annotation";
		case "causeMapAnnotation":
			return "cause-map";
		default:
			return `annotation-${annotationType}`;
	}
}

// ============================================================================
// Font Size Parsing
// ============================================================================

/**
 * Parse a font size value (can be number or string like "144px")
 */
export function parseFontSize(
	value: FontSize | undefined,
	defaultSize: number = DEFAULT_FONT_SIZE
): number {
	if (value === undefined) return defaultSize;
	if (typeof value === "number") return value;
	return parseInt(value.replace("px", ""), 10) || defaultSize;
}

// ============================================================================
// Annotation Data Mapping
// ============================================================================

/**
 * Maps annotation properties to plugin-specific data format
 */
export function mapAnnotationDataToPluginData(
	annotationType: string,
	additionalProps: Record<string, unknown> | null | undefined
): Record<string, unknown> {
	switch (annotationType) {
		case "textAnnotation": {
			const props = (additionalProps ?? {}) as TextAnnotationProps;
			const style = props.style ?? {};

			return {
				content: props.text ?? "",
				fontSize: parseFontSize(style.fontSize),
				fontFamily: style.fontFamily ?? "sans-serif",
				fontWeight: style.fontWeight ?? "normal",
				fontStyle: style.fontStyle ?? "normal",
				textAlign: style.textAlign ?? "left",
				color: style.fill ?? "#000000",
				backgroundColor: "transparent",
			};
		}

		case "stickyAnnotation": {
			const props = (additionalProps ?? {}) as StickyAnnotationProps;
			const style = props.style ?? {};

			return {
				content: props.text ?? "",
				backgroundColor: style.backgroundColor ?? "#fff59d",
				textColor: style.color ?? "#333",
				borderColor: style.borderColor ?? "rgba(83, 88, 127, 0.24)",
				borderWidth: style.borderWidth ?? 2,
				borderRadius: style.borderRadius ?? 4,
			};
		}

		case "labelAnnotation": {
			const props = (additionalProps ?? {}) as LabelAnnotationProps;
			const style = props.style ?? {};

			return {
				text: props.text ?? "",
				fontSize: parseFontSize(style.fontSize, DEFAULT_LABEL_FONT_SIZE),
				color: style.fill ?? "#000000",
				backgroundColor: "transparent",
			};
		}

		case "commentAnnotation": {
			const props = additionalProps as
				| { text?: string; author?: string; timestamp?: string }
				| undefined;
			return {
				content: props?.text ?? "",
				author: props?.author ?? "",
				timestamp: props?.timestamp,
			};
		}

		case "shapeAnnotation":
		case "rectangleAnnotation":
		case "ellipseAnnotation": {
			const props = (additionalProps ?? {}) as ShapeAnnotationProps;
			const style = props.style ?? {};
			return {
				shape: SHAPE_BY_ANNOTATION_TYPE[annotationType] ?? props.shapeType,
				fill: style.fill ?? style.fillColor ?? DEFAULT_SHAPE_FILL_COLOR,
				stroke: style.stroke ?? style.strokeColor ?? DEFAULT_SHAPE_STROKE_COLOR,
				strokeWidth: style.strokeWidth ?? DEFAULT_SHAPE_STROKE_WIDTH,
			};
		}

		case "lineAnnotation": {
			const props = (additionalProps ?? {}) as ShapeAnnotationProps;
			const style = props.style ?? {};

			return {
				shape: "line" as const,
				fill: style.fill ?? "transparent",
				stroke: style.stroke ?? style.strokeColor ?? DEFAULT_SHAPE_STROKE_COLOR,
				strokeWidth: style.strokeWidth ?? DEFAULT_SHAPE_STROKE_WIDTH,
				points: props.points ?? [],
			};
		}

		case "polylineAnnotation": {
			const props = (additionalProps ?? {}) as PolylineAnnotationProps;
			const style = props.style ?? {};
			const strokeWidth = style.strokeWidth ?? 2;
			const geometry = polylineGeometry(props.vertices ?? [], strokeWidth);

			return {
				points: geometry.points,
				stroke: style.stroke ?? "#000000",
				strokeWidth,
				startEndType: props.startEndType ?? "none",
				endEndType: props.endEndType ?? "none",
				fill: style.fill ?? "transparent",
				opacity: style.opacity ?? 1,
			};
		}

		default:
			return { ...(additionalProps ?? {}) };
	}
}

// ============================================================================
// Text Size Estimation
// ============================================================================

/**
 * Estimate text dimensions based on content and font size
 */
export function estimateTextSize(
	text: string,
	fontSize: number
): { width: number; height: number } {
	const estimatedWidth = Math.max(
		text.length * fontSize * TEXT_CHAR_WIDTH_RATIO,
		MIN_TEXT_WIDTH
	);
	const estimatedHeight = Math.max(
		fontSize * TEXT_LINE_HEIGHT_RATIO,
		MIN_TEXT_HEIGHT
	);

	return {
		width: Math.ceil(estimatedWidth),
		height: Math.ceil(estimatedHeight),
	};
}

export interface PolylineGeometry {
	x: number;
	y: number;
	width: number;
	height: number;
	points: Array<{ x: number; y: number }>;
}

/**
 * CDF polyline vertices are absolute canvas coordinates and have no x/y box.
 * Convert them to a node origin plus points relative to that box so the
 * stroke stays inside the node instead of painting across the viewport.
 */
export function polylineGeometry(
	vertices: Array<{ x: number; y: number }>,
	strokeWidth: number
): PolylineGeometry {
	if (vertices.length === 0) {
		return {
			x: 0,
			y: 0,
			width: DEFAULT_ANNOTATION_WIDTH,
			height: DEFAULT_ANNOTATION_HEIGHT,
			points: [],
		};
	}

	let minX = vertices[0].x;
	let maxX = vertices[0].x;
	let minY = vertices[0].y;
	let maxY = vertices[0].y;
	for (const vertex of vertices) {
		minX = Math.min(minX, vertex.x);
		maxX = Math.max(maxX, vertex.x);
		minY = Math.min(minY, vertex.y);
		maxY = Math.max(maxY, vertex.y);
	}

	const padding = Math.max(strokeWidth, 2) / 2 + 2;
	const x = minX - padding;
	const y = minY - padding;

	return {
		x,
		y,
		width: Math.max(maxX - minX + padding * 2, 20),
		height: Math.max(maxY - minY + padding * 2, 20),
		points: vertices.map((vertex) => ({
			x: vertex.x - x,
			y: vertex.y - y,
		})),
	};
}

/**
 * Calculate size for an annotation node
 */
export function calculateAnnotationSize(
	annotationType: string,
	additionalProps: Record<string, unknown> | null | undefined
): { width: number; height: number } {
	const explicitWidth = additionalProps?.width as number | undefined;
	const explicitHeight = additionalProps?.height as number | undefined;

	if (explicitWidth !== undefined && explicitHeight !== undefined) {
		return { width: explicitWidth, height: explicitHeight };
	}

	if (annotationType === "textAnnotation") {
		const props = additionalProps as TextAnnotationProps | undefined;
		const text = props?.text ?? "";
		const style = props?.style ?? {};
		const fontSize = parseFontSize(style.fontSize);

		const estimated = estimateTextSize(text, fontSize);
		return {
			width: explicitWidth ?? estimated.width,
			height: explicitHeight ?? estimated.height,
		};
	}

	return {
		width: explicitWidth ?? DEFAULT_ANNOTATION_WIDTH,
		height: explicitHeight ?? DEFAULT_ANNOTATION_HEIGHT,
	};
}
