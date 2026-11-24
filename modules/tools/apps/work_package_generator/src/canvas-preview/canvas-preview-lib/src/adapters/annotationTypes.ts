/**
 * Annotation Style Types
 * Types matching the Canvas API annotation structure
 */

// ============================================================================
// Base Types
// ============================================================================

/** Font size can be a number or a string like "144px" */
export type FontSize = number | string;

/** Point/vertex type for polylines */
export interface Vertex {
	x: number;
	y: number;
}

// ============================================================================
// Style Properties
// ============================================================================

/** Base style properties shared by most annotations */
export interface BaseStyleProperties {
	fill?: string;
	stroke?: string;
	strokeWidth?: number;
	shouldEnableStrokeScale?: boolean;
	opacity?: number;
	dash?: number[];
}

/** Line/polyline style properties */
export interface LineStyleProperties extends BaseStyleProperties {
	dashEnabled?: boolean;
	lineType?: "straight" | "elbow" | "curved";
}

/** Text annotation style properties */
export interface TextStyleProperties {
	fontSize?: FontSize;
	fill?: string;
	fontFamily?: string;
	fontWeight?: "normal" | "bold";
	fontStyle?: "normal" | "italic";
	textAlign?: "left" | "center" | "right";
	padding?: number;
	lineHeight?: number;
}

/** Sticky note style properties */
export interface StickyStyleProperties {
	color?: string;
	backgroundColor?: string;
	padding?: number;
	lineHeight?: number;
	borderColor?: string;
	borderWidth?: number;
	borderRadius?: number;
}

/** Shape style properties */
export interface ShapeStyleProperties extends BaseStyleProperties {
	fillColor?: string;
	strokeColor?: string;
}

// ============================================================================
// Annotation Props (from API)
// ============================================================================

/** Text annotation additional properties from the API */
export interface TextAnnotationProps {
	x?: number;
	y?: number;
	width?: number;
	height?: number;
	text?: string;
	style?: TextStyleProperties;
	zIndex?: number;
}

/** Sticky annotation additional properties from the API */
export interface StickyAnnotationProps {
	x?: number;
	y?: number;
	width?: number;
	height?: number;
	text?: string;
	style?: StickyStyleProperties;
	zIndex?: number;
}

/** Polyline annotation additional properties from the API */
export interface PolylineAnnotationProps {
	vertices?: Vertex[];
	style?: LineStyleProperties;
	startEndType?: string;
	endEndType?: string;
	fromId?: string;
	toId?: string;
	zIndex?: number;
}

/** Shape annotation additional properties from the API */
export interface ShapeAnnotationProps {
	x?: number;
	y?: number;
	width?: number;
	height?: number;
	shapeType?: string;
	style?: ShapeStyleProperties;
	points?: Array<{ x: number; y: number }>; // For line shapes
	zIndex?: number;
}

/** Label annotation additional properties from the API */
export interface LabelAnnotationProps {
	x?: number;
	y?: number;
	width?: number;
	height?: number;
	text?: string;
	style?: TextStyleProperties;
	zIndex?: number;
}
