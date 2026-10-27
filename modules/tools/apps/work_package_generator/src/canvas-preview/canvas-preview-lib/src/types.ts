import type { Point, Size } from "@dune-canvas/canvas-lib";
import type {
	CauseMapNodeData,
	CommentNodeData,
	ImageAnnotationNodeData,
	ImageNodeData,
	LabelNodeData,
	PDFNodeData,
	PolylineNodeData,
	ShapeNodeData,
	StickyNodeData,
	SvgNodeData,
	TableNodeData,
	TextNodeData,
	TimeseriesNodeData,
	VideoNodeData,
} from "@dune-canvas/canvas-plugins";

interface BaseNodeDef {
	id: string;
	position: Point;
	size: Size;
	zIndex: number;
	rotation?: number;
	locked?: boolean;
	parentId?: string;
	relativePosition?: Point;
	relativeSize?: Size;
}

export type PdfNodeDef = BaseNodeDef & { type: "pdf"; data: PDFNodeData };
export type ShapeNodeDef = BaseNodeDef & { type: "shape"; data: ShapeNodeData };
export type TimeseriesNodeDef = BaseNodeDef & {
	type: "timeseries";
	data: TimeseriesNodeData;
};
export type TableNodeDef = BaseNodeDef & { type: "table"; data: TableNodeData };
export type TextNodeDef = BaseNodeDef & { type: "text"; data: TextNodeData };
export type StickyNodeDef = BaseNodeDef & {
	type: "sticky";
	data: StickyNodeData;
};
export type LabelNodeDef = BaseNodeDef & { type: "label"; data: LabelNodeData };
export type CommentNodeDef = BaseNodeDef & {
	type: "comment";
	data: CommentNodeData;
};
export type PolylineNodeDef = BaseNodeDef & {
	type: "polyline";
	data: PolylineNodeData;
};
export type SvgNodeDef = BaseNodeDef & { type: "svg"; data: SvgNodeData };
export type ImageNodeDef = BaseNodeDef & { type: "image"; data: ImageNodeData };
export type ImageAnnotationNodeDef = BaseNodeDef & {
	type: "image-annotation";
	data: ImageAnnotationNodeData;
};
export type CauseMapNodeDef = BaseNodeDef & {
	type: "cause-map";
	data: CauseMapNodeData;
};
export type VideoNodeDef = BaseNodeDef & { type: "video"; data: VideoNodeData };

export type CanvasNodeDef =
	| PdfNodeDef
	| ShapeNodeDef
	| TimeseriesNodeDef
	| TableNodeDef
	| TextNodeDef
	| StickyNodeDef
	| LabelNodeDef
	| CommentNodeDef
	| PolylineNodeDef
	| SvgNodeDef
	| ImageNodeDef
	| ImageAnnotationNodeDef
	| CauseMapNodeDef
	| VideoNodeDef;
