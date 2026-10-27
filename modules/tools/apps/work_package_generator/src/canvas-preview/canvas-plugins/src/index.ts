/**
 * Plugin exports
 *
 * Node type plugins the read-only preview registers (see
 * canvas-preview-lib/src/register.ts) and their data types.
 */

export { type CommentNodeData, commentPlugin } from "./comment/index.tsx";
export {
	type CauseMapNodeData,
	causeMapPlugin,
} from "./experimental/cause-map/index.tsx";
export {
	type VideoNodeData,
	videoPlugin,
} from "./experimental/video/index.tsx";
export { type ImageNodeData, imagePlugin } from "./image/index.tsx";
export {
	type ImageAnnotationNodeData,
	imageAnnotationPlugin,
} from "./image-annotation/index.tsx";
export { type LabelNodeData, labelPlugin } from "./label/index.tsx";
export { type PDFNodeData, pdfPlugin } from "./pdf/index.tsx";
export { type PolylineNodeData, polylinePlugin } from "./polyline/index.tsx";
export { type ShapeNodeData, shapePlugin } from "./shape/index.tsx";
export { type StickyNodeData, stickyPlugin } from "./sticky/index.tsx";
export { type SvgNodeData, svgPlugin } from "./svg/index.tsx";
export { type TableNodeData, tablePlugin } from "./table/index.tsx";
export { type TextNodeData, textPlugin } from "./text/index.tsx";
export {
	type TimeseriesNodeData,
	timeseriesPlugin,
} from "./timeseries/index.tsx";
