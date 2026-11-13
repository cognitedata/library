// Provider that owns the preview QueryClient

// Canvas adapter — aggregate raw FDM data into CanvasNodeDef[]
export {
	type AggregateCanvasNodesOptions,
	aggregateCanvasNodes,
} from "./adapters/canvasAdapter";
export {
	CanvasPreviewProvider,
	type CanvasPreviewProviderProps,
} from "./CanvasPreviewProvider";
// Query types needed by aggregateCanvasNodes consumers
export type {
	CanvasAnnotation,
	FdmInstanceContainerReference,
} from "./canvasQueries";
// Main component
export { PreviewCanvas, type PreviewCanvasProps } from "./PreviewCanvas";

// Typed node definitions
export type { CanvasNodeDef } from "./types";
