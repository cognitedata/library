/**
 * Vendored subset of @dune-canvas/canvas-app.
 *
 * The work-package previewer only needs the client provider and the
 * data-resolution hooks. The upstream demo shell (CanvasApp, toolbars, export
 * dialog) and the Canvas CRUD SDK are not included.
 */

export type {
	FdmReference,
	FlexibleFileData,
	FlexibleTimeseriesData,
} from "./adapters/dataSources";
export { useDocumentAnnotations } from "./hooks/useDocumentAnnotations";
export {
	useResolvedFile,
	useResolvedTableData,
	useResolvedTimeseries,
} from "./hooks/useResolvedData";
export { SDKProvider } from "./sdk/SDKProvider";
