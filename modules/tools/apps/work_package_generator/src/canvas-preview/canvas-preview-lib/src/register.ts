import { registerNodeType } from "@dune-canvas/canvas-lib";
import {
	causeMapPlugin,
	commentPlugin,
	imageAnnotationPlugin,
	imagePlugin,
	labelPlugin,
	pdfPlugin,
	polylinePlugin,
	shapePlugin,
	stickyPlugin,
	svgPlugin,
	tablePlugin,
	textPlugin,
	timeseriesPlugin,
	videoPlugin,
} from "@dune-canvas/canvas-plugins";

let registered = false;

/**
 * Registers the node types aggregateCanvasNodes can produce: file, timeseries
 * and table nodes for FDM instances, plus every annotation type.
 */
export function ensurePluginsRegistered(): void {
	if (registered) return;

	registerNodeType(pdfPlugin);
	registerNodeType(shapePlugin);
	registerNodeType(timeseriesPlugin);
	registerNodeType(tablePlugin);
	registerNodeType(textPlugin);
	registerNodeType(stickyPlugin);
	registerNodeType(labelPlugin);
	registerNodeType(commentPlugin);
	registerNodeType(polylinePlugin);
	registerNodeType(svgPlugin);
	registerNodeType(imagePlugin);
	registerNodeType(imageAnnotationPlugin);
	registerNodeType(causeMapPlugin);
	registerNodeType(videoPlugin);

	registered = true;
}
