import { describe, expect, it } from "vitest";

import { annotationToNode } from "./canvasAdapter";
import { polylineGeometry } from "./mappers";

describe(polylineGeometry.name, () => {
	it("places a polyline from its vertices instead of the canvas origin", () => {
		const geometry = polylineGeometry(
			[
				{ x: 3058.21, y: -266.98 },
				{ x: 3065.01, y: 997.31 },
			],
			12
		);

		expect(geometry.x).toBeCloseTo(3058.21 - 8);
		expect(geometry.y).toBeCloseTo(-266.98 - 8);
		expect(geometry.points[0]).toEqual({ x: 8, y: 8 });
		expect(geometry.width).toBeGreaterThan(20);
		expect(geometry.height).toBeGreaterThan(1000);
	});
});

describe(annotationToNode.name, () => {
	it("keeps polyline strokes inside the node box", () => {
		const node = annotationToNode({
			space: "cdf_industrial_canvas",
			externalId: "61bdabdc-3e76-41b0-809d-811041b57206",
			properties: {
				id: "61bdabdc-3e76-41b0-809d-811041b57206",
				annotationType: "polylineAnnotation",
				properties: {
					vertices: [
						{ x: 3058.21, y: -266.98 },
						{ x: 3065.01, y: 997.31 },
					],
					style: { stroke: "rgb(66, 85, 187)", strokeWidth: 12 },
				},
			},
		});

		const { points } = node.data as { points: Array<{ x: number; y: number }> };
		expect(node.position.x).toBeGreaterThan(3000);
		expect(
			points.every((point) => point.x >= 0 && point.x <= node.size.width)
		).toBe(true);
		expect(
			points.every((point) => point.y >= 0 && point.y <= node.size.height)
		).toBe(true);
	});
});
