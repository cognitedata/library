import { describe, expect, it } from "vitest";

import {
	calculateAnnotationSize,
	estimateTextSize,
	mapAnnotationDataToPluginData,
	mapAnnotationTypeToNodeType,
	parseFontSize,
	polylineGeometry,
} from "./mappers";

describe("mapAnnotationTypeToNodeType", () => {
	it.each([
		["textAnnotation", "text"],
		["stickyAnnotation", "sticky"],
		["labelAnnotation", "label"],
		["commentAnnotation", "comment"],
		["shapeAnnotation", "shape"],
		["rectangleAnnotation", "shape"],
		["ellipseAnnotation", "shape"],
		["lineAnnotation", "shape"],
		["polylineAnnotation", "polyline"],
		["svgAnnotation", "svg"],
		["imageAnnotation", "image-annotation"],
		["causeMapAnnotation", "cause-map"],
		["mystery", "annotation-mystery"],
	])("maps %s", (input, output) => {
		expect(mapAnnotationTypeToNodeType(input)).toBe(output);
	});
});

describe("parseFontSize", () => {
	it("parses numbers, pixel strings, and falls back", () => {
		expect(parseFontSize(undefined, 10)).toBe(10);
		expect(parseFontSize(18)).toBe(18);
		expect(parseFontSize("24px")).toBe(24);
		expect(parseFontSize("nope", 11)).toBe(11);
	});
});

describe("mapAnnotationDataToPluginData", () => {
	it("maps each annotation family", () => {
		expect(
			mapAnnotationDataToPluginData("textAnnotation", { text: "Hi" })
		).toMatchObject({ content: "Hi" });
		expect(mapAnnotationDataToPluginData("textAnnotation", null)).toMatchObject(
			{
				content: "",
			}
		);
		expect(
			mapAnnotationDataToPluginData("stickyAnnotation", { text: "N" })
		).toMatchObject({ content: "N", backgroundColor: "#fff59d" });
		expect(
			mapAnnotationDataToPluginData("labelAnnotation", { text: "L" })
		).toMatchObject({ text: "L" });
		expect(
			mapAnnotationDataToPluginData("commentAnnotation", {
				text: "C",
				author: "Sam",
			})
		).toMatchObject({ content: "C", author: "Sam" });
		expect(
			mapAnnotationDataToPluginData("rectangleAnnotation", {})
		).toMatchObject({ shape: "rectangle" });
		expect(
			mapAnnotationDataToPluginData("ellipseAnnotation", {})
		).toMatchObject({ shape: "ellipse" });
		expect(
			mapAnnotationDataToPluginData("shapeAnnotation", { shapeType: "ellipse" })
		).toMatchObject({ shape: "ellipse" });
		expect(
			mapAnnotationDataToPluginData("lineAnnotation", {
				points: [{ x: 1, y: 2 }],
			})
		).toMatchObject({ shape: "line" });
		expect(
			mapAnnotationDataToPluginData("polylineAnnotation", {
				vertices: [
					{ x: 0, y: 0 },
					{ x: 10, y: 0 },
				],
			})
		).toMatchObject({ strokeWidth: 2 });
		expect(mapAnnotationDataToPluginData("other", { a: 1 })).toEqual({ a: 1 });
		expect(mapAnnotationDataToPluginData("other", null)).toEqual({});
	});
});

describe("estimateTextSize", () => {
	it("grows with the text", () => {
		const small = estimateTextSize("a", 12);
		const large = estimateTextSize("a much longer string", 12);
		expect(large.width).toBeGreaterThan(small.width);
	});
});

describe("calculateAnnotationSize", () => {
	it("uses an explicit box, estimates text, and falls back", () => {
		expect(
			calculateAnnotationSize("textAnnotation", { width: 10, height: 20 })
		).toEqual({ width: 10, height: 20 });
		expect(
			calculateAnnotationSize("textAnnotation", { text: "Hello", width: 80 })
				.height
		).toBeGreaterThan(0);
		expect(
			calculateAnnotationSize("stickyAnnotation", {}).width
		).toBeGreaterThan(0);
	});
});

describe("polylineGeometry", () => {
	it("uses the default box when there are no vertices", () => {
		expect(polylineGeometry([], 2).points).toEqual([]);
	});
});
