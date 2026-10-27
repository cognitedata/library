import { describe, expect, it } from "vitest";
import { ASSET_VIEW_ID } from "@/__mocks__/app-config";
import {
	extractInstanceProperties,
	getArrayProp,
	getNumberProp,
	getOptionalStringProp,
	getRefKey,
	getRefKeyFromNode,
	getStringProp,
	isCdfNode,
	mapNodeToResourceInstance,
	toCdfNode,
} from "./cdf-helpers";

describe("isCdfNode / toCdfNode", () => {
	it("rejects non-objects", () => {
		expect(isCdfNode(null)).toBe(false);
		expect(isCdfNode("x")).toBe(false);
		expect(toCdfNode(null)).toBeNull();
	});

	it("accepts minimal node", () => {
		const n = { externalId: "e", space: "s" };
		expect(isCdfNode(n)).toBe(true);
		expect(toCdfNode(n)).toEqual(n);
	});
});

describe("extractInstanceProperties", () => {
	const vid = ASSET_VIEW_ID;
	const key = `${vid.externalId}/${vid.version}`;

	const baseNode = { externalId: "test-id", space: "test-space" };

	it("returns empty when no properties", () => {
		expect(extractInstanceProperties({ ...baseNode }, vid)).toEqual({});
		expect(
			extractInstanceProperties({ ...baseNode, properties: null }, vid)
		).toEqual({});
	});

	it("reads nested space + view key", () => {
		const node = {
			...baseNode,
			properties: {
				[vid.space]: {
					[key]: { name: "Asset1" },
				},
			},
		};
		expect(extractInstanceProperties(node, vid)).toEqual({ name: "Asset1" });
	});

	it("falls back to flat view key", () => {
		const node = {
			...baseNode,
			properties: {
				[key]: { name: "Flat" },
			},
		};
		expect(extractInstanceProperties(node, vid)).toEqual({ name: "Flat" });
	});
});

describe("property getters", () => {
	const props = { a: "s", n: 3, bad: "x", arr: [1] };

	it("getStringProp", () => {
		expect(getStringProp(props, "a")).toBe("s");
		expect(getStringProp(props, "n", "d")).toBe("d");
	});

	it("getOptionalStringProp", () => {
		expect(getOptionalStringProp(props, "a")).toBe("s");
		expect(getOptionalStringProp(props, "n")).toBeUndefined();
	});

	it("getNumberProp", () => {
		expect(getNumberProp(props, "n", 0)).toBe(3);
		expect(getNumberProp(props, "a", 9)).toBe(9);
	});

	it("getArrayProp", () => {
		expect(getArrayProp<number>(props, "arr")).toEqual([1]);
		expect(getArrayProp(props, "missing")).toEqual([]);
	});
});

describe("mapNodeToResourceInstance", () => {
	it("maps node with props", () => {
		const vid = ASSET_VIEW_ID;
		const key = `${vid.externalId}/${vid.version}`;
		const node = {
			externalId: "e1",
			space: "sp",
			properties: { [vid.space]: { [key]: { name: "N", description: "D" } } },
		};
		const r = mapNodeToResourceInstance(node, vid);
		expect(r.externalId).toBe("e1");
		expect(r.name).toBe("N");
		expect(r.description).toBe("D");
		expect(r.properties.name).toBe("N");
	});
});

describe("getRefKey / getRefKeyFromNode", () => {
	it("creates consistent reference keys", () => {
		expect(getRefKey("asset123", "industrial")).toBe("asset123:industrial");
		expect(getRefKey("", "space")).toBe(":space");
		expect(getRefKey("ext", "")).toBe("ext:");
	});

	it("works with node objects", () => {
		const node = { externalId: "pump001", space: "equipment" };
		expect(getRefKeyFromNode(node)).toBe("pump001:equipment");
		expect(getRefKeyFromNode(node)).toBe(
			getRefKey(node.externalId, node.space)
		);
	});
});
