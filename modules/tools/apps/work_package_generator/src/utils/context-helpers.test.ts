import { describe, expect, it } from "vitest";
import type { Asset, WorkOrder } from "../types";
import {
	extractAssetContext,
	extractWorkOrderContext,
} from "./context-helpers";

describe("extractWorkOrderContext", () => {
	it("returns empty for undefined", () => {
		expect(extractWorkOrderContext(undefined)).toEqual({});
	});

	it("extracts configured properties", () => {
		const wo: WorkOrder = {
			externalId: "e",
			space: "s",
			name: "WO",
			status: "open",
		};
		const ctx = extractWorkOrderContext(wo);
		expect(ctx.name).toBe("WO");
		expect(ctx.status).toBe("open");
	});
});

describe("extractAssetContext", () => {
	it("returns empty for undefined", () => {
		expect(extractAssetContext(undefined)).toEqual({});
	});

	it("prefers assetName over name", () => {
		const asset: Asset = {
			assetExternalId: "a",
			space: "s",
			assetName: "AN",
		};
		const ctx = extractAssetContext(asset);
		expect(ctx.name).toBe("AN");
	});

	it("uses name when no assetName", () => {
		const raw: Record<string, unknown> = {
			name: "N",
			assetExternalId: "a",
			space: "s",
		};
		const ctx = extractAssetContext(raw);
		expect(ctx.name).toBe("N");
	});

	it("stringifies numeric and boolean work order properties from config", () => {
		const wo: Record<string, unknown> = {
			externalId: "e",
			space: "s",
			name: "WO",
			status: "open",
			estimatedDuration: 120,
			criticality: true,
		};
		const ctx = extractWorkOrderContext(wo);
		expect(ctx.estimatedDuration).toBe("120");
		expect(ctx.criticality).toBe("true");
	});

	it("handles array and object-shaped property values on work order", () => {
		const wo: Record<string, unknown> = {
			externalId: "e",
			space: "s",
			name: "WO",
			status: "open",
			assignedTo: ["a", "b"],
			equipment: { name: "Pump", externalId: "p1" },
		};
		const ctx = extractWorkOrderContext(wo);
		expect(ctx.assignedTo).toBe("a, b");
		expect(ctx.equipment).toContain("Pump");
	});

	it("uses value key and generic object serialization for asset properties", () => {
		const asset: Record<string, unknown> = {
			assetExternalId: "a",
			space: "s",
			assetName: "AN",
			specifications: { value: "spec-x" },
			process: { k: 1, m: 2 },
		};
		const ctx = extractAssetContext(asset);
		expect(ctx.specifications).toBe("spec-x");
		expect(ctx.process).toContain("k");
	});

	it("stringifies relation-like objects via externalId when name is absent", () => {
		const wo: Record<string, unknown> = {
			externalId: "e",
			space: "s",
			name: "WO",
			status: "open",
			equipment: { externalId: "eq-1" },
		};
		const ctx = extractWorkOrderContext(wo);
		expect(ctx.equipment).toBe("eq-1");
	});

	it("serializes plain objects without name, externalId, or value via entries", () => {
		const wo: Record<string, unknown> = {
			externalId: "e",
			space: "s",
			name: "WO",
			status: "open",
			location: { building: "A", floor: 2 },
		};
		const ctx = extractWorkOrderContext(wo);
		expect(ctx.location).toContain("building");
		expect(ctx.location).toContain("A");
	});

	it("joins empty array property to empty string after filter", () => {
		const wo: Record<string, unknown> = {
			externalId: "e",
			space: "s",
			name: "WO",
			status: "open",
			assignedTo: [],
		};
		const ctx = extractWorkOrderContext(wo);
		expect(ctx.assignedTo).toBe("");
	});

	it("stringifies bigint property values", () => {
		const wo: Record<string, unknown> = {
			externalId: "e",
			space: "s",
			name: "WO",
			status: "open",
			workOrderNumber: 9007199254740993n,
		};
		const ctx = extractWorkOrderContext(wo);
		expect(ctx.workOrderNumber).toBe("9007199254740993");
	});

	it("handles deeply nested objects without producing '[object Object]'", () => {
		const wo: Record<string, unknown> = {
			externalId: "e",
			space: "s",
			name: "WO",
			status: "open",
			location: {
				building: "A",
				floor: 2,
				coordinates: { lat: 59.9139, lng: 10.7522 },
				equipment: { name: "Pump", type: "centrifugal" },
				tags: ["urgent", "safety"],
			},
		};
		const ctx = extractWorkOrderContext(wo);

		// Should not contain '[object Object]'
		expect(ctx.location).not.toContain("[object Object]");

		// Should contain meaningful information from nested objects
		expect(ctx.location).toContain("building: A");
		expect(ctx.location).toContain("floor: 2");
		expect(ctx.location).toContain("coordinates:");
		expect(ctx.location).toContain("lat:");
		expect(ctx.location).toContain("equipment: Pump"); // Uses name property
		expect(ctx.location).toContain("tags: urgent, safety"); // Handles arrays
	});
});
