import { describe, expect, it } from "vitest";
import type { ResourceInstance } from "@/types";
import { resourceRowLabel } from "./ResourceItemsTable";

describe("resourceRowLabel", () => {
	it("prefers a non-empty properties.name", () => {
		const item: ResourceInstance = {
			externalId: "x",
			space: "s",
			name: "fallback",
			properties: { name: " From props " },
		};
		expect(resourceRowLabel(item)).toBe(" From props ");
	});

	it("falls back to name then externalId", () => {
		expect(
			resourceRowLabel({
				externalId: "ext",
				space: "s",
				name: "Named",
				properties: {},
			})
		).toBe("Named");
		expect(
			resourceRowLabel({
				externalId: "ext",
				space: "s",
				properties: { name: "  " },
			})
		).toBe("ext");
	});
});
