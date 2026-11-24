import { describe, expect, it } from "vitest";
import { generateUuid } from "./uuid";

describe("generateUuid", () => {
	it("returns a string in UUID v4 format", () => {
		const id = generateUuid();
		expect(id).toMatch(
			/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
		);
	});

	it("generates unique values", () => {
		const ids = new Set(Array.from({ length: 20 }, () => generateUuid()));
		expect(ids.size).toBe(20);
	});
});
