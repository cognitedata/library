import { describe, expect, it } from "vitest";

import { isCdfSource, isDirectSource, isFdmSource } from "./dataSources";

describe("source guards", () => {
	it("matches each source type", () => {
		expect(isDirectSource({ type: "direct" })).toBe(true);
		expect(isFdmSource({ type: "fdm" })).toBe(true);
		expect(isCdfSource({ type: "cdf" })).toBe(true);
		expect(isDirectSource({ type: "fdm" })).toBe(false);
	});
});
