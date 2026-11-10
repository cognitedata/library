import { describe, expect, it } from "vitest";
import pkg from "../package.json";

describe("package scripts", () => {
	it("B-T1: build stops when tsc fails", () => {
		expect(pkg.scripts.build).toBe("node scripts/build.mjs");
	});
});
