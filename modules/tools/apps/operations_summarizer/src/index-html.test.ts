import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

describe("index.html viewport", () => {
	it("does not block pinch-zoom (WCAG 1.4.4)", () => {
		const html = readFileSync(
			path.resolve(import.meta.dirname, "../index.html"),
			"utf8"
		);
		expect(html).not.toMatch(
			/user-scalable\s*=\s*no|maximum-scale\s*=\s*1(\.0)?\b/i
		);
	});
});
