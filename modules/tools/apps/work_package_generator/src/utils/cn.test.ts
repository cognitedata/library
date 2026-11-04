import { describe, expect, it } from "vitest";
import { cn } from "./cn";

describe("cn", () => {
	it("merges tailwind classes and drops falsy values", () => {
		expect(cn("px-2", false && "hidden", "px-4")).toBe("px-4");
	});
});
