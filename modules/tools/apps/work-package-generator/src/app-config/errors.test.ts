import { describe, expect, it } from "vitest";
import { AppConfigError } from "./errors";

describe("AppConfigError", () => {
	it("lists every issue in the message", () => {
		const error = new AppConfigError(["missing org", "bad type"]);
		expect(error.name).toBe("AppConfigError");
		expect(error.issues).toEqual(["missing org", "bad type"]);
		expect(error.message).toContain("missing org");
		expect(error.message).toContain("bad type");
	});
});
