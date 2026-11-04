import { describe, expect, it } from "vitest";
import { createMockAppConfig } from "@/__mocks__/app-config";
import rawAppJson from "../__mocks__/app-config.fixture.json";
import { getAppConfig, loadAppConfig, resolveAppConfig } from "./load";

describe("load", () => {
	it("returns the loaded config when no override is passed", () => {
		expect(resolveAppConfig()).toBe(getAppConfig());
	});

	it("returns the override when one is passed", () => {
		const override = createMockAppConfig();
		expect(resolveAppConfig(override)).toBe(override);
	});

	it("parses node properties into app config", () => {
		expect(loadAppConfig(rawAppJson).agent.model).toBe("cognite/fast");
	});
});
