import { describe, expect, it } from "vitest";
import { createMockAppConfig } from "@/__mocks__/app-config";
import {
	dataModelView,
	extractClusterFromUrl,
	isBaseTemplateId,
	toCanvasConfig,
} from "./runtime";

describe("runtime", () => {
	it("reads views and canvas config from the override", () => {
		const config = createMockAppConfig();
		expect(dataModelView("activity", config).externalId).toBe(
			config.dataModel.views.activity.externalId
		);
		expect(toCanvasConfig(config).canvasSpaceInstance).toBe(
			config.canvas.instanceSpace
		);
		expect(isBaseTemplateId(config.templates.baseTemplate.id, config)).toBe(
			true
		);
		expect(isBaseTemplateId("other", config)).toBe(false);
		expect(
			extractClusterFromUrl("https://az-eastus-1.cognitedata.com", config)
		).toBe("az-eastus-1");
	});
});
