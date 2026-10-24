import { describe, expect, it } from "vitest";
import schemaYaml from "../../data_modeling/dm_sol_wpg_app.Space.yaml?raw";
import instanceYaml from "../../data_modeling/inst_wpg_app_config.Space.yaml?raw";
import viewYaml from "../../data_modeling/WPGConfig.View.yaml?raw";
import modelYaml from "../../data_modeling/WPGConfig_SOL.DataModel.yaml?raw";
import nodeYaml from "../../data_modeling/wpg_config.Node.yaml?raw";
import fixture from "../__mocks__/app-config.fixture.json";
import { WPG_CONFIG_LOCATION } from "./config-location";

describe("toolkit config drift", () => {
	it("keeps the fixed identifiers aligned with the toolkit YAML", () => {
		expect(schemaYaml).toContain(`space: ${WPG_CONFIG_LOCATION.schemaSpace}`);
		expect(instanceYaml).toContain(
			`space: ${WPG_CONFIG_LOCATION.instanceSpace}`
		);
		expect(modelYaml).toContain(
			`externalId: ${WPG_CONFIG_LOCATION.dataModelExternalId}`
		);
		expect(viewYaml).toContain(
			`externalId: ${WPG_CONFIG_LOCATION.viewExternalId}`
		);
		expect(nodeYaml).toContain(`space: ${WPG_CONFIG_LOCATION.instanceSpace}`);
		expect(nodeYaml).toContain(
			`externalId: ${WPG_CONFIG_LOCATION.nodeExternalId}`
		);
	});

	it("keeps literal node sections aligned with the fixture", () => {
		for (const section of [
			"resourceTypes",
			"context",
			"prompts",
			"ui",
		] as const) {
			const match = new RegExp(`^\\s+${section}: (\\{.*\\})\\s*$`, "m").exec(
				nodeYaml
			);
			expect(match, section).not.toBeNull();
			expect(JSON.parse(match?.[1] ?? "")).toEqual(fixture[section]);
		}
	});
});
