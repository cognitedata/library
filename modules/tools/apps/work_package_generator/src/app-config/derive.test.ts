import { describe, expect, it } from "vitest";
import { createMockAppConfig } from "@/__mocks__/app-config";
import {
	clusterIdFromBaseUrl,
	clusterQueryHost,
	fusionOrganizationFromHostname,
	fusionOrganizationFromRuntime,
	getAgentExternalId,
	getAiSelectableTemplates,
	getBaseTemplateCanvas,
	getViewKey,
	isBaseTemplateId,
	toViewReference,
	viewIdToSource,
} from "./derive";

describe("derive", () => {
	it("builds a view reference and source", () => {
		const config = createMockAppConfig();
		const view = toViewReference(config.dataModel.views.activity);
		expect(view.type).toBe("view");
		expect(viewIdToSource(view)).toEqual({ source: view });
		expect(getViewKey(view)).toBe(`${view.externalId}/${view.version}`);
	});

	it("reads the Fusion org from the hostname", () => {
		const suffix = ".fusion.cognite.com";
		expect(
			fusionOrganizationFromHostname("cog-demo.fusion.cognite.com", suffix)
		).toBe("cog-demo");
		expect(fusionOrganizationFromHostname("localhost", suffix)).toBeNull();
	});

	it("reads the deployed org from the parent Fusion origin", () => {
		expect(
			fusionOrganizationFromRuntime(
				{
					hostname: "apps.cognite.com",
					ancestorOrigins: ["https://cog-demo.fusion.cognite.com"],
				},
				".fusion.cognite.com"
			)
		).toBe("cog-demo");
	});
});

describe("derive cluster and template helpers", () => {
	const clusterSettings = {
		apiHostSuffix: "cognitedata.com",
		hostAliases: { "az-eastus-1.cognitedata.com": "az-eastus-1-alias" },
	};

	it("maps an aliased host to its cluster id", () => {
		expect(
			clusterIdFromBaseUrl(
				"https://az-eastus-1.cognitedata.com",
				clusterSettings
			)
		).toBe("az-eastus-1-alias");
	});

	it("strips the api host suffix when no alias matches", () => {
		expect(
			clusterIdFromBaseUrl(
				"https://westeurope-1.cognitedata.com",
				clusterSettings
			)
		).toBe("westeurope-1");
	});

	it("keeps unrelated hosts as the cluster id", () => {
		expect(
			clusterIdFromBaseUrl("https://api.example.test", clusterSettings)
		).toBe("api.example.test");
	});

	it("resolves the query host from alias, full host, or bare id", () => {
		expect(clusterQueryHost("az-eastus-1-alias", clusterSettings)).toBe(
			"az-eastus-1.cognitedata.com"
		);
		expect(clusterQueryHost("x.cognitedata.com", clusterSettings)).toBe(
			"x.cognitedata.com"
		);
		expect(clusterQueryHost("x", clusterSettings)).toBe("x.cognitedata.com");
	});

	it("uses the existing agent id when configured, else project + suffix", () => {
		// Arrange
		const config = createMockAppConfig();
		const withExisting = {
			...config,
			agent: { ...config.agent, existingAgentExternalId: "fixed-agent" },
		};
		const withoutExisting = {
			...config,
			agent: { ...config.agent, existingAgentExternalId: undefined },
		};

		// Assert
		expect(getAgentExternalId("proj", withExisting)).toBe("fixed-agent");
		expect(getAgentExternalId("proj", withoutExisting)).toBe(
			`proj_${config.agent.externalIdSuffix}`
		);
	});

	it("recognises and exposes the base template", () => {
		// Arrange
		const config = createMockAppConfig();
		const base = getBaseTemplateCanvas(config);
		const other = { ...base, externalId: "other" };

		// Assert
		expect(isBaseTemplateId(base.externalId, config)).toBe(true);
		expect(isBaseTemplateId(null, config)).toBe(false);
		expect(getAiSelectableTemplates([base, other], config)).toEqual([other]);
	});

	it("returns null for non-Fusion, nested, empty, or malformed hosts", () => {
		expect(
			fusionOrganizationFromHostname("localhost", ".fusion.cognite.com")
		).toBeNull();
		expect(
			fusionOrganizationFromHostname(
				"a.b.fusion.cognite.com",
				".fusion.cognite.com"
			)
		).toBeNull();
		expect(
			fusionOrganizationFromHostname(
				".fusion.cognite.com",
				".fusion.cognite.com"
			)
		).toBeNull();
		expect(
			fusionOrganizationFromHostname("", ".fusion.cognite.com")
		).toBeNull();
		expect(
			fusionOrganizationFromHostname("http://", ".fusion.cognite.com")
		).toBeNull();
		expect(
			fusionOrganizationFromHostname(
				"org.fusion.cognite.com:443/path",
				".fusion.cognite.com"
			)
		).toBe("org");
	});
});
