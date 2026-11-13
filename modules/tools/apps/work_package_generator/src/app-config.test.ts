import { describe, expect, it } from "vitest";
import { createMockAppConfig } from "./__mocks__/app-config";
import rawAppJson from "./__mocks__/app-config.fixture.json";
import {
	AppConfigError,
	cdfClusterQueryHost,
	extractClusterFromUrl,
	getAgentExternalId,
	getAiSelectableTemplates,
	getBaseTemplateCanvas,
	getResourceConfigs,
	getViewKey,
	isBaseTemplateId,
	loadAppConfig,
	toViewReference,
	viewIdToSource,
} from "./app-config";

describe("loadAppConfig", () => {
	it("parses the fixture config node", () => {
		const config = loadAppConfig(rawAppJson);
		expect(config.fusion.hostSuffix).toBe(".fusion.cognite.com");
		expect(config.fusion.fallbackOrganization).toBeUndefined();
		expect(config.clusterSettings.apiHostSuffix).toBe("cognitedata.com");
	});

	it("matches the previous in-code constants", () => {
		const config = loadAppConfig(rawAppJson);

		expect(config.dataModel.views.asset).toEqual({
			space: "cdf_cdm",
			externalId: "CogniteAsset",
			version: "v1",
		});
		expect(config.dataModel.views.equipment.externalId).toBe(
			"CogniteEquipment"
		);
		expect(config.dataModel.views.activity.externalId).toBe("CogniteActivity");
		expect(config.dataModel.views.timeSeries.externalId).toBe(
			"CogniteTimeSeries"
		);
		expect(config.dataModel.views.file.externalId).toBe("CogniteFile");
		expect(config.dataModel.views.solutionTag).toEqual({
			space: "cdf_apps_shared",
			externalId: "CogniteSolutionTag",
			version: "v1",
		});
		expect(config.dataModel.activityContainer).toEqual({
			space: "cdf_cdm",
			externalId: "CogniteActivity",
		});
		expect(config.canvas.schemaSpace).toBe("cdf_industrial_canvas");
		expect(config.canvas.instanceSpace).toBe("IndustrialCanvasInstanceSpace");
		expect(config.canvas.canvasView.externalId).toBe("Canvas");
		expect(config.canvas.instanceView.externalId).toBe(
			"FdmInstanceContainerReference"
		);
		expect(config.canvas.annotationView.externalId).toBe("CanvasAnnotation");
		expect(config.canvas.dimensions).toEqual({
			x: 0,
			y: 0,
			width: 600,
			height: 400,
			maxWidth: 1200,
			maxHeight: 1200,
		});
		expect(config.canvas.userId).toBe("work-package-generator");
		expect(config.resourceTypes.asset.assetRelationProperty).toBe("parent");
		expect(config.resourceTypes.asset.assetRelationIsArray).toBe(false);
		expect(config.resourceTypes.timeseries.activityContainerIdentifier).toBe(
			"timeSeries"
		);
		expect(config.agent).toEqual({
			model: "cognite/fast",
			runtimeVersion: "1.2.0",
			externalIdSuffix: "work_package_generator_helper_agent",
			name: "Work Package Generator Helper",
			description:
				"AI agent for assisting with work package generation and analysis.",
			instructions: "",
			tools: [],
		});
		expect(config.templates.namePrefix).toBe("template");
		expect(config.templates.baseTemplate).toEqual({
			id: "__base_all_resources__",
			name: "All Resources",
			visibility: "public",
			space: "local",
		});
		expect(config.solutionTag).toEqual({
			instanceSpace: "SolutionTagsInstanceSpace",
			externalId: "wpg-work-package",
			name: "Work Package",
		});
		expect(config.limits).toEqual({
			instancePageSize: 1000,
			upsertBatchSize: 1000,
			deleteBatchSize: 1000,
			searchResultLimit: 20,
			resourcesDisplayLimit: 50,
			maxCanvasListScanItems: 10_000,
			maxResourceInstancesTotal: 2000,
			maxTemplateCanvasChildItems: 5000,
			maxCanvasCleanupItems: 100_000,
			findCanvasByNameMaxItems: 500,
		});
		expect(config.clusterSettings).toEqual({
			apiHostSuffix: "cognitedata.com",
			hostAliases: { "api.cognitedata.com": "api" },
		});
		expect(config.context.workOrder.properties).toContain("workOrderNumber");
		expect(config.context.asset.properties).toContain("serialNumber");
	});

	it("equals createMockAppConfig for the shipped file", () => {
		expect(createMockAppConfig()).toEqual(loadAppConfig(rawAppJson));
	});

	it("names every missing group in one error", () => {
		const {
			dataModel: _dataModel,
			agent: _agent,
			...raw
		} = structuredClone(rawAppJson);

		expect(() => loadAppConfig(raw)).toThrow(AppConfigError);
		try {
			loadAppConfig(raw);
		} catch (error) {
			expect(error).toBeInstanceOf(AppConfigError);
			const issues = (error as AppConfigError).issues;
			expect(issues).toContain("WPGConfig.dataModel: required");
			expect(issues).toContain("WPGConfig.agent: required");
		}
	});

	it("rejects an unknown key", () => {
		const raw = structuredClone(rawAppJson);
		(raw.limits as { pageSize?: number }).pageSize = 10;

		expect(() => loadAppConfig(raw)).toThrow(
			/WPGConfig\.limits\.pageSize: unknown key/
		);
	});

	it("rejects the wrong type", () => {
		const raw = structuredClone(rawAppJson);
		(raw.agent as { model: unknown }).model = 5;

		expect(() => loadAppConfig(raw)).toThrow(
			/WPGConfig\.agent\.model: expected string/
		);
	});

	it("rejects a DMS batch size above 1000", () => {
		const raw = structuredClone(rawAppJson);
		raw.limits.instancePageSize = 1001;

		expect(() => loadAppConfig(raw)).toThrow(
			/WPGConfig\.limits\.instancePageSize: expected integer between 1 and 1000/
		);
	});

	it("rejects a resource type view that is not declared", () => {
		const raw = structuredClone(rawAppJson);
		raw.resourceTypes.file.view = "notAView";

		expect(() => loadAppConfig(raw)).toThrow(
			/WPGConfig\.resourceTypes\.file\.view: unknown view "notAView"/
		);
	});
});

describe("config helpers", () => {
	const config = createMockAppConfig();
	const activityView = toViewReference(config.dataModel.views.activity);

	it("maps a view to the CDF SDK source shape", () => {
		expect(viewIdToSource(activityView)).toEqual({ source: activityView });
	});

	it("joins externalId and version", () => {
		expect(getViewKey(activityView)).toBe(
			`${activityView.externalId}/${activityView.version}`
		);
	});

	it("links child assets via the parent property", () => {
		expect(getResourceConfigs(config).asset.assetRelationProperty).toBe(
			"parent"
		);
		expect(getResourceConfigs(config).asset.assetRelationIsArray).toBe(false);
	});

	it("appends the helper agent suffix to the project", () => {
		expect(getAgentExternalId("proj", config)).toBe(
			"proj_work_package_generator_helper_agent"
		);
	});

	it("maps configured aliases and public hosts to cluster ids", () => {
		for (const [host, clusterId] of Object.entries(
			config.clusterSettings.hostAliases
		)) {
			expect(extractClusterFromUrl(`https://${host}`, config)).toBe(clusterId);
			expect(cdfClusterQueryHost(clusterId, config)).toBe(host);
		}

		const clusterId = "aw-was-gp-001";
		expect(
			extractClusterFromUrl(
				`https://${clusterId}.${config.clusterSettings.apiHostSuffix}/`,
				config
			)
		).toBe(clusterId);
	});

	it("identifies the synthetic base template", () => {
		const baseId = config.templates.baseTemplate.id;
		expect(isBaseTemplateId(baseId, config)).toBe(true);
		expect(isBaseTemplateId("other", config)).toBe(false);
		expect(isBaseTemplateId(null, config)).toBe(false);
		expect(getBaseTemplateCanvas(config).externalId).toBe(baseId);
	});

	it("excludes the base template from AI selection", () => {
		const realTemplate = {
			externalId: "real-template",
			name: "Real",
			visibility: "public" as const,
			space: "s",
		};
		const filtered = getAiSelectableTemplates(
			[realTemplate, getBaseTemplateCanvas(config)],
			config
		);
		expect(filtered).toEqual([realTemplate]);
	});
});
