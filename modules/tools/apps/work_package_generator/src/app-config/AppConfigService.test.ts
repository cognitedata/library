import { type CogniteClient, CogniteError } from "@cognite/sdk";
import { describe, expect, it, vi } from "vitest";
import rawConfig from "@/__mocks__/app-config.fixture.json";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import {
	AppConfigLoadError,
	createDmsAppConfigService,
} from "./AppConfigService";
import { WPG_CONFIG_LOCATION } from "./config-location";

const viewVersion = "v4";

describe(createDmsAppConfigService.name, () => {
	it("lists the latest data model and retrieves the config node with that view", async () => {
		const client = clientWith({
			models: [model("v2"), model(viewVersion)],
			items: [nodeItem(rawConfig, viewVersion)],
		});

		const config = await createDmsAppConfigService(client).load();

		expect(config.agent.model).toBe("cognite/fast");
		expect(client.dataModels.list).toHaveBeenCalledWith({
			space: WPG_CONFIG_LOCATION.schemaSpace,
			allVersions: false,
			limit: 1000,
			inlineViews: true,
		});
		expect(client.instances.retrieve).toHaveBeenCalledWith({
			items: [
				{
					instanceType: "node",
					space: WPG_CONFIG_LOCATION.instanceSpace,
					externalId: WPG_CONFIG_LOCATION.nodeExternalId,
				},
			],
			sources: [
				{
					source: {
						type: "view",
						space: WPG_CONFIG_LOCATION.schemaSpace,
						externalId: WPG_CONFIG_LOCATION.viewExternalId,
						version: viewVersion,
					},
				},
			],
		});
	});

	it("reports schemaMissing when the data model is absent", async () => {
		const client = clientWith({ models: [], items: [] });
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toMatchObject({
			kind: "schemaMissing",
		});
	});

	it("reports schemaMissing when the view is absent", async () => {
		const client = clientWith({
			models: [
				{
					...model("v1"),
					views: [
						{
							type: "view" as const,
							space: WPG_CONFIG_LOCATION.schemaSpace,
							externalId: "OtherView",
							version: "v1",
						},
					],
				},
			],
			items: [],
		});
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toMatchObject({
			kind: "schemaMissing",
		});
		expect(client.instances.retrieve).not.toHaveBeenCalled();
	});

	it("reports notFound when retrieve returns no items", async () => {
		const client = clientWith({ models: [model(viewVersion)], items: [] });
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toMatchObject({
			kind: "notFound",
		});
	});

	it("reads properties under the resolved view key", async () => {
		const client = clientWith({
			models: [model(viewVersion)],
			items: [nodeItem(rawConfig, "v1")],
		});
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toMatchObject({
			kind: "invalid",
		});
	});

	it("reports forbidden on 403", async () => {
		const client = clientWith({
			models: [model(viewVersion)],
			items: [],
			retrieveError: new CogniteError("denied", 403),
		});
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toBeInstanceOf(AppConfigLoadError);
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toMatchObject({
			kind: "forbidden",
		});
	});

	it("reports network for other errors", async () => {
		const client = clientWith({
			models: [model(viewVersion)],
			items: [],
			retrieveError: new CogniteError("unavailable", 503),
		});
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toMatchObject({
			kind: "network",
		});
	});

	it("reports unsupportedVersion when schemaVersion is not supported", async () => {
		const client = clientWith({
			models: [model(viewVersion)],
			items: [nodeItem({ ...rawConfig, schemaVersion: 2 }, viewVersion)],
		});
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toMatchObject({
			kind: "unsupportedVersion",
		});
	});

	it("reports notFound on 404", async () => {
		const client = clientWith({
			models: [model(viewVersion)],
			items: [],
			retrieveError: new CogniteError("missing", 404),
		});
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toMatchObject({ kind: "notFound" });
	});

	it("reports network when the data model list fails", async () => {
		const client = clientWith({ models: [], items: [] });
		vi.mocked(client.dataModels.list).mockImplementation(() => {
			throw new Error("list failed");
		});
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toMatchObject({ kind: "network", message: "list failed" });
	});

	it("picks the highest numbered model version", async () => {
		const client = clientWith({
			models: [model("v10"), model("v9")],
			items: [nodeItem(rawConfig, "v10")],
		});
		await createDmsAppConfigService(client).load();
		expect(client.instances.retrieve).toHaveBeenCalledWith(
			expect.objectContaining({
				sources: [
					expect.objectContaining({
						source: expect.objectContaining({ version: "v10" }),
					}),
				],
			})
		);
	});

	it("falls back to string comparison for non-numbered versions", async () => {
		const client = clientWith({
			models: [model("beta"), model("alpha")],
			items: [nodeItem(rawConfig, "beta")],
		});
		await createDmsAppConfigService(client).load();
		expect(client.instances.retrieve).toHaveBeenCalledOnce();
	});

	it("reports an unknown error as network with a generic message", async () => {
		const client = clientWith({
			models: [model(viewVersion)],
			items: [],
			retrieveError: "boom",
		});
		await expect(
			createDmsAppConfigService(client).load()
		).rejects.toMatchObject({ kind: "network", message: "Unknown error" });
	});
});

function model(version: string) {
	return {
		externalId: WPG_CONFIG_LOCATION.dataModelExternalId,
		space: WPG_CONFIG_LOCATION.schemaSpace,
		version,
		createdTime: 0,
		lastUpdatedTime: 0,
		isGlobal: false,
		views: [
			{
				type: "view" as const,
				space: WPG_CONFIG_LOCATION.schemaSpace,
				externalId: WPG_CONFIG_LOCATION.viewExternalId as string,
				version,
			},
		],
	};
}

function nodeItem(properties: object, version: string) {
	return {
		instanceType: "node" as const,
		space: WPG_CONFIG_LOCATION.instanceSpace,
		externalId: WPG_CONFIG_LOCATION.nodeExternalId,
		properties: {
			[WPG_CONFIG_LOCATION.schemaSpace]: {
				[`${WPG_CONFIG_LOCATION.viewExternalId}/${version}`]: properties,
			},
		},
	};
}

function clientWith({
	models,
	items,
	retrieveError,
}: {
	models: ReturnType<typeof model>[];
	items: ReturnType<typeof nodeItem>[];
	retrieveError?: unknown;
}): CogniteClient & {
	dataModels: { list: ReturnType<typeof vi.fn> };
	instances: { retrieve: ReturnType<typeof vi.fn> };
} {
	const list = vi.fn(() => ({
		autoPagingToArray: vi.fn(async () => models),
	}));
	const retrieve = vi.fn(async () => {
		if (retrieveError) throw retrieveError;
		return { items };
	});
	return cdfClientTestDouble({
		dataModels: { list },
		instances: { retrieve },
	}) as CogniteClient & {
		dataModels: { list: ReturnType<typeof vi.fn> };
		instances: { retrieve: ReturnType<typeof vi.fn> };
	};
}
