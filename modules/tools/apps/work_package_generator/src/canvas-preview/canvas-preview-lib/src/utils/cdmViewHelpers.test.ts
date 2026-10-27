import type { CogniteClient, ViewDefinition } from "@cognite/sdk";
import { describe, expect, it, vi } from "vitest";

import {
	CDF_CDM_VIEWS,
	determineCdmViewType,
	getViewCacheKey,
	resolveView,
	resolveViews,
} from "./cdmViewHelpers";

function view(
	overrides: Partial<ViewDefinition> & Pick<ViewDefinition, "externalId">
): ViewDefinition {
	return {
		space: "cdf_cdm",
		version: "v1",
		createdTime: 1,
		lastUpdatedTime: 1,
		writable: true,
		usedFor: "node",
		isGlobal: false,
		properties: {},
		...overrides,
	} as ViewDefinition;
}

describe("getViewCacheKey", () => {
	it("includes the version when one is set", () => {
		expect(getViewCacheKey({ space: "s", externalId: "v", version: "1" })).toBe(
			"s:v:1"
		);
		expect(getViewCacheKey({ space: "s", externalId: "v" })).toBe("s:v");
	});
});

describe("determineCdmViewType", () => {
	it("returns UNKNOWN when the view is missing or unrecognized", () => {
		expect(determineCdmViewType(undefined)).toBe("UNKNOWN");
		expect(determineCdmViewType(view({ externalId: "Custom" }))).toBe(
			"UNKNOWN"
		);
	});

	it("matches a view by external id and by implements", () => {
		expect(determineCdmViewType(view({ externalId: CDF_CDM_VIEWS.FILE }))).toBe(
			"FILE"
		);
		expect(
			determineCdmViewType(
				view({
					externalId: "SiteFile",
					implements: [
						{
							space: "cdf_cdm",
							externalId: CDF_CDM_VIEWS.TIMESERIES,
							version: "v1",
							type: "view",
						},
					],
				})
			)
		).toBe("TIMESERIES");
	});

	it("prefers the more specific type", () => {
		expect(
			determineCdmViewType(
				view({
					externalId: CDF_CDM_VIEWS.ASSET,
					implements: [
						{
							space: "cdf_cdm",
							externalId: CDF_CDM_VIEWS.EQUIPMENT,
							version: "v1",
							type: "view",
						},
					],
				})
			)
		).toBe("EQUIPMENT");
	});
});

describe("resolveViews", () => {
	it("returns an empty map for no refs", async () => {
		const sdk = { views: { retrieve: vi.fn() } } as object as CogniteClient;
		await expect(resolveViews(sdk, [], new Map())).resolves.toEqual(new Map());
	});

	it("reuses the cache and fetches versioned and latest views", async () => {
		const cached = view({ externalId: "Cached", version: "v1" });
		const versioned = view({
			externalId: "Versioned",
			version: "v2",
			createdTime: 2,
		});
		const older = view({ externalId: "Latest", version: "v1", createdTime: 1 });
		const newer = view({ externalId: "Latest", version: "v3", createdTime: 9 });
		const retrieve = vi.fn(async (items: Array<{ version?: string }>) => {
			if (items[0]?.version) return { items: [versioned] };
			return { items: [older, newer] };
		});
		const sdk = { views: { retrieve } } as object as CogniteClient;
		const cache = new Map<string, ViewDefinition>([["s:Cached:v1", cached]]);

		const result = await resolveViews(
			sdk,
			[
				{ space: "s", externalId: "Cached", version: "v1" },
				{ space: "s", externalId: "Cached", version: "v1" },
				{ space: "cdf_cdm", externalId: "Versioned", version: "v2" },
				{ space: "cdf_cdm", externalId: "Latest" },
				{ space: "cdf_cdm", externalId: "Latest" },
			],
			cache
		);

		expect(result.get("s:Cached:v1")).toBe(cached);
		expect(result.get("cdf_cdm:Versioned:v2")?.version).toBe("v2");
		expect(result.get("cdf_cdm:Latest")?.version).toBe("v3");
		expect(retrieve).toHaveBeenCalledTimes(2);
	});

	it("schedules view retrieves through the CDF runner", async () => {
		const retrieve = vi.fn(async () => ({ items: [] }));
		const sdk = { views: { retrieve } } as object as CogniteClient;
		let scheduled = 0;
		const schedule = <T>(fn: () => Promise<T>): Promise<T> => {
			scheduled += 1;
			return fn();
		};
		await resolveViews(
			sdk,
			[{ space: "s", externalId: "Only", version: "v1" }],
			new Map(),
			schedule
		);
		expect(scheduled).toBeGreaterThan(0);
	});

	it("swallows retrieve failures", async () => {
		const sdk = {
			views: { retrieve: vi.fn(async () => Promise.reject(new Error("down"))) },
		} as object as CogniteClient;
		const result = await resolveViews(
			sdk,
			[{ space: "s", externalId: "Missing", version: "v1" }],
			new Map()
		);
		expect(result.size).toBe(0);
	});

	it("resolves a single view", async () => {
		const found = view({ externalId: "One", version: "v1" });
		const sdk = {
			views: { retrieve: vi.fn(async () => ({ items: [found] })) },
		} as object as CogniteClient;
		await expect(
			resolveView(
				sdk,
				{ space: "cdf_cdm", externalId: "One", version: "v1" },
				new Map()
			)
		).resolves.toMatchObject({ externalId: "One" });
	});
});
