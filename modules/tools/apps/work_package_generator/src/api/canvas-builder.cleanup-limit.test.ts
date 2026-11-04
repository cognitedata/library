import type { CogniteClient } from "@cognite/sdk";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	createMockAppConfig,
	DEFAULT_CANVAS_CONFIG,
} from "@/__mocks__/app-config";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { installInlineApiRuntime } from "@/__mocks__/inline-api-runtime";
import type { Asset, WorkOrder } from "../types";
import { updateCanvasFromTemplateAndSelections } from "./canvas-builder";

installInlineApiRuntime();

// vi.mock: UUID is non-deterministic; fixed value keeps canvas item IDs assertable.
vi.mock("../utils/uuid", () => ({
	generateUuid: vi.fn().mockReturnValue("uuid-fixed"),
}));

const templateReads = {
	getTemplateCanvasInstances: vi.fn().mockResolvedValue([
		{
			label: "Main asset",
			viewExternalId: "CogniteAsset",
			viewSpace: "cdf_cdm",
			viewVersion: "v1",
			x: 0,
			y: 0,
			width: 600,
			height: 400,
			maxWidth: 1200,
			maxHeight: 1200,
			properties: {},
		},
	]),
	getTemplateCanvasAnnotations: vi.fn().mockResolvedValue([]),
};

const config = DEFAULT_CANVAS_CONFIG;
const smallCleanupConfig = createMockAppConfig();
smallCleanupConfig.limits.maxCanvasCleanupItems = 2;
const asset: Asset = { assetExternalId: "a", space: "s", assetName: "A" };
const workOrder: WorkOrder = { externalId: "w", space: "s", name: "W" };

describe("cleanCanvasContent cleanup limit", () => {
	let list: ReturnType<typeof vi.fn>;
	let query: ReturnType<typeof vi.fn>;
	let deleteFn: ReturnType<typeof vi.fn>;
	let client: CogniteClient;

	beforeEach(() => {
		list = vi.fn();
		query = vi.fn();
		deleteFn = vi.fn().mockResolvedValue(undefined);
		client = cdfClientTestDouble({
			instances: {
				upsert: vi.fn().mockResolvedValue(undefined),
				list,
				query,
				delete: deleteFn,
			},
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
			getBaseUrl: () => "https://x",
		});
	});

	it("throws when more child nodes remain after the configured cleanup limit", async () => {
		const n = (id: string) => ({ externalId: id, space: "sp" });
		query
			.mockResolvedValueOnce({
				items: { nodes: [n("existing-id_x1"), n("existing-id_x2")] },
				nextCursor: { nodes: "more" },
			})
			.mockResolvedValue({ items: { nodes: [], edges: [] }, nextCursor: {} });

		await expect(
			updateCanvasFromTemplateAndSelections(
				client,
				config,
				"existing-id",
				"N",
				"tpl",
				{},
				asset,
				workOrder,
				{},
				smallCleanupConfig,
				templateReads
			)
		).rejects.toThrow(/cleanup limit/i);

		expect(deleteFn).toHaveBeenCalled();
	});
});
